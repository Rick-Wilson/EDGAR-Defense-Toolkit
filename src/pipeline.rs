//! Pipeline functions for programmatic use by both CLI and GUI.
//!
//! These are extracted/adapted versions of the workflow logic from `bbo_csv.rs`,
//! returning structured data instead of printing to stdout.

use anyhow::{Context, Result};
use bridge_parsers::lin::{parse_lin_from_url, LinData};
use bridge_parsers::tinyurl::UrlResolver;
use bridge_parsers::{Direction, Vulnerability};
use bridge_solver::{CLUB, DIAMOND, EAST, HEART, NORTH, SOUTH, SPADE, WEST};
use csv::{ReaderBuilder, StringRecord, Writer};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

// ============================================================================
// Fetch Cardplay
// ============================================================================

/// Configuration for the fetch-cardplay operation.
pub struct FetchCardplayConfig {
    /// Input CSV path
    pub input: PathBuf,
    /// Output CSV path (the merged cardplay CSV)
    pub output: PathBuf,
    /// Path for the intermediate tinyurl lookup file
    pub lookup_output: PathBuf,
    /// Column name containing the TinyURL/BBO URL
    pub url_column: String,
    /// Delay between URL requests in milliseconds
    pub delay_ms: u64,
    /// Number of requests before a longer pause
    pub batch_size: usize,
    /// Duration of the longer pause in milliseconds
    pub batch_delay_ms: u64,
    /// Resume from previous run (skip rows with existing cardplay data)
    pub resume: bool,
}

/// Progress information for the fetch-cardplay operation.
pub struct FetchProgress {
    /// Number of rows completed so far
    pub completed: usize,
    /// Total number of rows to process
    pub total: usize,
    /// Number of errors encountered
    pub errors: usize,
    /// Number of rows skipped (already had data in resume mode)
    pub skipped: usize,
}

/// Fetch cardplay data from BBO TinyURLs in a CSV file.
///
/// Phase 1: Generate tinyurl lookup file (if it doesn't already exist or needs resume).
/// Phase 2: Merge lookup data into the output cardplay CSV.
///
/// Calls `on_progress` after each row. Return `false` from the callback to cancel.
/// Returns a summary string on success.
pub fn fetch_cardplay(
    config: &FetchCardplayConfig,
    mut on_progress: impl FnMut(&FetchProgress) -> bool,
) -> Result<String> {
    let total_rows = count_csv_rows(&config.input)?;

    // Phase 1: Generate tinyurl lookup file
    let phase1_summary = generate_lookup_file(config, &mut on_progress, total_rows)?;

    // Phase 2: Merge lookup into cardplay CSV
    merge_lookup_to_cardplay(config)?;

    Ok(phase1_summary)
}

/// Phase 1: Generate the tinyurl lookup file by resolving URLs and parsing LIN data.
///
/// Skips entirely if the lookup file already has the expected row count.
/// Resumes from where it left off if partially complete.
fn generate_lookup_file(
    config: &FetchCardplayConfig,
    on_progress: &mut impl FnMut(&FetchProgress) -> bool,
    total_rows: usize,
) -> Result<String> {
    // Check if lookup file is already complete
    let existing_rows = if config.lookup_output.exists() {
        count_csv_rows(&config.lookup_output)?
    } else {
        0
    };

    if existing_rows >= total_rows {
        return Ok(format!(
            "Lookup file already complete ({} rows). Skipped URL resolution.",
            existing_rows
        ));
    }

    let csv_data = read_bbo_csv_fixed(&config.input)?;
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_reader(csv_data.as_bytes());
    let headers = reader.headers()?.clone();

    let url_col_idx = headers
        .iter()
        .position(|h| h == config.url_column)
        .ok_or_else(|| anyhow::anyhow!("Column '{}' not found in CSV", config.url_column))?;

    let mut resolver =
        UrlResolver::with_config(config.delay_ms, config.batch_size, config.batch_delay_ms);

    // Open lookup file for writing (append if resuming)
    let resuming = existing_rows > 0;
    let file: std::fs::File = if resuming {
        std::fs::OpenOptions::new()
            .append(true)
            .open(&config.lookup_output)?
    } else {
        std::fs::File::create(&config.lookup_output)?
    };
    let mut out = csv::WriterBuilder::new()
        .flexible(true)
        .has_headers(false)
        .from_writer(std::io::BufWriter::new(file));

    if !resuming {
        out.write_record(LOOKUP_FIELDS)?;
    }

    let mut processed = 0usize;
    let mut skipped = 0usize;
    let mut errors = 0usize;

    for (row_num, result) in reader.records().enumerate() {
        let record = result.context("Failed to read CSV row")?;
        let board_id = row_num + 1;
        processed += 1;

        // Skip rows already in the lookup file
        if row_num < existing_rows {
            skipped += 1;
            let keep_going = on_progress(&FetchProgress {
                completed: processed,
                total: total_rows,
                errors,
                skipped,
            });
            if !keep_going {
                out.flush()?;
                return Ok(format!(
                    "Cancelled after {} of {} rows ({} errors, {} skipped)",
                    processed, total_rows, errors, skipped
                ));
            }
            continue;
        }

        // Report progress and check for cancellation
        let keep_going = on_progress(&FetchProgress {
            completed: processed,
            total: total_rows,
            errors,
            skipped,
        });
        if !keep_going {
            out.flush()?;
            return Ok(format!(
                "Cancelled after {} of {} rows ({} errors, {} skipped)",
                processed, total_rows, errors, skipped
            ));
        }

        let tinyurl = record.get(url_col_idx).unwrap_or("").trim();

        if tinyurl.is_empty() {
            write_lookup_empty_row(&mut out, board_id, tinyurl)?;
            continue;
        }

        match resolve_and_parse_url(&mut resolver, tinyurl) {
            Ok((lin, resolved_url)) => {
                write_lookup_row(&mut out, board_id, tinyurl, &lin, &resolved_url)?;
            }
            Err(e) => {
                log::warn!(
                    "Row {}: Error processing URL '{}': {}",
                    board_id,
                    tinyurl,
                    e
                );
                errors += 1;

                if e.to_string().contains("Rate limited") {
                    log::warn!("Rate limited - pausing for 60 seconds...");
                    std::thread::sleep(std::time::Duration::from_secs(60));
                    resolver.reset_batch();
                }

                write_lookup_empty_row(&mut out, board_id, tinyurl)?;
            }
        }

        if processed.is_multiple_of(100) {
            out.flush()?;
        }
    }

    out.flush()?;
    Ok(format!(
        "Done! Processed {} rows ({} errors, {} skipped)",
        processed, errors, skipped
    ))
}

/// Phase 2: Read the lookup file and original CSV, merge Cardplay + LIN_URL into the output.
fn merge_lookup_to_cardplay(config: &FetchCardplayConfig) -> Result<()> {
    // Load lookup data: Board_ID (1-based index) → (Cardplay, LIN_URL)
    let lookup = load_lookup_data(&config.lookup_output)?;

    let csv_data = read_bbo_csv_fixed(&config.input)?;
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_reader(csv_data.as_bytes());
    let headers = reader.headers()?.clone();

    let cardplay_col_idx = headers.iter().position(|h| h == "Cardplay");
    let lin_url_col_idx = headers.iter().position(|h| h == "LIN_URL");

    let mut output_headers = headers.clone();
    if cardplay_col_idx.is_none() {
        output_headers.push_field("Cardplay");
        output_headers.push_field("LIN_URL");
    }

    let mut writer = csv::WriterBuilder::new()
        .flexible(true)
        .from_path(&config.output)
        .context("Failed to create output CSV")?;
    writer.write_record(&output_headers)?;

    for (row_num, result) in reader.records().enumerate() {
        let record = result.context("Failed to read CSV row")?;
        let board_id = row_num + 1;

        let (cardplay, lin_url) = lookup
            .get(&board_id)
            .cloned()
            .unwrap_or_else(|| (String::new(), String::new()));

        let mut output_record: Vec<String> = record.iter().map(|s| s.to_string()).collect();

        if let (Some(cp_idx), Some(lu_idx)) = (cardplay_col_idx, lin_url_col_idx) {
            if cp_idx < output_record.len() {
                output_record[cp_idx] = cardplay;
            }
            if lu_idx < output_record.len() {
                output_record[lu_idx] = lin_url;
            }
        } else {
            output_record.push(cardplay);
            output_record.push(lin_url);
        }
        writer.write_record(&output_record)?;
    }

    writer.flush()?;
    Ok(())
}

/// Resolve a URL (following tinyurl/bit.ly redirects) and parse its LIN data.
fn resolve_and_parse_url(resolver: &mut UrlResolver, url: &str) -> Result<(LinData, String)> {
    let resolved_url = if url.contains("tinyurl.com") || url.contains("bit.ly") {
        resolver.resolve(url)?
    } else {
        url.to_string()
    };

    let lin_data = parse_lin_from_url(&resolved_url)?;
    Ok((lin_data, resolved_url))
}

/// Load lookup data from the tinyurl lookup file.
///
/// Returns a map of Board_ID → (Cardplay, LIN_URL).
fn load_lookup_data(lookup_path: &Path) -> Result<HashMap<usize, (String, String)>> {
    let mut data = HashMap::new();
    let csv_data = read_bbo_csv_fixed(lookup_path)?;
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_reader(csv_data.as_bytes());

    let headers = reader.headers()?.clone();
    let board_id_idx = headers
        .iter()
        .position(|h| h == "Board_ID")
        .context("Board_ID column not found in lookup file")?;
    let cardplay_idx = headers
        .iter()
        .position(|h| h == "Cardplay")
        .context("Cardplay column not found in lookup file")?;
    let lin_url_idx = headers
        .iter()
        .position(|h| h == "LIN_URL")
        .context("LIN_URL column not found in lookup file")?;

    for result in reader.records() {
        let record = result?;
        let board_id: usize = record
            .get(board_id_idx)
            .unwrap_or("0")
            .trim()
            .parse()
            .unwrap_or(0);
        let cardplay = record.get(cardplay_idx).unwrap_or("").trim().to_string();
        let lin_url = record.get(lin_url_idx).unwrap_or("").trim().to_string();

        if board_id > 0 {
            data.insert(board_id, (cardplay, lin_url));
        }
    }

    Ok(data)
}

/// Load tinyurl → (Board_ID, LIN_URL) mapping from lookup file.
///
/// Used during anonymization to replace tinyurls with Board IDs.
pub fn load_lookup_board_ids(lookup_path: &Path) -> Result<HashMap<String, (String, String)>> {
    let mut data = HashMap::new();
    let csv_data = read_bbo_csv_fixed(lookup_path)?;
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_reader(csv_data.as_bytes());

    let headers = reader.headers()?.clone();
    let board_id_idx = headers
        .iter()
        .position(|h| h == "Board_ID")
        .context("Board_ID column not found in lookup file")?;
    let tinyurl_idx = headers
        .iter()
        .position(|h| h == "TinyURL")
        .context("TinyURL column not found in lookup file")?;
    let lin_url_idx = headers
        .iter()
        .position(|h| h == "LIN_URL")
        .context("LIN_URL column not found in lookup file")?;

    for result in reader.records() {
        let record = result?;
        let board_id = record.get(board_id_idx).unwrap_or("").trim().to_string();
        let tinyurl = record.get(tinyurl_idx).unwrap_or("").trim();
        let lin_url = record.get(lin_url_idx).unwrap_or("").trim().to_string();

        if !board_id.is_empty() && !tinyurl.is_empty() {
            data.insert(normalize_tinyurl(tinyurl), (board_id, lin_url));
        }
    }

    Ok(data)
}

/// Count the number of data rows (excluding header) in a CSV file.
pub fn count_csv_rows(path: &Path) -> Result<usize> {
    use std::io::BufRead;
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);
    Ok(reader.lines().count().saturating_sub(1))
}

// ============================================================================
// Display Hand
// ============================================================================

/// Display a single hand from a CSV file, returning formatted text.
///
/// This is the library version of the CLI's `display-hand` subcommand.
/// Instead of printing to stdout, it returns the formatted output as a String.
pub fn display_hand(input: &Path, row_num: usize) -> Result<String> {
    if row_num == 0 {
        return Err(anyhow::anyhow!("Row number must be 1 or greater"));
    }

    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_path(input)
        .context("Failed to open input CSV")?;
    let headers = reader.headers()?.clone();

    let find_col = |name: &str| headers.iter().position(|h| h == name);

    let north_col = find_col("North hand").or_else(|| find_col("N hand"));
    let south_col = find_col("South hand").or_else(|| find_col("S hand"));
    let east_col = find_col("East hand").or_else(|| find_col("E hand"));
    let west_col = find_col("West hand").or_else(|| find_col("W hand"));
    let contract_col = find_col("Contract");
    let declarer_col = find_col("Dec");
    let result_col = find_col("Result");
    let cardplay_col = find_col("Cardplay");
    let dd_col = find_col("DD_Analysis");
    let n_col = find_col("N");
    let s_col = find_col("S");
    let e_col = find_col("E");
    let w_col = find_col("W");
    let ref_col = find_col("Ref #");

    let record = reader
        .records()
        .nth(row_num - 1)
        .ok_or_else(|| anyhow::anyhow!("Row {} not found in file", row_num))?
        .context("Failed to read CSV row")?;

    let get = |col: Option<usize>| col.and_then(|i| record.get(i)).unwrap_or("");

    let north_hand = get(north_col);
    let south_hand = get(south_col);
    let east_hand = get(east_col);
    let west_hand = get(west_col);
    let contract = get(contract_col);
    let declarer = get(declarer_col);
    let result = get(result_col);
    let cardplay = get(cardplay_col);
    let dd_analysis = get(dd_col);
    let north_player = get(n_col);
    let south_player = get(s_col);
    let east_player = get(e_col);
    let west_player = get(w_col);
    let ref_num = get(ref_col);

    let mut out = String::new();

    // Header
    writeln!(
        out,
        "{:=^80}",
        format!(" Hand #{} (Ref: {}) ", row_num, ref_num)
    )?;
    writeln!(
        out,
        "\nContract: {} by {}    Result: {}",
        contract, declarer, result
    )?;
    writeln!(
        out,
        "Players: N={} S={} E={} W={}",
        north_player, south_player, east_player, west_player
    )?;

    // Deal
    writeln!(out, "\n{:^40}", "DEAL")?;
    writeln!(out, "{:-<40}", "")?;

    let format_suit = |hand: &str, suit_char: char| -> String {
        for part in hand.split_whitespace() {
            let lower_suit = suit_char.to_ascii_lowercase();
            if part.starts_with(suit_char) || part.starts_with(lower_suit) {
                if let Some(cards) = part.get(2..) {
                    return cards.to_string();
                }
            }
        }
        "-".to_string()
    };

    let format_hand_lines = |hand: &str| -> [String; 4] {
        [
            format!("S: {}", format_suit(hand, 'S')),
            format!("H: {}", format_suit(hand, 'H')),
            format!("D: {}", format_suit(hand, 'D')),
            format!("C: {}", format_suit(hand, 'C')),
        ]
    };

    let north_lines = format_hand_lines(north_hand);
    let south_lines = format_hand_lines(south_hand);
    let east_lines = format_hand_lines(east_hand);
    let west_lines = format_hand_lines(west_hand);

    // North
    writeln!(out, "{:^40}", "North")?;
    for line in &north_lines {
        writeln!(out, "{:^40}", line)?;
    }

    // West and East side by side
    writeln!(out)?;
    writeln!(out, "{:<20}{:>20}", "West", "East")?;
    for i in 0..4 {
        writeln!(out, "{:<20}{:>20}", west_lines[i], east_lines[i])?;
    }

    // South
    writeln!(out)?;
    writeln!(out, "{:^40}", "South")?;
    for line in &south_lines {
        writeln!(out, "{:^40}", line)?;
    }

    // Cardplay
    writeln!(out, "\n{:=^80}", " CARDPLAY ")?;

    if cardplay.is_empty() {
        writeln!(out, "(No cardplay recorded)")?;
    } else {
        let initial_leader = match declarer.chars().next() {
            Some('N') => 'E',
            Some('E') => 'S',
            Some('S') => 'W',
            Some('W') => 'N',
            _ => '?',
        };

        // Parse DD analysis costs
        let dd_costs = parse_dd_costs(dd_analysis);

        writeln!(
            out,
            "\n{:>5} | {:^8} {:^8} {:^8} {:^8} | {:^20}",
            "Trick", "Leader", "2nd", "3rd", "4th", "DD Cost (L/2/3/4)"
        )?;
        writeln!(out, "{:-<80}", "")?;

        let mut current_leader = initial_leader;

        for (trick_idx, trick_str) in cardplay.split('|').enumerate() {
            if trick_str.is_empty() {
                continue;
            }

            let trick_num = trick_idx + 1;
            let cards: Vec<&str> = trick_str.split_whitespace().collect();

            if cards.len() != 4 {
                continue;
            }

            let seats = get_seat_order(current_leader);
            let card_strs: Vec<String> = cards
                .iter()
                .enumerate()
                .map(|(i, c)| format!("{}:{}", seats[i], c))
                .collect();

            let cost_str = if let Some(c) = dd_costs.get(&trick_num) {
                format!("{},{},{},{}", c[0], c[1], c[2], c[3])
            } else {
                "-".to_string()
            };

            writeln!(
                out,
                "{:>5} | {:^8} {:^8} {:^8} {:^8} | {:^20}",
                trick_num,
                card_strs.first().map(|s| s.as_str()).unwrap_or("-"),
                card_strs.get(1).map(|s| s.as_str()).unwrap_or("-"),
                card_strs.get(2).map(|s| s.as_str()).unwrap_or("-"),
                card_strs.get(3).map(|s| s.as_str()).unwrap_or("-"),
                cost_str
            )?;

            if let Some(winner_seat) =
                determine_trick_winner_for_display(&cards, current_leader, contract)
            {
                current_leader = winner_seat;
            }
        }
    }

    // DD Analysis summary
    if !dd_analysis.is_empty() && !dd_analysis.starts_with("ERROR") {
        writeln!(out, "\n{:=^80}", " DD ANALYSIS SUMMARY ")?;

        let mut seat_costs: HashMap<char, u64> = HashMap::new();
        let mut seat_plays: HashMap<char, u64> = HashMap::new();
        let mut seat_errors: HashMap<char, u64> = HashMap::new();

        let initial_leader = match declarer.chars().next() {
            Some('N') => 'E',
            Some('E') => 'S',
            Some('S') => 'W',
            Some('W') => 'N',
            _ => '?',
        };

        let tricks: Vec<&str> = cardplay.split('|').collect();
        let mut current_leader = initial_leader;

        for (trick_idx, trick_str) in dd_analysis.split('|').enumerate() {
            if let Some(colon_idx) = trick_str.find(':') {
                let costs: Vec<u8> = trick_str[colon_idx + 1..]
                    .split(',')
                    .filter_map(|s| s.trim().parse().ok())
                    .collect();

                if costs.len() == 4 {
                    let seats = get_seat_order(current_leader);
                    for (i, &cost) in costs.iter().enumerate() {
                        let seat = seats[i];
                        *seat_costs.entry(seat).or_insert(0) += cost as u64;
                        *seat_plays.entry(seat).or_insert(0) += 1;
                        if cost > 0 {
                            *seat_errors.entry(seat).or_insert(0) += 1;
                        }
                    }

                    if trick_idx < tricks.len() {
                        let cards: Vec<&str> = tricks[trick_idx].split_whitespace().collect();
                        if let Some(winner) =
                            determine_trick_winner_for_display(&cards, current_leader, contract)
                        {
                            current_leader = winner;
                        }
                    }
                }
            }
        }

        let declaring_seats: [char; 2] = match declarer.chars().next() {
            Some('N') | Some('S') => ['N', 'S'],
            Some('E') | Some('W') => ['E', 'W'],
            _ => ['?', '?'],
        };

        writeln!(
            out,
            "\n{:<10} {:>10} {:>10} {:>12} {:>10}",
            "Seat", "Plays", "Errors", "Total Cost", "Role"
        )?;
        writeln!(out, "{:-<60}", "")?;

        for seat in ['N', 'E', 'S', 'W'] {
            let plays = seat_plays.get(&seat).unwrap_or(&0);
            let errors = seat_errors.get(&seat).unwrap_or(&0);
            let cost = seat_costs.get(&seat).unwrap_or(&0);
            let role = if declaring_seats.contains(&seat) {
                "Declaring"
            } else {
                "Defending"
            };
            writeln!(
                out,
                "{:<10} {:>10} {:>10} {:>12} {:>10}",
                seat, plays, errors, cost, role
            )?;
        }
    } else if dd_analysis.starts_with("ERROR") {
        writeln!(out, "\n{:=^80}", " DD ANALYSIS ")?;
        writeln!(out, "Error: {}", dd_analysis)?;
    }

    writeln!(out, "\n{:=^80}", "")?;

    Ok(out)
}

// ============================================================================
// Stats
// ============================================================================

/// Compute DD error statistics and return formatted text.
pub fn compute_stats(input: &Path, top_n: usize) -> Result<String> {
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_path(input)
        .context("Failed to open input CSV")?;
    let headers = reader.headers()?.clone();

    let n_col = headers
        .iter()
        .position(|h| h == "N")
        .ok_or_else(|| anyhow::anyhow!("Column 'N' not found"))?;
    let s_col = headers
        .iter()
        .position(|h| h == "S")
        .ok_or_else(|| anyhow::anyhow!("Column 'S' not found"))?;
    let e_col = headers
        .iter()
        .position(|h| h == "E")
        .ok_or_else(|| anyhow::anyhow!("Column 'E' not found"))?;
    let w_col = headers
        .iter()
        .position(|h| h == "W")
        .ok_or_else(|| anyhow::anyhow!("Column 'W' not found"))?;
    let dec_col = headers
        .iter()
        .position(|h| h == "Dec")
        .ok_or_else(|| anyhow::anyhow!("Column 'Dec' not found"))?;

    let dd_n_plays_col = headers
        .iter()
        .position(|h| h == "DD_N_Plays")
        .ok_or_else(|| anyhow::anyhow!("Column 'DD_N_Plays' not found - run analyze-dd first"))?;
    let dd_s_plays_col = headers
        .iter()
        .position(|h| h == "DD_S_Plays")
        .ok_or_else(|| anyhow::anyhow!("Column 'DD_S_Plays' not found"))?;
    let dd_e_plays_col = headers
        .iter()
        .position(|h| h == "DD_E_Plays")
        .ok_or_else(|| anyhow::anyhow!("Column 'DD_E_Plays' not found"))?;
    let dd_w_plays_col = headers
        .iter()
        .position(|h| h == "DD_W_Plays")
        .ok_or_else(|| anyhow::anyhow!("Column 'DD_W_Plays' not found"))?;
    let dd_n_errors_col = headers
        .iter()
        .position(|h| h == "DD_N_Errors")
        .ok_or_else(|| anyhow::anyhow!("Column 'DD_N_Errors' not found"))?;
    let dd_s_errors_col = headers
        .iter()
        .position(|h| h == "DD_S_Errors")
        .ok_or_else(|| anyhow::anyhow!("Column 'DD_S_Errors' not found"))?;
    let dd_e_errors_col = headers
        .iter()
        .position(|h| h == "DD_E_Errors")
        .ok_or_else(|| anyhow::anyhow!("Column 'DD_E_Errors' not found"))?;
    let dd_w_errors_col = headers
        .iter()
        .position(|h| h == "DD_W_Errors")
        .ok_or_else(|| anyhow::anyhow!("Column 'DD_W_Errors' not found"))?;

    let mut player_stats: HashMap<String, PlayerStats> = HashMap::new();
    let mut processed = 0u64;
    let mut skipped = 0u64;

    for result in reader.records() {
        let record = result.context("Failed to read CSV row")?;
        processed += 1;

        let north = record.get(n_col).unwrap_or("").to_lowercase();
        let south = record.get(s_col).unwrap_or("").to_lowercase();
        let east = record.get(e_col).unwrap_or("").to_lowercase();
        let west = record.get(w_col).unwrap_or("").to_lowercase();

        let declarer = record.get(dec_col).unwrap_or("").trim().to_uppercase();
        if declarer.is_empty() {
            skipped += 1;
            continue;
        }

        let n_plays: u64 = record
            .get(dd_n_plays_col)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let s_plays: u64 = record
            .get(dd_s_plays_col)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let e_plays: u64 = record
            .get(dd_e_plays_col)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let w_plays: u64 = record
            .get(dd_w_plays_col)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let n_errors: u64 = record
            .get(dd_n_errors_col)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let s_errors: u64 = record
            .get(dd_s_errors_col)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let e_errors: u64 = record
            .get(dd_e_errors_col)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let w_errors: u64 = record
            .get(dd_w_errors_col)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        if n_plays == 0 && s_plays == 0 && e_plays == 0 && w_plays == 0 {
            skipped += 1;
            continue;
        }

        let (declarer_name, dummy_name) = match declarer.chars().next() {
            Some('N') => (&north, &south),
            Some('S') => (&south, &north),
            Some('E') => (&east, &west),
            Some('W') => (&west, &east),
            _ => {
                skipped += 1;
                continue;
            }
        };

        let seat_data = [
            (&north, n_plays, n_errors),
            (&south, s_plays, s_errors),
            (&east, e_plays, e_errors),
            (&west, w_plays, w_errors),
        ];

        for (player_name, plays, errors) in &seat_data {
            if player_name.is_empty() {
                continue;
            }

            let is_declarer = *player_name == declarer_name;
            let is_dummy = *player_name == dummy_name;
            let is_declaring_side = is_declarer || is_dummy;

            if is_declaring_side {
                let stats = player_stats
                    .entry(declarer_name.clone())
                    .or_insert_with(|| PlayerStats::new(declarer_name));
                stats.declaring_plays += plays;
                stats.declaring_errors += errors;
            } else {
                let stats = player_stats
                    .entry((*player_name).clone())
                    .or_insert_with(|| PlayerStats::new(player_name));
                stats.defending_plays += plays;
                stats.defending_errors += errors;
            }
        }

        for (player_name, _, _) in &seat_data {
            if player_name.is_empty() {
                continue;
            }
            let stats = player_stats
                .entry((*player_name).clone())
                .or_insert_with(|| PlayerStats::new(player_name));
            stats.total_deals += 1;
            if *player_name == declarer_name {
                stats.declaring_deals += 1;
            } else if *player_name != dummy_name {
                stats.defending_deals += 1;
            }
        }
    }

    // Sort by total deals
    let mut players: Vec<_> = player_stats.values().cloned().collect();
    players.sort_by(|a, b| b.total_deals.cmp(&a.total_deals));

    // Build "Field" from everyone except top 2
    let top_2: std::collections::HashSet<String> =
        players.iter().take(2).map(|p| p.name.clone()).collect();
    let mut field_stats = PlayerStats::new("FIELD");
    for player in &players {
        if !top_2.contains(&player.name) {
            field_stats.merge(player);
        }
    }

    // Format output
    let mut out = String::new();
    writeln!(out, "Processed {} deals ({} skipped)", processed, skipped)?;
    writeln!(out, "Found {} unique players\n", player_stats.len())?;

    writeln!(out, "{:=^126}", " DD Error Rate Analysis ")?;
    writeln!(
        out,
        "\n{:<20} {:>8} {:>6} {:>6} {:>12} {:>10} {:>12} {:>10} {:>10} {:>8}",
        "Player",
        "Deals",
        "Decl",
        "Def",
        "Decl Plays",
        "Decl Err%",
        "Def Plays",
        "Def Err%",
        "Diff",
        "Rel%"
    )?;
    writeln!(out, "{:-<126}", "")?;

    for player in players.iter().take(top_n) {
        let decl_rate = player.declaring_error_rate();
        let def_rate = player.defending_error_rate();
        let diff = decl_rate - def_rate;
        let rel_pct = if decl_rate > 0.0 {
            -diff / decl_rate * 100.0
        } else {
            0.0
        };

        writeln!(
            out,
            "{:<20} {:>8} {:>6} {:>6} {:>12} {:>9.2}% {:>12} {:>9.2}% {:>+9.2}% {:>+7.1}%",
            truncate_name(&player.name, 20),
            player.total_deals,
            player.declaring_deals,
            player.defending_deals,
            player.declaring_plays,
            decl_rate,
            player.defending_plays,
            def_rate,
            diff,
            rel_pct
        )?;

        let decl_ci = player.declaring_ci();
        let def_ci = player.defending_ci();
        if !decl_ci.is_nan() || !def_ci.is_nan() {
            writeln!(
                out,
                "{:<20} {:>8} {:>6} {:>6} {:>12} {:>10} {:>12} {:>10}",
                "",
                "",
                "",
                "",
                format!("(\u{00b1}{:.2}%)", decl_ci),
                "",
                format!("(\u{00b1}{:.2}%)", def_ci),
                ""
            )?;
        }
    }

    // Field aggregate
    writeln!(out, "{:-<126}", "")?;
    let decl_rate = field_stats.declaring_error_rate();
    let def_rate = field_stats.defending_error_rate();
    let diff = decl_rate - def_rate;
    let rel_pct = if decl_rate > 0.0 {
        -diff / decl_rate * 100.0
    } else {
        0.0
    };

    writeln!(
        out,
        "{:<20} {:>8} {:>6} {:>6} {:>12} {:>9.2}% {:>12} {:>9.2}% {:>+9.2}% {:>+7.1}%",
        "FIELD (others)",
        field_stats.total_deals,
        field_stats.declaring_deals,
        field_stats.defending_deals,
        field_stats.declaring_plays,
        decl_rate,
        field_stats.defending_plays,
        def_rate,
        diff,
        rel_pct
    )?;

    writeln!(out, "\n{:=^100}", "")?;
    writeln!(out, "\nInterpretation:")?;
    writeln!(
        out,
        "  - Decl Err%: Percentage of plays with DD cost > 0 when declaring/dummy"
    )?;
    writeln!(
        out,
        "  - Def Err%:  Percentage of plays with DD cost > 0 when defending"
    )?;
    writeln!(
        out,
        "  - Diff:      Decl% - Def% (negative means more errors on defense)"
    )?;

    Ok(out)
}

// ============================================================================
// Anonymize
// ============================================================================

/// Result of CSV anonymization, including name mappings for subsequent text processing.
pub struct AnonymizeResult {
    /// Human-readable summary.
    pub summary: String,
    /// All name mappings (original lowercase -> replacement), sorted longest-first.
    pub name_mappings: Vec<(String, String)>,
    /// Subject-only mappings (from explicit map), sorted longest-first.
    /// Use for hotspot reports which only contain the 2 subject players.
    pub subject_mappings: Vec<(String, String)>,
    /// URL mappings (normalized tinyurl key -> anonymized full handviewer URL).
    pub url_mappings: HashMap<String, String>,
}

/// Configuration for anonymize operation.
pub struct AnonymizeConfig {
    /// Input CSV path
    pub input: std::path::PathBuf,
    /// Output CSV path
    pub output: std::path::PathBuf,
    /// Secret key for hashing
    pub key: String,
    /// Explicit name mappings ("old=New,old2=New2")
    pub map: String,
    /// Columns to anonymize
    pub columns: Vec<String>,
}

/// Run anonymize and return a result with summary and name mappings.
///
/// When `board_id_map` is non-empty, the BBO column is replaced with Board_IDs
/// from the tinyurl lookup file. Calls `on_progress` after each row.
pub fn anonymize_csv(
    config: &AnonymizeConfig,
    board_id_map: &HashMap<String, (String, String)>,
    total_rows: usize,
    on_progress: &mut impl FnMut(&AnonProgress) -> bool,
) -> Result<AnonymizeResult> {
    if config.key.is_empty() {
        return Err(anyhow::anyhow!("Anonymization key is required"));
    }

    let csv_data = read_bbo_csv_fixed(&config.input)?;
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_reader(csv_data.as_bytes());
    let headers = reader.headers()?.clone();

    let col_indices: Vec<usize> = config
        .columns
        .iter()
        .filter_map(|col| headers.iter().position(|h| h == col))
        .collect();

    if col_indices.is_empty() {
        return Err(anyhow::anyhow!(
            "None of the specified columns found in CSV"
        ));
    }

    let lin_url_idx = headers.iter().position(|h| h == "LIN_URL");
    let bbo_col_idx = headers.iter().position(|h| h == "BBO");

    let mut anonymizer = Anonymizer::new(&config.key, &config.map);
    let mut url_mappings: HashMap<String, String> = HashMap::new();

    let mut writer =
        csv::Writer::from_path(&config.output).context("Failed to create output CSV")?;
    writer.write_record(&headers)?;

    let mut processed = 0u64;

    for result in reader.records() {
        let record = result.context("Failed to read CSV row")?;
        processed += 1;

        let mut output_fields: Vec<String> = Vec::with_capacity(record.len());

        for (i, field) in record.iter().enumerate() {
            if col_indices.contains(&i) && !field.is_empty() {
                output_fields.push(anonymizer.anonymize(field));
            } else if Some(i) == lin_url_idx && !field.is_empty() {
                output_fields.push(anonymize_lin_url(field, &mut anonymizer));
            } else if Some(i) == bbo_col_idx && !field.is_empty() && !board_id_map.is_empty() {
                // Replace tinyurl with Board_ID
                let key = normalize_tinyurl(field);
                if let Some((board_id, _)) = board_id_map.get(&key) {
                    output_fields.push(board_id.clone());
                } else {
                    output_fields.push(field.to_string());
                }
            } else {
                output_fields.push(field.to_string());
            }
        }

        // Collect tinyurl -> anonymized LIN_URL mapping
        if let (Some(bbo_idx), Some(lin_idx)) = (bbo_col_idx, lin_url_idx) {
            let bbo_url = record.get(bbo_idx).unwrap_or("").trim();
            if !bbo_url.is_empty() && lin_idx < output_fields.len() {
                let anon_lin = &output_fields[lin_idx];
                if !anon_lin.is_empty() {
                    url_mappings.insert(normalize_tinyurl(bbo_url), anon_lin.clone());
                }
            }
        }

        writer.write_record(&output_fields)?;

        if processed.is_multiple_of(500) || processed as usize == total_rows {
            let keep_going = on_progress(&AnonProgress {
                completed: processed as usize,
                total: total_rows,
                phase: "Anonymizing CSV...",
            });
            if !keep_going {
                writer.flush()?;
                return Err(anyhow::anyhow!(
                    "Cancelled after {} of {} rows",
                    processed,
                    total_rows
                ));
            }
        }
    }

    writer.flush()?;

    // Collect all mappings, sorted longest-first for safe text replacement
    let mut name_mappings: Vec<(String, String)> = anonymizer
        .explicit_maps
        .iter()
        .chain(anonymizer.generated_maps.iter())
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    name_mappings.sort_by(|a, b| b.0.len().cmp(&a.0.len()));

    // Subject-only mappings (explicit maps) for hotspot reports
    let mut subject_mappings: Vec<(String, String)> = anonymizer
        .explicit_maps
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    subject_mappings.sort_by(|a, b| b.0.len().cmp(&a.0.len()));

    Ok(AnonymizeResult {
        summary: format!(
            "Anonymization complete:\n  Rows processed: {}\n  Explicit mappings: {}\n  Generated names: {}\n  Total unique names: {}",
            processed,
            anonymizer.explicit_maps.len(),
            anonymizer.generated_maps.len(),
            anonymizer.used_names.len()
        ),
        name_mappings,
        subject_mappings,
        url_mappings,
    })
}

/// Anonymize player names in a text file using pre-built name mappings.
///
/// Performs case-insensitive replacement with column-aware spacing: when a name
/// is followed by whitespace, the space count is adjusted so that column
/// alignment is preserved. Uses simple string matching (no regex).
///
/// When `board_id_map` is non-empty (hotspot reports), tinyurls are replaced
/// with Board_IDs and the anonymized LIN_URL (from `url_mappings`) is appended
/// at the end of each matching line.
pub fn anonymize_text_file(
    input: &Path,
    output: &Path,
    name_mappings: &[(String, String)],
    url_mappings: &HashMap<String, String>,
    board_id_map: &HashMap<String, (String, String)>,
) -> Result<()> {
    let content = std::fs::read_to_string(input)
        .with_context(|| format!("Failed to read text file: {}", input.display()))?;
    let had_trailing_newline = content.ends_with('\n');

    let mut lines: Vec<String> = content.lines().map(|l| l.to_string()).collect();

    // Column-aware name replacement: process ALL names on each line in a single
    // pass so that column debt from one replacement (e.g. "adwilliams" -> "Bob"
    // inside "adwilliams-spwilliams") carries forward to the next whitespace gap.
    let name_lowers: Vec<(String, usize, &str)> = name_mappings
        .iter()
        .map(|(orig, repl)| (orig.to_lowercase(), orig.len(), repl.as_str()))
        .collect();

    for line in &mut lines {
        let line_lower = line.to_lowercase();

        // Find all name matches on this line
        let mut matches: Vec<(usize, usize, &str)> = Vec::new(); // (start, orig_len, replacement)
        for (orig_lower, orig_len, replacement) in &name_lowers {
            let mut start = 0;
            while let Some(pos) = line_lower[start..].find(orig_lower.as_str()) {
                matches.push((start + pos, *orig_len, replacement));
                start += pos + orig_len;
            }
        }

        if matches.is_empty() {
            continue;
        }

        // Sort by position; for same position, prefer longest match
        matches.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));

        // Remove overlapping matches (keep earliest/longest)
        let mut filtered: Vec<(usize, usize, &str)> = Vec::new();
        for m in matches {
            if let Some(last) = filtered.last() {
                if m.0 < last.0 + last.1 {
                    continue;
                }
            }
            filtered.push(m);
        }

        // Build result with cumulative column debt tracking
        let mut result = String::with_capacity(line.len());
        let mut pos = 0;
        let mut col_debt: isize = 0;

        for (match_start, orig_len, replacement) in &filtered {
            // Copy text between previous position and this match
            result.push_str(&line[pos..*match_start]);
            result.push_str(replacement);
            col_debt += *orig_len as isize - replacement.len() as isize;

            let after_name = match_start + orig_len;
            if after_name < line.len() {
                let remaining = &line[after_name..];
                let ws_len = remaining.len() - remaining.trim_start().len();
                if ws_len > 0 {
                    // Absorb accumulated column debt into this whitespace gap
                    let new_ws = (ws_len as isize + col_debt).max(1) as usize;
                    result.push_str(&" ".repeat(new_ws));
                    col_debt = 0;
                    pos = after_name + ws_len;
                } else {
                    pos = after_name;
                }
            } else {
                pos = after_name;
            }
        }

        result.push_str(&line[pos..]);
        *line = result;
    }

    // Tinyurl replacement: replace with Board_ID, append anonymized LIN_URL
    if !board_id_map.is_empty() {
        let url_prefixes = [
            "http://tinyurl.com/",
            "https://tinyurl.com/",
            "http://bit.ly/",
            "https://bit.ly/",
        ];
        for line in &mut lines {
            let url_start = url_prefixes
                .iter()
                .filter_map(|prefix| line.find(prefix))
                .min();
            if let Some(start) = url_start {
                let url_end = line[start..]
                    .find(char::is_whitespace)
                    .map(|p| start + p)
                    .unwrap_or(line.len());
                let url = &line[start..url_end];
                let key = normalize_tinyurl(url);
                let board_id = board_id_map
                    .get(&key)
                    .map(|(id, _)| id.as_str())
                    .unwrap_or("[unknown]");
                let anon_lin = url_mappings.get(&key).map(|s| s.as_str()).unwrap_or("");
                let mut new_line = format!("{}{}{}", &line[..start], board_id, &line[url_end..]);
                if !anon_lin.is_empty() {
                    new_line.push(' ');
                    new_line.push_str(anon_lin);
                }
                *line = new_line;
            }
        }
    }

    let mut result = lines.join("\n");
    if had_trailing_newline && !result.ends_with('\n') {
        result.push('\n');
    }

    std::fs::write(output, &result)
        .with_context(|| format!("Failed to write anonymized text: {}", output.display()))?;
    Ok(())
}

/// Configuration for anonymizing all case files (CSV + text reports).
pub struct AnonymizeAllConfig {
    /// Input cardplay CSV path.
    pub csv_input: PathBuf,
    /// Output cardplay CSV path.
    pub csv_output: PathBuf,
    /// Secret key for hashing.
    pub key: String,
    /// Explicit name mappings ("old=New,old2=New2").
    pub map: String,
    /// Columns to anonymize in the CSV.
    pub columns: Vec<String>,
    /// Optional concise report input path.
    pub concise_input: Option<PathBuf>,
    /// Optional concise report output path.
    pub concise_output: Option<PathBuf>,
    /// Optional hotspot report input path.
    pub hotspot_input: Option<PathBuf>,
    /// Optional hotspot report output path.
    pub hotspot_output: Option<PathBuf>,
}

/// Progress information for the anonymize operation.
pub struct AnonProgress {
    /// Number of CSV rows completed so far
    pub completed: usize,
    /// Total number of CSV rows to process
    pub total: usize,
    /// Current phase description
    pub phase: &'static str,
}

/// Run full anonymization: CSV first (to build mappings), then text files.
///
/// Calls `on_progress` after each CSV row. Returns a summary string.
pub fn anonymize_all(
    config: &AnonymizeAllConfig,
    mut on_progress: impl FnMut(&AnonProgress) -> bool,
) -> Result<String> {
    // Load tinyurl → Board_ID mapping from lookup file if available
    let lookup_path = derive_lookup_path(&config.csv_input);
    let board_id_map = if lookup_path.exists() {
        load_lookup_board_ids(&lookup_path)?
    } else {
        HashMap::new()
    };

    let total_rows = count_csv_rows(&config.csv_input)?;

    let csv_config = AnonymizeConfig {
        input: config.csv_input.clone(),
        output: config.csv_output.clone(),
        key: config.key.clone(),
        map: config.map.clone(),
        columns: config.columns.clone(),
    };
    let csv_result = anonymize_csv(&csv_config, &board_id_map, total_rows, &mut on_progress)?;

    let summary = csv_result.summary.clone();

    let empty_urls = HashMap::new();
    let empty_board_ids: HashMap<String, (String, String)> = HashMap::new();

    if let (Some(input), Some(output)) = (&config.concise_input, &config.concise_output) {
        on_progress(&AnonProgress {
            completed: total_rows,
            total: total_rows,
            phase: "Processing concise report...",
        });
        anonymize_text_file(
            input,
            output,
            &csv_result.name_mappings,
            &empty_urls,
            &empty_board_ids,
        )?;
    }

    if let (Some(input), Some(output)) = (&config.hotspot_input, &config.hotspot_output) {
        on_progress(&AnonProgress {
            completed: total_rows,
            total: total_rows,
            phase: "Processing hotspot report...",
        });
        // Hotspot reports only contain the 2 subject players — use subject_mappings
        // to avoid spurious matches (e.g. player named "None" replacing "Vul: None")
        anonymize_text_file(
            input,
            output,
            &csv_result.subject_mappings,
            &csv_result.url_mappings,
            &board_id_map,
        )?;
    }

    Ok(summary)
}

// ============================================================================
// Helpers
// ============================================================================

/// Get seat order starting from leader going clockwise.
fn get_seat_order(leader: char) -> [char; 4] {
    match leader {
        'N' => ['N', 'E', 'S', 'W'],
        'E' => ['E', 'S', 'W', 'N'],
        'S' => ['S', 'W', 'N', 'E'],
        'W' => ['W', 'N', 'E', 'S'],
        _ => ['N', 'E', 'S', 'W'],
    }
}

/// Parse DD analysis string into a map of trick_num -> costs.
fn parse_dd_costs(dd_analysis: &str) -> HashMap<usize, Vec<u8>> {
    let mut dd_costs = HashMap::new();
    if dd_analysis.is_empty() || dd_analysis.starts_with("ERROR") {
        return dd_costs;
    }
    for trick_str in dd_analysis.split('|') {
        if let Some(colon_idx) = trick_str.find(':') {
            let trick_num_str = &trick_str[1..colon_idx]; // Skip 'T'
            if let Ok(trick_num) = trick_num_str.parse::<usize>() {
                let costs: Vec<u8> = trick_str[colon_idx + 1..]
                    .split(',')
                    .filter_map(|s| s.trim().parse().ok())
                    .collect();
                if costs.len() == 4 {
                    dd_costs.insert(trick_num, costs);
                }
            }
        }
    }
    dd_costs
}

/// Determine trick winner based on cards played.
fn determine_trick_winner_for_display(
    cards: &[&str],
    leader: char,
    contract: &str,
) -> Option<char> {
    if cards.len() != 4 {
        return None;
    }

    let trump = if contract.contains('N') {
        None
    } else if contract.contains('S') {
        Some('S')
    } else if contract.contains('H') {
        Some('H')
    } else if contract.contains('D') {
        Some('D')
    } else if contract.contains('C') {
        Some('C')
    } else {
        None
    };

    let parse_card = |s: &str| -> Option<(char, u8)> {
        let s = s.trim();
        if s.len() < 2 {
            return None;
        }
        let suit = s.chars().next()?;
        let rank_char = s.chars().nth(1)?;
        let rank = match rank_char {
            'A' => 14,
            'K' => 13,
            'Q' => 12,
            'J' => 11,
            'T' | '1' => 10,
            '9' => 9,
            '8' => 8,
            '7' => 7,
            '6' => 6,
            '5' => 5,
            '4' => 4,
            '3' => 3,
            '2' => 2,
            _ => return None,
        };
        Some((suit, rank))
    };

    let parsed: Vec<Option<(char, u8)>> = cards.iter().map(|c| parse_card(c)).collect();
    let lead_suit = parsed[0].map(|(s, _)| s)?;

    let mut winner_idx = 0;
    let mut winning_card = parsed[0]?;

    for (i, card_opt) in parsed.iter().enumerate().skip(1) {
        if let Some((suit, rank)) = card_opt {
            let dominated = if let Some(t) = trump {
                if *suit == t && winning_card.0 != t {
                    true
                } else if *suit == t && winning_card.0 == t {
                    *rank > winning_card.1
                } else if winning_card.0 == t {
                    false
                } else if *suit == lead_suit {
                    *rank > winning_card.1
                } else {
                    false
                }
            } else {
                *suit == lead_suit && *rank > winning_card.1
            };

            if dominated {
                winner_idx = i;
                winning_card = (*suit, *rank);
            }
        }
    }

    let seats = get_seat_order(leader);
    Some(seats[winner_idx])
}

// ============================================================================
// CSV Truncation (for deal limit)
// ============================================================================

/// Truncate a CSV file to the first `first_n` data rows, writing to a temp file.
///
/// Returns the path to the truncated file in the system temp directory.
/// The caller is responsible for cleaning up the temp file when done.
pub fn truncate_csv(input: &Path, first_n: usize) -> Result<std::path::PathBuf> {
    let csv_data = read_bbo_csv_fixed(input)?;
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_reader(csv_data.as_bytes());
    let headers = reader.headers()?.clone();

    let stem = input
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("input");
    let temp_path = std::env::temp_dir().join(format!("{}_first{}.csv", stem, first_n));

    let mut writer =
        csv::Writer::from_path(&temp_path).context("Failed to create truncated CSV")?;
    writer.write_record(&headers)?;

    let mut count = 0;
    for result in reader.records() {
        let record = result.context("Failed to read CSV row")?;
        writer.write_record(&record)?;
        count += 1;
        if count >= first_n {
            break;
        }
    }

    writer.flush()?;
    Ok(temp_path)
}

// ============================================================================
// Lookup File Helpers
// ============================================================================

/// Derive the tinyurl lookup file path from a cardplay output path.
///
/// Replaces "cardplay" with "tinyurl lookup" in the filename.
/// e.g., "AWilliams cardplay.csv" → "AWilliams tinyurl lookup.csv"
pub fn derive_lookup_path(output: &Path) -> PathBuf {
    let stem = output
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();
    let new_stem = stem.replace("cardplay", "tinyurl lookup");
    let ext = output
        .extension()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();
    output.with_file_name(format!("{}.{}", new_stem, ext))
}

/// Format the auction as a human-readable string (e.g., "1C-p-1H-2S").
fn format_auction(lin: &LinData) -> String {
    lin.auction
        .iter()
        .map(|b| {
            let mut s = b.bid.clone();
            if b.alert {
                s.push('!');
            }
            s
        })
        .collect::<Vec<_>>()
        .join("-")
}

/// Format explanations as pipe-separated bid=annotation pairs.
///
/// e.g., "2C=capp+transfer+to+diam|3H=sp"
fn format_explanations(lin: &LinData) -> String {
    lin.auction
        .iter()
        .filter_map(|b| {
            b.annotation.as_ref().map(|ann| {
                let encoded = ann.replace(' ', "+");
                format!("{}={}", b.bid, encoded)
            })
        })
        .collect::<Vec<_>>()
        .join("|")
}

/// Format vulnerability as a short string.
fn format_vulnerability(v: &Vulnerability) -> &'static str {
    match v {
        Vulnerability::None => "None",
        Vulnerability::NorthSouth => "NS",
        Vulnerability::EastWest => "EW",
        Vulnerability::Both => "Both",
    }
}

/// Write a single lookup row from parsed LIN data.
fn write_lookup_row(
    out: &mut csv::Writer<impl std::io::Write>,
    board_id: usize,
    tinyurl: &str,
    lin: &LinData,
    lin_url: &str,
) -> Result<()> {
    let board_header = lin.board_header.as_deref().unwrap_or("");
    let cardplay = lin.format_cardplay_by_trick();
    let claim = lin.claim.map(|c| c.to_string()).unwrap_or_default();
    let deal_pbn = lin.deal.to_pbn(Direction::North);
    let auction = format_auction(lin);
    let explanations = format_explanations(lin);
    let vulnerability = format_vulnerability(&lin.vulnerability);

    out.write_record([
        &board_id.to_string(),
        tinyurl,
        board_header,
        &lin.player_names[0], // S
        &lin.player_names[1], // W
        &lin.player_names[2], // N
        &lin.player_names[3], // E
        &format!("{:?}", lin.dealer),
        vulnerability,
        &deal_pbn,
        &auction,
        &explanations,
        &cardplay,
        &claim,
        lin_url,
    ])?;
    Ok(())
}

/// Write an empty lookup row (for missing/error URLs).
fn write_lookup_empty_row(
    out: &mut csv::Writer<impl std::io::Write>,
    board_id: usize,
    tinyurl: &str,
) -> Result<()> {
    out.write_record([
        &board_id.to_string(),
        tinyurl,
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
    ])?;
    Ok(())
}

/// The header fields for the tinyurl lookup CSV.
const LOOKUP_FIELDS: &[&str] = &[
    "Board_ID",
    "TinyURL",
    "Board_Header",
    "Player_S",
    "Player_W",
    "Player_N",
    "Player_E",
    "Dealer",
    "Vulnerability",
    "Deal_PBN",
    "Auction",
    "Explanations",
    "Cardplay",
    "Claim",
    "LIN_URL",
];

// ============================================================================
// Internal Helpers
// ============================================================================

/// Truncate a name to fit in a column.
fn truncate_name(name: &str, max_len: usize) -> String {
    if name.len() <= max_len {
        name.to_string()
    } else {
        format!("{}...", &name[..max_len - 3])
    }
}

/// Read a BBO CSV file and fix malformed lines.
fn read_bbo_csv_fixed(path: &Path) -> Result<String> {
    use std::io::{BufRead, BufReader};

    let file = std::fs::File::open(path).context("Failed to open input file")?;
    let reader = BufReader::new(file);
    let mut output = String::new();

    for line in reader.lines() {
        let line = line.context("Failed to read line")?;
        let fixed = fix_bbo_csv_line(&line);
        output.push_str(&fixed);
        output.push('\n');
    }

    Ok(output)
}

/// Fix BBO's malformed CSV quoting.
fn fix_bbo_csv_line(line: &str) -> String {
    if !line.trim_end().ends_with('"') {
        return line.to_string();
    }

    if let Some(last_comma_quote) = line.rfind(",\"") {
        let prefix = &line[..last_comma_quote + 1];
        let quoted_field = &line[last_comma_quote + 1..];

        if quoted_field.len() > 2
            && quoted_field.starts_with('"')
            && quoted_field.trim_end().ends_with('"')
        {
            let inner = &quoted_field[1..quoted_field.trim_end().len() - 1];
            if inner.contains('"') {
                let fixed_inner = inner.replace('"', "'");
                return format!("{}\"{}\"", prefix, fixed_inner);
            }
        }
    }

    line.to_string()
}

// ============================================================================
// Player Stats (for compute_stats)
// ============================================================================

#[derive(Default, Clone)]
struct PlayerStats {
    name: String,
    total_deals: u64,
    declaring_plays: u64,
    declaring_errors: u64,
    declaring_deals: u64,
    defending_plays: u64,
    defending_errors: u64,
    defending_deals: u64,
}

impl PlayerStats {
    fn new(name: &str) -> Self {
        PlayerStats {
            name: name.to_string(),
            ..Default::default()
        }
    }

    fn declaring_error_rate(&self) -> f64 {
        if self.declaring_plays == 0 {
            0.0
        } else {
            self.declaring_errors as f64 / self.declaring_plays as f64 * 100.0
        }
    }

    fn defending_error_rate(&self) -> f64 {
        if self.defending_plays == 0 {
            0.0
        } else {
            self.defending_errors as f64 / self.defending_plays as f64 * 100.0
        }
    }

    fn merge(&mut self, other: &PlayerStats) {
        self.total_deals += other.total_deals;
        self.declaring_plays += other.declaring_plays;
        self.declaring_errors += other.declaring_errors;
        self.declaring_deals += other.declaring_deals;
        self.defending_plays += other.defending_plays;
        self.defending_errors += other.defending_errors;
        self.defending_deals += other.defending_deals;
    }

    fn declaring_ci(&self) -> f64 {
        if self.declaring_plays < 30 {
            return f64::NAN;
        }
        let p = self.declaring_errors as f64 / self.declaring_plays as f64;
        let n = self.declaring_plays as f64;
        1.96 * (p * (1.0 - p) / n).sqrt() * 100.0
    }

    fn defending_ci(&self) -> f64 {
        if self.defending_plays < 30 {
            return f64::NAN;
        }
        let p = self.defending_errors as f64 / self.defending_plays as f64;
        let n = self.defending_plays as f64;
        1.96 * (p * (1.0 - p) / n).sqrt() * 100.0
    }
}

// ============================================================================
// Anonymizer
// ============================================================================

/// Common first names for anonymization.
const FIRST_NAMES: &[&str] = &[
    "Aaron",
    "Abigail",
    "Adam",
    "Adrian",
    "Aiden",
    "Alex",
    "Alice",
    "Allison",
    "Amanda",
    "Amber",
    "Amy",
    "Andrea",
    "Andrew",
    "Angela",
    "Anna",
    "Anthony",
    "Ashley",
    "Austin",
    "Barbara",
    "Benjamin",
    "Beth",
    "Brandon",
    "Brenda",
    "Brian",
    "Brittany",
    "Bruce",
    "Bryan",
    "Caleb",
    "Cameron",
    "Carl",
    "Carlos",
    "Carol",
    "Caroline",
    "Catherine",
    "Charles",
    "Charlotte",
    "Chelsea",
    "Chris",
    "Christina",
    "Christine",
    "Christopher",
    "Cindy",
    "Claire",
    "Clara",
    "Cody",
    "Colin",
    "Connor",
    "Craig",
    "Crystal",
    "Cynthia",
    "Dale",
    "Daniel",
    "Danielle",
    "Darren",
    "David",
    "Dawn",
    "Deborah",
    "Denise",
    "Dennis",
    "Derek",
    "Diana",
    "Diane",
    "Donald",
    "Donna",
    "Dorothy",
    "Douglas",
    "Dylan",
    "Edward",
    "Eileen",
    "Eleanor",
    "Elizabeth",
    "Ellen",
    "Emily",
    "Emma",
    "Eric",
    "Erica",
    "Erin",
    "Ethan",
    "Eugene",
    "Eva",
    "Evan",
    "Evelyn",
    "Frances",
    "Francis",
    "Frank",
    "Gabriel",
    "Gary",
    "George",
    "Gerald",
    "Gloria",
    "Grace",
    "Gregory",
    "Hannah",
    "Harold",
    "Harry",
    "Heather",
    "Helen",
    "Henry",
    "Holly",
    "Howard",
    "Ian",
    "Isaac",
    "Isabella",
    "Jack",
    "Jacob",
    "Jacqueline",
    "Jake",
    "James",
    "Jamie",
    "Jane",
    "Janet",
    "Janice",
    "Jason",
    "Jean",
    "Jeffrey",
    "Jennifer",
    "Jeremy",
    "Jerry",
    "Jesse",
    "Jessica",
    "Jill",
    "Joan",
    "Joe",
    "Joel",
    "John",
    "Jonathan",
    "Jordan",
    "Jose",
    "Joseph",
    "Joshua",
    "Joyce",
    "Juan",
    "Judith",
    "Julia",
    "Julie",
    "Justin",
    "Karen",
    "Katherine",
    "Kathleen",
    "Kathryn",
    "Katie",
    "Keith",
    "Kelly",
    "Kenneth",
    "Kevin",
    "Kim",
    "Kimberly",
    "Kyle",
    "Larry",
    "Laura",
    "Lauren",
    "Lawrence",
    "Leah",
    "Leonard",
    "Leslie",
    "Lillian",
    "Linda",
    "Lindsay",
    "Lisa",
    "Logan",
    "Lori",
    "Louis",
    "Lucas",
    "Lucy",
    "Luke",
    "Lynn",
    "Madison",
    "Margaret",
    "Maria",
    "Marie",
    "Marilyn",
    "Mark",
    "Martha",
    "Martin",
    "Mary",
    "Mason",
    "Matthew",
    "Megan",
    "Melanie",
    "Melissa",
    "Michael",
    "Michelle",
    "Mike",
    "Mildred",
    "Monica",
    "Nancy",
    "Natalie",
    "Nathan",
    "Nicholas",
    "Nicole",
    "Noah",
    "Norma",
    "Oliver",
    "Olivia",
    "Oscar",
    "Pamela",
    "Patricia",
    "Patrick",
    "Paul",
    "Paula",
    "Peggy",
    "Peter",
    "Philip",
    "Rachel",
    "Ralph",
    "Randy",
    "Raymond",
    "Rebecca",
    "Regina",
    "Richard",
    "Robert",
    "Robin",
    "Roger",
    "Ronald",
    "Rose",
    "Roy",
    "Russell",
    "Ruth",
    "Ryan",
    "Samantha",
    "Samuel",
    "Sandra",
    "Sara",
    "Sarah",
    "Scott",
    "Sean",
    "Sharon",
    "Shawn",
    "Sheila",
    "Shirley",
    "Sophia",
    "Stephanie",
    "Stephen",
    "Steve",
    "Steven",
    "Susan",
    "Tammy",
    "Teresa",
    "Terry",
    "Theresa",
    "Thomas",
    "Tiffany",
    "Timothy",
    "Tina",
    "Todd",
    "Tom",
    "Tony",
    "Tracy",
    "Travis",
    "Tyler",
    "Valerie",
    "Vanessa",
    "Victor",
    "Victoria",
    "Vincent",
    "Virginia",
    "Walter",
    "Wanda",
    "Wayne",
    "Wendy",
    "William",
    "Willie",
    "Zachary",
];

/// Common surnames for anonymization.
const SURNAMES: &[&str] = &[
    "Adams",
    "Allen",
    "Anderson",
    "Bailey",
    "Baker",
    "Barnes",
    "Bell",
    "Bennett",
    "Brooks",
    "Brown",
    "Bryant",
    "Butler",
    "Campbell",
    "Carter",
    "Clark",
    "Coleman",
    "Collins",
    "Cook",
    "Cooper",
    "Cox",
    "Cruz",
    "Davis",
    "Diaz",
    "Edwards",
    "Evans",
    "Fisher",
    "Flores",
    "Ford",
    "Foster",
    "Garcia",
    "Gibson",
    "Gomez",
    "Gonzalez",
    "Gordon",
    "Graham",
    "Gray",
    "Green",
    "Griffin",
    "Hall",
    "Hamilton",
    "Harris",
    "Harrison",
    "Hayes",
    "Henderson",
    "Hernandez",
    "Hill",
    "Holmes",
    "Howard",
    "Hughes",
    "Hunt",
    "Jackson",
    "James",
    "Jenkins",
    "Johnson",
    "Jones",
    "Jordan",
    "Kelly",
    "Kennedy",
    "Kim",
    "King",
    "Lee",
    "Lewis",
    "Long",
    "Lopez",
    "Marshall",
    "Martin",
    "Martinez",
    "Mason",
    "Matthews",
    "Mcdonald",
    "Miller",
    "Mitchell",
    "Moore",
    "Morales",
    "Morgan",
    "Morris",
    "Murphy",
    "Murray",
    "Nelson",
    "Nguyen",
    "Ortiz",
    "Owens",
    "Parker",
    "Patterson",
    "Perez",
    "Perry",
    "Peterson",
    "Phillips",
    "Powell",
    "Price",
    "Ramirez",
    "Reed",
    "Reyes",
    "Reynolds",
    "Richardson",
    "Rivera",
    "Roberts",
    "Robinson",
    "Rodriguez",
    "Rogers",
    "Ross",
    "Russell",
    "Sanchez",
    "Sanders",
    "Scott",
    "Simmons",
    "Smith",
    "Stewart",
    "Sullivan",
    "Taylor",
    "Thomas",
    "Thompson",
    "Torres",
    "Turner",
    "Walker",
    "Wallace",
    "Ward",
    "Washington",
    "Watson",
    "West",
    "White",
    "Williams",
    "Wilson",
    "Wood",
    "Wright",
    "Young",
];

struct Anonymizer {
    key: String,
    explicit_maps: HashMap<String, String>,
    generated_maps: HashMap<String, String>,
    used_names: std::collections::HashSet<String>,
}

impl Anonymizer {
    fn new(key: &str, explicit_map_str: &str) -> Self {
        let mut explicit_maps = HashMap::new();
        let mut used_names = std::collections::HashSet::new();

        for mapping in explicit_map_str.split(',') {
            let mapping = mapping.trim();
            if mapping.is_empty() {
                continue;
            }
            if let Some((old, new)) = mapping.split_once('=') {
                let old = old.trim().to_lowercase();
                let new = new.trim().to_string();
                used_names.insert(new.clone());
                explicit_maps.insert(old, new);
            }
        }

        Anonymizer {
            key: key.to_string(),
            explicit_maps,
            generated_maps: HashMap::new(),
            used_names,
        }
    }

    fn anonymize(&mut self, username: &str) -> String {
        let username_lower = username.to_lowercase();

        if let Some(mapped) = self.explicit_maps.get(&username_lower) {
            return mapped.clone();
        }

        if let Some(mapped) = self.generated_maps.get(&username_lower) {
            return mapped.clone();
        }

        let new_name = self.generate_name(&username_lower);
        self.generated_maps.insert(username_lower, new_name.clone());
        new_name
    }

    fn generate_name(&mut self, username: &str) -> String {
        let combined = format!("{}:{}", self.key, username);
        let hash = self.simple_hash(&combined);

        let first_idx = (hash % FIRST_NAMES.len() as u64) as usize;
        let surname_idx = ((hash / FIRST_NAMES.len() as u64) % SURNAMES.len() as u64) as usize;

        let mut candidate = format!("{}_{}", FIRST_NAMES[first_idx], SURNAMES[surname_idx]);

        let mut suffix = 2;
        while self.used_names.contains(&candidate) {
            candidate = format!(
                "{}_{}_{suffix}",
                FIRST_NAMES[first_idx], SURNAMES[surname_idx]
            );
            suffix += 1;
        }

        self.used_names.insert(candidate.clone());
        candidate
    }

    fn simple_hash(&self, s: &str) -> u64 {
        let mut hash: u64 = 0xcbf29ce484222325;
        for byte in s.bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }
}

/// Anonymize player names embedded in a BBO LIN URL.
fn anonymize_lin_url(url: &str, anonymizer: &mut Anonymizer) -> String {
    use regex::Regex;

    lazy_static::lazy_static! {
        static ref PN_ENCODED: Regex = Regex::new(r"(?i)pn%7C([^%]+(?:%2C[^%]+)*)%7C").unwrap();
        static ref PN_LITERAL: Regex = Regex::new(r"pn\|([^|]+)\|").unwrap();
    }

    let result = PN_ENCODED.replace(url, |caps: &regex::Captures| {
        let names_str = &caps[1];
        let anon_names: Vec<String> = names_str
            .split("%2C")
            .map(|name| {
                let name = name.trim();
                if name.is_empty() {
                    String::new()
                } else {
                    anonymizer.anonymize(name)
                }
            })
            .collect();
        format!("pn%7C{}%7C", anon_names.join("%2C"))
    });

    let result = PN_LITERAL.replace(&result, |caps: &regex::Captures| {
        let names = &caps[1];
        let anon_names: Vec<String> = names
            .split(',')
            .map(|name| {
                let name = name.trim();
                if name.is_empty() {
                    String::new()
                } else {
                    anonymizer.anonymize(name)
                }
            })
            .collect();
        format!("pn|{}|", anon_names.join(","))
    });

    result.to_string()
}

// ============================================================================
// Analyze DD
// ============================================================================

/// Configuration for the DD analysis operation.
pub struct AnalyzeDdConfig {
    /// Input CSV path (must have Cardplay column and deal columns)
    pub input: PathBuf,
    /// Output CSV path
    pub output: PathBuf,
    /// Number of parallel threads (None = number of CPU cores)
    pub threads: Option<usize>,
    /// Resume from previous run (skip rows with existing DD analysis)
    pub resume: bool,
    /// Save progress every N rows
    pub checkpoint_interval: usize,
}

/// Progress information for the DD analysis operation.
pub struct DdProgress {
    /// Number of rows completed so far
    pub completed: usize,
    /// Total number of rows to process
    pub total: usize,
    /// Number of errors encountered
    pub errors: usize,
    /// Number of rows skipped (already processed in resume mode)
    pub skipped: usize,
}

/// Represents a row to be processed for DD analysis.
#[derive(Clone)]
struct DdWorkItem {
    row_idx: usize,
    #[allow(dead_code)]
    ref_id: String,
    deal_pbn: String,
    cardplay: String,
    contract: String,
    declarer: String,
    max_dd: Option<i8>,
}

/// Result stored for each processed row.
struct DdResultEntry {
    analysis: String,
    computed_dd: Option<u8>,
    input_max_dd: Option<i8>,
    ol_error: u8,
    plays_n: u8,
    plays_s: u8,
    plays_e: u8,
    plays_w: u8,
    errors_n: u8,
    errors_s: u8,
    errors_e: u8,
    errors_w: u8,
}

/// Result from DD analysis including validation info.
struct DdAnalysisOutput {
    analysis: String,
    initial_dd: u8,
    ol_error: u8,
    plays_n: u8,
    plays_s: u8,
    plays_e: u8,
    plays_w: u8,
    errors_n: u8,
    errors_s: u8,
    errors_e: u8,
    errors_w: u8,
}

/// Column indices for required fields in the CSV.
struct DdColumnIndices {
    ref_col: usize,
    cardplay_col: usize,
    contract_col: usize,
    declarer_col: usize,
    max_dd_col: Option<usize>,
    dec_hand_col: usize,
    dummy_hand_col: usize,
    leader_hand_col: usize,
    third_hand_col: usize,
}

/// Data extracted from a row for DD analysis.
struct DdRowData {
    deal_pbn: String,
    contract: String,
    declarer: String,
}

/// Run double-dummy analysis on a CSV of cardplay data.
///
/// Calls `on_progress` periodically (~10 times/second). Return `false` to cancel.
pub fn analyze_dd(
    config: &AnalyzeDdConfig,
    on_progress: impl FnMut(&DdProgress) -> bool + Send,
) -> Result<String> {
    // Configure thread pool
    if let Some(n) = config.threads {
        rayon::ThreadPoolBuilder::new()
            .num_threads(n)
            .build_global()
            .ok();
    }

    // Read input CSV with flexible field count
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_path(&config.input)
        .context("Failed to open input CSV")?;
    let headers = reader.headers()?.clone();

    let col_indices = dd_find_required_columns(&headers)?;
    let dd_col_exists = headers.iter().any(|h| h == "DD_Analysis");

    // Load existing results if resuming
    let existing_refs: HashSet<String> = if config.resume && config.output.exists() {
        dd_load_existing_refs(&config.output, "DD_Analysis")?
    } else {
        HashSet::new()
    };

    // Prepare output headers
    let mut output_headers = headers.clone();
    if !dd_col_exists {
        output_headers.push_field("Contract_DD");
        output_headers.push_field("DD_Match");
        output_headers.push_field("DD_OL_Error");
        output_headers.push_field("DD_N_Plays");
        output_headers.push_field("DD_S_Plays");
        output_headers.push_field("DD_E_Plays");
        output_headers.push_field("DD_W_Plays");
        output_headers.push_field("DD_N_Errors");
        output_headers.push_field("DD_S_Errors");
        output_headers.push_field("DD_E_Errors");
        output_headers.push_field("DD_W_Errors");
        output_headers.push_field("DD_Analysis");
    }

    // Collect all rows and prepare work items
    let mut all_records: Vec<StringRecord> = Vec::new();
    let mut work_items: Vec<DdWorkItem> = Vec::new();
    let mut skipped_incomplete = 0usize;
    let mut skipped_passout = 0usize;
    let mut skipped_resume = 0usize;

    for (row_idx, result) in reader.records().enumerate() {
        let record = result.context("Failed to read CSV row")?;
        all_records.push(record.clone());

        let ref_id = record.get(col_indices.ref_col).unwrap_or("").to_string();

        if config.resume && existing_refs.contains(&ref_id) {
            skipped_resume += 1;
            continue;
        }

        let max_dd: Option<i8> = col_indices
            .max_dd_col
            .and_then(|col| record.get(col))
            .and_then(|s| s.parse::<i8>().ok());

        if max_dd == Some(-1) {
            skipped_incomplete += 1;
            continue;
        }

        let cardplay = record
            .get(col_indices.cardplay_col)
            .unwrap_or("")
            .to_string();

        if cardplay.is_empty() || cardplay.starts_with("ERROR:") {
            continue;
        }

        if let Some(row_data) = dd_extract_row_data(&record, &col_indices) {
            let contract_upper = row_data.contract.to_uppercase();
            if contract_upper.starts_with('0') || contract_upper == "P" || contract_upper == "PASS"
            {
                skipped_passout += 1;
                continue;
            }

            work_items.push(DdWorkItem {
                row_idx,
                ref_id,
                deal_pbn: row_data.deal_pbn,
                cardplay,
                contract: row_data.contract,
                declarer: row_data.declarer,
                max_dd,
            });
        }
    }

    let total_rows = all_records.len();
    let to_process = work_items.len();
    let skipped_no_work = total_rows - to_process - skipped_resume;

    if to_process == 0 {
        return Ok(format!(
            "Nothing to process ({} rows, {} already done, {} incomplete, {} passout)",
            total_rows, skipped_resume, skipped_incomplete, skipped_passout
        ));
    }

    // Shared atomics for progress
    let processed_count = AtomicUsize::new(0);
    let error_count = AtomicUsize::new(0);
    let cancelled = AtomicBool::new(false);
    let done = AtomicBool::new(false);

    // Results map
    let results: Mutex<HashMap<usize, DdResultEntry>> = Mutex::new(HashMap::new());

    // Run monitor thread + parallel processing within a scope.
    // std::thread::scope automatically joins all spawned threads on exit.
    std::thread::scope(|s| {
        let processed_ref = &processed_count;
        let error_ref = &error_count;
        let cancelled_ref = &cancelled;
        let done_ref = &done;

        // skipped_all = rows not needing analysis (resume + incomplete/passout/etc)
        let skipped_all = skipped_no_work + skipped_resume;

        s.spawn(move || {
            let mut on_progress = on_progress;
            loop {
                std::thread::sleep(std::time::Duration::from_millis(100));
                let completed = processed_ref.load(Ordering::Relaxed);
                let errors = error_ref.load(Ordering::Relaxed);
                let progress = DdProgress {
                    completed: completed + skipped_all,
                    total: total_rows,
                    errors,
                    skipped: skipped_resume,
                };
                if !on_progress(&progress) {
                    cancelled_ref.store(true, Ordering::Relaxed);
                }
                if done_ref.load(Ordering::Relaxed) {
                    // Send final progress update
                    let completed = processed_ref.load(Ordering::Relaxed);
                    let errors = error_ref.load(Ordering::Relaxed);
                    let _ = on_progress(&DdProgress {
                        completed: completed + skipped_all,
                        total: total_rows,
                        errors,
                        skipped: skipped_resume,
                    });
                    break;
                }
            }
        });

        // Process work items in parallel.
        // Wrap each call in catch_unwind so bridge-solver panics don't kill
        // rayon threads and stall the entire analysis.
        work_items.par_iter().for_each(|item| {
            if cancelled.load(Ordering::Relaxed) {
                return;
            }

            let entry = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                dd_compute_analysis(item)
            })) {
                Ok(Ok(output)) => DdResultEntry {
                    analysis: output.analysis,
                    computed_dd: Some(output.initial_dd),
                    input_max_dd: item.max_dd,
                    ol_error: output.ol_error,
                    plays_n: output.plays_n,
                    plays_s: output.plays_s,
                    plays_e: output.plays_e,
                    plays_w: output.plays_w,
                    errors_n: output.errors_n,
                    errors_s: output.errors_s,
                    errors_e: output.errors_e,
                    errors_w: output.errors_w,
                },
                Ok(Err(e)) => {
                    error_count.fetch_add(1, Ordering::Relaxed);
                    log::warn!("Row {}: DD analysis error: {}", item.row_idx + 1, e);
                    DdResultEntry {
                        analysis: format!("ERROR: {}", e),
                        computed_dd: None,
                        input_max_dd: item.max_dd,
                        ol_error: 0,
                        plays_n: 0,
                        plays_s: 0,
                        plays_e: 0,
                        plays_w: 0,
                        errors_n: 0,
                        errors_s: 0,
                        errors_e: 0,
                        errors_w: 0,
                    }
                }
                Err(panic_info) => {
                    error_count.fetch_add(1, Ordering::Relaxed);
                    let msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                        (*s).to_string()
                    } else if let Some(s) = panic_info.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "unknown panic".to_string()
                    };
                    log::warn!("Row {}: DD solver panic: {}", item.row_idx + 1, msg);
                    DdResultEntry {
                        analysis: format!("PANIC: {}", msg),
                        computed_dd: None,
                        input_max_dd: item.max_dd,
                        ol_error: 0,
                        plays_n: 0,
                        plays_s: 0,
                        plays_e: 0,
                        plays_w: 0,
                        errors_n: 0,
                        errors_s: 0,
                        errors_e: 0,
                        errors_w: 0,
                    }
                }
            };

            results.lock().unwrap().insert(item.row_idx, entry);
            processed_count.fetch_add(1, Ordering::Relaxed);
        });

        done.store(true, Ordering::Relaxed);
    });

    let was_cancelled = cancelled.load(Ordering::Relaxed);

    // Write output CSV
    let results_map = results.into_inner().unwrap();
    let mut writer = Writer::from_path(&config.output).context("Failed to create output CSV")?;
    writer.write_record(&output_headers)?;

    let mut dd_matches = 0usize;
    let mut dd_mismatches: Vec<(usize, u8, i8)> = Vec::new();

    for (row_idx, record) in all_records.iter().enumerate() {
        let mut output_record = record.clone();

        if !dd_col_exists {
            if let Some(entry) = results_map.get(&row_idx) {
                output_record
                    .push_field(&entry.computed_dd.map(|d| d.to_string()).unwrap_or_default());
                let dd_match = match (entry.computed_dd, entry.input_max_dd) {
                    (Some(computed), Some(input)) if input >= 0 => {
                        if computed as i8 == input {
                            "true"
                        } else {
                            "false"
                        }
                    }
                    _ => "",
                };
                output_record.push_field(dd_match);
                output_record.push_field(&entry.ol_error.to_string());
                output_record.push_field(&entry.plays_n.to_string());
                output_record.push_field(&entry.plays_s.to_string());
                output_record.push_field(&entry.plays_e.to_string());
                output_record.push_field(&entry.plays_w.to_string());
                output_record.push_field(&entry.errors_n.to_string());
                output_record.push_field(&entry.errors_s.to_string());
                output_record.push_field(&entry.errors_e.to_string());
                output_record.push_field(&entry.errors_w.to_string());
                output_record.push_field(&entry.analysis);
            } else {
                for _ in 0..12 {
                    output_record.push_field("");
                }
            }
        }

        if let Some(entry) = results_map.get(&row_idx) {
            if let (Some(computed), Some(input_dd)) = (entry.computed_dd, entry.input_max_dd) {
                if input_dd >= 0 {
                    if computed as i8 == input_dd {
                        dd_matches += 1;
                    } else {
                        dd_mismatches.push((row_idx + 2, computed, input_dd));
                    }
                }
            }
        }

        writer.write_record(&output_record)?;

        if (row_idx + 1) % config.checkpoint_interval == 0 {
            writer.flush()?;
        }
    }

    writer.flush()?;

    // Build summary
    let errors = error_count.load(Ordering::Relaxed);
    let processed = processed_count.load(Ordering::Relaxed);
    let mut summary = if was_cancelled {
        format!(
            "Cancelled after {} of {} rows ({} errors)",
            processed, to_process, errors
        )
    } else {
        format!("Analyzed {} rows ({} errors)", to_process, errors)
    };

    if dd_matches > 0 || !dd_mismatches.is_empty() {
        write!(
            summary,
            "\nDD Validation: {} matches, {} mismatches",
            dd_matches,
            dd_mismatches.len()
        )
        .ok();
        for (row, computed, input) in dd_mismatches.iter().take(20) {
            write!(
                summary,
                "\n  Row {}: computed={}, input={}",
                row, computed, input
            )
            .ok();
        }
        if dd_mismatches.len() > 20 {
            write!(summary, "\n  ... and {} more", dd_mismatches.len() - 20).ok();
        }
    }

    Ok(summary)
}

/// Load existing refs from an output CSV for resume mode.
fn dd_load_existing_refs(output: &Path, column: &str) -> Result<HashSet<String>> {
    let mut refs = HashSet::new();
    let mut reader = ReaderBuilder::new().flexible(true).from_path(output)?;

    let headers = reader.headers()?.clone();
    let ref_idx = headers.iter().position(|h| h == "Ref #");
    let col_idx = headers.iter().position(|h| h == column);

    if ref_idx.is_none() || col_idx.is_none() {
        return Ok(refs);
    }

    let ref_idx = ref_idx.unwrap();
    let col_idx = col_idx.unwrap();

    for result in reader.records() {
        let record = result?;
        let ref_id = record.get(ref_idx).unwrap_or("");
        let value = record.get(col_idx).unwrap_or("");

        if !value.is_empty() && !value.starts_with("ERROR:") {
            refs.insert(ref_id.to_string());
        }
    }

    Ok(refs)
}

/// Find required columns in CSV headers for DD analysis.
fn dd_find_required_columns(headers: &StringRecord) -> Result<DdColumnIndices> {
    let find = |name: &str| -> Result<usize> {
        headers
            .iter()
            .position(|h| h == name)
            .ok_or_else(|| anyhow::anyhow!("Required column '{}' not found", name))
    };

    let find_optional = |name: &str| -> Option<usize> { headers.iter().position(|h| h == name) };

    Ok(DdColumnIndices {
        ref_col: find("Ref #")?,
        cardplay_col: find("Cardplay")?,
        contract_col: find("Con")?,
        declarer_col: find("Dec")?,
        max_dd_col: find_optional("Max DD"),
        dec_hand_col: find("Dec Hand")?,
        dummy_hand_col: find("Dummy Hand")?,
        leader_hand_col: find("Leader Hand")?,
        third_hand_col: find("Third Hand")?,
    })
}

/// Extract deal, contract, and declarer from a CSV row.
///
/// Maps role-based hand columns (Dec Hand, Dummy Hand, Leader Hand, Third Hand)
/// to compass positions using the Dec column.
fn dd_extract_row_data(record: &StringRecord, cols: &DdColumnIndices) -> Option<DdRowData> {
    let contract = record.get(cols.contract_col)?.to_string();
    let declarer = record.get(cols.declarer_col)?.to_string();

    if contract.is_empty() || declarer.is_empty() {
        return None;
    }

    let dec_hand = record.get(cols.dec_hand_col).unwrap_or("");
    let dummy_hand = record.get(cols.dummy_hand_col).unwrap_or("");
    let leader_hand = record.get(cols.leader_hand_col).unwrap_or("");
    let third_hand = record.get(cols.third_hand_col).unwrap_or("");

    if dec_hand.is_empty() || dummy_hand.is_empty() {
        return None;
    }

    // Map role-based hands to compass positions.
    // Leader = LHO (next clockwise from declarer), Third = RHO (leader's partner).
    //   Dec=N: N=Dec, E=Leader, S=Dummy, W=Third
    //   Dec=E: N=Third, E=Dec, S=Leader, W=Dummy
    //   Dec=S: N=Dummy, E=Third, S=Dec, W=Leader
    //   Dec=W: N=Leader, E=Dummy, S=Third, W=Dec
    let (north, east, south, west) = match declarer.to_uppercase().as_str() {
        "N" => (dec_hand, leader_hand, dummy_hand, third_hand),
        "E" => (third_hand, dec_hand, leader_hand, dummy_hand),
        "S" => (dummy_hand, third_hand, dec_hand, leader_hand),
        "W" => (leader_hand, dummy_hand, third_hand, dec_hand),
        _ => return None,
    };

    // Convert BBO hand format (S-K8543 H-873 D-Q75 C-K3) to PBN (K8543.873.Q75.K3)
    let n = bbo_hand_to_pbn(north)?;
    let e = bbo_hand_to_pbn(east)?;
    let s = bbo_hand_to_pbn(south)?;
    let w = bbo_hand_to_pbn(west)?;

    let deal_pbn = format!("N:{} {} {} {}", n, e, s, w);

    Some(DdRowData {
        deal_pbn,
        contract,
        declarer,
    })
}

/// Convert a BBO-format hand (`S-K8543 H-873 D-Q75 C-K3`) to PBN (`K8543.873.Q75.K3`).
///
/// Suits must appear in S, H, D, C order. Returns None if the format is unexpected.
fn bbo_hand_to_pbn(hand: &str) -> Option<String> {
    let mut suits: Vec<&str> = Vec::with_capacity(4);
    for part in hand.split_whitespace() {
        if let Some(cards) = part.get(2..) {
            suits.push(cards);
        } else {
            return None;
        }
    }
    if suits.len() == 4 {
        Some(suits.join("."))
    } else {
        None
    }
}

/// Compute DD analysis for a single work item.
fn dd_compute_analysis(item: &DdWorkItem) -> Result<DdAnalysisOutput> {
    use crate::dd_analysis::compute_dd_costs;

    let result = compute_dd_costs(
        &item.deal_pbn,
        &item.cardplay,
        &item.contract,
        &item.declarer,
        false,
    )
    .map_err(|e| anyhow::anyhow!("{}", e))?;

    if result.costs.is_empty() {
        return Ok(DdAnalysisOutput {
            analysis: String::new(),
            initial_dd: result.initial_dd,
            ol_error: 0,
            plays_n: 0,
            plays_s: 0,
            plays_e: 0,
            plays_w: 0,
            errors_n: 0,
            errors_s: 0,
            errors_e: 0,
            errors_w: 0,
        });
    }

    let mut plays = [0u8; 4];
    let mut errors = [0u8; 4];

    let ol_error = if !result.costs[0].is_empty() && result.costs[0][0] > 0 {
        1
    } else {
        0
    };

    let tricks: Vec<Vec<&str>> = item
        .cardplay
        .split('|')
        .filter(|s| !s.is_empty())
        .map(|t| t.split_whitespace().collect())
        .collect();

    let initial_leader = (result.declarer_seat + 1) % 4;
    let mut current_leader = initial_leader;

    for (trick_idx, card_costs) in result.costs.iter().enumerate() {
        let mut seat = current_leader;

        for &cost in card_costs.iter() {
            plays[seat] += 1;
            if cost > 0 {
                errors[seat] += 1;
            }
            seat = (seat + 1) % 4;
        }

        if trick_idx < tricks.len() && tricks[trick_idx].len() == 4 {
            let trump = dd_parse_trump_for_winner(&item.contract);
            if let Some(winner) =
                dd_determine_trick_winner(&tricks[trick_idx], trump, current_leader)
            {
                current_leader = winner;
            }
        }
    }

    let trick_results: Vec<String> = result
        .costs
        .iter()
        .enumerate()
        .map(|(trick_num, card_costs)| {
            let costs_str = card_costs
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>()
                .join(",");
            format!("T{}:{}", trick_num + 1, costs_str)
        })
        .collect();

    Ok(DdAnalysisOutput {
        analysis: trick_results.join("|"),
        initial_dd: result.initial_dd,
        ol_error,
        plays_n: plays[NORTH],
        plays_s: plays[SOUTH],
        plays_e: plays[EAST],
        plays_w: plays[WEST],
        errors_n: errors[NORTH],
        errors_s: errors[SOUTH],
        errors_e: errors[EAST],
        errors_w: errors[WEST],
    })
}

/// Parse trump suit from contract for trick winner determination.
fn dd_parse_trump_for_winner(contract: &str) -> Option<usize> {
    let contract = contract.trim().to_uppercase();
    if contract.contains("NT") {
        return None;
    }
    for c in contract.chars() {
        match c {
            'S' => return Some(SPADE),
            'H' => return Some(HEART),
            'D' => return Some(DIAMOND),
            'C' => return Some(CLUB),
            _ => continue,
        }
    }
    None
}

/// Determine trick winner from card strings.
fn dd_determine_trick_winner(cards: &[&str], trump: Option<usize>, leader: usize) -> Option<usize> {
    if cards.len() != 4 {
        return None;
    }

    let parsed: Vec<Option<(usize, u8)>> = cards
        .iter()
        .map(|s| {
            if s.len() < 2 {
                return None;
            }
            let suit = match s.chars().next()? {
                'S' | 's' => SPADE,
                'H' | 'h' => HEART,
                'D' | 'd' => DIAMOND,
                'C' | 'c' => CLUB,
                _ => return None,
            };
            let rank_char = s.chars().nth(1)?;
            let rank = match rank_char {
                'A' | 'a' => 14,
                'K' | 'k' => 13,
                'Q' | 'q' => 12,
                'J' | 'j' => 11,
                'T' | 't' | '1' => 10,
                '9' => 9,
                '8' => 8,
                '7' => 7,
                '6' => 6,
                '5' => 5,
                '4' => 4,
                '3' => 3,
                '2' => 2,
                _ => return None,
            };
            Some((suit, rank))
        })
        .collect();

    let cards_parsed: Vec<(usize, u8)> = parsed.into_iter().collect::<Option<Vec<_>>>()?;

    let led_suit = cards_parsed[0].0;
    let mut winner_idx = 0;
    let mut winner_card = cards_parsed[0];

    for (i, &(suit, rank)) in cards_parsed.iter().enumerate().skip(1) {
        let dominated = if let Some(trump_suit) = trump {
            if suit == trump_suit && winner_card.0 != trump_suit {
                true
            } else if suit == trump_suit && winner_card.0 == trump_suit {
                rank > winner_card.1
            } else if winner_card.0 == trump_suit {
                false
            } else if suit == led_suit && winner_card.0 == led_suit {
                rank > winner_card.1
            } else {
                suit == led_suit
            }
        } else if suit == led_suit && winner_card.0 == led_suit {
            rank > winner_card.1
        } else {
            suit == led_suit
        };

        if dominated {
            winner_idx = i;
            winner_card = (suit, rank);
        }
    }

    Some((leader + winner_idx) % 4)
}

// ============================================================================
// Case Folder Scanning
// ============================================================================

/// Results of scanning a case folder for EDGAR report files.
#[derive(Debug, Clone, Default)]
pub struct CaseFiles {
    /// BBO hand records CSV file
    pub csv_file: Option<PathBuf>,
    /// Concise EDGAR report text file
    pub concise_file: Option<PathBuf>,
    /// Hotspot EDGAR report text file
    pub hotspot_file: Option<PathBuf>,
}

/// Anonymized versions of case files found in the EDGAR Defense folder.
#[derive(Debug, Clone)]
pub struct AnonCaseFiles {
    /// Anonymized CSV file
    pub csv_file: PathBuf,
    /// Anonymized concise report
    pub concise_file: PathBuf,
    /// Anonymized hotspot report
    pub hotspot_file: PathBuf,
}

/// Recursively scan a folder for EDGAR case files (CSV, Concise report, Hotspot report).
pub fn scan_case_folder(folder: &Path) -> CaseFiles {
    let mut result = CaseFiles::default();
    scan_dir_recursive(folder, &mut result);
    result
}

/// Recursive directory walker for case file detection.
fn scan_dir_recursive(dir: &Path, result: &mut CaseFiles) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            // Skip EDGAR Defense output folder to avoid picking up generated files
            let dir_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if dir_name == "EDGAR Defense" {
                continue;
            }
            scan_dir_recursive(&path, result);
        } else if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            let lower = name.to_lowercase();
            if lower.ends_with(".csv") && result.csv_file.is_none() {
                result.csv_file = Some(path);
            } else if lower.ends_with(".txt")
                && lower.contains("concise")
                && result.concise_file.is_none()
            {
                result.concise_file = Some(path);
            } else if lower.ends_with(".txt")
                && lower.contains("hotspot")
                && result.hotspot_file.is_none()
            {
                result.hotspot_file = Some(path);
            }
        }
    }
}

/// Parse the Concise EDGAR Report to extract subject player BBO usernames.
///
/// Reads lines between the header row ("Name  Detector ...") and the separator
/// ("---..."). The first whitespace-delimited token on each data line is the
/// username. "pair" is skipped. Returns unique names in order of first appearance.
pub fn parse_concise_usernames(path: &Path) -> Vec<String> {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };

    let mut in_data = false;
    let mut seen = std::collections::HashSet::new();
    let mut names = Vec::new();

    for line in content.lines() {
        let trimmed = line.trim();

        // Detect the header row to know data follows
        if trimmed.starts_with("Name") && trimmed.contains("Detector") {
            in_data = true;
            continue;
        }

        // Stop at separator line
        if in_data && trimmed.starts_with("---") {
            break;
        }

        if in_data {
            if let Some(name) = trimmed.split_whitespace().next() {
                if name != "pair" && !name.is_empty() && seen.insert(name.to_string()) {
                    names.push(name.to_string());
                }
            }
        }
    }

    names
}

/// Extract the subject name from a Concise report filename.
///
/// Given a filename like "Concise AWilliams.txt", returns "AWilliams".
/// Strips a leading "Concise" (case-insensitive) prefix and the file extension.
pub fn extract_concise_subject(path: &Path) -> Option<String> {
    let stem = path.file_stem()?.to_str()?;
    let name = if let Some(rest) = stem.strip_prefix("Concise ") {
        rest.trim()
    } else if let Some(rest) = stem.strip_prefix("concise ") {
        rest.trim()
    } else {
        stem.trim()
    };
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

/// Look for anonymized versions of case files in the EDGAR Defense folder.
///
/// Constructs exact filenames from the subject name and deal limit:
/// - Anon CSV: `{subject} [N] DD anon.csv`
/// - Anon concise: `{concise_stem} anon.txt`
/// - Anon hotspot: `{hotspot_stem} anon.txt`
///
/// Returns `None` if any required file does not exist.
pub fn find_anon_files(
    edgar_dir: &Path,
    case_files: &CaseFiles,
    deal_limit: Option<usize>,
) -> Option<AnonCaseFiles> {
    // Find anon concise: {concise_stem} anon.txt
    let anon_concise = case_files.concise_file.as_ref().and_then(|p| {
        let stem = p.file_stem()?.to_str()?;
        let anon_path = edgar_dir.join(format!("{} anon.txt", stem));
        if anon_path.exists() {
            Some(anon_path)
        } else {
            None
        }
    })?;

    // Find anon hotspot: {hotspot_stem} anon.txt
    let anon_hotspot = case_files.hotspot_file.as_ref().and_then(|p| {
        let stem = p.file_stem()?.to_str()?;
        let anon_path = edgar_dir.join(format!("{} anon.txt", stem));
        if anon_path.exists() {
            Some(anon_path)
        } else {
            None
        }
    })?;

    // Construct anon CSV filename: "{subject} [N] DD anon.csv"
    let subject = case_files
        .concise_file
        .as_deref()
        .and_then(extract_concise_subject)?;
    let anon_csv_name = match deal_limit {
        Some(n) => format!("{} {} DD anon.csv", subject, n),
        None => format!("{} DD anon.csv", subject),
    };
    let anon_csv = edgar_dir.join(&anon_csv_name);
    if !anon_csv.exists() {
        return None;
    }

    Some(AnonCaseFiles {
        csv_file: anon_csv,
        concise_file: anon_concise,
        hotspot_file: anon_hotspot,
    })
}

// ============================================================================
// Package Workbook
// ============================================================================

/// A single parsed hotspot entry from the hotspot report.
#[derive(Debug, Clone)]
pub struct HotspotEntry {
    /// Category name (e.g. "PassedForce", "Suit_Overeasy")
    pub category: String,
    /// Sub-index within the category (1-based)
    pub subindex: u32,
    /// "Hit" or "Miss"
    pub hit_miss: String,
    /// Contract (e.g. "2N", "5D")
    pub contract: String,
    /// Opening lead card (e.g. "S6", "H7")
    pub lead: String,
    /// TinyURL to BBO hand viewer (or LIN_URL for anon format)
    pub tinyurl: String,
    /// Subject player BBO username
    pub subject_player: String,
    /// Board ID (only set for anon format hotspot reports)
    pub board_id: Option<String>,
    /// Anonymized LIN URL (only set for anon format hotspot reports)
    pub lin_url: Option<String>,
}

/// Parse a hotspot report text file into a vector of HotspotEntry.
///
/// Handles both original format (tinyurl + player) and anon format
/// (board_id + player + LIN_URL).
pub fn parse_hotspot_report(path: &Path) -> Result<Vec<HotspotEntry>> {
    use regex::Regex;

    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read hotspot report: {}", path.display()))?;

    // Anon format: ... date board_id player lin_url
    //  1. PassedForce Hit   Contract: 2N   Lead: S6   2021-10-21 42 Bob https://www.bridgebase.com/...
    let anon_re = Regex::new(
        r"^\s*(\d+)\.\s+(\S+)\s+(Hit|Miss)\s+\S*\s*Contract:\s+(\S+)\s+Lead:\s+(\S+)\s+\S+\s+(\d+)\s+(\S+)\s+(https?://\S+)",
    )?;

    // Original format: ... date tinyurl player
    //  1. PassedForce Hit   Contract: 2N   Lead: S6   2021-10-21 http://tinyurl.com/abc player
    let orig_re = Regex::new(
        r"^\s*(\d+)\.\s+(\S+)\s+(Hit|Miss)\s+\S*\s*Contract:\s+(\S+)\s+Lead:\s+(\S+)\s+\S+\s+(https?://\S+)\s+(\S+)",
    )?;

    let mut entries = Vec::new();

    for line in content.lines() {
        if let Some(caps) = anon_re.captures(line) {
            let lin_url = caps[8].to_string();
            entries.push(HotspotEntry {
                subindex: caps[1].parse().unwrap_or(0),
                category: caps[2].to_string(),
                hit_miss: caps[3].to_string(),
                contract: caps[4].to_string(),
                lead: caps[5].to_string(),
                tinyurl: lin_url.clone(),
                subject_player: caps[7].to_string(),
                board_id: Some(caps[6].to_string()),
                lin_url: Some(lin_url),
            });
        } else if let Some(caps) = orig_re.captures(line) {
            entries.push(HotspotEntry {
                subindex: caps[1].parse().unwrap_or(0),
                category: caps[2].to_string(),
                hit_miss: caps[3].to_string(),
                contract: caps[4].to_string(),
                lead: caps[5].to_string(),
                tinyurl: caps[6].to_string(),
                subject_player: caps[7].to_string(),
                board_id: None,
                lin_url: None,
            });
        }
    }

    Ok(entries)
}

/// Normalize a tinyurl for matching between CSV and hotspot report.
///
/// Extracts the path component after `tinyurl.com/` and lowercases it.
/// Falls back to trimmed lowercase of the full URL if not a tinyurl.
pub fn normalize_tinyurl(url: &str) -> String {
    let trimmed = url.trim().trim_end_matches('/');
    let lower = trimmed.to_lowercase();
    if let Some(pos) = lower.find("tinyurl.com/") {
        lower[pos + "tinyurl.com/".len()..].to_string()
    } else {
        lower
    }
}

/// Percent-decode a URL string (`%7C` → `|`, `%2C` → `,`, etc.).
///
/// Used before passing URLs to `rust_xlsxwriter::Url::new()` to avoid
/// double-encoding (the library encodes the URL itself).
fn percent_decode_url(s: &str) -> String {
    let mut result = Vec::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = match bytes[i + 1] {
                b'0'..=b'9' => Some(bytes[i + 1] - b'0'),
                b'a'..=b'f' => Some(bytes[i + 1] - b'a' + 10),
                b'A'..=b'F' => Some(bytes[i + 1] - b'A' + 10),
                _ => None,
            };
            let lo = match bytes[i + 2] {
                b'0'..=b'9' => Some(bytes[i + 2] - b'0'),
                b'a'..=b'f' => Some(bytes[i + 2] - b'a' + 10),
                b'A'..=b'F' => Some(bytes[i + 2] - b'A' + 10),
                _ => None,
            };
            if let (Some(h), Some(l)) = (hi, lo) {
                result.push(h * 16 + l);
                i += 3;
                continue;
            }
        }
        result.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&result).to_string()
}

/// Normalize a URL to use https scheme.
fn to_https(url: &str) -> String {
    let trimmed = url.trim();
    if let Some(rest) = trimmed.strip_prefix("http://") {
        format!("https://{rest}")
    } else {
        trimmed.to_string()
    }
}

/// Convert a 0-based column index to an Excel column letter (A, B, ..., Z, AA, AB, ...).
fn col_letter(idx: u32) -> String {
    let mut result = String::new();
    let mut n = idx;
    loop {
        result.insert(0, (b'A' + (n % 26) as u8) as char);
        if n < 26 {
            break;
        }
        n = n / 26 - 1;
    }
    result
}

/// Configuration for the package workbook command.
pub struct PackageConfig {
    /// Path to the hand records CSV file
    pub csv_file: PathBuf,
    /// Path to the hotspot report text file
    pub hotspot_file: PathBuf,
    /// Path to the concise report text file
    pub concise_file: PathBuf,
    /// Output xlsx path
    pub output: PathBuf,
    /// Case folder path (for display in Summary)
    pub case_folder: String,
    /// Subject player usernames (for conditional formatting)
    pub subject_players: Vec<String>,
    /// Optional deal limit for testing (only include this many boards)
    pub deal_limit: Option<usize>,
    /// Optional path to cardplay CSV (output of fetch step)
    pub cardplay_file: Option<PathBuf>,
    /// Whether this is an anonymized package (changes link handling)
    pub is_anon: bool,
}

/// Create a packaged Excel workbook from the three EDGAR case files.
///
/// Produces a workbook with Summary, Boards, and Hotspots sheets.
/// Returns a summary string on success.
pub fn package_workbook(config: &PackageConfig) -> Result<String> {
    use rust_xlsxwriter::{
        ConditionalFormatText, ConditionalFormatTextRule, Format, FormatAlign, FormatUnderline,
        Formula, Url, Workbook,
    };

    // -- Read CSV --
    let csv_data = read_bbo_csv_fixed(&config.csv_file)?;
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_reader(csv_data.as_bytes());
    let headers = reader.headers()?.clone();
    let mut records: Vec<csv::StringRecord> = reader
        .records()
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("Failed to read CSV records")?;
    if let Some(limit) = config.deal_limit {
        records.truncate(limit);
    }

    // Find BBO column index in CSV
    let bbo_col_csv = headers
        .iter()
        .position(|h| h == "BBO")
        .ok_or_else(|| anyhow::anyhow!("'BBO' column not found in CSV"))?;

    // Find LIN_URL column index in CSV (used for anon link creation)
    let lin_url_col_csv = headers.iter().position(|h| h == "LIN_URL");

    // -- Parse hotspot report --
    let hotspot_entries = parse_hotspot_report(&config.hotspot_file)?;

    // Build key -> (hotspot_id_1based, category) for first match
    // For anon: key by board_id; for original: key by normalized tinyurl
    let mut url_to_hotspot: HashMap<String, (u32, String)> = HashMap::new();
    for (i, entry) in hotspot_entries.iter().enumerate() {
        let key = if let Some(bid) = &entry.board_id {
            bid.clone()
        } else {
            normalize_tinyurl(&entry.tinyurl)
        };
        url_to_hotspot
            .entry(key)
            .or_insert(((i + 1) as u32, entry.category.clone()));
    }

    // Collect unique category names in order of first appearance (for conditional formatting)
    let mut unique_categories: Vec<String> = Vec::new();
    for entry in &hotspot_entries {
        if !unique_categories.contains(&entry.category) {
            unique_categories.push(entry.category.clone());
        }
    }

    // Category color palette
    let category_colors = [
        "#DAEEF3", "#E2EFDA", "#FCE4D6", "#D9E2F3", "#EDEDED", "#FFF2CC", "#E4DFEC", "#F8CBAD",
        "#D6DCE4", "#C5E0B4",
    ];

    // -- Create workbook --
    let mut workbook = Workbook::new();
    let bold = Format::new().set_bold();
    let header_fmt = Format::new().set_bold();

    // Hyperlink style: blue underlined text (so HYPERLINK formulas render as links)
    let link_fmt = Format::new()
        .set_font_color("#0563C1")
        .set_underline(FormatUnderline::Single);

    // Conditional formatting fill colors
    let player1_fill = Format::new().set_background_color("#C6EFCE"); // light green
    let player2_fill = Format::new().set_background_color("#BDD7EE"); // light blue
    let hit_fill = Format::new().set_background_color("#FFC7CE"); // light pink
    let miss_fill = Format::new().set_background_color("#C6EFCE"); // light green

    // Helper: extract filename from path
    let extract_filename = |p: &Path| -> String {
        p.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("(unknown)")
            .to_string()
    };

    // Helper: get file modified time as formatted string
    let file_date = |p: &Path| -> String {
        std::fs::metadata(p)
            .and_then(|m| m.modified())
            .ok()
            .map(|t| {
                let dt: chrono::DateTime<chrono::Local> = t.into();
                dt.format("%Y-%m-%d %H:%M").to_string()
            })
            .unwrap_or_else(|| "(unknown)".to_string())
    };

    // ---------------------------------------------------------------
    // Boards sheet column layout:
    //   Board ID | Link | Hotspot ID | Hotspot Category | [CSV cols...]
    //   After the OB column, 3 extra columns are inserted:
    //     Overcaller | Responder | Advancer
    //   (BBO column within CSV columns is hidden)
    //
    // Hotspots sheet column layout:
    //   Hotspot ID | Link | Tinyurl(hidden) | Board ID | Category |
    //   Subindex | Subject Player | Hit/Miss | Contract | Lead
    // ---------------------------------------------------------------

    // Find OB and player-seat columns in CSV headers
    let ob_col_csv = headers.iter().position(|h| h == "OB name");
    let n_col_csv = headers.iter().position(|h| h == "N");
    let s_col_csv = headers.iter().position(|h| h == "S");
    let e_col_csv = headers.iter().position(|h| h == "E");
    let w_col_csv = headers.iter().position(|h| h == "W");

    // Number of extra columns inserted after OB (Overcaller, Responder, Advancer)
    let extra_cols: u16 = if ob_col_csv.is_some() { 3 } else { 0 };

    // Boards: CSV columns start at offset 4 (Board ID=0, Link=1, HsID=2, HsCat=3)
    let csv_col_offset: u16 = 4;

    // Map a CSV column index to its Boards sheet column, accounting for inserted columns
    let boards_col = |csv_idx: usize| -> u16 {
        if let Some(ob) = ob_col_csv {
            if csv_idx > ob {
                csv_col_offset + csv_idx as u16 + extra_cols
            } else {
                csv_col_offset + csv_idx as u16
            }
        } else {
            csv_col_offset + csv_idx as u16
        }
    };

    // BBO column position in Boards sheet
    let bbo_col_boards = boards_col(bbo_col_csv);
    // Total Boards columns = 4 + number of CSV headers + extra inserted columns
    let boards_last_col = csv_col_offset + headers.len() as u16 - 1 + extra_cols;

    // BBO column letter for Hotspots INDEX/MATCH formula
    let bbo_col_letter = col_letter(bbo_col_boards as u32);

    // ---------------------------------------------------------------
    // Summary sheet (first tab)
    // ---------------------------------------------------------------
    {
        let sheet = workbook.add_worksheet();
        sheet.set_name("Summary")?;

        let title_fmt = Format::new().set_bold().set_font_size(16);
        let left_fmt = Format::new().set_align(FormatAlign::Left);
        let mut row: u32 = 0;

        sheet.write_string_with_format(row, 0, "EDGAR Case Package", &title_fmt)?;
        row += 2;

        sheet.write_string_with_format(row, 0, "Case Folder", &bold)?;
        sheet.write_string_with_format(row, 1, &config.case_folder, &left_fmt)?;
        row += 2;

        // Subject players (one per row)
        sheet.write_string_with_format(row, 0, "Subject Players", &bold)?;
        if config.subject_players.is_empty() {
            sheet.write_string_with_format(row, 1, "(none)", &left_fmt)?;
            row += 1;
        } else {
            for player in &config.subject_players {
                sheet.write_string_with_format(row, 1, player, &left_fmt)?;
                row += 1;
            }
        }
        row += 1;

        let csv_name = extract_filename(&config.csv_file);
        let concise_name = extract_filename(&config.concise_file);
        let hotspot_name = extract_filename(&config.hotspot_file);

        sheet.write_string_with_format(row, 0, "Hand Records CSV", &bold)?;
        sheet.write_string_with_format(row, 1, &csv_name, &left_fmt)?;
        row += 1;
        sheet.write_string_with_format(row, 0, "Concise Report", &bold)?;
        sheet.write_string_with_format(row, 1, &concise_name, &left_fmt)?;
        row += 1;
        sheet.write_string_with_format(row, 0, "Hotspot Report", &bold)?;
        sheet.write_string_with_format(row, 1, &hotspot_name, &left_fmt)?;
        row += 2;

        sheet.write_string_with_format(row, 0, "CSV Modified", &bold)?;
        sheet.write_string_with_format(row, 1, file_date(&config.csv_file), &left_fmt)?;
        row += 1;
        sheet.write_string_with_format(row, 0, "Concise Modified", &bold)?;
        sheet.write_string_with_format(row, 1, file_date(&config.concise_file), &left_fmt)?;
        row += 1;
        sheet.write_string_with_format(row, 0, "Hotspot Modified", &bold)?;
        sheet.write_string_with_format(row, 1, file_date(&config.hotspot_file), &left_fmt)?;
        row += 1;

        let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        sheet.write_string_with_format(row, 0, "Package Date", &bold)?;
        sheet.write_string_with_format(row, 1, &now, &left_fmt)?;
        row += 2;

        // Use formulas so counts stay live if sheets are edited
        sheet.write_string_with_format(row, 0, "Number of Boards", &bold)?;
        sheet.write_formula_with_format(row, 1, Formula::new("COUNTA(Boards!A:A)-1"), &left_fmt)?;
        row += 1;
        sheet.write_string_with_format(row, 0, "Number of Hotspots", &bold)?;
        sheet.write_formula_with_format(
            row,
            1,
            Formula::new("COUNTA(Hotspots!A:A)-1"),
            &left_fmt,
        )?;
        row += 1;

        sheet.write_string_with_format(row, 0, "Hit Count", &bold)?;
        sheet.write_formula_with_format(
            row,
            1,
            Formula::new("COUNTIF(Hotspots!H:H,\"Hit\")"),
            &left_fmt,
        )?;
        row += 1;
        sheet.write_string_with_format(row, 0, "Miss Count", &bold)?;
        sheet.write_formula_with_format(
            row,
            1,
            Formula::new("COUNTIF(Hotspots!H:H,\"Miss\")"),
            &left_fmt,
        )?;
        let summary_last_row = row;

        sheet.set_column_width(0, 22)?;
        // Column B ~3 inches wide (≈28 character widths at default font)
        sheet.set_column_width(1, 28)?;

        // Conditional formatting for subject player names on Summary
        if summary_last_row > 0 {
            if let Some(p1) = config.subject_players.first() {
                let cf = ConditionalFormatText::new()
                    .set_rule(ConditionalFormatTextRule::Contains(p1.clone()))
                    .set_format(&player1_fill);
                sheet.add_conditional_format(1, 0, summary_last_row, 1, &cf)?;
            }
            if let Some(p2) = config.subject_players.get(1) {
                let cf = ConditionalFormatText::new()
                    .set_rule(ConditionalFormatTextRule::Contains(p2.clone()))
                    .set_format(&player2_fill);
                sheet.add_conditional_format(1, 0, summary_last_row, 1, &cf)?;
            }
        }
    }

    // ---------------------------------------------------------------
    // Boards sheet (second tab)
    // ---------------------------------------------------------------
    let num_board_rows = records.len() as u32;
    {
        let sheet = workbook.add_worksheet();
        sheet.set_name("Boards")?;

        // Header row: Board ID | Link | Hotspot ID | Hotspot Category | [CSV cols...]
        // with Overcaller/Responder/Advancer inserted after OB
        sheet.write_string_with_format(0, 0, "Board ID", &header_fmt)?;
        sheet.write_string_with_format(0, 1, "Link", &header_fmt)?;
        sheet.write_string_with_format(0, 2, "Hotspot ID", &header_fmt)?;
        sheet.write_string_with_format(0, 3, "Hotspot Category", &header_fmt)?;
        for (j, h) in headers.iter().enumerate() {
            sheet.write_string_with_format(0, boards_col(j), h, &header_fmt)?;
        }
        // Insert extra column headers after OB
        if let Some(ob) = ob_col_csv {
            let insert_at = csv_col_offset + ob as u16 + 1;
            sheet.write_string_with_format(0, insert_at, "Overcaller", &header_fmt)?;
            sheet.write_string_with_format(0, insert_at + 1, "Responder", &header_fmt)?;
            sheet.write_string_with_format(0, insert_at + 2, "Advancer", &header_fmt)?;
        }

        // Data rows
        for (i, record) in records.iter().enumerate() {
            let row = (i + 1) as u32;
            let excel_row = row + 1; // 1-based for formulas

            // Board ID (sequential number)
            sheet.write_number(row, 0, (i + 1) as f64)?;

            // Link: hyperlink to hand viewer
            let bbo_url = record.get(bbo_col_csv).unwrap_or("").trim();
            if config.is_anon {
                // Anon: BBO column has Board_ID, use LIN_URL for the link.
                // Use write_url (cell hyperlink) instead of HYPERLINK formula
                // because LIN URLs exceed Excel's 255-char formula URL limit.
                if let Some(lin_idx) = lin_url_col_csv {
                    let lin_url = record.get(lin_idx).unwrap_or("").trim();
                    if !lin_url.is_empty() {
                        let decoded = percent_decode_url(lin_url);
                        let url = Url::new(&decoded).set_text("link");
                        sheet.write_url_with_format(row, 1, &url, &link_fmt)?;
                    }
                }
            } else if !bbo_url.is_empty() {
                // Original: BBO column has tinyurl
                let bbo_cell = format!(
                    "{col}${row}",
                    col = col_letter(bbo_col_boards as u32),
                    row = excel_row
                );
                let link_formula = format!("HYPERLINK({cell},\"link\")", cell = bbo_cell);
                sheet.write_formula_with_format(row, 1, Formula::new(link_formula), &link_fmt)?;
            }

            // Hotspot ID and Category
            if !bbo_url.is_empty() {
                // Anon: BBO has Board_ID, match directly; Original: normalize tinyurl
                let key = if config.is_anon {
                    bbo_url.to_string()
                } else {
                    normalize_tinyurl(bbo_url)
                };
                if let Some((hs_id, hs_cat)) = url_to_hotspot.get(&key) {
                    let hs_link = format!(
                        "HYPERLINK(\"#Hotspots!A\"&MATCH({id},Hotspots!$A:$A,0),{id})",
                        id = hs_id
                    );
                    sheet.write_formula_with_format(row, 2, Formula::new(hs_link), &link_fmt)?;
                    sheet.write_string(row, 3, hs_cat)?;
                }
            }

            // CSV columns (normalize tinyurls to https, write numbers as numbers)
            for (j, field) in record.iter().enumerate() {
                let col = boards_col(j);
                let trimmed = field.trim();
                if j == bbo_col_csv && !trimmed.is_empty() {
                    sheet.write_string(row, col, to_https(field))?;
                } else if trimmed.is_empty() {
                    sheet.write_string(row, col, "")?;
                } else if let Ok(n) = trimmed.parse::<f64>() {
                    sheet.write_number(row, col, n)?;
                } else {
                    sheet.write_string(row, col, field)?;
                }
            }

            // Compute Overcaller/Responder/Advancer from OB name
            if let Some(ob) = ob_col_csv {
                let ob_name = record.get(ob).unwrap_or("").trim();
                if !ob_name.is_empty() {
                    // Find which seat the OB is sitting in
                    let get_player = |col: Option<usize>| -> &str {
                        col.and_then(|c| record.get(c))
                            .map(|s| s.trim())
                            .unwrap_or("")
                    };
                    let n_name = get_player(n_col_csv);
                    let e_name = get_player(e_col_csv);
                    let s_name = get_player(s_col_csv);
                    let w_name = get_player(w_col_csv);

                    let ob_lower = ob_name.to_lowercase();
                    // Clockwise: N -> E -> S -> W -> N
                    // Overcaller = next clockwise, Responder = partner, Advancer = opp of overcaller
                    let roles = if ob_lower == n_name.to_lowercase() {
                        Some((e_name, s_name, w_name))
                    } else if ob_lower == e_name.to_lowercase() {
                        Some((s_name, w_name, n_name))
                    } else if ob_lower == s_name.to_lowercase() {
                        Some((w_name, n_name, e_name))
                    } else if ob_lower == w_name.to_lowercase() {
                        Some((n_name, e_name, s_name))
                    } else {
                        None
                    };

                    if let Some((overcaller, responder, advancer)) = roles {
                        let insert_at = csv_col_offset + ob as u16 + 1;
                        sheet.write_string(row, insert_at, overcaller)?;
                        sheet.write_string(row, insert_at + 1, responder)?;
                        sheet.write_string(row, insert_at + 2, advancer)?;
                    }
                }
            }
        }

        // Column widths
        sheet.set_column_width(0, 10)?; // Board ID
        sheet.set_column_width(1, 8)?; // Link
        sheet.set_column_width(2, 12)?; // Hotspot ID
        sheet.set_column_width(3, 20)?; // Hotspot Category
        if let Some(ob) = ob_col_csv {
            let insert_at = csv_col_offset + ob as u16 + 1;
            sheet.set_column_width(insert_at, 15)?; // Overcaller
            sheet.set_column_width(insert_at + 1, 15)?; // Responder
            sheet.set_column_width(insert_at + 2, 15)?; // Advancer
        }

        // Hide BBO column
        sheet.set_column_hidden(bbo_col_boards)?;

        // Auto-filter on header row
        if num_board_rows > 0 {
            sheet.autofilter(0, 0, num_board_rows, boards_last_col)?;
        }

        // Conditional formatting for subject player names across all data columns
        if num_board_rows > 0 {
            let last_row = num_board_rows;
            let last_col = boards_last_col;

            if let Some(p1) = config.subject_players.first() {
                let cf = ConditionalFormatText::new()
                    .set_rule(ConditionalFormatTextRule::Contains(p1.clone()))
                    .set_format(&player1_fill);
                sheet.add_conditional_format(1, 0, last_row, last_col, &cf)?;
            }
            if let Some(p2) = config.subject_players.get(1) {
                let cf = ConditionalFormatText::new()
                    .set_rule(ConditionalFormatTextRule::Contains(p2.clone()))
                    .set_format(&player2_fill);
                sheet.add_conditional_format(1, 0, last_row, last_col, &cf)?;
            }

            // Category conditional formatting (column 3 = Hotspot Category)
            for (idx, cat) in unique_categories.iter().enumerate() {
                let color = category_colors[idx % category_colors.len()];
                let cat_fmt = Format::new().set_background_color(color);
                let cf = ConditionalFormatText::new()
                    .set_rule(ConditionalFormatTextRule::Contains(cat.clone()))
                    .set_format(&cat_fmt);
                sheet.add_conditional_format(1, 3, last_row, 3, &cf)?;
            }
        }
    }

    // ---------------------------------------------------------------
    // Hotspots sheet (third tab)
    // ---------------------------------------------------------------
    {
        let sheet = workbook.add_worksheet();
        sheet.set_name("Hotspots")?;

        let hs_headers = [
            "Hotspot ID",
            "Link",
            "Tinyurl",
            "Board ID",
            "Category",
            "Subindex",
            "Subject Player",
            "Hit/Miss",
            "Contract",
            "Lead",
        ];
        for (c, h) in hs_headers.iter().enumerate() {
            sheet.write_string_with_format(0, c as u16, *h, &header_fmt)?;
        }

        let num_hs_rows = hotspot_entries.len() as u32;

        for (i, entry) in hotspot_entries.iter().enumerate() {
            let row = (i + 1) as u32;
            let excel_row = row + 1; // 1-based Excel row number for formulas

            // Hotspot ID (sequential number)
            sheet.write_number(row, 0, (i + 1) as f64)?;

            // Link to hand viewer
            if let Some(ref url_str) = entry.lin_url {
                // Anon: use write_url (cell hyperlink) instead of HYPERLINK formula
                // because LIN URLs exceed Excel's 255-char formula URL limit.
                // Percent-decode first to avoid double-encoding by rust_xlsxwriter.
                let decoded = percent_decode_url(url_str);
                let url = Url::new(&decoded).set_text("Link");
                sheet.write_url_with_format(row, 1, &url, &link_fmt)?;
                // Column C: LIN_URL as plain text (for reference)
                sheet.write_string(row, 2, url_str)?;
            } else {
                // Original: HYPERLINK formula referencing tinyurl in column C
                let link_formula = format!("HYPERLINK(C{0},\"Link\")", excel_row);
                sheet.write_formula_with_format(row, 1, Formula::new(link_formula), &link_fmt)?;
                // Column C: tinyurl (normalized to https)
                sheet.write_string(row, 2, to_https(&entry.tinyurl))?;
            }

            // Board ID: HYPERLINK back to Boards row
            if let Some(ref bid) = entry.board_id {
                // Anon: direct board_id lookup against Boards!$A:$A
                let bid_num: u32 = bid.parse().unwrap_or(0);
                let formula = format!(
                    "HYPERLINK(\"#Boards!A\"&MATCH({id},Boards!$A:$A,0),{id})",
                    id = bid_num
                );
                sheet.write_formula_with_format(row, 3, Formula::new(formula), &link_fmt)?;
            } else {
                // Original: INDEX/MATCH via tinyurl against BBO column
                let board_id_formula = format!(
                    "IFERROR(HYPERLINK(\"#Boards!A\"&MATCH(C{row},Boards!${col}:${col},0),INDEX(Boards!$A:$A,MATCH(C{row},Boards!${col}:${col},0))),\"\")",
                    row = excel_row,
                    col = bbo_col_letter,
                );
                sheet.write_formula_with_format(
                    row,
                    3,
                    Formula::new(board_id_formula),
                    &link_fmt,
                )?;
            }

            sheet.write_string(row, 4, &entry.category)?;
            sheet.write_number(row, 5, entry.subindex as f64)?;
            sheet.write_string(row, 6, &entry.subject_player)?;
            sheet.write_string(row, 7, &entry.hit_miss)?;
            sheet.write_string(row, 8, &entry.contract)?;
            sheet.write_string(row, 9, &entry.lead)?;
        }

        // Column widths
        sheet.set_column_width(0, 10)?; // Hotspot ID
        sheet.set_column_width(1, 8)?; // Link
        sheet.set_column_width(2, 35)?; // Tinyurl (hidden)
        sheet.set_column_width(4, 18)?; // Category
        sheet.set_column_width(6, 18)?; // Subject Player

        // Hide Tinyurl column C
        sheet.set_column_hidden(2)?;

        // Auto-filter on header row
        if num_hs_rows > 0 {
            sheet.autofilter(0, 0, num_hs_rows, 9)?;
        }

        // Conditional formatting: Hit = light pink, Miss = light green
        if num_hs_rows > 0 {
            let hit_cf = ConditionalFormatText::new()
                .set_rule(ConditionalFormatTextRule::Contains("Hit".to_string()))
                .set_format(&hit_fill);
            sheet.add_conditional_format(1, 7, num_hs_rows, 7, &hit_cf)?;

            let miss_cf = ConditionalFormatText::new()
                .set_rule(ConditionalFormatTextRule::Contains("Miss".to_string()))
                .set_format(&miss_fill);
            sheet.add_conditional_format(1, 7, num_hs_rows, 7, &miss_cf)?;
        }

        // Conditional formatting for subject player names
        if num_hs_rows > 0 {
            if let Some(p1) = config.subject_players.first() {
                let cf = ConditionalFormatText::new()
                    .set_rule(ConditionalFormatTextRule::Contains(p1.clone()))
                    .set_format(&player1_fill);
                sheet.add_conditional_format(1, 0, num_hs_rows, 9, &cf)?;
            }
            if let Some(p2) = config.subject_players.get(1) {
                let cf = ConditionalFormatText::new()
                    .set_rule(ConditionalFormatTextRule::Contains(p2.clone()))
                    .set_format(&player2_fill);
                sheet.add_conditional_format(1, 0, num_hs_rows, 9, &cf)?;
            }

            // Category conditional formatting (column 4 = Category)
            for (idx, cat) in unique_categories.iter().enumerate() {
                let color = category_colors[idx % category_colors.len()];
                let cat_fmt = Format::new().set_background_color(color);
                let cf = ConditionalFormatText::new()
                    .set_rule(ConditionalFormatTextRule::Contains(cat.clone()))
                    .set_format(&cat_fmt);
                sheet.add_conditional_format(1, 4, num_hs_rows, 4, &cf)?;
            }
        }
    }

    // ---------------------------------------------------------------
    // Cardplay sheet (optional, from fetch output)
    // ---------------------------------------------------------------
    let mut cardplay_count: usize = 0;
    if let Some(cp_path) = &config.cardplay_file {
        if cp_path.exists() {
            let cp_data = read_bbo_csv_fixed(cp_path)?;
            let mut cp_reader = ReaderBuilder::new()
                .flexible(true)
                .from_reader(cp_data.as_bytes());
            let cp_headers = cp_reader.headers()?.clone();

            let cp_cardplay_idx = cp_headers.iter().position(|h| h == "Cardplay");

            if let Some(cardplay_idx) = cp_cardplay_idx {
                let sheet = workbook.add_worksheet();
                sheet.set_name("Cardplay")?;

                // Headers
                sheet.write_string_with_format(0, 0, "Board ID", &header_fmt)?;
                sheet.write_string_with_format(0, 1, "Cardplay", &header_fmt)?;

                let mut cp_row: u32 = 1;
                for (i, result) in cp_reader.records().enumerate() {
                    let rec = result.context("Failed to read cardplay CSV row")?;

                    // Apply deal limit to match Boards sheet
                    if let Some(limit) = config.deal_limit {
                        if i >= limit {
                            break;
                        }
                    }

                    let cardplay = rec.get(cardplay_idx).unwrap_or("").trim();

                    // Skip empty or errored rows
                    if cardplay.is_empty() || cardplay.starts_with("ERROR:") {
                        continue;
                    }

                    let board_id = (i + 1) as f64;

                    // Board ID with hyperlink to Boards sheet
                    let link = format!(
                        "HYPERLINK(\"#Boards!A\"&MATCH({id},Boards!$A:$A,0),{id})",
                        id = board_id as u32
                    );
                    sheet.write_formula_with_format(cp_row, 0, Formula::new(link), &link_fmt)?;

                    // Cardplay data
                    sheet.write_string(cp_row, 1, cardplay)?;

                    cardplay_count += 1;
                    cp_row += 1;
                }

                // Column widths
                sheet.set_column_width(0, 10)?; // Board ID
                sheet.set_column_width(1, 120)?; // Cardplay (wide for trick data)

                // Autofilter
                if cardplay_count > 0 {
                    sheet.autofilter(0, 0, cp_row - 1, 1)?;
                }
            }
        }
    }

    // Ensure output directory exists
    if let Some(parent) = config.output.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Save
    workbook
        .save(&config.output)
        .map_err(|e| anyhow::anyhow!("Failed to save workbook: {}", e))?;

    let mut summary = format!(
        "Package created: {}\n  Boards: {}\n  Hotspots: {}",
        config.output.display(),
        records.len(),
        hotspot_entries.len(),
    );
    if cardplay_count > 0 {
        summary.push_str(&format!("\n  Cardplay: {}", cardplay_count));
    }
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_https() {
        assert_eq!(
            to_https("http://tinyurl.com/abc"),
            "https://tinyurl.com/abc"
        );
        assert_eq!(
            to_https("https://tinyurl.com/abc"),
            "https://tinyurl.com/abc"
        );
        assert_eq!(
            to_https("  http://tinyurl.com/abc  "),
            "https://tinyurl.com/abc"
        );
        assert_eq!(to_https("ftp://example.com"), "ftp://example.com");
    }

    #[test]
    fn test_normalize_tinyurl() {
        assert_eq!(normalize_tinyurl("http://tinyurl.com/27g7hbuc"), "27g7hbuc");
        assert_eq!(
            normalize_tinyurl("https://tinyurl.com/27g7hbuc"),
            "27g7hbuc"
        );
        assert_eq!(
            normalize_tinyurl("http://tinyurl.com/27g7hbuc/"),
            "27g7hbuc"
        );
        assert_eq!(
            normalize_tinyurl("http://tinyurl.com/27G7HBUC "),
            "27g7hbuc"
        );
        assert_eq!(
            normalize_tinyurl("  https://tinyurl.com/2KWYNAEA/  "),
            "2kwynaea"
        );
    }

    #[test]
    fn test_col_letter() {
        assert_eq!(col_letter(0), "A");
        assert_eq!(col_letter(1), "B");
        assert_eq!(col_letter(25), "Z");
        assert_eq!(col_letter(26), "AA");
        assert_eq!(col_letter(27), "AB");
        assert_eq!(col_letter(51), "AZ");
        assert_eq!(col_letter(52), "BA");
    }

    #[test]
    fn test_parse_hotspot_report() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hotspot.txt");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "HOTSPOT REPORTS FOR test\n").unwrap();
        writeln!(f, "PassedForce pair Hit : 1 Miss: 1\n").unwrap();
        writeln!(f, "--------------------------------------------------").unwrap();
        writeln!(f, "PassedForce Hotspot report for ('player1', 'player2')").unwrap();
        writeln!(f, "PassedForce Hit  1").unwrap();
        writeln!(f, "PassedForce Miss 1").unwrap();
        writeln!(f, "--------------------------------------------------").unwrap();
        writeln!(
            f,
            " 1. PassedForce Hit   Contract: 2N   Lead: S6   2021-10-21 http://tinyurl.com/abc123 player1"
        )
        .unwrap();
        writeln!(f).unwrap();
        writeln!(f, "--------------------------------------------------").unwrap();
        writeln!(
            f,
            " 2. PassedForce Miss  Contract: 1S   Lead: D4   2023-07-03 http://tinyurl.com/def456 player2"
        )
        .unwrap();
        writeln!(f).unwrap();
        writeln!(f, "--------------------------------------------------").unwrap();
        writeln!(f, "==========").unwrap();
        writeln!(
            f,
            " 1. Weird_OLs Hit  AQ_UnderT Contract: 5D   Lead: H7   2020-06-28 http://tinyurl.com/ghi789 player1"
        )
        .unwrap();

        let entries = parse_hotspot_report(&path).unwrap();
        assert_eq!(entries.len(), 3);

        assert_eq!(entries[0].category, "PassedForce");
        assert_eq!(entries[0].subindex, 1);
        assert_eq!(entries[0].hit_miss, "Hit");
        assert_eq!(entries[0].contract, "2N");
        assert_eq!(entries[0].lead, "S6");
        assert_eq!(entries[0].tinyurl, "http://tinyurl.com/abc123");
        assert_eq!(entries[0].subject_player, "player1");
        assert!(entries[0].board_id.is_none());
        assert!(entries[0].lin_url.is_none());

        assert_eq!(entries[1].category, "PassedForce");
        assert_eq!(entries[1].subindex, 2);
        assert_eq!(entries[1].hit_miss, "Miss");

        assert_eq!(entries[2].category, "Weird_OLs");
        assert_eq!(entries[2].subindex, 1);
        assert_eq!(entries[2].hit_miss, "Hit");
        assert_eq!(entries[2].contract, "5D");
        assert_eq!(entries[2].lead, "H7");
        assert_eq!(entries[2].subject_player, "player1");
    }

    #[test]
    fn test_parse_hotspot_report_anon() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hotspot_anon.txt");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "HOTSPOT REPORTS FOR test\n").unwrap();
        writeln!(f, "--------------------------------------------------").unwrap();
        writeln!(
            f,
            " 1. PassedForce Hit   Contract: 2N   Lead: S6   2021-10-21 42 Bob https://www.bridgebase.com/tools/handviewer.html?lin=pn|Bob,Alice,Carol,Dave|"
        )
        .unwrap();
        writeln!(f).unwrap();
        writeln!(
            f,
            " 2. Weird_OLs Miss  AQ_UnderT Contract: 5D   Lead: H7   2020-06-28 99 Alice https://www.bridgebase.com/tools/handviewer.html?lin=pn|Alice,Bob,Dave,Carol|"
        )
        .unwrap();

        let entries = parse_hotspot_report(&path).unwrap();
        assert_eq!(entries.len(), 2);

        assert_eq!(entries[0].category, "PassedForce");
        assert_eq!(entries[0].subindex, 1);
        assert_eq!(entries[0].hit_miss, "Hit");
        assert_eq!(entries[0].contract, "2N");
        assert_eq!(entries[0].lead, "S6");
        assert_eq!(entries[0].subject_player, "Bob");
        assert_eq!(entries[0].board_id.as_deref(), Some("42"));
        assert!(entries[0].lin_url.is_some());
        assert!(entries[0].lin_url.as_ref().unwrap().starts_with("https://"));

        assert_eq!(entries[1].category, "Weird_OLs");
        assert_eq!(entries[1].subindex, 2);
        assert_eq!(entries[1].hit_miss, "Miss");
        assert_eq!(entries[1].subject_player, "Alice");
        assert_eq!(entries[1].board_id.as_deref(), Some("99"));
    }

    #[test]
    fn test_anonymize_text_column_alignment() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("report.txt");
        let output = dir.path().join("report_anon.txt");

        let mut f = std::fs::File::create(&input).unwrap();
        // Columns: name(12 chars padded) + spaces + next_field
        writeln!(f, "Name         Score  Result").unwrap();
        writeln!(f, "longplayer1  100    Win").unwrap();
        writeln!(f, "player2      85     Loss").unwrap();
        // Name followed by a single space — still column-adjusted
        writeln!(f, "longplayer1 _Dec       122160").unwrap();
        // Name at end of line — no trailing whitespace, simple replacement
        writeln!(f, "played by longplayer1").unwrap();
        // Name adjacent to punctuation — simple replacement
        writeln!(f, "longplayer1:").unwrap();
        // Compound name: both names joined by dash, column debt carries across
        writeln!(f, "longplayer1-player2             Hit :  1").unwrap();
        // Two names on same line with separate whitespace gaps
        writeln!(f, "longplayer1 (N) player2 (S)     Leader").unwrap();
        f.flush().unwrap();

        let mappings = vec![
            ("longplayer1".to_string(), "Bob".to_string()),
            ("player2".to_string(), "Sally".to_string()),
        ];
        let empty_urls = HashMap::new();
        let empty_board_ids: HashMap<String, (String, String)> = HashMap::new();

        anonymize_text_file(&input, &output, &mappings, &empty_urls, &empty_board_ids).unwrap();

        let result = std::fs::read_to_string(&output).unwrap();
        let lines: Vec<&str> = result.lines().collect();

        // "longplayer1  100" (11+2=13 chars before "100")
        // -> "Bob          100" (3+10=13 chars before "100") — columns preserved
        assert_eq!(lines[1], "Bob          100    Win");
        // "player2      85" (7+6=13 chars before "85")
        // -> "Sally        85" (5+8=13 chars before "85") — columns preserved
        assert_eq!(lines[2], "Sally        85     Loss");
        // "longplayer1 _Dec" (11+1=12 chars before "_Dec")
        // -> "Bob         _Dec" (3+9=12 chars) — single-space column also preserved
        assert_eq!(lines[3], "Bob         _Dec       122160");
        // Name at end of line — simple replacement, no space adjustment
        assert_eq!(lines[4], "played by Bob");
        // Name adjacent to punctuation — simple replacement
        assert_eq!(lines[5], "Bob:");
        // Compound name: "longplayer1-player2             Hit" (11+1+7+13=32 before "Hit")
        // -> "Bob-Sally                       Hit" (3+1+5+23=32) — cumulative debt absorbed
        assert_eq!(lines[6], "Bob-Sally                       Hit :  1");
        // Two names: each whitespace gap absorbs its own debt independently
        // "longplayer1 (N) player2 (S)     Leader" — (N) at col 12, (S) at col 20, Leader at col 32
        // -> "Bob         (N) Sally   (S)     Leader" — same column positions
        assert_eq!(lines[7], "Bob         (N) Sally   (S)     Leader");
    }

    #[test]
    fn test_anonymize_text_url_replacement() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("hotspot.txt");
        let output = dir.path().join("hotspot_anon.txt");

        let mut f = std::fs::File::create(&input).unwrap();
        writeln!(
            f,
            " 1. PassedForce Hit   Contract: 2N   Lead: S6   2021-10-21 http://tinyurl.com/abc123 player1"
        )
        .unwrap();
        writeln!(
            f,
            " 2. PassedForce Miss  Contract: 1S   Lead: D4   2023-07-03 http://tinyurl.com/DEF456 player2"
        )
        .unwrap();
        // Unmatched tinyurl (not in board_id_map) — should show [unknown]
        writeln!(
            f,
            " 3. PassedForce Hit   Contract: 3N   Lead: H2   2022-05-10 http://tinyurl.com/NOMATCH player1"
        )
        .unwrap();
        f.flush().unwrap();

        let name_mappings = vec![
            ("player1".to_string(), "Bob".to_string()),
            ("player2".to_string(), "Sally".to_string()),
        ];
        let mut url_mappings = HashMap::new();
        url_mappings.insert(
            "abc123".to_string(),
            "https://www.bridgebase.com/tools/handviewer.html?lin=anon1".to_string(),
        );
        url_mappings.insert(
            "def456".to_string(),
            "https://www.bridgebase.com/tools/handviewer.html?lin=anon2".to_string(),
        );

        let mut board_id_map: HashMap<String, (String, String)> = HashMap::new();
        board_id_map.insert(
            "abc123".to_string(),
            (
                "42".to_string(),
                "https://original.example.com/1".to_string(),
            ),
        );
        board_id_map.insert(
            "def456".to_string(),
            (
                "99".to_string(),
                "https://original.example.com/2".to_string(),
            ),
        );

        anonymize_text_file(
            &input,
            &output,
            &name_mappings,
            &url_mappings,
            &board_id_map,
        )
        .unwrap();

        let result = std::fs::read_to_string(&output).unwrap();
        let lines: Vec<&str> = result.lines().collect();

        // Tinyurl replaced with Board_ID, anonymized LIN_URL appended
        assert!(lines[0].contains(" 42 "));
        assert!(lines[0].ends_with("handviewer.html?lin=anon1"));
        assert!(lines[1].contains(" 99 "));
        assert!(lines[1].ends_with("handviewer.html?lin=anon2"));
        // Unmatched tinyurl shows [unknown], no LIN_URL appended
        assert!(lines[2].contains("[unknown]"));
        // Tinyurls removed
        assert!(!result.contains("tinyurl.com"));
        // Names replaced
        assert!(result.contains("Bob"));
        assert!(result.contains("Sally"));
        assert!(!result.contains("player1"));
        assert!(!result.contains("player2"));
    }
}
