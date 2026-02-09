//! Pipeline functions for programmatic use by both CLI and GUI.
//!
//! These are extracted/adapted versions of the workflow logic from `bbo_csv.rs`,
//! returning structured data instead of printing to stdout.

use anyhow::{Context, Result};
use bridge_parsers::lin::parse_lin_from_url;
use bridge_parsers::tinyurl::UrlResolver;
use csv::ReaderBuilder;
use std::collections::HashMap;
use std::fmt::Write;
use std::path::Path;

// ============================================================================
// Fetch Cardplay
// ============================================================================

/// Configuration for the fetch-cardplay operation.
pub struct FetchCardplayConfig {
    /// Input CSV path
    pub input: PathBuf,
    /// Output CSV path
    pub output: PathBuf,
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
/// Calls `on_progress` after each row. Return `false` from the callback to cancel.
/// Returns a summary string on success.
pub fn fetch_cardplay(
    config: &FetchCardplayConfig,
    mut on_progress: impl FnMut(&FetchProgress) -> bool,
) -> Result<String> {
    let csv_data = read_bbo_csv_fixed(&config.input)?;
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_reader(csv_data.as_bytes());
    let headers = reader.headers()?.clone();

    let url_col_idx = headers
        .iter()
        .position(|h| h == config.url_column)
        .ok_or_else(|| anyhow::anyhow!("Column '{}' not found in CSV", config.url_column))?;

    let ref_col_idx = headers.iter().position(|h| h == "Ref #");
    let cardplay_col_idx = headers.iter().position(|h| h == "Cardplay");
    let lin_url_col_idx = headers.iter().position(|h| h == "LIN_URL");

    let existing_data: HashMap<String, (String, String)> =
        if config.resume && config.output.exists() {
            load_existing_cardplay_data(&config.output)?
        } else {
            HashMap::new()
        };

    let mut output_headers = headers.clone();
    if cardplay_col_idx.is_none() {
        output_headers.push_field("Cardplay");
        output_headers.push_field("LIN_URL");
    }

    let mut resolver =
        UrlResolver::with_config(config.delay_ms, config.batch_size, config.batch_delay_ms);

    let total_rows = count_csv_rows(&config.input)?;

    let mut writer = csv::WriterBuilder::new()
        .flexible(true)
        .from_path(&config.output)
        .context("Failed to create output CSV")?;
    writer.write_record(&output_headers)?;

    let mut processed = 0usize;
    let mut skipped = 0usize;
    let mut errors = 0usize;

    for (row_num, result) in reader.records().enumerate() {
        let record = result.context("Failed to read CSV row")?;
        processed += 1;

        let ref_id = ref_col_idx
            .and_then(|i| record.get(i))
            .unwrap_or("")
            .to_string();
        let existing = existing_data.get(&ref_id);

        // Report progress and check for cancellation
        let keep_going = on_progress(&FetchProgress {
            completed: processed,
            total: total_rows,
            errors,
            skipped,
        });
        if !keep_going {
            writer.flush()?;
            return Ok(format!(
                "Cancelled after {} of {} rows ({} errors, {} skipped)",
                processed, total_rows, errors, skipped
            ));
        }

        let (cardplay, lin_url) = if let Some((existing_lin, existing_cardplay)) = existing {
            if !existing_cardplay.is_empty() && !existing_cardplay.starts_with("ERROR:") {
                skipped += 1;
                (existing_cardplay.clone(), existing_lin.clone())
            } else {
                fetch_cardplay_for_url(&mut resolver, &record, url_col_idx, row_num, &mut errors)
            }
        } else {
            fetch_cardplay_for_url(&mut resolver, &record, url_col_idx, row_num, &mut errors)
        };

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

        if processed.is_multiple_of(100) {
            writer.flush()?;
        }
    }

    writer.flush()?;
    Ok(format!(
        "Done! Processed {} rows ({} errors, {} skipped)",
        processed, errors, skipped
    ))
}

/// Resolve a URL and parse its LIN data, returning (cardplay, resolved_url).
fn process_url(resolver: &mut UrlResolver, url: &str) -> Result<(String, String)> {
    let resolved_url = if url.contains("tinyurl.com") || url.contains("bit.ly") {
        resolver.resolve(url)?
    } else {
        url.to_string()
    };

    let lin_data = parse_lin_from_url(&resolved_url)?;
    let cardplay = lin_data.format_cardplay_by_trick();

    Ok((cardplay, resolved_url))
}

/// Fetch cardplay for a single URL, handling errors gracefully.
fn fetch_cardplay_for_url(
    resolver: &mut UrlResolver,
    record: &csv::StringRecord,
    url_col_idx: usize,
    row_num: usize,
    errors: &mut usize,
) -> (String, String) {
    let url = record.get(url_col_idx).unwrap_or("").trim();

    if url.is_empty() {
        return (String::new(), String::new());
    }

    match process_url(resolver, url) {
        Ok((cp, lu)) => (cp, lu),
        Err(e) => {
            log::warn!("Row {}: Error processing URL '{}': {}", row_num + 1, url, e);
            *errors += 1;

            if e.to_string().contains("Rate limited") {
                log::warn!("Rate limited - pausing for 60 seconds...");
                std::thread::sleep(std::time::Duration::from_secs(60));
                resolver.reset_batch();
            }

            (format!("ERROR: {}", e), String::new())
        }
    }
}

/// Load existing cardplay data from an output file for resume support.
fn load_existing_cardplay_data(output: &Path) -> Result<HashMap<String, (String, String)>> {
    let mut data = HashMap::new();
    let mut reader = ReaderBuilder::new().flexible(true).from_path(output)?;

    let headers = reader.headers()?.clone();
    let ref_idx = headers.iter().position(|h| h == "Ref #");
    let lin_url_idx = headers.iter().position(|h| h == "LIN_URL");
    let cardplay_idx = headers.iter().position(|h| h == "Cardplay");

    if ref_idx.is_none() || cardplay_idx.is_none() {
        return Ok(data);
    }

    let ref_idx = ref_idx.unwrap();
    let cardplay_idx = cardplay_idx.unwrap();

    for result in reader.records() {
        let record = result?;
        let ref_id = record.get(ref_idx).unwrap_or("").to_string();
        let lin_url = lin_url_idx
            .and_then(|i| record.get(i))
            .unwrap_or("")
            .to_string();
        let cardplay = record.get(cardplay_idx).unwrap_or("").to_string();

        if !ref_id.is_empty() {
            data.insert(ref_id, (lin_url, cardplay));
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
pub fn anonymize_csv(config: &AnonymizeConfig) -> Result<AnonymizeResult> {
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

    Ok(AnonymizeResult {
        summary: format!(
            "Anonymization complete:\n  Rows processed: {}\n  Explicit mappings: {}\n  Generated names: {}\n  Total unique names: {}",
            processed,
            anonymizer.explicit_maps.len(),
            anonymizer.generated_maps.len(),
            anonymizer.used_names.len()
        ),
        name_mappings,
        url_mappings,
    })
}

/// Anonymize player names in a text file using pre-built name mappings.
///
/// Performs column-aware, case-insensitive replacement: when a name is followed
/// by whitespace (spaces or tabs), the space count is adjusted so that the
/// character position after the whitespace is preserved. Also replaces tinyurls
/// with full anonymized handviewer URLs when `url_mappings` is non-empty.
pub fn anonymize_text_file(
    input: &Path,
    output: &Path,
    name_mappings: &[(String, String)],
    url_mappings: &HashMap<String, String>,
) -> Result<()> {
    let content = std::fs::read_to_string(input)
        .with_context(|| format!("Failed to read text file: {}", input.display()))?;

    let mut result = content;

    // Column-aware name replacement (longest names first, already sorted)
    for (original, replacement) in name_mappings {
        // First pass: replace name followed by whitespace, adjusting space count
        let col_pattern =
            regex::RegexBuilder::new(&format!("{}([ \\t]+)", regex::escape(original)))
                .case_insensitive(true)
                .build()
                .with_context(|| format!("Failed to build column regex for '{}'", original))?;

        let orig_len = original.len();
        let repl_len = replacement.len();
        let repl = replacement.clone();
        result = col_pattern
            .replace_all(&result, |caps: &regex::Captures| -> String {
                let ws = caps.get(1).unwrap().as_str();
                let new_spaces = (ws.len() + orig_len).saturating_sub(repl_len).max(1);
                format!("{}{}", repl, " ".repeat(new_spaces))
            })
            .to_string();

        // Second pass: replace remaining (non-column) occurrences — name at
        // end of line, adjacent to punctuation, etc.
        let pattern = regex::RegexBuilder::new(&regex::escape(original))
            .case_insensitive(true)
            .build()
            .with_context(|| format!("Failed to build regex for '{}'", original))?;
        result = pattern
            .replace_all(&result, replacement.as_str())
            .to_string();
    }

    // Replace tinyurls with full anonymized handviewer URLs
    if !url_mappings.is_empty() {
        let url_pattern = regex::Regex::new(r"https?://(?:tinyurl\.com|bit\.ly)/\S+")
            .context("Failed to build tinyurl regex")?;
        result = url_pattern
            .replace_all(&result, |caps: &regex::Captures| -> String {
                let url = &caps[0];
                let key = normalize_tinyurl(url);
                match url_mappings.get(&key) {
                    Some(full_url) => full_url.clone(),
                    None => "[URL redacted]".to_string(),
                }
            })
            .to_string();
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

/// Run full anonymization: CSV first (to build mappings), then text files.
///
/// Returns a summary string describing all operations performed.
pub fn anonymize_all(config: &AnonymizeAllConfig) -> Result<String> {
    let csv_config = AnonymizeConfig {
        input: config.csv_input.clone(),
        output: config.csv_output.clone(),
        key: config.key.clone(),
        map: config.map.clone(),
        columns: config.columns.clone(),
    };
    let csv_result = anonymize_csv(&csv_config)?;

    let mut summary = csv_result.summary.clone();

    let empty_urls = HashMap::new();

    if let (Some(input), Some(output)) = (&config.concise_input, &config.concise_output) {
        anonymize_text_file(input, output, &csv_result.name_mappings, &empty_urls)?;
        summary.push_str(&format!(
            "\n  Concise report: {}",
            output.file_name().unwrap_or_default().to_string_lossy()
        ));
    }

    if let (Some(input), Some(output)) = (&config.hotspot_input, &config.hotspot_output) {
        anonymize_text_file(
            input,
            output,
            &csv_result.name_mappings,
            &csv_result.url_mappings,
        )?;
        summary.push_str(&format!(
            "\n  Hotspot report: {}",
            output.file_name().unwrap_or_default().to_string_lossy()
        ));
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
// Package Workbook
// ============================================================================

use std::path::PathBuf;

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
    /// TinyURL to BBO hand viewer
    pub tinyurl: String,
    /// Subject player BBO username
    pub subject_player: String,
}

/// Parse a hotspot report text file into a vector of HotspotEntry.
pub fn parse_hotspot_report(path: &Path) -> Result<Vec<HotspotEntry>> {
    use regex::Regex;

    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read hotspot report: {}", path.display()))?;

    // Match lines like:
    //  1. PassedForce Hit   Contract: 2N   Lead: S6   2021-10-21 http://tinyurl.com/abc player
    //  1. Weird_OLs Hit  AQ_UnderT Contract: 5D   Lead: H7   2020-06-28 http://... player
    let entry_re = Regex::new(
        r"^\s*(\d+)\.\s+(\S+)\s+(Hit|Miss)\s+\S*\s*Contract:\s+(\S+)\s+Lead:\s+(\S+)\s+\S+\s+(https?://\S+)\s+(\S+)",
    )?;

    let mut entries = Vec::new();

    for line in content.lines() {
        if let Some(caps) = entry_re.captures(line) {
            entries.push(HotspotEntry {
                subindex: caps[1].parse().unwrap_or(0),
                category: caps[2].to_string(),
                hit_miss: caps[3].to_string(),
                contract: caps[4].to_string(),
                lead: caps[5].to_string(),
                tinyurl: caps[6].to_string(),
                subject_player: caps[7].to_string(),
            });
        }
    }

    Ok(entries)
}

/// Normalize a tinyurl for matching between CSV and hotspot report.
///
/// Extracts the path component after `tinyurl.com/` and lowercases it.
/// Falls back to trimmed lowercase of the full URL if not a tinyurl.
fn normalize_tinyurl(url: &str) -> String {
    let trimmed = url.trim().trim_end_matches('/');
    let lower = trimmed.to_lowercase();
    if let Some(pos) = lower.find("tinyurl.com/") {
        lower[pos + "tinyurl.com/".len()..].to_string()
    } else {
        lower
    }
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
}

/// Create a packaged Excel workbook from the three EDGAR case files.
///
/// Produces a workbook with Summary, Boards, and Hotspots sheets.
/// Returns a summary string on success.
pub fn package_workbook(config: &PackageConfig) -> Result<String> {
    use rust_xlsxwriter::{
        ConditionalFormatText, ConditionalFormatTextRule, Format, FormatAlign, FormatUnderline,
        Formula, Workbook,
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

    // -- Parse hotspot report --
    let hotspot_entries = parse_hotspot_report(&config.hotspot_file)?;

    // Build normalized tinyurl -> (hotspot_id_1based, category) for first match
    let mut url_to_hotspot: HashMap<String, (u32, String)> = HashMap::new();
    for (i, entry) in hotspot_entries.iter().enumerate() {
        let key = normalize_tinyurl(&entry.tinyurl);
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

            // Link: =HYPERLINK(BBO_cell, "link") with blue underline formatting
            let bbo_url = record.get(bbo_col_csv).unwrap_or("").trim();
            if !bbo_url.is_empty() {
                let bbo_cell = format!(
                    "{col}${row}",
                    col = col_letter(bbo_col_boards as u32),
                    row = excel_row
                );
                let link_formula = format!("HYPERLINK({cell},\"link\")", cell = bbo_cell);
                sheet.write_formula_with_format(row, 1, Formula::new(link_formula), &link_fmt)?;
            }

            // Hotspot ID and Category (matched via normalized tinyurl)
            if !bbo_url.is_empty() {
                let key = normalize_tinyurl(bbo_url);
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

            // Link: =HYPERLINK(C{row}, "Link") with blue underline formatting
            let link_formula = format!("HYPERLINK(C{0},\"Link\")", excel_row);
            sheet.write_formula_with_format(row, 1, Formula::new(link_formula), &link_fmt)?;

            // Tinyurl (normalized to https, plain text for MATCH reference)
            sheet.write_string(row, 2, to_https(&entry.tinyurl))?;

            // Board ID: HYPERLINK back to Boards row, with INDEX/MATCH lookup
            let board_id_formula = format!(
                "IFERROR(HYPERLINK(\"#Boards!A\"&MATCH(C{row},Boards!${col}:${col},0),INDEX(Boards!$A:$A,MATCH(C{row},Boards!${col}:${col},0))),\"\")",
                row = excel_row,
                col = bbo_col_letter,
            );
            sheet.write_formula_with_format(row, 3, Formula::new(board_id_formula), &link_fmt)?;

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
        f.flush().unwrap();

        let mappings = vec![
            ("longplayer1".to_string(), "Bob".to_string()),
            ("player2".to_string(), "Sally".to_string()),
        ];
        let empty_urls = HashMap::new();

        anonymize_text_file(&input, &output, &mappings, &empty_urls).unwrap();

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
        // Unmatched tinyurl (not in url_mappings) — should be redacted
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

        anonymize_text_file(&input, &output, &name_mappings, &url_mappings).unwrap();

        let result = std::fs::read_to_string(&output).unwrap();
        // Tinyurls replaced with full handviewer URLs
        assert!(result.contains("handviewer.html?lin=anon1"));
        assert!(result.contains("handviewer.html?lin=anon2"));
        // Tinyurls removed (matched ones replaced, unmatched ones redacted)
        assert!(!result.contains("tinyurl.com"));
        assert!(result.contains("[URL redacted]"));
        // Names replaced
        assert!(result.contains("Bob"));
        assert!(result.contains("Sally"));
        assert!(!result.contains("player1"));
        assert!(!result.contains("player2"));
    }
}
