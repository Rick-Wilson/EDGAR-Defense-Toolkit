//! Generate a PBN/PDF report of hotspot miss boards.
//!
//! Reads the EDGAR hotspot report text file, parses miss entries,
//! extracts deal data from the embedded LIN URLs, and produces a PBN
//! file. Optionally converts to PDF via pbn-to-pdf.

use anyhow::{Context, Result};
use bridge_parsers::lin::parse_lin_from_url;
use clap::Parser;
use std::path::PathBuf;

/// Opening lead hotspot categories (vs bidding-related ones)
const LEAD_CATEGORIES: &[&str] = &["Kxx_vsSuit", "Weird_OLs", "Ax+_Low", "Suit_Overeasy"];

#[derive(Parser)]
#[command(name = "hotspot-report")]
#[command(about = "Generate PBN/PDF report of hotspot miss boards")]
struct Cli {
    /// Input hotspot report text file (anonymized)
    #[arg(long)]
    hotspot: PathBuf,

    /// Output PBN file (default: same directory as input, hotspot_misses.pbn)
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Path to pbn-to-pdf binary (auto-detected if not specified)
    #[arg(long)]
    pbn_to_pdf: Option<PathBuf>,

    /// Boards per page in PDF output
    #[arg(long, default_value = "4")]
    boards_per_page: u8,

    /// Include hit boards too (default: misses only)
    #[arg(long)]
    include_hits: bool,
}

/// A parsed hotspot entry from the report text file.
struct HotspotEntry {
    category: String,
    subclass: String,
    hit_miss: String,
    contract: String,
    lead: String,
    date: String,
    _board_id: String,
    player: String,
    lin_url: String,
}

/// Parse a hotspot entry line from the report.
///
/// Format: `N. Category Hit/Miss [Subclass] Contract: XX  Lead: XX  DATE BOARD_ID PLAYER URL`
fn parse_hotspot_line(line: &str) -> Option<HotspotEntry> {
    let line = line.trim();

    // Must start with a number followed by a period
    let dot_pos = line.find(". ")?;
    let _index: u32 = line[..dot_pos].trim().parse().ok()?;
    let rest = &line[dot_pos + 2..];

    // Split by whitespace to extract fields
    let parts: Vec<&str> = rest.split_whitespace().collect();
    if parts.len() < 7 {
        return None;
    }

    // First token is category
    let category = parts[0].to_string();

    // Second token is Hit/Miss
    let hit_miss_idx = parts.iter().position(|p| *p == "Hit" || *p == "Miss")?;
    let hit_miss = parts[hit_miss_idx].to_string();

    // Subclass is between category and Hit/Miss (if any)
    let subclass = if hit_miss_idx > 1 {
        parts[1..hit_miss_idx].join(" ")
    } else {
        String::new()
    };

    // Find Contract: and Lead: fields
    let contract_idx = parts.iter().position(|p| *p == "Contract:")?;
    let contract = parts.get(contract_idx + 1)?.to_string();

    let lead_idx = parts.iter().position(|p| *p == "Lead:")?;
    let lead = parts.get(lead_idx + 1)?.to_string();

    // After Lead: value, we have DATE BOARD_ID PLAYER URL
    let after_lead = lead_idx + 2;
    if after_lead + 3 >= parts.len() {
        return None;
    }

    let date = parts[after_lead].to_string();
    let board_id = parts[after_lead + 1].to_string();
    let player = parts[after_lead + 2].to_string();
    let lin_url = parts[after_lead + 3].to_string();

    Some(HotspotEntry {
        category,
        subclass,
        hit_miss,
        contract,
        lead,
        date,
        _board_id: board_id,
        player,
        lin_url,
    })
}

/// Shorten player names: "FirstName_LastName" -> "FirstName..."
fn shorten_name(name: &str) -> String {
    if let Some(pos) = name.find('_') {
        format!("{}...", &name[..pos])
    } else {
        name.to_string()
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let content = std::fs::read_to_string(&cli.hotspot)
        .with_context(|| format!("Failed to read {}", cli.hotspot.display()))?;

    // Parse all hotspot entries
    let mut entries: Vec<HotspotEntry> = Vec::new();
    for line in content.lines() {
        if let Some(entry) = parse_hotspot_line(line) {
            if entry.hit_miss == "Miss" || cli.include_hits {
                entries.push(entry);
            }
        }
    }

    println!(
        "Parsed {} {} entries from {}",
        entries.len(),
        if cli.include_hits { "hotspot" } else { "miss" },
        cli.hotspot.display()
    );

    // Convert to boards
    let mut boards = Vec::new();
    for (i, entry) in entries.iter().enumerate() {
        let lin_data = match parse_lin_from_url(&entry.lin_url) {
            Ok(data) => data,
            Err(e) => {
                eprintln!(
                    "  Warning: failed to parse LIN URL for entry {}: {}",
                    i + 1,
                    e
                );
                continue;
            }
        };

        let mut board = lin_data.to_board(Some((i + 1) as u32));

        // Shorten player names for display
        if let Some(ref mut names) = board.player_names {
            names.north = names.north.as_ref().map(|n| shorten_name(n));
            names.east = names.east.as_ref().map(|n| shorten_name(n));
            names.south = names.south.as_ref().map(|n| shorten_name(n));
            names.west = names.west.as_ref().map(|n| shorten_name(n));
        }

        // Set date
        board.date = Some(entry.date.replace('-', "."));

        // For opening lead categories, keep only the first trick (opening lead)
        let is_lead_category = LEAD_CATEGORIES.iter().any(|c| *c == entry.category);
        if !is_lead_category {
            // For non-lead categories, remove the play sequence
            board.play = None;
        } else if let Some(ref mut play) = board.play {
            // Keep only the opening lead (first card of first trick)
            if !play.tricks.is_empty() {
                let first_trick = &play.tricks[0];
                let lead_card = first_trick.cards[0];
                let mut new_trick = bridge_parsers::Trick::new(play.opening_leader);
                if let Some(card) = lead_card {
                    new_trick.cards[0] = Some(card);
                }
                play.tricks = vec![new_trick];
            }
        }

        // Build commentary
        let mut commentary = String::new();
        if !entry.category.is_empty() {
            commentary.push_str(&format!("<b>Hotspot:</b> {}", entry.category));
            if !entry.subclass.is_empty() {
                commentary.push_str(&format!(" / {}", entry.subclass));
            }
            commentary.push('\n');
        }
        commentary.push_str(&format!(
            "<b>Contract:</b> {}  <b>Lead:</b> {}  <b>Player:</b> {}",
            entry.contract, entry.lead, entry.player
        ));
        commentary.push_str(&format!("\n<b>Date:</b> {}", entry.date));
        commentary.push_str(&format!("\n<b>Result:</b> {}", entry.hit_miss));
        board.commentary.push(commentary);

        boards.push(board);
    }

    println!("  Converted {} boards", boards.len());

    // Write PBN
    let output_dir = cli
        .hotspot
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    let pbn_path = cli
        .output
        .unwrap_or_else(|| output_dir.join("hotspot_misses.pbn"));
    let pbn_content = bridge_parsers::pbn::writer::write_pbn(&boards);
    std::fs::write(&pbn_path, &pbn_content)
        .with_context(|| format!("Failed to write {}", pbn_path.display()))?;
    println!("  Wrote PBN: {}", pbn_path.display());

    // Run pbn-to-pdf
    let pdf_path = pbn_path.with_extension("pdf");
    let pbn_to_pdf = cli
        .pbn_to_pdf
        .unwrap_or_else(|| PathBuf::from("/Applications/Bridge Utilities/pbn-to-pdf"));

    if pbn_to_pdf.exists() {
        let status = std::process::Command::new(&pbn_to_pdf)
            .arg(&pbn_path)
            .arg("-o")
            .arg(&pdf_path)
            .arg("-n")
            .arg(cli.boards_per_page.to_string())
            .status()
            .with_context(|| format!("Failed to run {}", pbn_to_pdf.display()))?;

        if status.success() {
            println!("  Wrote PDF: {}", pdf_path.display());
        } else {
            eprintln!("  pbn-to-pdf exited with: {}", status);
        }
    } else {
        println!("  pbn-to-pdf not found at {}", pbn_to_pdf.display());
        println!("  PBN file ready for manual conversion");
    }

    Ok(())
}
