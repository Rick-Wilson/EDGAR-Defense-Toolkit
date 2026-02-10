//! BBO CSV Tool - Extract cardplay data and perform DD analysis
//!
//! This tool processes CSV files containing TinyURLs to BBO hand records,
//! extracting the complete cardplay sequence and optionally performing
//! double-dummy analysis.

use anyhow::{Context, Result};
#[cfg(test)]
use bridge_parsers::{Card, Rank, Suit};
#[cfg(test)]
use bridge_solver::cards::card_of;
#[cfg(test)]
use bridge_solver::NOTRUMP;
#[cfg(test)]
use bridge_solver::{CLUB, DIAMOND, EAST, HEART, NORTH, SOUTH, SPADE, WEST};
use clap::{Parser, Subcommand};
use csv::{ReaderBuilder, Writer};
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Write as IoWrite};
use std::path::PathBuf;

// ============================================================================
// BBO CSV Preprocessing - Fix malformed quoted fields
// ============================================================================

/// Fix BBO's malformed CSV where the last field (alerts) may contain unescaped quotes.
/// BBO wraps fields containing commas/quotes in double quotes, but doesn't escape
/// internal quotes. Example:
///   ..."2N=Ogust+see+partner"s+response|3S=good+hand,+good+suit"
/// Should become:
///   ..."2N=Ogust+see+partner's+response|3S=good+hand,+good+suit"
fn fix_bbo_csv_line(line: &str) -> String {
    // Quick check: if line doesn't end with a quote, nothing to fix
    if !line.trim_end().ends_with('"') {
        return line.to_string();
    }

    // Find the last field by looking for the pattern: ,"...anything..."
    // We need to find where the last quoted field starts
    lazy_static::lazy_static! {
        // Match: comma, then opening quote, then content, then closing quote at end
        // The content may contain unescaped quotes that we need to fix
        static ref LAST_FIELD_PATTERN: Regex = Regex::new(r#",("[^"]*(?:"[^"]*)*")$"#).unwrap();
    }

    // Alternative simpler approach: find the last comma followed by a quote
    if let Some(last_comma_quote) = line.rfind(",\"") {
        let prefix = &line[..last_comma_quote + 1]; // includes the comma
        let quoted_field = &line[last_comma_quote + 1..]; // starts with quote

        // Check if this quoted field has internal quotes (more than just start/end)
        if quoted_field.len() > 2
            && quoted_field.starts_with('"')
            && quoted_field.trim_end().ends_with('"')
        {
            let inner = &quoted_field[1..quoted_field.trim_end().len() - 1];

            // If inner content has quotes, replace them with single quotes
            if inner.contains('"') {
                let fixed_inner = inner.replace('"', "'");
                return format!("{}\"{}\"", prefix, fixed_inner);
            }
        }
    }

    line.to_string()
}

/// Read a BBO CSV file and preprocess to fix malformed lines
fn read_bbo_csv_fixed(path: &PathBuf) -> Result<String> {
    let file = File::open(path).context("Failed to open input file")?;
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

#[derive(Parser)]
#[command(name = "bbo-csv")]
#[command(about = "Extract cardplay data from BBO hand records in CSV files")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Fetch cardplay data from TinyURLs and add to CSV
    FetchCardplay {
        /// Input CSV file
        #[arg(short, long)]
        input: PathBuf,

        /// Output CSV file
        #[arg(short, long)]
        output: PathBuf,

        /// Column name containing the TinyURL/BBO URL
        #[arg(long, default_value = "BBO")]
        url_column: String,

        /// Delay between URL requests in milliseconds
        #[arg(long, default_value = "200")]
        delay_ms: u64,

        /// Number of requests before a longer pause
        #[arg(long, default_value = "10")]
        batch_size: usize,

        /// Duration of the longer pause in milliseconds
        #[arg(long, default_value = "2000")]
        batch_delay_ms: u64,

        /// Resume from previous run (skip rows with existing cardplay data)
        #[arg(long)]
        resume: bool,
    },

    /// Analyze double-dummy cost for each card played
    AnalyzeDd {
        /// Input CSV file (must have Cardplay column and deal columns)
        #[arg(short, long)]
        input: PathBuf,

        /// Output CSV file
        #[arg(short, long)]
        output: PathBuf,

        /// Number of parallel threads (default: number of CPU cores)
        #[arg(short, long)]
        threads: Option<usize>,

        /// Resume from previous run (skip rows with existing DD analysis)
        #[arg(long)]
        resume: bool,

        /// Save progress every N rows
        #[arg(long, default_value = "100")]
        checkpoint_interval: usize,
    },

    /// Anonymize usernames in CSV file.
    ///
    /// Replaces player names with anonymized versions using keyed hashing for
    /// reproducibility. The same name always maps to the same anonymized value
    /// when using the same key. Also processes player names in LIN_URL column.
    Anonymize {
        /// Input CSV file
        #[arg(short, long)]
        input: PathBuf,

        /// Output CSV file
        #[arg(short, long)]
        output: PathBuf,

        /// Secret key for reproducible hashing. Same key produces same mappings.
        /// Set via BBO_ANON_KEY env var to avoid exposing in shell history.
        #[arg(short, long, env = "BBO_ANON_KEY")]
        key: String,

        /// Explicit name mappings to use instead of hashing.
        /// Format: "oldname=NewName,oldname2=NewName2"
        /// Example: --map "JohnDoe=Player1,JaneSmith=Player2"
        #[arg(short, long, default_value = "")]
        map: String,

        /// Columns containing usernames to anonymize.
        /// LIN_URL column is also processed automatically (pn| tag).
        #[arg(
            long,
            default_value = "N,S,E,W,OB name,Dec name,Leader",
            value_delimiter = ','
        )]
        columns: Vec<String>,
    },

    /// Analyze DD error statistics by player and role (declaring vs defending)
    Stats {
        /// Input CSV file (must have DD_Analysis column)
        #[arg(short, long)]
        input: PathBuf,

        /// Number of top players to show individually (default: 10)
        #[arg(long, default_value = "10")]
        top_n: usize,

        /// Output detailed CSV with per-player stats
        #[arg(short, long)]
        output: Option<PathBuf>,
    },

    /// Display a single hand with DD analysis for spot-checking
    DisplayHand {
        /// Input CSV file (must have Cardplay and DD_Analysis columns)
        #[arg(short, long)]
        input: PathBuf,

        /// Row number to display (1-indexed, not counting header)
        #[arg(short = 'n', long)]
        row: usize,
    },

    /// Create Excel workbook(s) from case folder.
    ///
    /// Scans the folder for BBO CSV, Concise report, and Hotspot report.
    /// Creates "EDGAR Defense {subject}.xlsx" in the EDGAR Defense subfolder.
    /// If anonymized files are found, also creates an anonymized workbook.
    Package {
        /// Case folder path (scanned for CSV, Concise, Hotspot files)
        #[arg(short, long)]
        folder: PathBuf,

        /// Subject player usernames for conditional formatting (comma-separated)
        #[arg(short, long, value_delimiter = ',')]
        subjects: Vec<String>,

        /// Optional deal limit (only include first N boards)
        #[arg(short, long)]
        limit: Option<usize>,
    },
}

fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::FetchCardplay {
            input,
            output,
            url_column,
            delay_ms,
            batch_size,
            batch_delay_ms,
            resume,
        } => {
            use edgar_defense_toolkit::pipeline;
            let lookup_output = pipeline::derive_lookup_path(&output);
            let config = pipeline::FetchCardplayConfig {
                input,
                output,
                lookup_output,
                url_column,
                delay_ms,
                batch_size,
                batch_delay_ms,
                resume,
            };
            let summary = pipeline::fetch_cardplay(&config, |p| {
                eprint!(
                    "\r[{}/{}] Processing... ({} errors, {} skipped)    ",
                    p.completed, p.total, p.errors, p.skipped
                );
                std::io::stderr().flush().ok();
                true // never cancel from CLI
            })?;
            eprintln!("\n{}", summary);
        }
        Commands::AnalyzeDd {
            input,
            output,
            threads,
            resume,
            checkpoint_interval,
        } => {
            use edgar_defense_toolkit::pipeline;
            let config = pipeline::AnalyzeDdConfig {
                input,
                output,
                threads,
                resume,
                checkpoint_interval,
            };
            let summary = pipeline::analyze_dd(&config, |p| {
                eprint!(
                    "\r[{}/{}] Analyzing DD... ({} errors)    ",
                    p.completed, p.total, p.errors
                );
                std::io::stderr().flush().ok();
                true // never cancel from CLI
            })?;
            eprintln!("\n{}", summary);
        }
        Commands::Anonymize {
            input,
            output,
            key,
            map,
            columns,
        } => {
            anonymize_csv(&input, &output, &key, &map, &columns)?;
        }
        Commands::Stats {
            input,
            top_n,
            output,
        } => {
            compute_stats(&input, top_n, output.as_ref())?;
        }
        Commands::DisplayHand { input, row } => {
            display_hand(&input, row)?;
        }
        Commands::Package {
            folder,
            subjects,
            limit,
        } => {
            use edgar_defense_toolkit::pipeline;

            let case_files = pipeline::scan_case_folder(&folder);
            let csv = case_files
                .csv_file
                .clone()
                .ok_or_else(|| anyhow::anyhow!("No CSV file found in {}", folder.display()))?;
            let concise = case_files.concise_file.clone().ok_or_else(|| {
                anyhow::anyhow!("No Concise report found in {}", folder.display())
            })?;
            let hotspot = case_files.hotspot_file.clone().ok_or_else(|| {
                anyhow::anyhow!("No Hotspot report found in {}", folder.display())
            })?;

            let subject =
                pipeline::extract_concise_subject(&concise).unwrap_or_else(|| "Report".to_string());

            let edgar_dir = folder.join("EDGAR Defense");
            let output = edgar_dir.join(format!("EDGAR Defense {}.xlsx", subject));

            // Look for cardplay CSV in EDGAR Defense folder
            let cardplay_file = std::fs::read_dir(&edgar_dir).ok().and_then(|entries| {
                entries
                    .flatten()
                    .find(|e| {
                        let name = e.file_name().to_string_lossy().to_lowercase();
                        name.ends_with(".csv")
                            && name.contains("cardplay")
                            && !name.contains("anon")
                    })
                    .map(|e| e.path())
            });

            let config = pipeline::PackageConfig {
                csv_file: csv,
                hotspot_file: hotspot,
                concise_file: concise,
                output: output.clone(),
                case_folder: folder.display().to_string(),
                subject_players: subjects.clone(),
                deal_limit: limit,
                cardplay_file,
                is_anon: false,
            };

            eprintln!("Creating {}...", output.display());
            let summary = pipeline::package_workbook(&config)?;
            eprintln!("{}", summary);

            // Check for anon files and create anon workbook
            if let Some(anon_files) = pipeline::find_anon_files(&edgar_dir, &case_files, limit) {
                let anon_output = edgar_dir.join(format!("EDGAR Defense {} anon.xlsx", subject));

                let anon_config = pipeline::PackageConfig {
                    csv_file: anon_files.csv_file,
                    hotspot_file: anon_files.hotspot_file,
                    concise_file: anon_files.concise_file,
                    output: anon_output.clone(),
                    case_folder: folder.display().to_string(),
                    subject_players: subjects,
                    deal_limit: limit,
                    cardplay_file: config.cardplay_file.clone(),
                    is_anon: true,
                };

                eprintln!("\nCreating {}...", anon_output.display());
                let anon_summary = pipeline::package_workbook(&anon_config)?;
                eprintln!("{}", anon_summary);
            }
        }
    }

    Ok(())
}

fn count_csv_rows(path: &PathBuf) -> Result<usize> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    // Subtract 1 for header row
    Ok(reader.lines().count().saturating_sub(1))
}

// ============================================================================
// Test-only helpers
// ============================================================================

// Functions below are used by tests only
#[cfg(test)]
fn parse_trump(contract: &str) -> Result<usize> {
    let contract = contract.trim().to_uppercase();

    if contract.contains("NT") || contract.contains("N") && !contract.contains("S") {
        return Ok(NOTRUMP);
    }

    // Find suit letter
    for c in contract.chars() {
        match c {
            'S' => return Ok(SPADE),
            'H' => return Ok(HEART),
            'D' => return Ok(DIAMOND),
            'C' => return Ok(CLUB),
            _ => continue,
        }
    }

    Err(anyhow::anyhow!(
        "Could not parse trump from contract: {}",
        contract
    ))
}

#[cfg(test)]
fn parse_declarer(declarer: &str) -> Result<usize> {
    match declarer.trim().to_uppercase().chars().next() {
        Some('N') => Ok(NORTH),
        Some('E') => Ok(EAST),
        Some('S') => Ok(SOUTH),
        Some('W') => Ok(WEST),
        _ => Err(anyhow::anyhow!("Invalid declarer: {}", declarer)),
    }
}

#[cfg(test)]
fn parse_cardplay(cardplay: &str) -> Result<Vec<Vec<Card>>> {
    let mut tricks = Vec::new();

    for trick_str in cardplay.split('|') {
        if trick_str.is_empty() {
            continue;
        }

        let mut trick = Vec::new();
        for card_str in trick_str.split(' ') {
            let card = parse_card_str(card_str)?;
            trick.push(card);
        }

        if !trick.is_empty() {
            tricks.push(trick);
        }
    }

    Ok(tricks)
}

#[cfg(test)]
fn parse_card_str(s: &str) -> Result<Card> {
    let s = s.trim();
    if s.len() < 2 {
        return Err(anyhow::anyhow!("Invalid card: {}", s));
    }

    let mut chars = s.chars();
    let suit_char = chars.next().unwrap();
    let rank_char = chars.next().unwrap();

    let suit = match suit_char.to_ascii_uppercase() {
        'S' => Suit::Spades,
        'H' => Suit::Hearts,
        'D' => Suit::Diamonds,
        'C' => Suit::Clubs,
        _ => return Err(anyhow::anyhow!("Invalid suit: {}", suit_char)),
    };

    let rank =
        Rank::from_char(rank_char).ok_or_else(|| anyhow::anyhow!("Invalid rank: {}", rank_char))?;

    Ok(Card::new(suit, rank))
}

#[cfg(test)]
#[allow(dead_code)]
fn bridge_card_to_solver(card: Card) -> Result<usize> {
    let suit = match card.suit {
        Suit::Spades => SPADE,
        Suit::Hearts => HEART,
        Suit::Diamonds => DIAMOND,
        Suit::Clubs => CLUB,
    };

    let rank = match card.rank {
        Rank::Ace => 12,
        Rank::King => 11,
        Rank::Queen => 10,
        Rank::Jack => 9,
        Rank::Ten => 8,
        Rank::Nine => 7,
        Rank::Eight => 6,
        Rank::Seven => 5,
        Rank::Six => 4,
        Rank::Five => 3,
        Rank::Four => 2,
        Rank::Three => 1,
        Rank::Two => 0,
    };

    Ok(card_of(suit, rank))
}

#[cfg(test)]
#[allow(dead_code)]
fn determine_trick_winner(
    cards: &[(usize, usize)], // (seat, card)
    trump: usize,
    leader: usize,
) -> usize {
    use bridge_solver::cards::suit_of;

    let _lead_suit = suit_of(cards[0].1);
    let mut winner_idx = 0;
    let mut winning_card = cards[0].1;

    for (i, (_seat, card)) in cards.iter().enumerate().skip(1) {
        let card_suit = suit_of(*card);

        // Check if this card beats the current winner
        let beats = if card_suit == suit_of(winning_card) {
            // Same suit - higher card wins (lower index = higher rank)
            *card < winning_card
        } else if card_suit == trump && trump < NOTRUMP {
            // Trump beats non-trump
            suit_of(winning_card) != trump
        } else {
            false
        };

        if beats {
            winner_idx = i;
            winning_card = *card;
        }
    }

    // Return the actual seat
    (leader + winner_idx) % 4
}

// ============================================================================
// Anonymize Implementation
// ============================================================================

/// Common first names (mix of male and female)
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

/// Common surnames
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

/// Anonymizer that maps usernames to fake names using keyed hashing
struct Anonymizer {
    key: String,
    explicit_maps: HashMap<String, String>,
    generated_maps: HashMap<String, String>,
    used_names: HashSet<String>,
    name_count: usize,
}

impl Anonymizer {
    fn new(key: &str, explicit_map_str: &str) -> Self {
        let mut explicit_maps = HashMap::new();
        let mut used_names = HashSet::new();

        // Parse explicit mappings
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
            name_count: 0,
        }
    }

    /// Get anonymous name for a username
    fn anonymize(&mut self, username: &str) -> String {
        let username_lower = username.to_lowercase();

        // Check explicit mapping first
        if let Some(mapped) = self.explicit_maps.get(&username_lower) {
            return mapped.clone();
        }

        // Check if we already generated a name for this user
        if let Some(mapped) = self.generated_maps.get(&username_lower) {
            return mapped.clone();
        }

        // Generate a new name using keyed hash
        let new_name = self.generate_name(&username_lower);
        self.generated_maps.insert(username_lower, new_name.clone());
        new_name
    }

    /// Generate a unique name using keyed hash
    fn generate_name(&mut self, username: &str) -> String {
        // Simple keyed hash: combine key + username, then hash
        let combined = format!("{}:{}", self.key, username);
        let hash = self.simple_hash(&combined);

        // Use hash to pick first name and surname
        let first_idx = (hash % FIRST_NAMES.len() as u64) as usize;
        let surname_idx = ((hash / FIRST_NAMES.len() as u64) % SURNAMES.len() as u64) as usize;

        let mut candidate = format!("{}_{}", FIRST_NAMES[first_idx], SURNAMES[surname_idx]);

        // If name is already used (collision or explicit), add a number
        let mut suffix = 2;
        while self.used_names.contains(&candidate) {
            candidate = format!(
                "{}_{}_{}",
                FIRST_NAMES[first_idx], SURNAMES[surname_idx], suffix
            );
            suffix += 1;
        }

        self.used_names.insert(candidate.clone());
        self.name_count += 1;
        candidate
    }

    /// Simple hash function (FNV-1a inspired)
    fn simple_hash(&self, s: &str) -> u64 {
        let mut hash: u64 = 0xcbf29ce484222325;
        for byte in s.bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }

    /// Print summary of mappings
    fn print_summary(&self) {
        eprintln!("\nAnonymization complete:");
        eprintln!("  Explicit mappings: {}", self.explicit_maps.len());
        eprintln!("  Generated names: {}", self.generated_maps.len());
        eprintln!("  Total unique names: {}", self.used_names.len());
    }
}

fn anonymize_csv(
    input: &PathBuf,
    output: &PathBuf,
    key: &str,
    map: &str,
    columns: &[String],
) -> Result<()> {
    use edgar_defense_toolkit::pipeline;

    if key.is_empty() {
        return Err(anyhow::anyhow!(
            "Anonymization key is required. Set BBO_ANON_KEY env var or use --key"
        ));
    }

    // Load tinyurl → Board_ID mapping from lookup file if available
    let lookup_path = pipeline::derive_lookup_path(input);
    let board_id_map = if lookup_path.exists() {
        eprintln!("Loading Board_ID mappings from: {}", lookup_path.display());
        pipeline::load_lookup_board_ids(&lookup_path)?
    } else {
        std::collections::HashMap::new()
    };

    // Read and preprocess input CSV to fix BBO's malformed quoting
    let csv_data = read_bbo_csv_fixed(input)?;
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_reader(csv_data.as_bytes());
    let headers = reader.headers()?.clone();

    // Find column indices to anonymize
    let col_indices: Vec<usize> = columns
        .iter()
        .filter_map(|col| headers.iter().position(|h| h == col))
        .collect();

    if col_indices.is_empty() {
        return Err(anyhow::anyhow!(
            "None of the specified columns ({}) found in CSV",
            columns.join(", ")
        ));
    }

    // Find LIN_URL column for special handling (contains embedded usernames)
    let lin_url_idx = headers.iter().position(|h| h == "LIN_URL");
    let bbo_col_idx = headers.iter().position(|h| h == "BBO");

    eprintln!(
        "Anonymizing columns: {:?}{}",
        col_indices
            .iter()
            .map(|&i| headers.get(i).unwrap_or("?"))
            .collect::<Vec<_>>(),
        if lin_url_idx.is_some() {
            " + LIN_URL (embedded names)"
        } else {
            ""
        }
    );

    // Create anonymizer
    let mut anonymizer = Anonymizer::new(key, map);

    // Count rows for progress
    let total_rows = count_csv_rows(input)?;

    // Open output
    let mut writer = Writer::from_path(output).context("Failed to create output CSV")?;
    writer.write_record(&headers)?;

    let mut processed = 0;

    for result in reader.records() {
        let record = result.context("Failed to read CSV row")?;
        processed += 1;

        if processed % 1000 == 0 {
            eprint!("\r[{}/{}] Anonymizing...    ", processed, total_rows);
            std::io::stderr().flush().ok();
        }

        // Build output record with anonymized columns
        let mut output_fields: Vec<String> = Vec::with_capacity(record.len());

        for (i, field) in record.iter().enumerate() {
            if col_indices.contains(&i) && !field.is_empty() {
                output_fields.push(anonymizer.anonymize(field));
            } else if Some(i) == lin_url_idx && !field.is_empty() {
                // Special handling for LIN_URL - anonymize embedded player names
                output_fields.push(anonymize_lin_url(field, &mut anonymizer));
            } else if Some(i) == bbo_col_idx && !field.is_empty() && !board_id_map.is_empty() {
                // Replace tinyurl with Board_ID
                let key = pipeline::normalize_tinyurl(field);
                if let Some((board_id, _)) = board_id_map.get(&key) {
                    output_fields.push(board_id.clone());
                } else {
                    output_fields.push(field.to_string());
                }
            } else {
                output_fields.push(field.to_string());
            }
        }

        writer.write_record(&output_fields)?;
    }

    writer.flush()?;
    eprint!("\r[{}/{}] Anonymizing...    ", processed, total_rows);
    anonymizer.print_summary();

    Ok(())
}

/// Anonymize player names embedded in a BBO LIN URL
/// LIN URLs contain player names in pn| tags, which may be URL-encoded:
/// - Literal: pn|player1,player2,player3,player4|
/// - Encoded: pn%7Cplayer1%2Cplayer2%2Cplayer3%2Cplayer4%7C
fn anonymize_lin_url(url: &str, anonymizer: &mut Anonymizer) -> String {
    lazy_static::lazy_static! {
        // Match URL-encoded format: pn%7C...%7C (where %7C = | and names separated by %2C = ,)
        static ref PN_ENCODED: Regex = Regex::new(r"(?i)pn%7C([^%]+(?:%2C[^%]+)*)%7C").unwrap();
        // Match literal format: pn|...|
        static ref PN_LITERAL: Regex = Regex::new(r"pn\|([^|]+)\|").unwrap();
    }

    // Try URL-encoded format first (more common in BBO URLs)
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

    // If no encoded match, try literal format
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
// Display Hand Implementation
// ============================================================================

fn display_hand(input: &PathBuf, row_num: usize) -> Result<()> {
    if row_num == 0 {
        return Err(anyhow::anyhow!("Row number must be 1 or greater"));
    }

    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_path(input)
        .context("Failed to open input CSV")?;
    let headers = reader.headers()?.clone();

    // Find required columns
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

    // Skip to the requested row
    let record = reader
        .records()
        .nth(row_num - 1)
        .ok_or_else(|| anyhow::anyhow!("Row {} not found in file", row_num))?
        .context("Failed to read CSV row")?;

    // Extract data
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

    // Print header
    println!(
        "\n{:=^80}",
        format!(" Hand #{} (Ref: {}) ", row_num, ref_num)
    );

    // Print contract info
    println!(
        "\nContract: {} by {}    Result: {}",
        contract, declarer, result
    );
    println!(
        "Players: N={} S={} E={} W={}",
        north_player, south_player, east_player, west_player
    );

    // Print the deal in a nice format
    println!("\n{:^80}", "DEAL");
    println!("{:-<80}", "");

    // Parse and display hands
    let format_suit = |hand: &str, suit_char: char| -> String {
        // Hand format: "S:AKQ H:JT9 D:876 C:5432" or similar
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

    // Print North
    println!("{:^40}", "North");
    for line in &north_lines {
        println!("{:^40}", line);
    }

    // Print West and East side by side
    println!();
    println!("{:<20}{:>20}", "West", "East");
    for i in 0..4 {
        println!("{:<20}{:>20}", west_lines[i], east_lines[i]);
    }

    // Print South
    println!();
    println!("{:^40}", "South");
    for line in &south_lines {
        println!("{:^40}", line);
    }

    // Print cardplay
    println!("\n{:=^80}", " CARDPLAY ");

    if cardplay.is_empty() {
        println!("(No cardplay recorded)");
    } else {
        // Determine initial leader (left of declarer)
        let initial_leader = match declarer.chars().next() {
            Some('N') => 'E',
            Some('E') => 'S',
            Some('S') => 'W',
            Some('W') => 'N',
            _ => '?',
        };

        // Parse DD analysis into a map: trick_num -> costs
        let mut dd_costs: HashMap<usize, Vec<u8>> = HashMap::new();
        if !dd_analysis.is_empty() && !dd_analysis.starts_with("ERROR") {
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
        }

        // Print header
        println!(
            "\n{:>5} | {:^8} {:^8} {:^8} {:^8} | {:^20}",
            "Trick", "Leader", "2nd", "3rd", "4th", "DD Cost (L/2/3/4)"
        );
        println!("{:-<80}", "");

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

            // Get seat order
            let seats = get_seat_order(current_leader);

            // Format cards with seats
            let card_strs: Vec<String> = cards
                .iter()
                .enumerate()
                .map(|(i, c)| format!("{}:{}", seats[i], c))
                .collect();

            // Get DD costs for this trick
            let costs = dd_costs.get(&trick_num);
            let cost_str = if let Some(c) = costs {
                format!("{},{},{},{}", c[0], c[1], c[2], c[3])
            } else {
                "-".to_string()
            };

            println!(
                "{:>5} | {:^8} {:^8} {:^8} {:^8} | {:^20}",
                trick_num,
                card_strs.first().map(|s| s.as_str()).unwrap_or("-"),
                card_strs.get(1).map(|s| s.as_str()).unwrap_or("-"),
                card_strs.get(2).map(|s| s.as_str()).unwrap_or("-"),
                card_strs.get(3).map(|s| s.as_str()).unwrap_or("-"),
                cost_str
            );

            // Determine trick winner for next trick's leader
            // We need to look at the actual cards to determine the winner
            if let Some(winner_seat) =
                determine_trick_winner_for_display(&cards, current_leader, contract)
            {
                current_leader = winner_seat;
            }
        }
    }

    // Print DD Analysis summary
    if !dd_analysis.is_empty() && !dd_analysis.starts_with("ERROR") {
        println!("\n{:=^80}", " DD ANALYSIS SUMMARY ");

        // Count total cost by seat
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

                    // Determine next leader
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

        // Determine declaring side
        let declaring_seats: [char; 2] = match declarer.chars().next() {
            Some('N') | Some('S') => ['N', 'S'],
            Some('E') | Some('W') => ['E', 'W'],
            _ => ['?', '?'],
        };

        println!(
            "\n{:<10} {:>10} {:>10} {:>12} {:>10}",
            "Seat", "Plays", "Errors", "Total Cost", "Role"
        );
        println!("{:-<60}", "");

        for seat in ['N', 'E', 'S', 'W'] {
            let plays = seat_plays.get(&seat).unwrap_or(&0);
            let errors = seat_errors.get(&seat).unwrap_or(&0);
            let cost = seat_costs.get(&seat).unwrap_or(&0);
            let role = if declaring_seats.contains(&seat) {
                "Declaring"
            } else {
                "Defending"
            };

            println!(
                "{:<10} {:>10} {:>10} {:>12} {:>10}",
                seat, plays, errors, cost, role
            );
        }
    } else if dd_analysis.starts_with("ERROR") {
        println!("\n{:=^80}", " DD ANALYSIS ");
        println!("Error: {}", dd_analysis);
    }

    println!("\n{:=^80}", "");

    Ok(())
}

/// Determine trick winner based on cards played (for display purposes)
fn determine_trick_winner_for_display(
    cards: &[&str],
    leader: char,
    contract: &str,
) -> Option<char> {
    if cards.len() != 4 {
        return None;
    }

    // Parse trump suit from contract
    let trump = if contract.contains('N') {
        None // NT
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

    // Parse cards
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

    // Lead suit
    let lead_suit = parsed[0].map(|(s, _)| s)?;

    // Find winner
    let mut winner_idx = 0;
    let mut winning_card = parsed[0]?;

    for (i, card_opt) in parsed.iter().enumerate().skip(1) {
        if let Some((suit, rank)) = card_opt {
            let dominated = if let Some(t) = trump {
                // Trump beats non-trump
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
                // No trump: must follow suit
                *suit == lead_suit && *rank > winning_card.1
            };

            if dominated {
                winner_idx = i;
                winning_card = (*suit, *rank);
            }
        }
    }

    // Map winner index to seat
    let seats = get_seat_order(leader);
    Some(seats[winner_idx])
}

// ============================================================================
// Stats Implementation
// ============================================================================

/// Statistics for a player
#[derive(Default, Clone)]
struct PlayerStats {
    name: String,
    // Total deals where this player participated (including as dummy)
    total_deals: u64,
    // Declaring stats
    declaring_plays: u64,
    declaring_errors: u64,
    declaring_deals: u64,
    // Defending stats
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

    fn total_deals(&self) -> u64 {
        self.total_deals
    }

    /// Merge another player's stats into this one (for "Field" aggregation)
    fn merge(&mut self, other: &PlayerStats) {
        self.total_deals += other.total_deals;
        self.declaring_plays += other.declaring_plays;
        self.declaring_errors += other.declaring_errors;
        self.declaring_deals += other.declaring_deals;
        self.defending_plays += other.defending_plays;
        self.defending_errors += other.defending_errors;
        self.defending_deals += other.defending_deals;
    }

    /// 95% confidence interval half-width for error rate (using normal approximation)
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

    /// Calculate the Def - Decl difference (expected to be positive for honest players)
    fn def_minus_decl(&self) -> f64 {
        self.defending_error_rate() - self.declaring_error_rate()
    }

    /// Standard error for the Def - Decl difference
    fn diff_se(&self) -> f64 {
        if self.declaring_plays < 30 || self.defending_plays < 30 {
            return f64::NAN;
        }
        let p1 = self.declaring_errors as f64 / self.declaring_plays as f64;
        let n1 = self.declaring_plays as f64;
        let p2 = self.defending_errors as f64 / self.defending_plays as f64;
        let n2 = self.defending_plays as f64;
        // SE of difference of two proportions
        ((p1 * (1.0 - p1) / n1) + (p2 * (1.0 - p2) / n2)).sqrt() * 100.0
    }
}

/// Z-test comparing two players' Def-Decl differences
/// Returns (z-score, p-value) for one-tailed test
fn z_test_diff_vs_baseline(subject: &PlayerStats, baseline: &PlayerStats) -> (f64, f64) {
    let diff_subj = subject.def_minus_decl();
    let diff_base = baseline.def_minus_decl();

    let se_subj = subject.diff_se();
    let se_base = baseline.diff_se();

    if se_subj.is_nan() || se_base.is_nan() {
        return (f64::NAN, f64::NAN);
    }

    // Combined SE for comparing two differences
    let se_combined = (se_subj.powi(2) + se_base.powi(2)).sqrt();

    // Z-score: how many SEs is subject's diff below baseline's diff?
    let z = (diff_subj - diff_base) / se_combined;

    // One-tailed p-value (testing if subject's diff is significantly LOWER than baseline)
    // P(Z <= z) where z is negative when subject has smaller gap than baseline
    // This gives the probability of seeing a gap this small or smaller by chance
    let p = 0.5 * (1.0 + erf(z / std::f64::consts::SQRT_2));

    (z, p)
}

/// Error function approximation (for p-value calculation)
fn erf(x: f64) -> f64 {
    // Horner form coefficients for erf approximation
    let a1 = 0.254829592;
    let a2 = -0.284496736;
    let a3 = 1.421413741;
    let a4 = -1.453152027;
    let a5 = 1.061405429;
    let p = 0.3275911;

    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs();
    let t = 1.0 / (1.0 + p * x);
    let y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-x * x).exp();
    sign * y
}

fn compute_stats(input: &PathBuf, top_n: usize, output: Option<&PathBuf>) -> Result<()> {
    // Read input CSV
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_path(input)
        .context("Failed to open input CSV")?;
    let headers = reader.headers()?.clone();

    // Find required columns
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

    // Find the new per-seat DD columns
    let dd_n_plays_col = headers
        .iter()
        .position(|h| h == "DD_N_Plays")
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Column 'DD_N_Plays' not found - run analyze-dd first with updated version"
            )
        })?;
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

    // Collect stats per player
    let mut player_stats: HashMap<String, PlayerStats> = HashMap::new();
    // Track partnership deal counts: (player1, player2) -> deal_count
    // Normalized so player1 < player2 alphabetically
    let mut partnership_counts: HashMap<(String, String), u64> = HashMap::new();
    let mut processed = 0;
    let mut skipped = 0;

    for result in reader.records() {
        let record = result.context("Failed to read CSV row")?;
        processed += 1;

        // Get player names (lowercase for consistency)
        let north = record.get(n_col).unwrap_or("").to_lowercase();
        let south = record.get(s_col).unwrap_or("").to_lowercase();
        let east = record.get(e_col).unwrap_or("").to_lowercase();
        let west = record.get(w_col).unwrap_or("").to_lowercase();

        // Track partnerships (N-S and E-W are partners)
        if !north.is_empty() && !south.is_empty() {
            let key = if north < south {
                (north.clone(), south.clone())
            } else {
                (south.clone(), north.clone())
            };
            *partnership_counts.entry(key).or_insert(0) += 1;
        }
        if !east.is_empty() && !west.is_empty() {
            let key = if east < west {
                (east.clone(), west.clone())
            } else {
                (west.clone(), east.clone())
            };
            *partnership_counts.entry(key).or_insert(0) += 1;
        }

        // Get declarer
        let declarer = record.get(dec_col).unwrap_or("").trim().to_uppercase();
        if declarer.is_empty() {
            skipped += 1;
            continue;
        }

        // Get per-seat DD plays and errors
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

        // Skip rows with no DD data (all plays are 0 means no cardplay analyzed)
        if n_plays == 0 && s_plays == 0 && e_plays == 0 && w_plays == 0 {
            skipped += 1;
            continue;
        }

        // Determine declarer, dummy, and defenders based on declarer direction
        let (declarer_name, dummy_name, _def1_name, _def2_name) = match declarer.chars().next() {
            Some('N') => (&north, &south, &east, &west),
            Some('S') => (&south, &north, &east, &west),
            Some('E') => (&east, &west, &north, &south),
            Some('W') => (&west, &east, &north, &south),
            _ => {
                skipped += 1;
                continue;
            }
        };

        // Map seat plays/errors to player names and roles
        // Declarer side: declarer + dummy plays/errors go to declarer's declaring stats
        // Defense side: each defender's plays/errors go to their own defending stats
        let seat_data = [
            (&north, 'N', n_plays, n_errors),
            (&south, 'S', s_plays, s_errors),
            (&east, 'E', e_plays, e_errors),
            (&west, 'W', w_plays, w_errors),
        ];

        for (player_name, _seat, plays, errors) in &seat_data {
            if player_name.is_empty() {
                continue;
            }

            let is_declarer = *player_name == declarer_name;
            let is_dummy = *player_name == dummy_name;
            let is_declaring_side = is_declarer || is_dummy;

            if is_declaring_side {
                // Declaring side plays/errors go to DECLARER's stats (not dummy)
                let stats = player_stats
                    .entry(declarer_name.clone())
                    .or_insert_with(|| PlayerStats::new(declarer_name));
                stats.declaring_plays += plays;
                stats.declaring_errors += errors;
            } else {
                // Defender's plays/errors go to their own stats
                let stats = player_stats
                    .entry((*player_name).clone())
                    .or_insert_with(|| PlayerStats::new(player_name));
                stats.defending_plays += plays;
                stats.defending_errors += errors;
            }
        }

        // Track deals per player
        for (player_name, _seat, _, _) in &seat_data {
            if player_name.is_empty() {
                continue;
            }
            let stats = player_stats
                .entry((*player_name).clone())
                .or_insert_with(|| PlayerStats::new(player_name));

            // All four players increment total_deals
            stats.total_deals += 1;

            // Only declarer counts as "declaring", only defenders count as "defending"
            // Dummy doesn't count for either
            if *player_name == declarer_name {
                stats.declaring_deals += 1;
            } else if *player_name != dummy_name {
                stats.defending_deals += 1;
            }
        }
    }

    eprintln!("Processed {} deals ({} skipped)", processed, skipped);
    eprintln!("Found {} unique players\n", player_stats.len());

    // Sort players by total deals (frequency)
    let mut players: Vec<_> = player_stats.values().cloned().collect();
    players.sort_by_key(|b| std::cmp::Reverse(b.total_deals()));

    // Identify top 2 players (the subjects)
    let top_2: HashSet<String> = players.iter().take(2).map(|p| p.name.clone()).collect();

    // Create "Field" by aggregating everyone except top 2
    let mut field_stats = PlayerStats::new("FIELD");
    for player in &players {
        if !top_2.contains(&player.name) {
            field_stats.merge(player);
        }
    }

    // Print header
    println!("\n{:=^126}", " DD Error Rate Analysis ");
    println!(
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
    );
    println!("{:-<126}", "");

    // Print top N players
    for player in players.iter().take(top_n) {
        let decl_rate = player.declaring_error_rate();
        let def_rate = player.defending_error_rate();
        let diff = decl_rate - def_rate;
        // Relative percent: how much better/worse is defense vs declaring
        // Negative means defense is better (fewer errors), positive means worse
        let rel_pct = if decl_rate > 0.0 {
            -diff / decl_rate * 100.0
        } else {
            0.0
        };
        let decl_ci = player.declaring_ci();
        let def_ci = player.defending_ci();

        println!(
            "{:<20} {:>8} {:>6} {:>6} {:>12} {:>9.2}% {:>12} {:>9.2}% {:>+9.2}% {:>+7.1}%",
            truncate_name(&player.name, 20),
            player.total_deals(),
            player.declaring_deals,
            player.defending_deals,
            player.declaring_plays,
            decl_rate,
            player.defending_plays,
            def_rate,
            diff,
            rel_pct
        );

        // Print confidence intervals on separate line if enough data
        if !decl_ci.is_nan() || !def_ci.is_nan() {
            println!(
                "{:<20} {:>8} {:>6} {:>6} {:>12} {:>10} {:>12} {:>10}",
                "",
                "",
                "",
                "",
                format!("(±{:.2}%)", decl_ci),
                "",
                format!("(±{:.2}%)", def_ci),
                ""
            );
        }
    }

    // Print Field aggregate
    println!("{:-<126}", "");
    let decl_rate = field_stats.declaring_error_rate();
    let def_rate = field_stats.defending_error_rate();
    let diff = decl_rate - def_rate;
    let rel_pct = if decl_rate > 0.0 {
        -diff / decl_rate * 100.0
    } else {
        0.0
    };

    println!(
        "{:<20} {:>8} {:>6} {:>6} {:>12} {:>9.2}% {:>12} {:>9.2}% {:>+9.2}% {:>+7.1}%",
        "FIELD (others)",
        field_stats.total_deals(),
        field_stats.declaring_deals,
        field_stats.defending_deals,
        field_stats.declaring_plays,
        decl_rate,
        field_stats.defending_plays,
        def_rate,
        diff,
        rel_pct
    );
    println!(
        "{:<20} {:>8} {:>6} {:>6} {:>12} {:>10} {:>12} {:>10}",
        "",
        "",
        "",
        "",
        format!("(±{:.2}%)", field_stats.declaring_ci()),
        "",
        format!("(±{:.2}%)", field_stats.defending_ci()),
        ""
    );

    // Partner Comparison Section (if we have at least 2 players)
    if players.len() >= 2 {
        let subj_a = &players[0];
        let subj_b = &players[1];

        println!("\n{:=^100}", " Partner Comparison ");
        println!("\nComparing {} vs {}:", subj_a.name, subj_b.name);

        // Declaring comparison
        let decl_gap = subj_a.declaring_error_rate() - subj_b.declaring_error_rate();
        println!("\n  DECLARING:");
        println!(
            "    {:<20}: {:.2}% error rate",
            subj_a.name,
            subj_a.declaring_error_rate()
        );
        println!(
            "    {:<20}: {:.2}% error rate",
            subj_b.name,
            subj_b.declaring_error_rate()
        );
        println!(
            "    Skill gap: {:+.2}% ({} has more errors declaring)",
            decl_gap,
            if decl_gap > 0.0 {
                &subj_a.name
            } else {
                &subj_b.name
            }
        );

        // Defending comparison
        let def_gap = subj_a.defending_error_rate() - subj_b.defending_error_rate();
        println!("\n  DEFENDING:");
        println!(
            "    {:<20}: {:.2}% error rate",
            subj_a.name,
            subj_a.defending_error_rate()
        );
        println!(
            "    {:<20}: {:.2}% error rate",
            subj_b.name,
            subj_b.defending_error_rate()
        );
        println!(
            "    Skill gap: {:+.2}% ({} has more errors defending)",
            def_gap,
            if def_gap > 0.0 {
                &subj_a.name
            } else {
                &subj_b.name
            }
        );

        // Convergence analysis
        println!("\n  CONVERGENCE ANALYSIS:");
        let convergence = decl_gap.abs() - def_gap.abs();
        if convergence > 1.0 {
            println!("    ⚠️  Skill gap NARROWS by {:.2}% on defense (declaring gap: {:.2}%, defense gap: {:.2}%)",
                convergence, decl_gap.abs(), def_gap.abs());
            println!("    This pattern (partners performing more similarly on defense) can indicate hand sharing.");
        } else if convergence < -1.0 {
            println!(
                "    Skill gap WIDENS by {:.2}% on defense - consistent with honest play",
                -convergence
            );
        } else {
            println!(
                "    Skill gap is similar in both roles ({:.2}% declaring, {:.2}% defending)",
                decl_gap.abs(),
                def_gap.abs()
            );
        }

        // Statistical Test Section
        println!("\n{:=^100}", " Statistical Analysis ");

        // Compare each subject to Field baseline
        for subj in [subj_a, subj_b] {
            let subj_diff = subj.def_minus_decl();
            let field_diff = field_stats.def_minus_decl();
            let (z, p) = z_test_diff_vs_baseline(subj, &field_stats);

            println!("\n  {} vs FIELD baseline:", subj.name);
            println!("    {} Def-Decl diff: {:+.2}%", subj.name, subj_diff);
            println!("    FIELD Def-Decl diff:      {:+.2}%", field_diff);

            if !z.is_nan() {
                println!("    Z-score: {:.2}", z);
                if p < 0.001 {
                    println!("    P-value: <0.001 (highly significant)");
                } else if p < 0.01 {
                    println!("    P-value: {:.4} (significant at 1%)", p);
                } else if p < 0.05 {
                    println!("    P-value: {:.4} (significant at 5%)", p);
                } else {
                    println!("    P-value: {:.4} (not statistically significant)", p);
                }

                if z < -1.96 {
                    println!("    ⚠️  {}'s defense error rate is SUSPICIOUSLY LOW relative to their declaring rate", subj.name);
                } else if z > 1.96 {
                    println!("    ✓ {}'s pattern is NORMAL - defense errors exceed declaring as expected", subj.name);
                } else {
                    println!("    Results inconclusive - need more data for reliable inference");
                }
            } else {
                println!("    (Insufficient data for statistical test)");
            }
        }
    }

    println!("\n{:=^100}", "");
    println!("\nInterpretation:");
    println!("  - Decl Err%: Percentage of plays with DD cost > 0 when declaring/dummy");
    println!("  - Def Err%:  Percentage of plays with DD cost > 0 when defending");
    println!("  - Diff:      Decl% - Def% (negative means more errors on defense)");
    println!("\n  EXPECTED for honest players:");
    println!("    Defense is HARDER than declaring (defender sees fewer cards)");
    println!("    So honest players typically have MORE errors on defense (negative Diff)");
    println!("    The FIELD baseline shows the typical Def-Decl difference");
    println!("\n  RED FLAGS for potential hand-sharing:");
    println!("    - Defense error rate LOWER than declaring (positive Diff)");
    println!("    - Def-Decl pattern significantly different from FIELD");
    println!("    - Partners' skill gap narrowing on defense vs declaring");
    println!("\n  STATISTICAL MEASURES:");
    println!(
        "    Z-score: How many standard deviations a player's pattern differs from the FIELD."
    );
    println!(
        "             Z < -1.96 means suspiciously better defense (only 2.5% chance if honest)."
    );
    println!("             Z > +1.96 means normal pattern (defense harder than declaring).");
    println!("    P-value: Probability of seeing this result if the player were honest.");
    println!("             P < 0.05 = significant (less than 5% chance if honest).");
    println!("             P < 0.01 = highly significant (less than 1% chance if honest).");

    // Suspicious Players Table: Def-Decl > 0.05% (defense better than declaring) and p < 0.20
    // Require minimum 50 deals for statistical reliability
    const MIN_DEALS_FOR_SUSPICIOUS: u64 = 50;
    let mut suspicious: Vec<_> = players
        .iter()
        .filter_map(|p| {
            // Skip players with insufficient data
            if p.total_deals() < MIN_DEALS_FOR_SUSPICIOUS {
                return None;
            }
            let def_minus_decl = p.def_minus_decl();
            // We want defense BETTER than declaring, which means def_err% < decl_err%
            // def_minus_decl = def% - decl%, so positive means more defense errors (normal)
            // We want NEGATIVE def_minus_decl (fewer defense errors = suspicious)
            // But user said "def-decl > 0.05%" - clarifying: they mean improvement in defense
            // i.e., declaring error rate > defending error rate by more than 0.05%
            // That's decl% - def% > 0.05, which is def_minus_decl < -0.05
            if def_minus_decl < -0.05 {
                let (z, p_val) = z_test_diff_vs_baseline(p, &field_stats);
                if !p_val.is_nan() && p_val < 0.20 {
                    Some((p.clone(), def_minus_decl, z, p_val))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    // Sort by p-value (most significant first)
    suspicious.sort_by(|a, b| a.3.partial_cmp(&b.3).unwrap_or(std::cmp::Ordering::Equal));

    if !suspicious.is_empty() {
        // Build set of suspicious player names for partnership lookup
        let suspicious_names: HashSet<String> = suspicious
            .iter()
            .map(|(p, _, _, _)| p.name.clone())
            .collect();

        // Find partnerships where both players are on the suspicious list
        // Partnership requires 60% of the smaller player's deals to be with this partner
        let mut partner_annotations: HashMap<String, usize> = HashMap::new();
        let mut next_partner_num = 1usize;

        for (player, _, _, _) in &suspicious {
            if partner_annotations.contains_key(&player.name) {
                continue; // Already assigned a partnership number
            }

            // Find this player's most frequent partner who is also suspicious
            let mut best_partner: Option<(String, u64)> = None;
            for ((p1, p2), &count) in &partnership_counts {
                let partner_name = if p1 == &player.name {
                    p2.clone()
                } else if p2 == &player.name {
                    p1.clone()
                } else {
                    continue;
                };

                if suspicious_names.contains(&partner_name)
                    && !partner_annotations.contains_key(&partner_name)
                {
                    // Check if this partnership is >= 60% of the smaller player's deals
                    let partner_stats = suspicious
                        .iter()
                        .find(|(p, _, _, _)| p.name == partner_name)
                        .map(|(p, _, _, _)| p);

                    if let Some(partner) = partner_stats {
                        let min_deals = player.total_deals().min(partner.total_deals());
                        let partnership_pct = count as f64 / min_deals as f64;

                        if partnership_pct >= 0.60
                            && (best_partner.is_none() || count > best_partner.as_ref().unwrap().1)
                        {
                            best_partner = Some((partner_name.clone(), count));
                        }
                    }
                }
            }

            // If we found a qualifying partner, assign them the same number
            if let Some((partner_name, _)) = best_partner {
                partner_annotations.insert(player.name.clone(), next_partner_num);
                partner_annotations.insert(partner_name, next_partner_num);
                next_partner_num += 1;
            }
        }

        println!(
            "\n{:=^124}",
            " Suspicious Patterns (Def better than Decl, p < 20%) "
        );
        println!(
            "\n{:<24} {:>8} {:>6} {:>6} {:>10} {:>10} {:>12} {:>8} {:>10} {:>10}",
            "Player",
            "Deals",
            "Decl",
            "Def",
            "Decl Err%",
            "Def Err%",
            "Def-Decl",
            "Rel%",
            "Z-score",
            "P-value"
        );
        println!("{:-<128}", "");

        for (player, def_minus_decl, z, p_val) in &suspicious {
            // Annotate player name with partnership number if applicable
            let display_name = if let Some(&num) = partner_annotations.get(&player.name) {
                format!("{} ({})", truncate_name(&player.name, 17), num)
            } else {
                truncate_name(&player.name, 24)
            };

            // Relative percent: how much better defense is vs declaring
            // Negative def_minus_decl means defense is better, so rel_pct is positive improvement
            let decl_rate = player.declaring_error_rate();
            let rel_pct = if decl_rate > 0.0 {
                -def_minus_decl / decl_rate * 100.0
            } else {
                0.0
            };

            println!(
                "{:<24} {:>8} {:>6} {:>6} {:>9.2}% {:>9.2}% {:>+11.2}% {:>+7.1}% {:>10.2} {:>9.4}",
                display_name,
                player.total_deals(),
                player.declaring_deals,
                player.defending_deals,
                decl_rate,
                player.defending_error_rate(),
                def_minus_decl,
                rel_pct,
                z,
                p_val
            );
        }
        println!("{:-<128}", "");
        println!("Note: These players show defense error rates LOWER than their declaring rates,");
        println!("      which is unusual (defense is typically harder than declaring).");
        if !partner_annotations.is_empty() {
            println!("      Numbers in parentheses indicate players who are partners (60%+ of deals together).");
        }
        // Count players with vs without partner annotations
        let partnered_count = suspicious
            .iter()
            .filter(|(p, _, _, _)| partner_annotations.contains_key(&p.name))
            .count();
        let non_partnered_count = suspicious.len() - partnered_count;
        if non_partnered_count > partnered_count {
            println!("      The majority of flagged players ({} of {}) are NOT partnered with others on this list,",
                non_partnered_count, suspicious.len());
            println!("      suggesting the Def-Decl pattern may be driven by factors other than hand-sharing");
            println!("      (e.g., natural defensive skill, bidding style) and warrants further analysis.");
        }
    }

    // Write detailed CSV if requested
    if let Some(output_path) = output {
        let mut writer = Writer::from_path(output_path).context("Failed to create output CSV")?;

        writer.write_record([
            "Player",
            "Total_Deals",
            "Decl_Deals",
            "Def_Deals",
            "Decl_Plays",
            "Decl_Errors",
            "Decl_Err_Pct",
            "Decl_CI",
            "Def_Plays",
            "Def_Errors",
            "Def_Err_Pct",
            "Def_CI",
            "Diff_Pct",
        ])?;

        for player in &players {
            writer.write_record([
                &player.name,
                &player.total_deals().to_string(),
                &player.declaring_deals.to_string(),
                &player.defending_deals.to_string(),
                &player.declaring_plays.to_string(),
                &player.declaring_errors.to_string(),
                &format!("{:.4}", player.declaring_error_rate()),
                &format!("{:.4}", player.declaring_ci()),
                &player.defending_plays.to_string(),
                &player.defending_errors.to_string(),
                &format!("{:.4}", player.defending_error_rate()),
                &format!("{:.4}", player.defending_ci()),
                &format!(
                    "{:.4}",
                    player.declaring_error_rate() - player.defending_error_rate()
                ),
            ])?;
        }

        // Add Field row
        writer.write_record([
            "FIELD",
            &field_stats.total_deals().to_string(),
            &field_stats.declaring_deals.to_string(),
            &field_stats.defending_deals.to_string(),
            &field_stats.declaring_plays.to_string(),
            &field_stats.declaring_errors.to_string(),
            &format!("{:.4}", field_stats.declaring_error_rate()),
            &format!("{:.4}", field_stats.declaring_ci()),
            &field_stats.defending_plays.to_string(),
            &field_stats.defending_errors.to_string(),
            &format!("{:.4}", field_stats.defending_error_rate()),
            &format!("{:.4}", field_stats.defending_ci()),
            &format!(
                "{:.4}",
                field_stats.declaring_error_rate() - field_stats.defending_error_rate()
            ),
        ])?;

        writer.flush()?;
        eprintln!("\nDetailed stats written to: {}", output_path.display());
    }

    Ok(())
}

/// Get seat order starting from leader going clockwise
fn get_seat_order(leader: char) -> [char; 4] {
    match leader {
        'N' => ['N', 'E', 'S', 'W'],
        'E' => ['E', 'S', 'W', 'N'],
        'S' => ['S', 'W', 'N', 'E'],
        'W' => ['W', 'N', 'E', 'S'],
        _ => ['N', 'E', 'S', 'W'],
    }
}

/// Truncate a name to fit in a column
fn truncate_name(name: &str, max_len: usize) -> String {
    if name.len() <= max_len {
        name.to_string()
    } else {
        format!("{}...", &name[..max_len - 3])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_trump() {
        assert_eq!(parse_trump("4S").unwrap(), SPADE);
        assert_eq!(parse_trump("3NT").unwrap(), NOTRUMP);
        assert_eq!(parse_trump("6H").unwrap(), HEART);
        assert_eq!(parse_trump("2D").unwrap(), DIAMOND);
        assert_eq!(parse_trump("5C").unwrap(), CLUB);
    }

    #[test]
    fn test_parse_declarer() {
        assert_eq!(parse_declarer("N").unwrap(), NORTH);
        assert_eq!(parse_declarer("E").unwrap(), EAST);
        assert_eq!(parse_declarer("S").unwrap(), SOUTH);
        assert_eq!(parse_declarer("W").unwrap(), WEST);
    }

    #[test]
    fn test_parse_cardplay() {
        let tricks = parse_cardplay("D2 DA D6 D5|S3 S2 SQ SA").unwrap();
        assert_eq!(tricks.len(), 2);
        assert_eq!(tricks[0].len(), 4);
        assert_eq!(tricks[1].len(), 4);
    }
}
