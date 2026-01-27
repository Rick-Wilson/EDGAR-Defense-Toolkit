# EDGAR Defense Toolkit

**E**rror **D**etection for **G**ame **A**nalysis and **R**eview

Tools for analyzing bridge cardplay using double-dummy analysis to detect suspicious patterns.

## Purpose

Bridge players facing accusations of online cheating often receive evidence in the form of thousands of TinyURLs linking to BBO hand records. While these URLs contain the complete cardplay data, accessing it requires manually clicking through each hand in the BBO viewer.

This toolkit automates:
1. **Cardplay extraction** from BBO hand records
2. **Double-dummy analysis** to compute the "cost" of each card played
3. **Statistical analysis** to identify patterns across many hands
4. **Anonymization** for sharing data without exposing player identities

## Installation

```bash
cargo install --git https://github.com/Rick-Wilson/EDGAR-Defense-Toolkit
```

Or build from source:

```bash
git clone https://github.com/Rick-Wilson/EDGAR-Defense-Toolkit
cd EDGAR-Defense-Toolkit
cargo build --release
```

Binaries will be available at:
- `target/release/bbo-csv` - Main analysis tool
- `target/release/dd-debug` - Single-hand debugging utility

## CLI Tools

### bbo-csv

The main CLI for bulk analysis of BBO hand records.

#### Step 1: Extract Cardplay Data

```bash
bbo-csv fetch-cardplay \
  --input "Hand Records.csv" \
  --output "Hand Records with Cardplay.csv" \
  --url-column "BBO" \
  --delay-ms 200 \
  --resume
```

**Options:**
- `--input`: Input CSV file containing TinyURLs
- `--output`: Output CSV file (input + new Cardplay column)
- `--url-column`: Column name containing TinyURLs (default: "BBO")
- `--delay-ms`: Delay between URL resolutions (default: 200ms)
- `--batch-size`: Requests before a longer pause (default: 10)
- `--batch-delay-ms`: Pause duration after each batch (default: 2000ms)
- `--resume`: Skip rows that already have cardplay data

**Output format:**
```
D2 DA D6 D5|S3 S2 SQ SA|DK D4 D3 D9|...
```
Each trick is separated by `|`, cards within a trick by spaces.

#### Step 2: Analyze Double-Dummy Costs

```bash
bbo-csv analyze-dd \
  --input "Hand Records with Cardplay.csv" \
  --output "Hand Records with DD Analysis.csv" \
  --threads 8 \
  --resume
```

**Options:**
- `--input`: CSV with cardplay data
- `--output`: Output CSV with DD analysis
- `--threads`: Parallel processing threads (default: CPU cores)
- `--checkpoint-interval`: Save progress every N rows (default: 100)
- `--resume`: Skip rows that already have DD analysis

**Output format:**
```
T1:0,0,0,0|T2:0,0,1,0|T3:0,0,0,0|...
```
- `0` = optimal or equivalent play
- Positive number = tricks lost by suboptimal play

#### Step 3: View Statistics

```bash
bbo-csv stats --input "Hand Records with DD Analysis.csv" --top-n 20
```

Shows per-player statistics including:
- Total hands played
- Declaring vs defending breakdown
- Average DD cost per hand
- Error frequency

#### Anonymize Data

Replace player names with anonymized identifiers:

```bash
bbo-csv anonymize \
  --input data.csv \
  --output anonymized.csv \
  --key "secret-key-for-hashing"
```

Or use explicit mappings:
```bash
bbo-csv anonymize \
  --input data.csv \
  --output anonymized.csv \
  --map "RealName1=Player1,RealName2=Player2"
```

#### Spot-Check Single Hands

```bash
bbo-csv display-hand --input data.csv --row 42
```

### dd-debug

Debug tool for analyzing a single hand in detail:

```bash
dd-debug http://tinyurl.com/xxxxx

# With mid-trick analysis (slower, more detailed)
dd-debug --mid-trick http://tinyurl.com/xxxxx
```

Shows card-by-card DD values for verification against BBO's handviewer.

## How It Works

### Double-Dummy Analysis

For each card played, the solver computes:
1. **DD Before**: Optimal tricks from position before the card
2. **DD After**: Optimal tricks from position after the card
3. **Cost**: The difference (0 = optimal play)

Costs are attributed to the player who played the card:
- **Declarer side**: Cost if DD decreases (lost tricks)
- **Defender side**: Cost if DD increases (gave away tricks)

### Rate Limiting

The tool includes configurable rate limiting to avoid being blocked:
- Default: 200ms delay between requests
- Batch mode: Longer pause after every N requests
- Automatic retry with backoff on errors

### Resume Functionality

If processing is interrupted, the tool can resume:
- Reads output file to find already-processed rows
- Only processes rows missing the target column
- Safe to run multiple times

## CSV File Format

The tool expects a CSV with at minimum:
- A URL column (default "BBO") containing TinyURLs to BBO hand records

The tool preserves all existing columns and appends new ones:
- `Cardplay`: Extracted cardplay sequence
- `DD_Analysis`: Per-card cost values
- `LIN_URL`: Resolved full BBO URL

## Library Usage

```rust
use edgar_defense_toolkit::dd_analysis::{compute_dd_costs, analyze_board, DdAnalysisConfig};
use bridge_parsers::lin::parse_lin_from_url;

// Parse a hand from BBO URL
let lin_data = parse_lin_from_url("https://www.bridgebase.com/...")?;

// Analyze with mid-trick DD
let config = DdAnalysisConfig::mid_trick();
if let Some(result) = analyze_board(&lin_data, &config) {
    for error in &result.errors {
        println!("{}: {} cost {} tricks", error.player, error.card, error.cost);
    }
}

// Or compute raw costs per card
let costs = compute_dd_costs(
    &deal_pbn,
    &cardplay,
    "4S",      // contract
    "South",   // declarer
    false      // debug output
)?;
```

## Dependencies

- **[bridge-parsers](https://github.com/Rick-Wilson/Bridge-Parsers)**: PBN/BWS/LIN parsing, URL resolution
- **[bridge-solver](https://github.com/Rick-Wilson/bridge-solver)**: Double-dummy solver engine
- **[bridge-types](https://github.com/Rick-Wilson/bridge-types)**: Core bridge data types

## License

Unlicense
