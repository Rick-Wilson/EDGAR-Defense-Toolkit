#!/usr/bin/env python3
"""Fix an unquoted lookup CSV by re-writing it with proper CSV quoting.

The lookup file has 15 columns. Fields like Explanations and LIN_URL may
contain commas, which broke the original unquoted format.  We reassemble
the 15 logical fields by exploiting the fact that fields 0-10 (Board_ID
through Auction) never contain commas, Cardplay (field 12) is always
recognisable (pipe-separated suit+rank pairs or empty), and Claim (field
13) is always a small integer or empty.

Usage:
    python3 fix_lookup_csv.py <lookup_file.csv>

A backup is saved as <file>.bak before overwriting.
"""

import csv
import re
import shutil
import sys
from pathlib import Path

EXPECTED_COLS = 15
# Indices in the logical 15-column layout
IDX_EXPLANATIONS = 11
IDX_CARDPLAY = 12
IDX_CLAIM = 13
IDX_LIN_URL = 14

# Cardplay is pipe-separated tricks of space-separated cards (e.g. "S4 H3 DA CK|...")
# Each card is a suit letter + rank char.  Empty string also OK.
CARDPLAY_RE = re.compile(
    r'^([SHDC][2-9TJQKA]( [SHDC][2-9TJQKA])*'  # first trick
    r'(\|[SHDC][2-9TJQKA]( [SHDC][2-9TJQKA])*)*)?$'  # subsequent tricks
)


def is_cardplay(s: str) -> bool:
    """Return True if s looks like a cardplay string (or is empty)."""
    return s == '' or bool(CARDPLAY_RE.match(s))


def is_claim(s: str) -> bool:
    """Claim is empty or a small integer (0-13)."""
    if s == '':
        return True
    try:
        n = int(s)
        return 0 <= n <= 13
    except ValueError:
        return False


def reassemble_row(raw_fields: list[str]) -> list[str]:
    """Given a raw split with possibly too many fields, return 15 logical fields."""
    n = len(raw_fields)

    if n == EXPECTED_COLS:
        return raw_fields  # already fine

    if n < EXPECTED_COLS:
        # Shouldn't happen much; pad with empty strings
        return raw_fields + [''] * (EXPECTED_COLS - n)

    # We have extra fields due to unquoted commas.
    # Fields 0-10 are always clean (no commas).  Take them as-is.
    head = raw_fields[:11]  # Board_ID .. Auction

    # From the tail end, we know the last field is LIN_URL (may contain commas),
    # second-to-last is Claim (int or empty), and before that is Cardplay.
    # Work backwards from the end to find Cardplay.

    # Strategy: scan backwards from the end looking for the cardplay field.
    # Everything between field 10 (Auction) and Cardplay is Explanations.
    # Everything after Claim to the end is LIN_URL.

    # Find Cardplay: scan from index 11 forward looking for a cardplay match.
    # The first one we find that also has a valid Claim right after it is our match.
    cardplay_idx = None
    for i in range(11, n):
        if is_cardplay(raw_fields[i]):
            # Check if the next field looks like Claim
            if i + 1 < n and is_claim(raw_fields[i + 1]):
                cardplay_idx = i
                break

    if cardplay_idx is None:
        # Fallback: if we can't find it, just rejoin extras into Explanations
        # and hope for the best.  This handles empty-cardplay error rows.
        explanations = ','.join(raw_fields[11:])
        return head + [explanations, '', '', '']

    explanations = ','.join(raw_fields[11:cardplay_idx])
    cardplay = raw_fields[cardplay_idx]
    claim = raw_fields[cardplay_idx + 1] if cardplay_idx + 1 < n else ''
    lin_url = ','.join(raw_fields[cardplay_idx + 2:]) if cardplay_idx + 2 < n else ''

    return head + [explanations, cardplay, claim, lin_url]


def fix_lookup_csv(path: Path) -> None:
    """Read the broken lookup CSV, fix it, write back with proper quoting."""
    # Read raw lines (can't use csv.reader reliably on the broken file)
    text = path.read_text(encoding='utf-8')
    lines = text.splitlines()

    if not lines:
        print("Empty file, nothing to do.")
        return

    header_line = lines[0]
    header_fields = header_line.split(',')

    if len(header_fields) != EXPECTED_COLS:
        print(f"Warning: header has {len(header_fields)} fields, expected {EXPECTED_COLS}")
        print(f"Header: {header_line[:200]}")

    # Backup
    backup = path.with_suffix(path.suffix + '.bak')
    shutil.copy2(path, backup)
    print(f"Backup saved to {backup}")

    rows_fixed = 0
    rows_total = 0
    output_rows = []

    # Keep header as-is (it has no commas in field values)
    output_rows.append(header_fields)

    for line in lines[1:]:
        if not line.strip():
            continue
        rows_total += 1
        raw = line.split(',')
        if len(raw) != EXPECTED_COLS:
            rows_fixed += 1
        logical = reassemble_row(raw)
        output_rows.append(logical)

    # Write with proper CSV quoting
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        for row in output_rows:
            writer.writerow(row)

    print(f"Done! {rows_total} data rows processed, {rows_fixed} rows fixed.")
    print(f"Output written to {path}")


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <lookup_file.csv>")
        sys.exit(1)

    lookup_path = Path(sys.argv[1])
    if not lookup_path.exists():
        print(f"Error: {lookup_path} not found")
        sys.exit(1)

    fix_lookup_csv(lookup_path)
