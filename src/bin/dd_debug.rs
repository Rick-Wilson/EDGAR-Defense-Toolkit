//! DD Debug Utility
//!
//! Takes a single TinyURL and displays DD analysis card-by-card
//! for verification against BBO handviewer.
//!
//! By default, computes DD at trick boundaries only. Use --mid-trick flag
//! to enable per-card mid-trick analysis.
//!
//! Usage: cargo run --bin dd-debug [--mid-trick] <tinyurl>

use anyhow::{Context, Result};
use bridge_parsers::lin::parse_lin_from_url;
use bridge_parsers::{Card, Direction, Rank, Suit};
use bridge_parsers::tinyurl::UrlResolver;
use bridge_solver::cards::{card_of, suit_of};
use bridge_solver::{CutoffCache, Hands, PartialTrick, PatternCache, Solver};
use bridge_solver::{CLUB, DIAMOND, EAST, HEART, NOTRUMP, NORTH, SOUTH, SPADE, WEST};

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    // Parse args
    let mut mid_trick_mode = false;
    let mut url_arg = None;

    for arg in &args[1..] {
        if arg == "--mid-trick" {
            mid_trick_mode = true;
        } else if !arg.starts_with('-') {
            url_arg = Some(arg.clone());
        }
    }

    let url = match url_arg {
        Some(u) => u,
        None => {
            eprintln!("Usage: {} [--mid-trick] <tinyurl>", args[0]);
            eprintln!("Example: {} http://tinyurl.com/27g7hbuc", args[0]);
            eprintln!("");
            eprintln!("Options:");
            eprintln!("  --mid-trick    Compute DD after every card (slower, may differ from BBO)");
            eprintln!("                 Default: compute DD at trick boundaries only");
            std::process::exit(1);
        }
    };

    // Resolve TinyURL if needed
    let resolved_url = if url.contains("tinyurl.com") || url.contains("bit.ly") {
        eprintln!("Resolving {}...", url);
        let mut resolver = UrlResolver::with_config(0, 10, 0);
        resolver.resolve(&url)?
    } else {
        url.clone()
    };

    // Parse LIN data
    let lin_data = parse_lin_from_url(&resolved_url).context("Failed to parse LIN from URL")?;

    // Display deal info
    println!("\n=== Deal Information ===");
    println!("Players: {:?}", lin_data.player_names);
    println!("Dealer: {:?}", lin_data.dealer);
    println!("Vulnerability: {:?}", lin_data.vulnerability);
    if let Some(ref header) = lin_data.board_header {
        println!("Board: {}", header);
    }

    // Display hands
    println!("\n=== Hands ===");
    for dir in Direction::ALL {
        let hand = lin_data.deal.hand(dir);
        println!("{:5}: {}", format!("{:?}", dir), hand.to_pbn());
    }

    // Extract contract and declarer
    let contract = extract_contract(&lin_data);
    let declarer = extract_declarer(&lin_data);

    println!("\n=== Contract ===");
    println!("Contract: {} by {}", contract, declarer);

    // Parse trump and declarer seat
    let trump = parse_trump(&contract)?;
    let declarer_seat = parse_declarer_seat(&declarer)?;
    let initial_leader = (declarer_seat + 1) % 4;
    let declarer_is_ns = declarer_seat == NORTH || declarer_seat == SOUTH;

    println!(
        "Trump: {}",
        match trump {
            SPADE => "Spades",
            HEART => "Hearts",
            DIAMOND => "Diamonds",
            CLUB => "Clubs",
            _ => "No Trump",
        }
    );
    println!(
        "Declarer seat: {} ({})",
        declarer_seat,
        seat_name(declarer_seat)
    );
    println!(
        "Opening leader: {} ({})",
        initial_leader,
        seat_name(initial_leader)
    );

    // Convert deal to solver format
    let pbn = lin_data.deal.to_pbn(Direction::North);
    let hands = Hands::from_pbn(&pbn).context("Failed to parse deal for solver")?;

    // Create caches for solver (reuse across all solves)
    let mut cutoff_cache = CutoffCache::new(16);
    let mut pattern_cache = PatternCache::new(16);

    // Initial DD
    let initial_ns = solve_position(&hands, trump, initial_leader, &mut cutoff_cache, &mut pattern_cache);
    let initial_declarer = if declarer_is_ns {
        initial_ns
    } else {
        13 - initial_ns
    };
    println!("\nInitial DD: Declarer makes {} tricks", initial_declarer);

    // Parse cardplay
    let cardplay = lin_data.format_cardplay_by_trick();
    println!("\nCardplay: {}", cardplay);

    // Analyze card-by-card
    if mid_trick_mode {
        println!("\n=== DD Analysis Card-by-Card (mid-trick mode) ===");
    } else {
        println!("\n=== DD Analysis at Trick Boundaries ===");
    }
    println!(
        "{:^6} | {:^4} | {:^6} | {:^6} | {:^10} | {:^10} | {:^6}",
        "Trick", "Card", "Player", "Played", "DD Before", "DD After", "Cost"
    );
    println!("{}", "-".repeat(72));

    let mut current_hands = hands;
    let mut current_leader = initial_leader;
    let tricks = parse_cardplay(&cardplay)?;
    let mut declarer_tricks_won: u8 = 0;

    if mid_trick_mode {
        // Mid-trick mode: compute DD before and after every card
        for (trick_num, trick) in tricks.iter().enumerate() {
            let mut seat = current_leader;
            let mut partial_trick = PartialTrick::new();
            let mut cards_in_trick: Vec<(usize, usize)> = Vec::new();

            for (card_idx, card) in trick.iter().enumerate() {
                let solver_card = bridge_card_to_solver(*card)?;

                // Compute DD BEFORE this card is played
                let dd_before = if partial_trick.is_empty() {
                    let ns = solve_position(&current_hands, trump, current_leader, &mut cutoff_cache, &mut pattern_cache);
                    if declarer_is_ns {
                        declarer_tricks_won + ns
                    } else {
                        declarer_tricks_won + (current_hands.num_tricks() as u8).saturating_sub(ns)
                    }
                } else {
                    let (ns, remaining) = solve_mid_trick(&current_hands, trump, &partial_trick, &mut cutoff_cache, &mut pattern_cache);
                    if declarer_is_ns {
                        declarer_tricks_won + ns
                    } else {
                        declarer_tricks_won + remaining.saturating_sub(ns)
                    }
                };

                // Play the card
                current_hands[seat].remove(solver_card);
                partial_trick.add(solver_card, seat);
                cards_in_trick.push((seat, solver_card));

                // Compute DD AFTER this card is played
                let dd_after = if card_idx == 3 {
                    let winner = determine_trick_winner(&cards_in_trick, trump, current_leader);
                    let declarer_won = if declarer_is_ns {
                        winner == NORTH || winner == SOUTH
                    } else {
                        winner == EAST || winner == WEST
                    };
                    let tricks_from_this = if declarer_won { 1u8 } else { 0u8 };

                    if current_hands.num_tricks() == 0 {
                        declarer_tricks_won + tricks_from_this
                    } else {
                        let ns = solve_position(&current_hands, trump, winner, &mut cutoff_cache, &mut pattern_cache);
                        if declarer_is_ns {
                            declarer_tricks_won + tricks_from_this + ns
                        } else {
                            let remaining = current_hands.num_tricks() as u8;
                            declarer_tricks_won + tricks_from_this + remaining.saturating_sub(ns)
                        }
                    }
                } else {
                    let (ns, remaining) = solve_mid_trick(&current_hands, trump, &partial_trick, &mut cutoff_cache, &mut pattern_cache);
                    if declarer_is_ns {
                        declarer_tricks_won + ns
                    } else {
                        declarer_tricks_won + remaining.saturating_sub(ns)
                    }
                };

                // Cost calculation
                let player_is_declarer_side = if declarer_is_ns {
                    seat == NORTH || seat == SOUTH
                } else {
                    seat == EAST || seat == WEST
                };

                let cost = if player_is_declarer_side {
                    if dd_after < dd_before { dd_before - dd_after } else { 0 }
                } else {
                    if dd_after > dd_before { dd_after - dd_before } else { 0 }
                };

                let card_str = format!("{}{}", card.suit.to_char(), card.rank.to_char());
                let position = match card_idx {
                    0 => "Lead", 1 => "2nd", 2 => "3rd", 3 => "4th", _ => "?",
                };

                println!(
                    "{:^6} | {:^4} | {:^6} | {:^6} | {:^10} | {:^10} | {:^6}",
                    if card_idx == 0 { format!("{}", trick_num + 1) } else { "".to_string() },
                    position, seat_name(seat), card_str, dd_before, dd_after,
                    if cost > 0 { format!("{}", cost) } else { "-".to_string() }
                );

                seat = (seat + 1) % 4;
            }

            // Update state after trick
            if cards_in_trick.len() == 4 {
                let winner = determine_trick_winner(&cards_in_trick, trump, current_leader);
                let declarer_won = if declarer_is_ns {
                    winner == NORTH || winner == SOUTH
                } else {
                    winner == EAST || winner == WEST
                };
                if declarer_won { declarer_tricks_won += 1; }
                current_leader = winner;
                println!("{}", "-".repeat(72));
            }
        }
    } else {
        // Trick-boundary mode: compute DD only at start and end of each trick
        for (trick_num, trick) in tricks.iter().enumerate() {
            let mut seat = current_leader;
            let mut cards_in_trick: Vec<(usize, usize)> = Vec::new();

            // DD at start of trick (before any card played)
            let dd_start = {
                let ns = solve_position(&current_hands, trump, current_leader, &mut cutoff_cache, &mut pattern_cache);
                if declarer_is_ns {
                    declarer_tricks_won + ns
                } else {
                    declarer_tricks_won + (current_hands.num_tricks() as u8).saturating_sub(ns)
                }
            };

            // Play all cards in the trick
            for (card_idx, card) in trick.iter().enumerate() {
                let solver_card = bridge_card_to_solver(*card)?;
                current_hands[seat].remove(solver_card);
                cards_in_trick.push((seat, solver_card));

                let card_str = format!("{}{}", card.suit.to_char(), card.rank.to_char());
                let position = match card_idx {
                    0 => "Lead", 1 => "2nd", 2 => "3rd", 3 => "4th", _ => "?",
                };

                // Only show DD values for first and last card of trick
                if card_idx == 0 {
                    println!(
                        "{:^6} | {:^4} | {:^6} | {:^6} | {:^10} |            |       ",
                        trick_num + 1, position, seat_name(seat), card_str, dd_start
                    );
                } else if card_idx == 3 {
                    // Compute DD at end of trick
                    let winner = determine_trick_winner(&cards_in_trick, trump, current_leader);
                    let declarer_won = if declarer_is_ns {
                        winner == NORTH || winner == SOUTH
                    } else {
                        winner == EAST || winner == WEST
                    };
                    let tricks_from_this = if declarer_won { 1u8 } else { 0u8 };

                    let dd_end = if current_hands.num_tricks() == 0 {
                        declarer_tricks_won + tricks_from_this
                    } else {
                        let ns = solve_position(&current_hands, trump, winner, &mut cutoff_cache, &mut pattern_cache);
                        if declarer_is_ns {
                            declarer_tricks_won + tricks_from_this + ns
                        } else {
                            let remaining = current_hands.num_tricks() as u8;
                            declarer_tricks_won + tricks_from_this + remaining.saturating_sub(ns)
                        }
                    };

                    // Cost = any change in DD during this trick
                    let cost = if dd_end < dd_start {
                        dd_start - dd_end
                    } else {
                        0
                    };

                    println!(
                        "{:^6} | {:^4} | {:^6} | {:^6} |            | {:^10} | {:^6}",
                        "", position, seat_name(seat), card_str, dd_end,
                        if cost > 0 { format!("{}", cost) } else { "-".to_string() }
                    );

                    // Update state
                    if declarer_won { declarer_tricks_won += 1; }
                    current_leader = winner;
                } else {
                    println!(
                        "{:^6} | {:^4} | {:^6} | {:^6} |            |            |       ",
                        "", position, seat_name(seat), card_str
                    );
                }

                seat = (seat + 1) % 4;
            }

            println!("{}", "-".repeat(72));
        }
    }

    println!("Final result: Declarer made {} tricks", declarer_tricks_won);

    // Print BBO link for verification
    println!("\n=== Verification Link ===");
    println!("{}", resolved_url);
    println!("\nClick link and use arrow keys to step through cards. Compare DD values shown.");

    Ok(())
}

fn seat_name(seat: usize) -> &'static str {
    match seat {
        WEST => "West",
        NORTH => "North",
        EAST => "East",
        SOUTH => "South",
        _ => "?",
    }
}

fn solve_position(
    hands: &Hands,
    trump: usize,
    leader: usize,
    cutoff_cache: &mut CutoffCache,
    pattern_cache: &mut PatternCache,
) -> u8 {
    if hands.num_tricks() == 0 {
        return 0;
    }
    let solver = Solver::new(*hands, trump, leader);
    solver.solve_with_caches(cutoff_cache, pattern_cache)
}

/// Solve mid-trick position and return (NS tricks, total tricks remaining)
///
/// The total tricks remaining is the max hand size, which is what the solver uses internally.
/// This is important for mid-trick positions where hands have different sizes.
fn solve_mid_trick(
    hands: &Hands,
    trump: usize,
    partial_trick: &PartialTrick,
    cutoff_cache: &mut CutoffCache,
    pattern_cache: &mut PatternCache,
) -> (u8, u8) {
    // Max hand size = hands that haven't played yet = total tricks remaining
    let max_hand_size = (0..4).map(|s| hands[s].size()).max().unwrap_or(0) as u8;

    if max_hand_size == 0 {
        return (0, 0);
    }
    if let Some(solver) = Solver::new_mid_trick(*hands, trump, partial_trick) {
        let ns = solver.solve_mid_trick(cutoff_cache, pattern_cache, partial_trick);
        (ns, max_hand_size)
    } else if let Some(leader) = partial_trick.leader() {
        let ns = solve_position(hands, trump, leader, cutoff_cache, pattern_cache);
        (ns, max_hand_size)
    } else {
        (0, max_hand_size)
    }
}

fn extract_contract(lin_data: &bridge_parsers::lin::LinData) -> String {
    let mut level = 0u8;
    let mut suit = String::new();
    let mut doubled = false;
    let mut redoubled = false;

    for bid in &lin_data.auction {
        let bid_str = bid.bid.to_uppercase();
        if bid_str == "P" || bid_str == "PASS" {
            continue;
        } else if bid_str == "D" || bid_str == "X" || bid_str == "DBL" {
            doubled = true;
            redoubled = false;
        } else if bid_str == "R" || bid_str == "XX" || bid_str == "RDBL" {
            redoubled = true;
        } else if let Some(c) = bid_str.chars().next() {
            if c.is_ascii_digit() {
                level = c.to_digit(10).unwrap_or(0) as u8;
                suit = bid_str[1..].to_string();
                doubled = false;
                redoubled = false;
            }
        }
    }

    if level == 0 {
        return "Passed Out".to_string();
    }

    let mut contract = format!("{}{}", level, suit);
    if redoubled {
        contract.push_str("XX");
    } else if doubled {
        contract.push_str("X");
    }
    contract
}

fn extract_declarer(lin_data: &bridge_parsers::lin::LinData) -> String {
    if !lin_data.play.is_empty() {
        let opening_lead = &lin_data.play[0];
        for dir in Direction::ALL {
            let hand = lin_data.deal.hand(dir);
            if hand.has_card(*opening_lead) {
                return match dir {
                    Direction::North => "West".to_string(),
                    Direction::East => "North".to_string(),
                    Direction::South => "East".to_string(),
                    Direction::West => "South".to_string(),
                };
            }
        }
    }
    "Unknown".to_string()
}

fn parse_trump(contract: &str) -> Result<usize> {
    let contract = contract.trim().to_uppercase();
    if contract.contains("NT") || contract.contains("N") && !contract.contains("S") {
        return Ok(NOTRUMP);
    }
    for c in contract.chars() {
        match c {
            'S' => return Ok(SPADE),
            'H' => return Ok(HEART),
            'D' => return Ok(DIAMOND),
            'C' => return Ok(CLUB),
            _ => continue,
        }
    }
    Err(anyhow::anyhow!("Could not parse trump from: {}", contract))
}

fn parse_declarer_seat(declarer: &str) -> Result<usize> {
    match declarer.trim().to_uppercase().chars().next() {
        Some('N') => Ok(NORTH),
        Some('E') => Ok(EAST),
        Some('S') => Ok(SOUTH),
        Some('W') => Ok(WEST),
        _ => Err(anyhow::anyhow!("Invalid declarer: {}", declarer)),
    }
}

fn parse_cardplay(cardplay: &str) -> Result<Vec<Vec<Card>>> {
    let mut tricks = Vec::new();
    for trick_str in cardplay.split('|') {
        if trick_str.is_empty() {
            continue;
        }
        let mut trick = Vec::new();
        for card_str in trick_str.split('-') {
            let card = parse_card_str(card_str)?;
            trick.push(card);
        }
        if !trick.is_empty() {
            tricks.push(trick);
        }
    }
    Ok(tricks)
}

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

    let rank = Rank::from_char(rank_char)
        .ok_or_else(|| anyhow::anyhow!("Invalid rank: {}", rank_char))?;

    Ok(Card::new(suit, rank))
}

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

fn determine_trick_winner(cards: &[(usize, usize)], trump: usize, leader: usize) -> usize {
    let mut winner_idx = 0;
    let mut winning_card = cards[0].1;

    for (i, (_seat, card)) in cards.iter().enumerate().skip(1) {
        let card_suit = suit_of(*card);
        let beats = if card_suit == suit_of(winning_card) {
            *card < winning_card // Lower card value = higher rank in bridge-solver
        } else if card_suit == trump && trump < NOTRUMP {
            suit_of(winning_card) != trump // Trump beats non-trump
        } else {
            false
        };

        if beats {
            winner_idx = i;
            winning_card = *card;
        }
    }

    (leader + winner_idx) % 4
}
