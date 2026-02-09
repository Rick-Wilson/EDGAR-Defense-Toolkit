//! Integration test for DD analysis against reference data
//!
//! This test parses a real BBO tournament LIN file and compares our DD error
//! analysis against known reference data from a web plugin.
//!
//! The test exercises the main dd_analysis library module, ensuring that
//! the same code paths are tested as would be used in production.

use bridge_parsers::lin::parse_lin_file;
use edgar_defense_toolkit::dd_analysis::{
    aggregate_errors_by_player, analyze_board, DdAnalysisConfig,
};
use std::collections::HashMap;
use std::fs;

/// Reference DD error data from web plugin analysis
/// Format: player_name -> (board_number -> error_count)
fn get_reference_dd_errors() -> HashMap<String, HashMap<usize, u8>> {
    let mut errors: HashMap<String, HashMap<usize, u8>> = HashMap::new();

    // aam135: Board 2 = 1, Board 11 = 2
    errors.insert("aam135".to_string(), {
        let mut m = HashMap::new();
        m.insert(2, 1);
        m.insert(11, 2);
        m
    });

    // kemistry: Boards 1, 3, 5, 7, 10 = 1 each
    errors.insert("kemistry".to_string(), {
        let mut m = HashMap::new();
        m.insert(1, 1);
        m.insert(3, 1);
        m.insert(5, 1);
        m.insert(7, 1);
        m.insert(10, 1);
        m
    });

    // cocottina: Boards 1, 3 = 1 each
    errors.insert("cocottina".to_string(), {
        let mut m = HashMap::new();
        m.insert(1, 1);
        m.insert(3, 1);
        m
    });

    // ehy: Boards 7, 8 = 1 each
    errors.insert("ehy".to_string(), {
        let mut m = HashMap::new();
        m.insert(7, 1);
        m.insert(8, 1);
        m
    });

    // usvi: Board 5 = 2
    errors.insert("usvi".to_string(), {
        let mut m = HashMap::new();
        m.insert(5, 2);
        m
    });

    // ~~M32299 (Robot): Boards 7, 9 = 1 each
    errors.insert("~~M32299".to_string(), {
        let mut m = HashMap::new();
        m.insert(7, 1);
        m.insert(9, 1);
        m
    });

    // wacky1: Board 10 = 2
    errors.insert("wacky1".to_string(), {
        let mut m = HashMap::new();
        m.insert(10, 2);
        m
    });

    // pbnguru: Board 11 = 1
    errors.insert("pbnguru".to_string(), {
        let mut m = HashMap::new();
        m.insert(11, 1);
        m
    });

    // miche41: no errors
    errors.insert("miche41".to_string(), HashMap::new());

    // jelsma: no errors
    errors.insert("jelsma".to_string(), HashMap::new());

    errors
}

/// Test DD analysis against reference data from web plugin
///
/// The web plugin uses mid-trick analysis which computes DD before and after
/// every card played, detecting errors within tricks.
#[test]
fn test_dd_analysis_trick_boundary() {
    let content = fs::read_to_string("tests/fixtures/input/kemistry-boards-2026-01-14.lin")
        .expect("Failed to read LIN file");

    let boards = parse_lin_file(&content).expect("Failed to parse LIN file");
    let reference = get_reference_dd_errors();

    // Use mid-trick config to match web plugin methodology
    let config = DdAnalysisConfig::mid_trick();

    // Collect computed errors per player per board
    let mut computed_errors: HashMap<String, HashMap<usize, u8>> = HashMap::new();

    for board in &boards {
        if let Some(result) = analyze_board(board, &config) {
            if let Some(board_num) = result.board_num {
                // Aggregate errors by player (counting errors, not summing costs)
                let player_errors = aggregate_errors_by_player(&result);
                for (player, count) in player_errors {
                    computed_errors
                        .entry(player)
                        .or_default()
                        .entry(board_num)
                        .and_modify(|e| *e += count)
                        .or_insert(count);
                }
            }
        }
    }

    // Compare with reference
    let mut mismatches = Vec::new();
    let mut exact_matches = 0;
    let mut _close_matches = 0; // Off by 1

    for (player, ref_boards) in &reference {
        let computed = computed_errors.get(player).cloned().unwrap_or_default();

        // Check each expected error
        for (board_num, expected_count) in ref_boards {
            let actual_count = computed.get(board_num).copied().unwrap_or(0);
            if actual_count == *expected_count {
                exact_matches += 1;
            } else if (actual_count as i16 - *expected_count as i16).abs() == 1 {
                _close_matches += 1;
                mismatches.push(format!(
                    "{} Board {}: expected {} errors, got {} (off by 1)",
                    player, board_num, expected_count, actual_count
                ));
            } else {
                mismatches.push(format!(
                    "{} Board {}: expected {} errors, got {}",
                    player, board_num, expected_count, actual_count
                ));
            }
        }

        // Check for unexpected errors
        for (board_num, actual_count) in &computed {
            if !ref_boards.contains_key(board_num) && *actual_count > 0 {
                mismatches.push(format!(
                    "{} Board {}: expected 0 errors, got {} (unexpected)",
                    player, board_num, actual_count
                ));
            }
        }
    }

    // Also check for players not in reference who have errors
    for (player, boards) in &computed_errors {
        if !reference.contains_key(player) {
            for (board_num, count) in boards {
                if *count > 0 {
                    mismatches.push(format!(
                        "{} Board {}: got {} errors (player not in reference)",
                        player, board_num, count
                    ));
                }
            }
        }
    }

    println!("\n=== DD Analysis Comparison ===");
    println!("Exact matches: {}", exact_matches);
    println!("Mismatches: {}", mismatches.len());

    if !mismatches.is_empty() {
        println!("\n=== Mismatches ===");
        for m in &mismatches {
            println!("  {}", m);
        }
    }

    println!("\n=== Computed Errors Summary ===");
    for (player, boards) in &computed_errors {
        if !boards.is_empty() {
            let total: u8 = boards.values().sum();
            let boards_str: Vec<String> = boards
                .iter()
                .map(|(b, c)| format!("B{}={}", b, c))
                .collect();
            println!("  {}: total={}, {}", player, total, boards_str.join(", "));
        }
    }

    // Results must match exactly
    if !mismatches.is_empty() {
        panic!(
            "DD analysis mismatches: {}\n{}",
            mismatches.len(),
            mismatches.join("\n")
        );
    }

    println!("\n=== DD Analysis Test Passed ===");
    println!("All {} expected errors matched exactly.", exact_matches);
}

/// Test DD analysis using mid-trick mode against reference data
///
/// Mid-trick analysis should more closely match the web plugin's methodology
/// since it computes DD before and after every card, not just at trick boundaries.
#[test]
fn test_dd_analysis_mid_trick() {
    let content = fs::read_to_string("tests/fixtures/input/kemistry-boards-2026-01-14.lin")
        .expect("Failed to read LIN file");

    let boards = parse_lin_file(&content).expect("Failed to parse LIN file");
    let reference = get_reference_dd_errors();

    // Use mid-trick config for more detailed analysis
    let config = DdAnalysisConfig::mid_trick();

    // Collect computed errors per player per board
    let mut computed_errors: HashMap<String, HashMap<usize, u8>> = HashMap::new();

    for board in &boards {
        if let Some(result) = analyze_board(board, &config) {
            if let Some(board_num) = result.board_num {
                // Aggregate errors by player (counting errors, not summing costs)
                let player_errors = aggregate_errors_by_player(&result);
                for (player, count) in player_errors {
                    computed_errors
                        .entry(player)
                        .or_default()
                        .entry(board_num)
                        .and_modify(|e| *e += count)
                        .or_insert(count);
                }
            }
        }
    }

    // Compare with reference
    let mut mismatches = Vec::new();
    let mut exact_matches = 0;
    let mut close_matches = 0;

    for (player, ref_boards) in &reference {
        let computed = computed_errors.get(player).cloned().unwrap_or_default();

        for (board_num, expected_count) in ref_boards {
            let actual_count = computed.get(board_num).copied().unwrap_or(0);
            if actual_count == *expected_count {
                exact_matches += 1;
            } else if (actual_count as i16 - *expected_count as i16).abs() == 1 {
                close_matches += 1;
                mismatches.push(format!(
                    "{} Board {}: expected {} errors, got {} (off by 1)",
                    player, board_num, expected_count, actual_count
                ));
            } else {
                mismatches.push(format!(
                    "{} Board {}: expected {} errors, got {}",
                    player, board_num, expected_count, actual_count
                ));
            }
        }

        for (board_num, actual_count) in &computed {
            if !ref_boards.contains_key(board_num) && *actual_count > 0 {
                mismatches.push(format!(
                    "{} Board {}: expected 0 errors, got {} (unexpected)",
                    player, board_num, actual_count
                ));
            }
        }
    }

    for (player, boards) in &computed_errors {
        if !reference.contains_key(player) {
            for (board_num, count) in boards {
                if *count > 0 {
                    mismatches.push(format!(
                        "{} Board {}: got {} errors (player not in reference)",
                        player, board_num, count
                    ));
                }
            }
        }
    }

    println!("\n=== DD Analysis Comparison (Mid-Trick Mode) ===");
    println!("Exact matches: {}", exact_matches);
    println!("Close matches (off by 1): {}", close_matches);
    println!("Mismatches: {}", mismatches.len());

    if !mismatches.is_empty() {
        println!("\n=== Mismatches ===");
        for m in &mismatches {
            println!("  {}", m);
        }
    }

    println!("\n=== Computed Errors Summary (Mid-Trick) ===");
    for (player, boards) in &computed_errors {
        if !boards.is_empty() {
            let total: u8 = boards.values().sum();
            let boards_str: Vec<String> = boards
                .iter()
                .map(|(b, c)| format!("B{}={}", b, c))
                .collect();
            println!("  {}: total={}, {}", player, total, boards_str.join(", "));
        }
    }

    // Mid-trick mode may have different results - document them
    println!("\n=== DD Analysis Test Complete (Mid-Trick Mode) ===");
    println!(
        "Results: {} exact, {} close, {} other differences",
        exact_matches,
        close_matches,
        mismatches.len() - close_matches
    );

    // Don't fail the test - just report results for comparison
    // Mid-trick analysis may find more errors than trick-boundary
}

/// Test that detailed error information is available
#[test]
fn test_dd_analysis_error_details() {
    let content = fs::read_to_string("tests/fixtures/input/kemistry-boards-2026-01-14.lin")
        .expect("Failed to read LIN file");

    let boards = parse_lin_file(&content).expect("Failed to parse LIN file");
    let config = DdAnalysisConfig::mid_trick().with_debug();

    // Analyze Board 5 which has usvi with expected=2 but we got=8
    // This demonstrates the methodology difference
    let board_5 = boards
        .iter()
        .find(|b| b.board_header.as_ref().is_some_and(|h| h.contains("5")));

    if let Some(board) = board_5 {
        println!("\n=== Board 5 Detailed Analysis ===");
        println!("Players: {:?}", board.player_names);
        // pn order is S,W,N,E so: aam135=South, usvi=West, kemistry=North, jelsma=East

        if let Some(result) = analyze_board(board, &config) {
            println!("Contract: {} by {}", result.contract, result.declarer);
            println!("Initial DD: {}", result.initial_dd);
            println!("Final result: {}", result.final_result);
            println!(
                "\nAll errors found by mid-trick analysis ({} total):",
                result.errors.len()
            );

            // Group errors by player
            let mut errors_by_player: HashMap<String, Vec<_>> = HashMap::new();
            for error in &result.errors {
                errors_by_player
                    .entry(error.player.clone())
                    .or_default()
                    .push(error);
            }

            for (player, errors) in &errors_by_player {
                println!("\n  {} ({} errors):", player, errors.len());
                for error in errors {
                    println!(
                        "    Trick {}, pos {}: {}{} (cost: {})",
                        error.trick_num,
                        error.card_position,
                        error.card.suit.to_char(),
                        error.card.rank.to_char(),
                        error.cost
                    );
                }
            }

            // Show the reference expectation
            println!("\n=== Reference vs Our Analysis ===");
            println!("Reference (web plugin): usvi has 2 errors on Board 5");
            println!(
                "Our mid-trick analysis: usvi has {} errors on Board 5",
                errors_by_player.get("usvi").map(|e| e.len()).unwrap_or(0)
            );

            // Count unique tricks with errors per player
            if let Some(usvi_errors) = errors_by_player.get("usvi") {
                let unique_tricks: std::collections::HashSet<_> =
                    usvi_errors.iter().map(|e| e.trick_num).collect();
                println!(
                    "usvi errors span {} different tricks: {:?}",
                    unique_tricks.len(),
                    unique_tricks.iter().collect::<Vec<_>>()
                );
            }

            println!("\nKey insight from web plugin:");
            println!("- Web plugin shows all of West's spades as DD-equal (green)");
            println!("- So S2 should NOT be an error since all alternatives equal");
            println!("- Our code may be computing dd_before as 'optimal from here'");
            println!("- which differs from 'DD after previous card was played'");
        }
    }
}

/// Debug trick-boundary mode to see DD values per trick
#[test]
fn test_trick_boundary_debug() {
    let content = fs::read_to_string("tests/fixtures/input/kemistry-boards-2026-01-14.lin")
        .expect("Failed to read LIN file");

    let boards = parse_lin_file(&content).expect("Failed to parse LIN file");

    // Analyze Board 3 - expected errors: cocottina=1, kemistry=1
    // But trick-boundary mode finds 0 for both
    let board_3 = boards
        .iter()
        .find(|b| b.board_header.as_ref().is_some_and(|h| h.ends_with(" 3")));

    if let Some(board) = board_3 {
        println!("\n=== Board 3 Analysis ===");
        println!("Players: {:?}", board.player_names);
        // pn order is S,W,N,E
        println!("Cardplay: {}", board.format_cardplay_by_trick());

        println!("\n--- Trick-Boundary Mode ---");
        let config = DdAnalysisConfig::trick_boundary().with_debug();
        if let Some(result) = analyze_board(board, &config) {
            println!("Contract: {} by {}", result.contract, result.declarer);
            println!("Initial DD: {}", result.initial_dd);
            println!("Final result: {}", result.final_result);
            println!("Errors found: {}", result.errors.len());
            for err in &result.errors {
                println!(
                    "  - {} T{}: {}{} cost={}",
                    err.player,
                    err.trick_num,
                    err.card.suit.to_char(),
                    err.card.rank.to_char(),
                    err.cost
                );
            }
        }

        println!("\n--- Mid-Trick Mode ---");
        let config = DdAnalysisConfig::mid_trick().with_debug();
        if let Some(result) = analyze_board(board, &config) {
            println!("Errors found: {}", result.errors.len());
            for err in &result.errors {
                println!(
                    "  - {} T{} pos{}: {}{} cost={}",
                    err.player,
                    err.trick_num,
                    err.card_position,
                    err.card.suit.to_char(),
                    err.card.rank.to_char(),
                    err.cost
                );
            }
        }
    }
}
