//! EDGAR Defense Toolkit
//!
//! Tools for detecting suspicious bridge play patterns through double-dummy analysis.
//!
//! This library provides:
//! - `dd_analysis`: Core double-dummy analysis engine for computing per-card costs
//!
//! Binaries:
//! - `bbo-csv`: Bulk analysis tool for BBO hand record CSVs
//! - `dd-debug`: Single-hand DD verification utility

pub mod dd_analysis;
pub mod pipeline;

// Re-export commonly used types from dependencies
pub use bridge_parsers::lin::LinData;
pub use bridge_parsers::{Card, Direction, Rank, Suit};
