# CLAUDE.md

This file provides guidance to Claude Code when working with this repository.

## Project Overview

<!-- TODO: Add project description -->

## Build & Test Commands

```bash
cargo build                    # Build debug
cargo build --release          # Build release
cargo test                     # Run all tests
cargo clippy -- -D warnings    # Lint (treat warnings as errors)
cargo fmt --check              # Check formatting
```

## Pre-commit Requirements

Before committing, always run and fix:
1. `cargo fmt --all` - Format all code
2. `cargo clippy --all-targets -- -D warnings` - Fix all clippy warnings
3. `cargo test` - Ensure all tests pass

## Code Standards

- No `unwrap()` or `expect()` outside test code - use proper error handling
- No `println!()` in library code (CLI binaries are OK)
- All public functions must have doc comments (`///`)
- All `unsafe` blocks must have a comment explaining why they're safe
- Prefer editing existing files over creating new ones

## Git Configuration

Use SSH for all GitHub operations:
- Clone/push/pull: `git@github.com:Rick-Wilson/repo.git` (not `https://`)
- Remote URLs should use SSH format

## Notifications

Send Pushover notifications when work is blocked or completed:

```bash
pushover "message" "title"    # title defaults to "Claude Code"
```

**When to notify:**
- Waiting for user input or permission
- Task completed after extended work
- Build/test failures that need attention
- Any situation where work is paused and user may not notice
