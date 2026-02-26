//! Shared anonymization utilities for PDF and DOCX tools.
//!
//! Provides URL mapping, player name mapping, image pixel modification,
//! text map loading, and TrueType font rendering functions used by both
//! `pdf-anon` and `docx-anon` binaries.

use ab_glyph::{Font, FontVec, ScaleFont};
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

// ─── Name area rectangles (as ratios of BBO screenshot dimensions) ───────────

/// Name rectangle defined as ratios of image width/height.
pub struct NameRect {
    pub x1: f64,
    pub y1: f64,
    pub x2: f64,
    pub y2: f64,
}

/// Returns name rectangles for [N, S, W, E] positions.
pub fn bbo_name_rects() -> [NameRect; 4] {
    [
        // N: right of N indicator, top center
        NameRect {
            x1: 0.400,
            y1: 0.010,
            x2: 0.665,
            y2: 0.076,
        },
        // S: right of S indicator, bottom center
        NameRect {
            x1: 0.400,
            y1: 0.675,
            x2: 0.665,
            y2: 0.737,
        },
        // W: right of W indicator, left panel
        NameRect {
            x1: 0.066,
            y1: 0.340,
            x2: 0.332,
            y2: 0.397,
        },
        // E: right of E indicator, right panel
        NameRect {
            x1: 0.732,
            y1: 0.340,
            x2: 0.998,
            y2: 0.397,
        },
    ]
}

// ─── URL / LIN helpers ───────────────────────────────────────────────────────

/// Normalize a tinyurl for matching: extract path after tinyurl.com/ and lowercase.
pub fn normalize_tinyurl(url: &str) -> String {
    let trimmed = url.trim().trim_end_matches('/');
    let lower = trimmed.to_lowercase();
    if let Some(pos) = lower.find("tinyurl.com/") {
        lower[pos + "tinyurl.com/".len()..].to_string()
    } else {
        lower
    }
}

/// Extract the `md` (deal) parameter from a BBO handviewer LIN URL.
/// This is the unique fingerprint for a hand's card distribution.
pub fn extract_deal_fingerprint(url: &str) -> Option<String> {
    let decoded = url
        .replace("%7C", "|")
        .replace("%7c", "|")
        .replace("%2C", ",")
        .replace("%2c", ",");

    if let Some(start) = decoded.find("|md|") {
        let rest = &decoded[start + 4..];
        if let Some(end) = rest.find('|') {
            return Some(rest[..end].to_lowercase());
        }
    }
    None
}

/// Extract player names from a BBO handviewer LIN URL.
/// LIN format: `pn|S,W,N,E|` — returns `[S, W, N, E]`.
/// The `pn` field may appear as the first field (`lin=pn|...`) or after a pipe
/// (`|pn|...`), so we search for both patterns.
pub fn extract_player_names(url: &str) -> Option<[String; 4]> {
    let decoded = url
        .replace("%7C", "|")
        .replace("%7c", "|")
        .replace("%2C", ",")
        .replace("%2c", ",");

    // Try |pn| first, then =pn| (first field after lin= query param)
    let pn_start = decoded
        .find("|pn|")
        .map(|p| p + 4)
        .or_else(|| decoded.find("=pn|").map(|p| p + 4))
        .or_else(|| {
            if decoded.starts_with("pn|") {
                Some(3)
            } else {
                None
            }
        });

    if let Some(start) = pn_start {
        let rest = &decoded[start..];
        if let Some(end) = rest.find('|') {
            let names: Vec<&str> = rest[..end].split(',').collect();
            if names.len() >= 4 {
                return Some([
                    names[0].to_string(), // S
                    names[1].to_string(), // W
                    names[2].to_string(), // N
                    names[3].to_string(), // E
                ]);
            }
        }
    }
    None
}

// ─── CSV mapping builders ────────────────────────────────────────────────────

/// Build a mapping from normalized tinyurl key -> anonymized LIN URL.
pub fn build_url_mapping(
    lookup_path: &PathBuf,
    anon_path: &PathBuf,
) -> Result<HashMap<String, String>> {
    let mut lookup_reader =
        csv::Reader::from_path(lookup_path).context("Failed to open lookup CSV")?;
    let lookup_headers = lookup_reader.headers()?.clone();
    let tinyurl_idx = lookup_headers
        .iter()
        .position(|h| h == "TinyURL")
        .context("TinyURL column not found in lookup CSV")?;

    let tinyurls: Vec<String> = lookup_reader
        .records()
        .filter_map(|r| r.ok())
        .map(|r| r.get(tinyurl_idx).unwrap_or("").trim().to_string())
        .collect();

    let mut anon_reader = csv::Reader::from_path(anon_path).context("Failed to open anon CSV")?;
    let anon_headers = anon_reader.headers()?.clone();
    let lin_url_idx = anon_headers
        .iter()
        .position(|h| h == "LIN_URL")
        .context("LIN_URL column not found in anon CSV")?;

    let anon_lins: Vec<String> = anon_reader
        .records()
        .filter_map(|r| r.ok())
        .map(|r| r.get(lin_url_idx).unwrap_or("").trim().to_string())
        .collect();

    let mut mapping = HashMap::new();
    for (tinyurl, anon_lin) in tinyurls.iter().zip(anon_lins.iter()) {
        if tinyurl.is_empty() || anon_lin.is_empty() {
            continue;
        }
        let key = normalize_tinyurl(tinyurl);
        mapping.entry(key).or_insert_with(|| anon_lin.clone());
    }

    Ok(mapping)
}

/// Build a comprehensive player name map (original BBO username -> anonymized name)
/// by reading player name columns from both CSVs row-by-row.
pub fn build_player_name_map(
    lookup_path: &PathBuf,
    anon_path: &PathBuf,
) -> Result<Vec<(String, String)>> {
    // Lookup CSV: Player_S, Player_W, Player_N, Player_E
    let mut lookup_reader =
        csv::Reader::from_path(lookup_path).context("Failed to open lookup CSV")?;
    let lh = lookup_reader.headers()?.clone();
    let ls = lh
        .iter()
        .position(|h| h == "Player_S")
        .context("Player_S not found")?;
    let lw = lh
        .iter()
        .position(|h| h == "Player_W")
        .context("Player_W not found")?;
    let ln = lh
        .iter()
        .position(|h| h == "Player_N")
        .context("Player_N not found")?;
    let le = lh
        .iter()
        .position(|h| h == "Player_E")
        .context("Player_E not found")?;

    let lookup_names: Vec<[String; 4]> = lookup_reader
        .records()
        .filter_map(|r| r.ok())
        .map(|r| {
            [
                r.get(ls).unwrap_or("").trim().to_string(),
                r.get(lw).unwrap_or("").trim().to_string(),
                r.get(ln).unwrap_or("").trim().to_string(),
                r.get(le).unwrap_or("").trim().to_string(),
            ]
        })
        .collect();

    // Anon CSV: S, W, N, E (same seat order)
    let mut anon_reader = csv::Reader::from_path(anon_path).context("Failed to open anon CSV")?;
    let ah = anon_reader.headers()?.clone();
    let a_s = ah
        .iter()
        .position(|h| h == "S")
        .context("S not found in anon CSV")?;
    let a_w = ah
        .iter()
        .position(|h| h == "W")
        .context("W not found in anon CSV")?;
    let a_n = ah
        .iter()
        .position(|h| h == "N")
        .context("N not found in anon CSV")?;
    let a_e = ah
        .iter()
        .position(|h| h == "E")
        .context("E not found in anon CSV")?;

    let anon_names: Vec<[String; 4]> = anon_reader
        .records()
        .filter_map(|r| r.ok())
        .map(|r| {
            [
                r.get(a_s).unwrap_or("").trim().to_string(),
                r.get(a_w).unwrap_or("").trim().to_string(),
                r.get(a_n).unwrap_or("").trim().to_string(),
                r.get(a_e).unwrap_or("").trim().to_string(),
            ]
        })
        .collect();

    // Collect unique original -> anon pairs
    let mut name_map: HashMap<String, String> = HashMap::new();
    for (orig_row, anon_row) in lookup_names.iter().zip(anon_names.iter()) {
        for (orig, anon) in orig_row.iter().zip(anon_row.iter()) {
            if !orig.is_empty() && !anon.is_empty() && orig != anon {
                name_map.entry(orig.clone()).or_insert_with(|| anon.clone());
            }
        }
    }

    // Sort by length descending so longer names are replaced first
    let mut pairs: Vec<(String, String)> = name_map.into_iter().collect();
    pairs.sort_by(|a, b| b.0.len().cmp(&a.0.len()));

    Ok(pairs)
}

/// Apply URL-encoded name replacements to all values in a URL mapping.
pub fn anonymize_mapping_urls(
    url_map: &mut HashMap<String, String>,
    name_pairs: &[(String, String)],
) {
    for url in url_map.values_mut() {
        for (orig, anon) in name_pairs {
            let encoded_orig = orig.replace('+', "%2B");
            let encoded_anon = anon.replace('+', "%2B");
            if url.contains(&encoded_orig) {
                *url = url.replace(&encoded_orig, &encoded_anon);
            }
        }
    }
}

/// Load extra mapping CSV (columns: ACBL_TinyURL, Anon_LIN_URL).
pub fn load_extra_mapping(path: &PathBuf) -> Result<HashMap<String, String>> {
    let mut reader = csv::Reader::from_path(path).context("Failed to open extra mapping CSV")?;
    let mut mapping = HashMap::new();
    for result in reader.records() {
        let record = result?;
        let tinyurl = record.get(0).unwrap_or("").trim();
        let anon_lin = record.get(1).unwrap_or("").trim();
        if !tinyurl.is_empty() && !anon_lin.is_empty() {
            mapping.insert(normalize_tinyurl(tinyurl), anon_lin.to_string());
        }
    }
    Ok(mapping)
}

/// Parse a name map string like "Spwilliams=Bob,Adwilliams=Sally" into pairs.
pub fn parse_name_map(s: &str) -> Vec<(String, String)> {
    s.split(',')
        .filter_map(|pair| {
            let parts: Vec<&str> = pair.splitn(2, '=').collect();
            if parts.len() == 2 {
                Some((parts[0].trim().to_string(), parts[1].trim().to_string()))
            } else {
                None
            }
        })
        .collect()
}

/// Load text replacement map from a file (one `old=new` pair per line).
/// Returns pairs sorted by key length descending so longer matches take priority.
pub fn load_text_map(path: &Path) -> Result<Vec<(String, String)>> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read text map: {}", path.display()))?;
    let mut pairs: Vec<(String, String)> = content
        .lines()
        .filter(|line| !line.trim().is_empty() && !line.starts_with('#'))
        .filter_map(|line| {
            let parts: Vec<&str> = line.splitn(2, '=').collect();
            if parts.len() == 2 && !parts[0].is_empty() {
                Some((parts[0].to_string(), parts[1].to_string()))
            } else {
                None
            }
        })
        .collect();
    // Sort by key length descending — longest matches first
    pairs.sort_by(|a, b| b.0.len().cmp(&a.0.len()));
    Ok(pairs)
}

// ─── TrueType text rendering ─────────────────────────────────────────────────

/// Load a system sans-serif font, trying several common paths.
pub fn load_system_font() -> Result<FontVec> {
    let candidates = [
        "/System/Library/Fonts/Helvetica.ttc",
        "/System/Library/Fonts/SFNSText.ttf",
        "/Library/Fonts/Arial.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/TTF/DejaVuSans.ttf",
    ];
    for path in &candidates {
        if let Ok(data) = std::fs::read(path) {
            if let Ok(font) = FontVec::try_from_vec_and_index(data.clone(), 0) {
                println!("  Loaded font: {}", path);
                return Ok(font);
            }
            if let Ok(font) = FontVec::try_from_vec(data) {
                println!("  Loaded font: {}", path);
                return Ok(font);
            }
        }
    }
    anyhow::bail!("No system font found. Tried: {}", candidates.join(", "))
}

/// Draw anti-aliased text onto raw pixel data using a TrueType font.
///
/// `channels` is the number of bytes per pixel (3 for RGB, 4 for RGBA).
/// `font_height` is the desired pixel height of the rendered text.
/// Text is alpha-blended over the existing background.
#[allow(clippy::too_many_arguments)]
pub fn draw_text(
    pixels: &mut [u8],
    img_w: usize,
    img_h: usize,
    channels: usize,
    font: &FontVec,
    text: &str,
    start_x: f32,
    start_y: f32,
    font_height: f32,
    fg: (u8, u8, u8),
) {
    let scale = ab_glyph::PxScale::from(font_height);
    let scaled_font = font.as_scaled(scale);

    let mut cursor_x = start_x;

    for ch in text.chars() {
        let glyph_id = scaled_font.glyph_id(ch);
        let glyph = glyph_id.with_scale_and_position(
            scale,
            ab_glyph::point(cursor_x, start_y + scaled_font.ascent()),
        );

        if let Some(outlined) = font.outline_glyph(glyph) {
            let bounds = outlined.px_bounds();
            outlined.draw(|gx, gy, coverage| {
                let px = gx as usize + bounds.min.x as usize;
                let py = gy as usize + bounds.min.y as usize;
                if px >= img_w || py >= img_h {
                    return;
                }
                let idx = (py * img_w + px) * channels;
                if idx + 2 >= pixels.len() {
                    return;
                }
                let alpha = coverage;
                let inv = 1.0 - alpha;
                pixels[idx] = (fg.0 as f32 * alpha + pixels[idx] as f32 * inv) as u8;
                pixels[idx + 1] = (fg.1 as f32 * alpha + pixels[idx + 1] as f32 * inv) as u8;
                pixels[idx + 2] = (fg.2 as f32 * alpha + pixels[idx + 2] as f32 * inv) as u8;
            });
        }

        cursor_x += scaled_font.h_advance(glyph_id);
    }
}

/// Measure the width of a string at a given font height (in pixels).
pub fn measure_text_width(font: &FontVec, text: &str, font_height: f32) -> f32 {
    let scale = ab_glyph::PxScale::from(font_height);
    let scaled = font.as_scaled(scale);
    text.chars()
        .map(|ch| scaled.h_advance(scaled.glyph_id(ch)))
        .sum()
}

/// Sample the dominant background colour from the rightmost column of a
/// rectangle in the pixel buffer. Falls back to white if the area is empty.
#[allow(clippy::too_many_arguments)]
pub fn sample_background(
    pixels: &[u8],
    img_w: usize,
    img_h: usize,
    channels: usize,
    x1: usize,
    _y1: usize,
    x2: usize,
    y2: usize,
) -> (u8, u8, u8) {
    let sample_x = if x2 > 20 { x2 - 15 } else { x1 };
    let mid_y = (_y1 + y2) / 2;
    let mut r_sum: u32 = 0;
    let mut g_sum: u32 = 0;
    let mut b_sum: u32 = 0;
    let mut count: u32 = 0;
    for dy in 0..10 {
        let py = mid_y.saturating_sub(5) + dy;
        if py >= img_h || sample_x >= img_w {
            continue;
        }
        let idx = (py * img_w + sample_x) * channels;
        if idx + 2 < pixels.len() {
            r_sum += pixels[idx] as u32;
            g_sum += pixels[idx + 1] as u32;
            b_sum += pixels[idx + 2] as u32;
            count += 1;
        }
    }
    if count == 0 {
        return (255, 255, 255);
    }
    (
        (r_sum / count) as u8,
        (g_sum / count) as u8,
        (b_sum / count) as u8,
    )
}

/// Overwrite the four player-name areas in a BBO screenshot with anonymized
/// names. `names` is `[S, W, N, E]` (the order from the LIN `pn` field).
/// `channels` is bytes per pixel (3 for RGB, 4 for RGBA).
pub fn modify_screenshot_pixels(
    pixels: &mut [u8],
    img_w: usize,
    img_h: usize,
    names: &[String; 4],
    font: &FontVec,
    channels: usize,
) {
    let rects = bbo_name_rects(); // [N, S, W, E]
                                  // Map rect index → names index: N→names[2], S→names[0], W→names[1], E→names[3]
    let name_indices = [2usize, 0, 1, 3];

    for (ri, rect) in rects.iter().enumerate() {
        let x1 = (rect.x1 * img_w as f64) as usize;
        let y1 = (rect.y1 * img_h as f64) as usize;
        let x2 = (rect.x2 * img_w as f64).min(img_w as f64) as usize;
        let y2 = (rect.y2 * img_h as f64).min(img_h as f64) as usize;

        // Sample background colour before we paint over it
        let bg = sample_background(pixels, img_w, img_h, channels, x1, y1, x2, y2);

        // Fill the name rectangle with the background colour
        for py in y1..y2 {
            for px in x1..x2 {
                let idx = (py * img_w + px) * channels;
                if idx + 2 < pixels.len() {
                    pixels[idx] = bg.0;
                    pixels[idx + 1] = bg.1;
                    pixels[idx + 2] = bg.2;
                }
            }
        }

        // Choose text colour: dark on light backgrounds, white on dark
        let luminance = bg.0 as u16 + bg.1 as u16 + bg.2 as u16;
        let fg = if luminance > 384 {
            (30u8, 30, 30) // dark text
        } else {
            (240u8, 240, 240) // light text
        };

        let name = &names[name_indices[ri]];
        let rect_h = (y2 - y1) as f32;
        let rect_w = (x2 - x1) as f32;

        // Start with font height at ~66% of the rect, shrink if needed to fit width
        let mut font_h = rect_h * 0.66;
        let padding = font_h * 0.3;
        loop {
            let text_w = measure_text_width(font, name, font_h);
            if text_w + padding <= rect_w || font_h <= 8.0 {
                break;
            }
            font_h -= 2.0;
        }

        // Center vertically, left-pad slightly
        let text_x = x1 as f32 + padding * 0.5;
        let text_y = y1 as f32 + (rect_h - font_h) * 0.5;

        draw_text(
            pixels, img_w, img_h, channels, font, name, text_x, text_y, font_h, fg,
        );
    }
}
