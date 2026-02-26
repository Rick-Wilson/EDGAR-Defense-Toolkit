//! Replace tinyurl hyperlinks in a PDF with anonymized LIN URLs and
//! overwrite player names in BBO hand-viewer screenshots.
//!
//! Two modes:
//!
//! 1. **Replace links** (default): Replace tinyurl annotations in a PDF
//!    and anonymize player names in BBO screenshot images.
//!    pdf-anon --pdf input.pdf --lookup lookup.csv --anon anon.csv -o output.pdf
//!
//! 2. **Resolve**: Resolve unmatched ACBL tinyurls by following redirects,
//!    fingerprinting the destination LIN data, and matching against the lookup.
//!    pdf-anon resolve --pdf input.pdf --lookup lookup.csv --anon anon.csv -o mapping.csv

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use edgar_defense_toolkit::anon_common::*;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "pdf-anon",
    about = "Replace tinyurl links in PDF with anonymized LIN URLs"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Input PDF file
    #[arg(long)]
    pdf: Option<PathBuf>,

    /// Tinyurl lookup CSV (must have TinyURL and LIN_URL columns)
    #[arg(long)]
    lookup: Option<PathBuf>,

    /// Anonymized DD CSV (must have LIN_URL column with anonymized URLs)
    #[arg(long)]
    anon: Option<PathBuf>,

    /// Extra tinyurl mapping CSV (ACBL_TinyURL,BBO_TinyURL from resolve step)
    #[arg(long)]
    extra_map: Option<PathBuf>,

    /// Output PDF file
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Skip BBO screenshot image anonymization
    #[arg(long)]
    no_images: bool,

    /// Name replacement map for remaining URLs (e.g., "Spwilliams=Bob,Adwilliams=Sally")
    #[arg(long)]
    name_map: Option<String>,

    /// Text map file for page text replacement (one "old=new" pair per line)
    #[arg(long)]
    text_map: Option<PathBuf>,
}

#[derive(Subcommand)]
enum Commands {
    /// Resolve unmatched tinyurls by following redirects and matching LIN data
    Resolve {
        /// PDF to scan for unmatched tinyurls
        #[arg(long)]
        pdf: Option<PathBuf>,

        /// Tinyurl lookup CSV
        #[arg(long)]
        lookup: PathBuf,

        /// Anonymized DD CSV
        #[arg(long)]
        anon: PathBuf,

        /// Output mapping CSV
        #[arg(short, long)]
        output: PathBuf,
    },
}

// Shared types and functions imported from edgar_defense_toolkit::anon_common

/// Build deal fingerprint -> (normalized_tinyurl_key, anon_lin_url) index.
fn build_fingerprint_index(
    lookup_path: &PathBuf,
    anon_path: &PathBuf,
) -> Result<HashMap<String, (String, String)>> {
    let mut lookup_reader =
        csv::Reader::from_path(lookup_path).context("Failed to open lookup CSV")?;
    let lookup_headers = lookup_reader.headers()?.clone();
    let tinyurl_idx = lookup_headers
        .iter()
        .position(|h| h == "TinyURL")
        .context("TinyURL column not found")?;
    let lin_url_idx = lookup_headers
        .iter()
        .position(|h| h == "LIN_URL")
        .context("LIN_URL column not found in lookup CSV")?;

    let lookup_rows: Vec<(String, String)> = lookup_reader
        .records()
        .filter_map(|r| r.ok())
        .map(|r| {
            (
                r.get(tinyurl_idx).unwrap_or("").trim().to_string(),
                r.get(lin_url_idx).unwrap_or("").trim().to_string(),
            )
        })
        .collect();

    let mut anon_reader = csv::Reader::from_path(anon_path).context("Failed to open anon CSV")?;
    let anon_headers = anon_reader.headers()?.clone();
    let anon_lin_idx = anon_headers
        .iter()
        .position(|h| h == "LIN_URL")
        .context("LIN_URL column not found in anon CSV")?;

    let anon_lins: Vec<String> = anon_reader
        .records()
        .filter_map(|r| r.ok())
        .map(|r| r.get(anon_lin_idx).unwrap_or("").trim().to_string())
        .collect();

    let mut index = HashMap::new();
    for (i, (tinyurl, lin_url)) in lookup_rows.iter().enumerate() {
        if tinyurl.is_empty() || lin_url.is_empty() {
            continue;
        }
        if let Some(fp) = extract_deal_fingerprint(lin_url) {
            let anon_lin = anon_lins.get(i).cloned().unwrap_or_default();
            if !anon_lin.is_empty() {
                index
                    .entry(fp)
                    .or_insert_with(|| (normalize_tinyurl(tinyurl), anon_lin));
            }
        }
    }

    Ok(index)
}

// ─── PDF helpers ─────────────────────────────────────────────────────────────

/// Extract all tinyurl URIs from a PDF document.
fn extract_pdf_tinyurls(doc: &lopdf::Document) -> Vec<String> {
    let mut urls = Vec::new();
    for obj in doc.objects.values() {
        if let lopdf::Object::Dictionary(ref dict) = obj {
            let is_uri = dict
                .get(b"S")
                .map(|s| matches!(s, lopdf::Object::Name(n) if n == b"URI"))
                .unwrap_or(false);
            if !is_uri {
                continue;
            }
            if let Ok(lopdf::Object::String(bytes, _)) = dict.get(b"URI") {
                let uri = String::from_utf8_lossy(bytes).to_string();
                if uri.contains("tinyurl.com") {
                    urls.push(uri);
                }
            }
        }
    }
    urls.sort();
    urls.dedup();
    urls
}

/// Resolve an indirect reference to the underlying object.
fn resolve_obj<'a>(doc: &'a lopdf::Document, obj: &'a lopdf::Object) -> &'a lopdf::Object {
    match obj {
        lopdf::Object::Reference(id) => doc.get_object(*id).unwrap_or(obj),
        _ => obj,
    }
}

/// Collect `(image_object_id, lin_url)` pairs for each page that has a BBO
/// screenshot (large image) and a handviewer link annotation.
fn collect_page_image_info(doc: &lopdf::Document) -> Vec<(lopdf::ObjectId, String)> {
    let mut results = Vec::new();
    let pages = doc.get_pages();

    for (&_page_num, &page_id) in &pages {
        let page_dict = match doc.get_object(page_id) {
            Ok(lopdf::Object::Dictionary(dict)) => dict,
            _ => continue,
        };

        // ── Find LIN URL from link annotations on this page ──
        let mut lin_url: Option<String> = None;
        if let Ok(annots_obj) = page_dict.get(b"Annots") {
            let annots_arr = match resolve_obj(doc, annots_obj) {
                lopdf::Object::Array(arr) => arr,
                _ => continue,
            };
            for annot_ref in annots_arr {
                let annot_dict = match resolve_obj(doc, annot_ref) {
                    lopdf::Object::Dictionary(d) => d,
                    _ => continue,
                };
                // Follow /A action dictionary (may be inline or a reference)
                let action = match annot_dict.get(b"A") {
                    Ok(a) => a,
                    _ => continue,
                };
                let action_dict = match resolve_obj(doc, action) {
                    lopdf::Object::Dictionary(d) => d,
                    _ => continue,
                };
                if let Ok(lopdf::Object::String(uri_bytes, _)) = action_dict.get(b"URI") {
                    let uri = String::from_utf8_lossy(uri_bytes).to_string();
                    if uri.contains("handviewer") || uri.contains("lin=") {
                        lin_url = Some(uri);
                        break;
                    }
                }
            }
        }

        let lin_url = match lin_url {
            Some(u) => u,
            None => continue,
        };

        // ── Find large image XObject on this page ──
        let resources = match page_dict.get(b"Resources") {
            Ok(r) => r,
            _ => continue,
        };
        let resources_dict = match resolve_obj(doc, resources) {
            lopdf::Object::Dictionary(d) => d,
            _ => continue,
        };
        let xobjects = match resources_dict.get(b"XObject") {
            Ok(x) => x,
            _ => continue,
        };
        let xobj_dict = match resolve_obj(doc, xobjects) {
            lopdf::Object::Dictionary(d) => d,
            _ => continue,
        };

        for (_name, value) in xobj_dict.iter() {
            let img_id = match value {
                lopdf::Object::Reference(id) => *id,
                _ => continue,
            };
            let stream = match doc.get_object(img_id) {
                Ok(lopdf::Object::Stream(s)) => s,
                _ => continue,
            };
            // Check if this is a large image (BBO screenshot)
            let width = stream
                .dict
                .get(b"Width")
                .ok()
                .and_then(|o| {
                    if let lopdf::Object::Integer(n) = o {
                        Some(*n as usize)
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            let height = stream
                .dict
                .get(b"Height")
                .ok()
                .and_then(|o| {
                    if let lopdf::Object::Integer(n) = o {
                        Some(*n as usize)
                    } else {
                        None
                    }
                })
                .unwrap_or(0);

            if width > 1000 && height > 1000 {
                results.push((img_id, lin_url.clone()));
                break; // one image per page
            }
        }
    }

    results
}

// TrueType text rendering and image modification functions are in anon_common.

/// Process all BBO screenshot images in the document, overwriting player name
/// areas with anonymized names extracted from the page's link annotation URL.
fn anonymize_bbo_images(doc: &mut lopdf::Document) -> Result<u32> {
    let font = load_system_font()?;

    // First pass: collect image object IDs and their associated LIN URLs
    let image_info = collect_page_image_info(doc);
    let mut modified = 0u32;

    for (img_id, lin_url) in &image_info {
        let names = match extract_player_names(lin_url) {
            Some(n) => n,
            None => {
                eprintln!(
                    "  Warning: could not parse player names from URL (obj {:?})",
                    img_id
                );
                continue;
            }
        };

        // Get image dimensions from the stream dictionary
        let (width, height) = {
            let stream = match doc.get_object(*img_id) {
                Ok(lopdf::Object::Stream(s)) => s,
                _ => continue,
            };
            let w = stream
                .dict
                .get(b"Width")
                .ok()
                .and_then(|o| {
                    if let lopdf::Object::Integer(n) = o {
                        Some(*n as usize)
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            let h = stream
                .dict
                .get(b"Height")
                .ok()
                .and_then(|o| {
                    if let lopdf::Object::Integer(n) = o {
                        Some(*n as usize)
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            (w, h)
        };

        if width == 0 || height == 0 {
            continue;
        }

        // Decompress and modify the stream content (raw RGB bytes)
        let stream = match doc.get_object_mut(*img_id) {
            Ok(lopdf::Object::Stream(s)) => s,
            _ => continue,
        };

        // Decompress the stream data
        if stream.decompress().is_err() {
            eprintln!(
                "  Warning: failed to decompress image stream (obj {:?})",
                img_id
            );
            continue;
        }

        let expected_len = width * height * 3;
        if stream.content.len() < expected_len {
            eprintln!(
                "  Warning: image data too short (obj {:?}): {} < {}",
                img_id,
                stream.content.len(),
                expected_len
            );
            continue;
        }

        // Modify the pixels
        modify_screenshot_pixels(&mut stream.content, width, height, &names, &font, 3);

        // Re-compress
        let _ = stream.compress();

        println!(
            "  Image {:?} ({}x{}): {} -> [N]{} [S]{} [W]{} [E]{}",
            img_id,
            width,
            height,
            lin_url.len(),
            names[2],
            names[0],
            names[1],
            names[3]
        );
        modified += 1;
    }

    Ok(modified)
}

// ─── URL resolution ──────────────────────────────────────────────────────────

/// Follow a tinyurl redirect and return the destination URL.
fn resolve_tinyurl(client: &reqwest::blocking::Client, url: &str) -> Result<String> {
    let resp = client
        .head(url)
        .send()
        .with_context(|| format!("Failed to resolve {}", url))?;
    Ok(resp.url().to_string())
}

/// Replace player names in all URI annotations (not just tinyurls).
/// This catches direct handviewer links that contain original player names.
fn replace_names_in_uris(doc: &mut lopdf::Document, name_pairs: &[(String, String)]) -> u32 {
    let mut count = 0u32;
    let obj_ids: Vec<_> = doc.objects.keys().copied().collect();

    for obj_id in obj_ids {
        let obj = match doc.objects.get(&obj_id) {
            Some(obj) => obj.clone(),
            None => continue,
        };

        if let lopdf::Object::Dictionary(ref dict) = obj {
            let is_uri = dict
                .get(b"S")
                .map(|s| matches!(s, lopdf::Object::Name(n) if n == b"URI"))
                .unwrap_or(false);
            if !is_uri {
                continue;
            }
            let uri_bytes = match dict.get(b"URI") {
                Ok(lopdf::Object::String(bytes, _)) => bytes.clone(),
                _ => continue,
            };
            let uri_str = String::from_utf8_lossy(&uri_bytes).to_string();

            // Check if any name in our map appears in this URL
            let mut new_uri = uri_str.clone();
            let mut changed = false;
            for (original, replacement) in name_pairs {
                if new_uri.contains(original.as_str()) {
                    new_uri = new_uri.replace(original.as_str(), replacement);
                    changed = true;
                }
            }

            if changed {
                let mut new_dict = dict.clone();
                new_dict.set(
                    "URI",
                    lopdf::Object::String(
                        new_uri.as_bytes().to_vec(),
                        lopdf::StringFormat::Literal,
                    ),
                );
                doc.objects
                    .insert(obj_id, lopdf::Object::Dictionary(new_dict));
                count += 1;
            }
        }
    }
    count
}

// ─── Page text replacement ───────────────────────────────────────────────────

/// Load text replacement map and convert to byte pairs for PDF content streams.
fn load_text_map_bytes(path: &std::path::Path) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let pairs = load_text_map(path)?;
    Ok(pairs
        .into_iter()
        .map(|(k, v)| (k.into_bytes(), v.into_bytes()))
        .collect())
}

/// Location of a single character byte within the parsed content operations.
#[derive(Clone)]
struct TextChar {
    op_idx: usize,
    /// For TJ: element index within the Array. For Tj: always 0.
    arr_idx: usize,
    /// Byte offset within that String operand.
    byte_idx: usize,
}

/// Replace text in a single page's content stream, matching across TJ operands.
///
/// Uses per-character in-place byte replacement: each replacement byte is written
/// to the exact operand position of the corresponding search byte.  This preserves
/// all original kerning, cursor advance, and font context.
fn replace_page_text(
    doc: &mut lopdf::Document,
    page_id: lopdf::ObjectId,
    replacements: &[(Vec<u8>, Vec<u8>)],
) -> Result<usize> {
    let mut content = doc.get_and_decode_page_content(page_id)?;
    let mut total = 0usize;
    let mut changed = false;

    // Build text blocks: collect all (byte, location) tuples between BT/ET.
    let mut blocks: Vec<(Vec<u8>, Vec<TextChar>)> = Vec::new();
    let mut cur_bytes: Vec<u8> = Vec::new();
    let mut cur_locs: Vec<TextChar> = Vec::new();
    let mut in_text = false;

    for (op_idx, op) in content.operations.iter().enumerate() {
        match op.operator.as_ref() {
            "BT" => {
                in_text = true;
                cur_bytes.clear();
                cur_locs.clear();
            }
            "ET" => {
                if !cur_bytes.is_empty() {
                    blocks.push((cur_bytes.clone(), cur_locs.clone()));
                }
                in_text = false;
            }
            "Tj" if in_text => {
                if let Some(lopdf::Object::String(bytes, _)) = op.operands.first() {
                    for (byte_idx, &b) in bytes.iter().enumerate() {
                        cur_bytes.push(b);
                        cur_locs.push(TextChar {
                            op_idx,
                            arr_idx: 0,
                            byte_idx,
                        });
                    }
                }
            }
            "TJ" if in_text => {
                if let Some(lopdf::Object::Array(arr)) = op.operands.first() {
                    for (arr_idx, item) in arr.iter().enumerate() {
                        match item {
                            lopdf::Object::String(bytes, _) => {
                                for (byte_idx, &b) in bytes.iter().enumerate() {
                                    cur_bytes.push(b);
                                    cur_locs.push(TextChar {
                                        op_idx,
                                        arr_idx,
                                        byte_idx,
                                    });
                                }
                            }
                            lopdf::Object::Integer(n) if *n < -100 => {
                                // Large negative kerning ≈ word space
                                cur_bytes.push(b' ');
                                cur_locs.push(TextChar {
                                    op_idx,
                                    arr_idx,
                                    byte_idx: usize::MAX,
                                });
                            }
                            _ => {}
                        }
                    }
                }
            }
            _ => {}
        }
    }

    // Apply replacements to each text block using per-character in-place
    // byte replacement.  Each replacement byte is written to the exact
    // operand position of the corresponding search byte, preserving all
    // original kerning and cursor advance.  If the replacement is shorter,
    // remaining matched positions are set to space (0x20).
    for (block_bytes, block_locs) in &blocks {
        for (search, replace) in replacements {
            if search.is_empty() {
                continue;
            }
            // Pad replacement to search length with spaces.
            let mut padded = replace.to_vec();
            while padded.len() < search.len() {
                padded.push(b' ');
            }
            let mut start = 0;
            while start + search.len() <= block_bytes.len() {
                if let Some(pos) = find_subsequence(&block_bytes[start..], search) {
                    let abs_pos = start + pos;
                    let match_locs = &block_locs[abs_pos..abs_pos + search.len()];

                    let mut any_written = false;
                    for (i, loc) in match_locs.iter().enumerate() {
                        if loc.byte_idx == usize::MAX {
                            continue; // synthetic space from kerning — skip
                        }
                        set_byte_at(
                            &mut content.operations,
                            loc.op_idx,
                            loc.arr_idx,
                            loc.byte_idx,
                            padded[i],
                        );
                        any_written = true;
                    }
                    if any_written {
                        changed = true;
                        total += 1;
                    }

                    start = abs_pos + search.len();
                } else {
                    break;
                }
            }
        }
    }

    if changed {
        let encoded = content.encode()?;
        doc.change_page_content(page_id, encoded)?;
    }

    Ok(total)
}

/// Modify a single byte in a Tj/TJ string operand in place.
fn set_byte_at(
    ops: &mut [lopdf::content::Operation],
    op_idx: usize,
    arr_idx: usize,
    byte_idx: usize,
    value: u8,
) {
    let op = &mut ops[op_idx];
    match op.operator.as_ref() {
        "Tj" => {
            if let Some(lopdf::Object::String(bytes, _)) = op.operands.first_mut() {
                if byte_idx < bytes.len() {
                    bytes[byte_idx] = value;
                }
            }
        }
        "TJ" => {
            if let Some(lopdf::Object::Array(arr)) = op.operands.first_mut() {
                if let Some(lopdf::Object::String(bytes, _)) = arr.get_mut(arr_idx) {
                    if byte_idx < bytes.len() {
                        bytes[byte_idx] = value;
                    }
                }
            }
        }
        _ => {}
    }
}

/// Find first occurrence of `needle` in `haystack`.
fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

// ─── Main commands ───────────────────────────────────────────────────────────

/// Replace link mode: replace tinyurl links and anonymize BBO screenshots.
#[allow(clippy::too_many_arguments)]
fn run_replace(
    pdf_path: &PathBuf,
    lookup_path: &PathBuf,
    anon_path: &PathBuf,
    extra_map_path: Option<&PathBuf>,
    output_path: &PathBuf,
    anon_images: bool,
    name_map: Option<&str>,
    text_map_path: Option<&PathBuf>,
) -> Result<()> {
    println!("Building URL mapping...");
    let mut url_map = build_url_mapping(lookup_path, anon_path)?;
    println!("  {} mappings from lookup + anon CSVs", url_map.len());

    if let Some(extra) = extra_map_path {
        let extra_map = load_extra_mapping(extra)?;
        println!(
            "  {} extra mappings from {}",
            extra_map.len(),
            extra.display()
        );
        url_map.extend(extra_map);
    }

    // Build comprehensive player name map and anonymize all replacement URLs
    let player_names = build_player_name_map(lookup_path, anon_path)?;
    println!("  {} unique player name mappings", player_names.len());
    anonymize_mapping_urls(&mut url_map, &player_names);

    println!("\nOpening PDF: {}", pdf_path.display());
    let mut doc = lopdf::Document::load(pdf_path).context("Failed to load PDF")?;

    println!("Replacing link annotations...");
    let mut count = 0;
    let mut unmatched = 0;

    let obj_ids: Vec<_> = doc.objects.keys().copied().collect();
    for obj_id in obj_ids {
        let obj = match doc.objects.get(&obj_id) {
            Some(obj) => obj.clone(),
            None => continue,
        };

        if let lopdf::Object::Dictionary(ref dict) = obj {
            let is_uri = dict
                .get(b"S")
                .map(|s| matches!(s, lopdf::Object::Name(n) if n == b"URI"))
                .unwrap_or(false);
            if !is_uri {
                continue;
            }
            let uri_bytes = match dict.get(b"URI") {
                Ok(lopdf::Object::String(bytes, _)) => bytes.clone(),
                _ => continue,
            };
            let uri_str = String::from_utf8_lossy(&uri_bytes).to_string();
            let key = normalize_tinyurl(&uri_str);

            if let Some(replacement) = url_map.get(&key) {
                let mut new_dict = dict.clone();
                new_dict.set(
                    "URI",
                    lopdf::Object::String(
                        replacement.as_bytes().to_vec(),
                        lopdf::StringFormat::Literal,
                    ),
                );
                doc.objects
                    .insert(obj_id, lopdf::Object::Dictionary(new_dict));
                count += 1;
            } else if uri_str.contains("tinyurl.com") {
                eprintln!("  UNMATCHED: {} (key: {})", uri_str, key);
                unmatched += 1;
            }
        }
    }

    println!("\nReplaced {} tinyurl links", count);
    if unmatched > 0 {
        eprintln!("{} tinyurl links had no match", unmatched);
    }

    // ── Replace player names in remaining URLs (direct handviewer links) ──
    // Build URL-encoded name pairs for URI replacement
    let mut uri_pairs: Vec<(String, String)> = player_names
        .iter()
        .map(|(orig, anon)| (orig.replace('+', "%2B"), anon.replace('+', "%2B")))
        .collect();
    // Also add any manual --name-map overrides
    if let Some(nm) = name_map {
        uri_pairs.extend(parse_name_map(nm));
    }
    if !uri_pairs.is_empty() {
        println!("\nReplacing player names in remaining URLs...");
        let name_count = replace_names_in_uris(&mut doc, &uri_pairs);
        println!("  {} URLs updated with name replacements", name_count);
    }

    // ── Replace visible page text ──
    if let Some(tm_path) = text_map_path {
        let text_pairs = load_text_map_bytes(tm_path)?;
        println!(
            "\nReplacing page text ({} rules from {})...",
            text_pairs.len(),
            tm_path.display()
        );
        let mut text_count = 0usize;
        let page_ids: Vec<lopdf::ObjectId> = doc.page_iter().collect();
        for &page_id in &page_ids {
            match replace_page_text(&mut doc, page_id, &text_pairs) {
                Ok(n) => text_count += n,
                Err(e) => eprintln!("  Warning: page {:?}: {}", page_id, e),
            }
        }
        println!(
            "  {} text replacements across {} pages",
            text_count,
            page_ids.len()
        );
    }

    // ── Anonymize BBO screenshot images ──
    if anon_images {
        println!("\nAnonymizing BBO screenshot images...");
        let img_count = anonymize_bbo_images(&mut doc)?;
        println!("Modified {} BBO screenshot images", img_count);
    }

    doc.save(output_path).context("Failed to save PDF")?;
    println!("\nSaved to: {}", output_path.display());
    Ok(())
}

/// Resolve mode: resolve unmatched ACBL tinyurls and produce a mapping CSV.
fn run_resolve(
    pdf_path: Option<&PathBuf>,
    lookup_path: &PathBuf,
    anon_path: &PathBuf,
    output_path: &PathBuf,
) -> Result<()> {
    let primary_map = build_url_mapping(lookup_path, anon_path)?;
    println!("{} primary mappings loaded", primary_map.len());

    println!("Building deal fingerprint index...");
    let fp_index = build_fingerprint_index(lookup_path, anon_path)?;
    println!("  {} unique deal fingerprints indexed", fp_index.len());

    let unmatched: Vec<String> = if let Some(pdf) = pdf_path {
        println!("\nScanning PDF for tinyurl links...");
        let doc = lopdf::Document::load(pdf).context("Failed to load PDF")?;
        let all_urls = extract_pdf_tinyurls(&doc);
        println!("  {} unique tinyurls found in PDF", all_urls.len());
        all_urls
            .into_iter()
            .filter(|u| !primary_map.contains_key(&normalize_tinyurl(u)))
            .collect()
    } else {
        anyhow::bail!("--pdf is required for resolve mode");
    };

    println!("  {} unmatched tinyurls to resolve\n", unmatched.len());

    let client = reqwest::blocking::Client::builder()
        .redirect(reqwest::redirect::Policy::limited(10))
        .timeout(std::time::Duration::from_secs(15))
        .build()?;

    let mut writer = csv::Writer::from_path(output_path)?;
    writer.write_record(["ACBL_TinyURL", "Anon_LIN_URL", "Match_Fingerprint"])?;

    let mut matched = 0;
    let mut failed = 0;

    for url in &unmatched {
        let key = normalize_tinyurl(url);
        print!("  {} -> ", key);

        match resolve_tinyurl(&client, url) {
            Ok(dest) => {
                if let Some(fp) = extract_deal_fingerprint(&dest) {
                    if let Some((_bbo_key, anon_lin)) = fp_index.get(&fp) {
                        println!("MATCHED (fp: {}...)", &fp[..fp.len().min(12)]);
                        writer.write_record([url.as_str(), anon_lin.as_str(), &fp])?;
                        matched += 1;
                    } else {
                        println!("no fingerprint match (fp: {}...)", &fp[..fp.len().min(12)]);
                        failed += 1;
                    }
                } else {
                    println!(
                        "no deal data in destination: {}",
                        &dest[..dest.len().min(80)]
                    );
                    failed += 1;
                }
            }
            Err(e) => {
                println!("FAILED: {}", e);
                failed += 1;
            }
        }
    }

    writer.flush()?;
    println!(
        "\nDone! {} matched, {} failed out of {} unmatched",
        matched,
        failed,
        unmatched.len()
    );
    println!("Saved to: {}", output_path.display());
    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Resolve {
            pdf,
            lookup,
            anon,
            output,
        }) => run_resolve(pdf.as_ref(), &lookup, &anon, &output),

        None => {
            let pdf = cli.pdf.context("--pdf is required")?;
            let lookup = cli.lookup.context("--lookup is required")?;
            let anon = cli.anon.context("--anon is required")?;
            let output = cli.output.context("--output is required")?;
            run_replace(
                &pdf,
                &lookup,
                &anon,
                cli.extra_map.as_ref(),
                &output,
                !cli.no_images,
                cli.name_map.as_deref(),
                cli.text_map.as_ref(),
            )
        }
    }
}
