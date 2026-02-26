//! Anonymize a DOCX disclosure document by replacing tinyurl hyperlinks
//! with anonymized BBO handviewer URLs, replacing visible text (names,
//! ACBL numbers), and anonymizing BBO screenshot images.
//!
//! Usage:
//!   docx-anon --docx input.docx --lookup lookup.csv --anon anon.csv \
//!     [--extra-map extra.csv] [--text-map map.txt] -o output.docx

use ab_glyph::FontVec;
use anyhow::{Context, Result};
use clap::Parser;
use edgar_defense_toolkit::anon_common::*;
use regex::Regex;
use std::collections::HashMap;
use std::io::{Cursor, Read, Write};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "docx-anon", about = "Anonymize a DOCX disclosure document")]
struct Cli {
    /// Input DOCX file
    #[arg(long)]
    docx: PathBuf,

    /// Tinyurl lookup CSV (must have TinyURL and LIN_URL columns)
    #[arg(long)]
    lookup: PathBuf,

    /// Anonymized DD CSV (must have LIN_URL column with anonymized URLs)
    #[arg(long)]
    anon: PathBuf,

    /// Extra tinyurl mapping CSV (ACBL_TinyURL, Anon_LIN_URL)
    #[arg(long)]
    extra_map: Option<PathBuf>,

    /// Output DOCX file
    #[arg(short, long)]
    output: PathBuf,

    /// Skip BBO screenshot image anonymization
    #[arg(long)]
    no_images: bool,

    /// Name replacement map for remaining URLs (e.g., "Spwilliams=Bob,Adwilliams=Sally")
    #[arg(long)]
    name_map: Option<String>,

    /// Text map file for document text replacement (one "old=new" per line)
    #[arg(long)]
    text_map: Option<PathBuf>,

    /// Start marker text for paragraph redaction (inclusive)
    #[arg(long)]
    redact_start: Option<String>,

    /// End marker text for paragraph redaction (exclusive — this paragraph is NOT redacted)
    #[arg(long)]
    redact_end: Option<String>,

    /// Comma-separated image filenames to replace with solid fill (e.g., "image1.png,image2.png")
    #[arg(long)]
    blank_images: Option<String>,

    /// Convention card image filename to redact player names from
    #[arg(long)]
    cc_redact: Option<String>,

    /// Replacement text for convention card names (e.g., "Bob & Sally")
    #[arg(long)]
    cc_names: Option<String>,

    /// Replacement text for redacted paragraphs (shown in first redacted paragraph)
    #[arg(long)]
    redact_replacement: Option<String>,
}

// ─── DOCX zip I/O ───────────────────────────────────────────────────────────

/// Read a DOCX zip into an ordered list of (entry_name, bytes) preserving order.
fn read_docx(path: &PathBuf) -> Result<Vec<(String, Vec<u8>)>> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("Failed to open DOCX: {}", path.display()))?;
    let mut archive = zip::ZipArchive::new(file)?;
    let mut entries = Vec::new();
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        let name = entry.name().to_string();
        let mut data = Vec::new();
        entry.read_to_end(&mut data)?;
        entries.insert(entries.len(), (name, data));
    }
    Ok(entries)
}

/// Write an ordered list of (entry_name, bytes) back to a DOCX zip.
/// Uses STORED for media files (images) and DEFLATED for everything else,
/// matching the typical DOCX layout that Word expects.
fn write_docx(path: &PathBuf, entries: &[(String, Vec<u8>)]) -> Result<()> {
    let file = std::fs::File::create(path)
        .with_context(|| format!("Failed to create output: {}", path.display()))?;
    let mut zip = zip::ZipWriter::new(file);
    let deflated = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated);
    let stored =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (name, data) in entries {
        let opts = if name.starts_with("word/media/") {
            stored
        } else {
            deflated
        };
        zip.start_file(name, opts)?;
        zip.write_all(data)?;
    }
    zip.finish()?;
    Ok(())
}

// ─── Rels URL replacement ───────────────────────────────────────────────────

/// Replace tinyurl Target attributes in the rels XML with anonymized URLs.
/// Returns the modified XML and the count of replacements.
fn replace_rels_urls(rels_xml: &str, url_map: &HashMap<String, String>) -> (String, u32) {
    let re =
        Regex::new(r#"Target="(https?://(?:www\.)?tinyurl\.com/[^"]+)""#).expect("invalid regex");
    let mut count = 0u32;
    let result = re.replace_all(rels_xml, |caps: &regex::Captures| {
        let original_url = &caps[1];
        let key = normalize_tinyurl(original_url);
        if let Some(replacement) = url_map.get(&key) {
            count += 1;
            // Escape & for XML attributes
            let escaped = replacement.replace('&', "&amp;");
            format!("Target=\"{}\"", escaped)
        } else {
            caps[0].to_string()
        }
    });
    (result.into_owned(), count)
}

// ─── Document text replacement ──────────────────────────────────────────────

/// Replace text across `<w:t>` runs within each `<w:p>` paragraph.
///
/// For each paragraph, concatenates all `<w:t>` text to form a virtual string,
/// applies all replacement rules, then distributes the modified text back into
/// the original `<w:t>` elements.  When a match spans multiple `<w:t>` elements,
/// the replacement goes into the first one and matched portions of subsequent
/// elements are removed.
fn replace_document_text(xml: &str, replacements: &[(String, String)]) -> (String, usize) {
    let mut result = xml.to_string();
    let mut total = 0usize;

    // Regex for <w:t> content within a paragraph
    let wt_re = Regex::new(r#"<w:t(?: [^>]*)?>([^<]*)</w:t>"#).expect("invalid regex");

    // Find all paragraph byte ranges in the original XML (before any modifications)
    // Using a simple stack-based approach for nested safety
    let paragraphs = find_paragraphs(&result);

    // Process in reverse order so modifications don't shift earlier offsets
    for (p_start, p_end) in paragraphs.into_iter().rev() {
        let para_xml = result[p_start..p_end].to_string();

        // Find all <w:t> elements in this paragraph (offsets relative to paragraph)
        let wt_matches: Vec<(usize, usize, String)> = wt_re
            .captures_iter(&para_xml)
            .map(|caps| {
                let text_match = caps.get(1).expect("no group 1");
                (
                    text_match.start(),
                    text_match.end(),
                    text_match.as_str().to_string(),
                )
            })
            .collect();

        if wt_matches.is_empty() {
            continue;
        }

        // Build virtual text and a char-to-wt mapping
        // For each char in virtual text: (wt_index, char_offset_within_wt)
        let mut virtual_text = String::new();
        let mut char_map: Vec<(usize, usize)> = Vec::new();
        for (wt_idx, (_, _, text)) in wt_matches.iter().enumerate() {
            for (char_offset, ch) in text.char_indices() {
                char_map.push((wt_idx, char_offset));
                virtual_text.push(ch);
            }
        }

        if virtual_text.is_empty() {
            continue;
        }

        // Use char array for searching — str::find() returns byte offsets which
        // diverge from char indices when the text contains multi-byte UTF-8
        // characters (smart quotes, em dashes, etc.)
        let vt_chars: Vec<char> = virtual_text.chars().collect();

        let mut modifications: Vec<(usize, usize, String)> = Vec::new();

        for (search, replace) in replacements {
            if search.is_empty() {
                continue;
            }
            let search_chars: Vec<char> = search.chars().collect();
            let slen = search_chars.len();
            let mut pos = 0;
            while pos + slen <= vt_chars.len() {
                if vt_chars[pos..pos + slen] == search_chars[..] {
                    let abs_start = pos;
                    let abs_end = pos + slen;
                    let overlaps = modifications
                        .iter()
                        .any(|(s, e, _)| abs_start < *e && abs_end > *s);
                    if !overlaps {
                        modifications.push((abs_start, abs_end, replace.clone()));
                        total += 1;
                    }
                    pos = abs_end;
                } else {
                    pos += 1;
                }
            }
        }

        if modifications.is_empty() {
            continue;
        }

        // Build new text for each <w:t> element by applying modifications
        let mut new_wt_texts: Vec<String> = wt_matches.iter().map(|(_, _, t)| t.clone()).collect();

        // Apply modifications to new_wt_texts
        // Sort by start position for easier processing
        modifications.sort_by_key(|(s, _, _)| *s);

        // Build a per-char replacement: for each position in vt_chars, what should it become?
        let mut char_replacements: Vec<Option<char>> = vt_chars.iter().copied().map(Some).collect();

        for (match_start, match_end, replacement) in &modifications {
            // Clear all matched positions
            for item in &mut char_replacements[*match_start..*match_end] {
                *item = None;
            }
            // Insert replacement chars at the match start
            for (i, ch) in replacement.chars().enumerate() {
                let pos = match_start + i;
                if pos < char_replacements.len() {
                    char_replacements[pos] = Some(ch);
                }
                // If replacement is longer than match, we'd need to insert.
                // For our use case, replacements are always <= match length,
                // so extra chars get appended to the first wt element.
            }
            // Handle replacement longer than match: append extra to first wt's position
            if replacement.chars().count() > (match_end - match_start) {
                // This shouldn't happen for our use case, but handle gracefully
                let extra: String = replacement.chars().skip(match_end - match_start).collect();
                let (first_wt_idx, _) = char_map[*match_start];
                new_wt_texts[first_wt_idx].push_str(&extra);
            }
        }

        // Rebuild each <w:t> text from the char_replacements
        for (wt_idx, text) in new_wt_texts.iter_mut().enumerate() {
            let mut new_text = String::new();
            for (i, &(ci_wt, _)) in char_map.iter().enumerate() {
                if ci_wt == wt_idx {
                    if let Some(ch) = char_replacements[i] {
                        new_text.push(ch);
                    }
                }
            }
            *text = new_text;
        }

        // Now replace the <w:t> contents in the paragraph XML (reverse order for offset safety)
        let mut new_para = para_xml.clone();
        for (wt_idx, &(content_start, content_end, _)) in wt_matches.iter().enumerate().rev() {
            new_para.replace_range(content_start..content_end, &new_wt_texts[wt_idx]);
        }

        // Replace the paragraph in the full result
        result.replace_range(p_start..p_end, &new_para);
    }

    (result, total)
}

/// Find all `<w:p ...>...</w:p>` paragraph byte ranges in the XML.
fn find_paragraphs(xml: &str) -> Vec<(usize, usize)> {
    let p_re = Regex::new(r"<w:p[ >]").expect("invalid regex");
    let p_end_re = Regex::new(r"</w:p>").expect("invalid regex");

    let starts: Vec<usize> = p_re.find_iter(xml).map(|m| m.start()).collect();
    let ends: Vec<usize> = p_end_re.find_iter(xml).map(|m| m.end()).collect();

    let mut paragraphs = Vec::new();
    for &ps in &starts {
        if let Some(&pe) = ends.iter().find(|&&pe| pe > ps) {
            paragraphs.push((ps, pe));
        }
    }
    paragraphs
}

// ─── Paragraph redaction ────────────────────────────────────────────────────

/// Redact paragraph text between start and end marker texts.
/// The start marker paragraph is KEPT (not redacted).  The end marker paragraph
/// is also kept.  Everything in between is cleared.  If `replacement` is provided,
/// the first redacted paragraph's text is set to that string instead of being
/// emptied.
fn redact_paragraphs(
    xml: &str,
    start_marker: &str,
    end_marker: &str,
    replacement: Option<&str>,
) -> (String, usize) {
    let wt_re = Regex::new(r#"<w:t(?: [^>]*)?>([^<]*)</w:t>"#).expect("invalid regex");
    let paragraphs = find_paragraphs(xml);

    // First pass (forward): identify paragraphs in the redaction range
    let mut in_range = false;
    let mut to_redact: Vec<(usize, usize)> = Vec::new();

    for &(p_start, p_end) in &paragraphs {
        let para = &xml[p_start..p_end];
        let vtext: String = wt_re
            .captures_iter(para)
            .filter_map(|c| c.get(1).map(|m| m.as_str().to_string()))
            .collect();

        if vtext.contains(start_marker) {
            in_range = true;
            continue; // Keep the start marker paragraph as-is
        }
        if in_range && vtext.contains(end_marker) {
            in_range = false;
            continue; // Keep the end marker paragraph as-is
        }
        if in_range {
            to_redact.push((p_start, p_end));
        }
    }

    // Second pass (reverse): clear <w:t> content using byte positions.
    // The replacement text goes into the first paragraph that actually has text.
    let mut result = xml.to_string();
    let count = to_redact.len();

    // Find the first paragraph with non-empty <w:t> content (for replacement text)
    let first_text_idx = to_redact
        .iter()
        .find(|&&(ps, pe)| {
            let para = &xml[ps..pe];
            wt_re
                .captures_iter(para)
                .any(|c| c.get(1).is_some_and(|m| !m.as_str().is_empty()))
        })
        .map(|&(s, _)| s);

    for &(p_start, p_end) in to_redact.iter().rev() {
        let para = result[p_start..p_end].to_string();
        let mut new_para = para.clone();

        let wt_contents: Vec<(usize, usize)> = wt_re
            .captures_iter(&para)
            .filter_map(|c| {
                let content = c.get(1).unwrap();
                if content.as_str().is_empty() {
                    None
                } else {
                    Some((content.start(), content.end()))
                }
            })
            .collect();

        // Put replacement text in the first <w:t> of the first text-bearing paragraph
        let is_replacement_para = Some(p_start) == first_text_idx;
        for (i, (start, end)) in wt_contents.iter().enumerate().rev() {
            if is_replacement_para && i == 0 {
                if let Some(rep) = replacement {
                    new_para.replace_range(*start..*end, rep);
                } else {
                    new_para.replace_range(*start..*end, "");
                }
            } else {
                new_para.replace_range(*start..*end, "");
            }
        }
        result.replace_range(p_start..p_end, &new_para);
    }

    (result, count)
}

// ─── Image blanking/redaction ───────────────────────────────────────────────

/// Create a solid-colored PNG of the same dimensions/format as the source image.
fn create_blank_png(png_data: &[u8]) -> Result<Vec<u8>> {
    let decoder = png::Decoder::new(Cursor::new(png_data));
    let reader = decoder.read_info().context("Failed to decode PNG")?;
    let info = reader.info().clone();

    // All-zero pixels = solid black (works for RGB, RGBA, Grayscale)
    let channels = match info.color_type {
        png::ColorType::Rgba => 4,
        png::ColorType::Rgb => 3,
        _ => 3,
    };
    let pixels = vec![0u8; info.width as usize * info.height as usize * channels];

    let mut output = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut output, info.width, info.height);
        encoder.set_color(info.color_type);
        encoder.set_depth(info.bit_depth);
        let mut writer = encoder.write_header()?;
        writer.write_image_data(&pixels)?;
    }
    Ok(output)
}

/// Redact the player names line from a convention card PNG image.
/// Paints over the top "NAMES ..." area with white and draws replacement text.
fn redact_cc_names(png_data: &[u8], replacement: &str, font: &FontVec) -> Result<Vec<u8>> {
    let decoder = png::Decoder::new(Cursor::new(png_data));
    let mut reader = decoder.read_info().context("Failed to decode PNG")?;
    let info = reader.info().clone();

    let mut buf = vec![0u8; reader.output_buffer_size()];
    reader.next_frame(&mut buf)?;

    let w = info.width as usize;
    let h = info.height as usize;
    let channels = match info.color_type {
        png::ColorType::Rgba => 4,
        png::ColorType::Rgb => 3,
        _ => anyhow::bail!("Unsupported color type: {:?}", info.color_type),
    };

    // Paint white rectangle over the names area (after "NAMES" label).
    // "NAMES" label ends at ~8% of width; names extend to ~85% of width.
    // Height covers the first ~2% of the image (enough for the full text line).
    let x_start = (w as f64 * 0.08) as usize;
    let x_end = (w as f64 * 0.85) as usize;
    let y_end = (h as f64 * 0.022).max(26.0) as usize;

    for py in 0..y_end.min(h) {
        for px in x_start..x_end.min(w) {
            let idx = (py * w + px) * channels;
            if idx + 2 < buf.len() {
                buf[idx] = 255; // R
                buf[idx + 1] = 255; // G
                buf[idx + 2] = 255; // B
            }
        }
    }

    // Draw replacement text in black
    let font_height = y_end as f32 * 0.75;
    let text_y = (y_end as f32 - font_height) * 0.3;
    draw_text(
        &mut buf,
        w,
        h,
        channels,
        font,
        replacement,
        x_start as f32 + 4.0,
        text_y,
        font_height,
        (0, 0, 0),
    );

    // Re-encode
    let mut output = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut output, info.width, info.height);
        encoder.set_color(info.color_type);
        encoder.set_depth(info.bit_depth);
        let mut writer = encoder.write_header()?;
        writer.write_image_data(&buf)?;
    }
    Ok(output)
}

/// Update visible hyperlink text in the document — replace displayed tinyurl text
/// with a short sequential label ("Hand 1", "Hand 2", …).
fn replace_hyperlink_text(
    xml: &str,
    url_map: &HashMap<String, String>,
    rels_map: &HashMap<String, String>,
) -> (String, u32) {
    let mut result = xml.to_string();
    let mut count = 0u32;

    let hl_re = Regex::new(r#"<w:hyperlink[^>]*r:id="([^"]+)"[^>]*>"#).expect("invalid regex");
    let hl_end = "</w:hyperlink>";
    let wt_re = Regex::new(r#"<w:t(?: [^>]*)?>([^<]*)</w:t>"#).expect("invalid regex");

    // Collect tinyurl hyperlinks in document order, assigning sequential hand numbers
    let mut hyperlinks: Vec<(usize, usize, String, u32)> = Vec::new();
    let mut hand_num = 0u32;

    for caps in hl_re.captures_iter(&result) {
        let full = caps.get(0).expect("no match");
        let rid = caps[1].to_string();
        let hl_start = full.start();

        // Only process tinyurl hyperlinks that have a mapping
        let is_mapped_tinyurl = rels_map
            .get(&rid)
            .filter(|url| url.contains("tinyurl.com"))
            .and_then(|url| url_map.get(&normalize_tinyurl(url)))
            .is_some();

        if !is_mapped_tinyurl {
            continue;
        }

        if let Some(end_pos) = result[hl_start..].find(hl_end) {
            hand_num += 1;
            hyperlinks.push((hl_start, hl_start + end_pos + hl_end.len(), rid, hand_num));
        }
    }

    // Process in reverse to preserve offsets
    for (hl_start, hl_end_pos, _rid, num) in hyperlinks.iter().rev() {
        let hl_xml = result[*hl_start..*hl_end_pos].to_string();
        if let Some(caps) = wt_re.captures(&hl_xml) {
            let text_match = caps.get(1).expect("no group 1");
            let abs_start = hl_start + text_match.start();
            let abs_end = hl_start + text_match.end();
            let label = format!("Hand {}", num);
            result.replace_range(abs_start..abs_end, &label);
            count += 1;
        }
    }

    (result, count)
}

// ─── Image-URL association ──────────────────────────────────────────────────

/// Parse rels XML into rId -> target URL/path mapping.
fn parse_rels(xml: &str) -> HashMap<String, (String, String)> {
    // Handle various attribute orderings in Relationship elements
    let re = Regex::new(r#"<Relationship\s+([^>]+)/>"#).expect("invalid regex");
    let id_re = Regex::new(r#"Id="([^"]+)""#).expect("invalid regex");
    let type_re = Regex::new(r#"Type="([^"]+)""#).expect("invalid regex");
    let target_re = Regex::new(r#"Target="([^"]+)""#).expect("invalid regex");

    let mut map = HashMap::new();
    for caps in re.captures_iter(xml) {
        let attrs = &caps[1];
        let id = id_re.captures(attrs).map(|c| c[1].to_string());
        let type_str = type_re.captures(attrs).map(|c| c[1].to_string());
        let target = target_re
            .captures(attrs)
            .map(|c| c[1].replace("&amp;", "&"));
        if let (Some(id), Some(type_str), Some(target)) = (id, type_str, target) {
            map.insert(id, (type_str, target));
        }
    }
    map
}

/// Build image_media_path -> anonymized_LIN_URL pairs by walking document.xml
/// in order.  Each image is paired with its nearest tinyurl hyperlink — either
/// the most recent one before it (common case) or the first one after it (handles
/// the first BBO screenshot which appears above its hyperlink).
fn build_image_url_pairs(
    doc_xml: &str,
    rels: &HashMap<String, (String, String)>,
    url_map: &HashMap<String, String>,
) -> Vec<(String, String)> {
    enum Item {
        Hyperlink(String), // anon_url
        Image(String),     // media_path
    }

    let re = Regex::new(r#"(?:w:hyperlink[^>]*r:id="([^"]+)"|r:embed="([^"]+)")"#)
        .expect("invalid regex");

    // Collect all tinyurl hyperlinks and images in document order
    let mut items: Vec<Item> = Vec::new();

    for caps in re.captures_iter(doc_xml) {
        if let Some(hyperlink_id) = caps.get(1) {
            let rid = hyperlink_id.as_str();
            if let Some((type_str, target)) = rels.get(rid) {
                if type_str.contains("hyperlink") && target.contains("tinyurl.com") {
                    let key = normalize_tinyurl(target);
                    if let Some(anon_url) = url_map.get(&key) {
                        items.push(Item::Hyperlink(anon_url.clone()));
                    }
                }
            }
        }
        if let Some(embed_id) = caps.get(2) {
            let rid = embed_id.as_str();
            if let Some((type_str, target)) = rels.get(rid) {
                if type_str.contains("image") {
                    items.push(Item::Image(target.clone()));
                }
            }
        }
    }

    let n = items.len();
    let mut used = vec![false; n];
    let mut pairs = Vec::new();

    // Pass 1 (forward): pair each image with most recent preceding unused hyperlink
    let mut last_hl: Option<usize> = None;
    for i in 0..n {
        match &items[i] {
            Item::Hyperlink(_) => {
                last_hl = Some(i);
            }
            Item::Image(media_path) => {
                if let Some(j) = last_hl {
                    if !used[j] {
                        if let Item::Hyperlink(ref anon_url) = items[j] {
                            pairs.push((media_path.clone(), anon_url.clone()));
                            used[j] = true;
                            used[i] = true;
                        }
                    }
                }
                last_hl = None;
            }
        }
    }

    // Pass 2 (backward): pair unpaired images with nearest following unused hyperlink
    let mut next_hl: Option<usize> = None;
    for i in (0..n).rev() {
        match &items[i] {
            Item::Hyperlink(_) if !used[i] => {
                next_hl = Some(i);
            }
            Item::Image(ref media_path) if !used[i] => {
                if let Some(j) = next_hl {
                    if let Item::Hyperlink(ref anon_url) = items[j] {
                        pairs.push((media_path.clone(), anon_url.clone()));
                        used[j] = true;
                    }
                }
                next_hl = None;
            }
            Item::Image(_) => {
                // Don't carry a hyperlink across an already-paired image
                next_hl = None;
            }
            _ => {}
        }
    }

    pairs
}

// ─── PNG anonymization ──────────────────────────────────────────────────────

/// Decode a PNG, anonymize BBO screenshot player names, re-encode.
fn anonymize_png(png_data: &[u8], names: &[String; 4], font: &FontVec) -> Result<Vec<u8>> {
    let decoder = png::Decoder::new(Cursor::new(png_data));
    let mut reader = decoder.read_info().context("Failed to decode PNG")?;
    let info = reader.info().clone();

    let mut buf = vec![0u8; reader.output_buffer_size()];
    reader.next_frame(&mut buf)?;

    let channels = match info.color_type {
        png::ColorType::Rgba => 4,
        png::ColorType::Rgb => 3,
        _ => anyhow::bail!("Unsupported PNG color type: {:?}", info.color_type),
    };

    modify_screenshot_pixels(
        &mut buf,
        info.width as usize,
        info.height as usize,
        names,
        font,
        channels,
    );

    // Re-encode
    let mut output = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut output, info.width, info.height);
        encoder.set_color(info.color_type);
        encoder.set_depth(info.bit_depth);
        let mut writer = encoder.write_header()?;
        writer.write_image_data(&buf)?;
    }

    Ok(output)
}

// ─── Main ───────────────────────────────────────────────────────────────────

fn run(cli: &Cli) -> Result<()> {
    // 1. Build URL mapping
    println!("Building URL mapping...");
    let mut url_map = build_url_mapping(&cli.lookup, &cli.anon)?;
    println!("  {} mappings from lookup + anon CSVs", url_map.len());

    if let Some(ref extra) = cli.extra_map {
        let extra_map = load_extra_mapping(extra)?;
        println!(
            "  {} extra mappings from {}",
            extra_map.len(),
            extra.display()
        );
        url_map.extend(extra_map);
    }

    // 2. Build player name map and anonymize URLs
    let player_names = build_player_name_map(&cli.lookup, &cli.anon)?;
    println!("  {} unique player name mappings", player_names.len());
    anonymize_mapping_urls(&mut url_map, &player_names);

    // 3. Load text map
    let text_replacements = if let Some(ref tm_path) = cli.text_map {
        let pairs = load_text_map(tm_path)?;
        println!("\nLoaded {} text replacement rules", pairs.len());
        pairs
    } else {
        Vec::new()
    };

    // 4. Read DOCX
    println!("\nOpening DOCX: {}", cli.docx.display());
    let mut entries = read_docx(&cli.docx)?;
    println!("  {} zip entries", entries.len());

    // 5. Process rels — replace tinyurl targets
    let rels_key = "word/_rels/document.xml.rels";
    let rels_xml = entries
        .iter()
        .find(|(name, _)| name == rels_key)
        .map(|(_, data)| String::from_utf8_lossy(data).to_string())
        .context("No word/_rels/document.xml.rels found")?;

    let rels_map_before = parse_rels(&rels_xml);
    // Build rId -> URL map for hyperlink text replacement
    let rid_to_url: HashMap<String, String> = rels_map_before
        .iter()
        .filter(|(_, (t, _))| t.contains("hyperlink"))
        .map(|(id, (_, target))| (id.clone(), target.clone()))
        .collect();

    let (new_rels, rels_count) = replace_rels_urls(&rels_xml, &url_map);
    println!("\nReplaced {} tinyurl targets in rels", rels_count);

    // Update rels entry
    if let Some((_, data)) = entries.iter_mut().find(|(name, _)| name == rels_key) {
        *data = new_rels.into_bytes();
    }

    // 6. Process document.xml — text replacement + hyperlink text
    let doc_key = "word/document.xml";
    let doc_xml = entries
        .iter()
        .find(|(name, _)| name == doc_key)
        .map(|(_, data)| String::from_utf8_lossy(data).to_string())
        .context("No word/document.xml found")?;

    // Build image-URL pairs before modifying the XML
    let image_url_pairs = if !cli.no_images {
        let rels_parsed = parse_rels(&rels_xml);
        build_image_url_pairs(&doc_xml, &rels_parsed, &url_map)
    } else {
        Vec::new()
    };

    let mut new_doc = doc_xml.clone();

    // Replace visible hyperlink text (tinyurl text -> anonymized URL)
    let (updated_doc, hl_count) = replace_hyperlink_text(&new_doc, &url_map, &rid_to_url);
    new_doc = updated_doc;
    println!("Replaced {} hyperlink display texts", hl_count);

    // Replace document text using text map
    if !text_replacements.is_empty() {
        let (updated_doc, text_count) = replace_document_text(&new_doc, &text_replacements);
        new_doc = updated_doc;
        println!(
            "Replaced {} text occurrences ({} rules)",
            text_count,
            text_replacements.len()
        );
    }

    // Redact paragraph range if requested
    if let (Some(ref start), Some(ref end)) = (&cli.redact_start, &cli.redact_end) {
        let rep = cli.redact_replacement.as_deref();
        let (updated_doc, redact_count) = redact_paragraphs(&new_doc, start, end, rep);
        new_doc = updated_doc;
        println!(
            "Redacted {} paragraphs (\"{}\" → \"{}\")",
            redact_count, start, end
        );
    }

    // Update document.xml entry
    if let Some((_, data)) = entries.iter_mut().find(|(name, _)| name == doc_key) {
        *data = new_doc.into_bytes();
    }

    // 7. Anonymize BBO screenshot images
    if !cli.no_images && !image_url_pairs.is_empty() {
        println!("\nAnonymizing BBO screenshot images...");
        let font = load_system_font()?;
        let mut modified = 0u32;

        for (media_path, anon_url) in &image_url_pairs {
            let names = match extract_player_names(anon_url) {
                Some(n) => n,
                None => {
                    eprintln!(
                        "  Warning: could not parse player names from URL for {}",
                        media_path
                    );
                    continue;
                }
            };

            // The media_path from rels is like "media/image5.png"
            let zip_path = format!("word/{}", media_path);

            let png_data = match entries.iter().find(|(name, _)| *name == zip_path) {
                Some((_, data)) => data.clone(),
                None => {
                    eprintln!("  Warning: {} not found in DOCX", zip_path);
                    continue;
                }
            };

            // Check dimensions — only modify large images (BBO screenshots)
            let decoder = png::Decoder::new(Cursor::new(&png_data));
            let reader = match decoder.read_info() {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("  Warning: failed to decode {}: {}", zip_path, e);
                    continue;
                }
            };
            let info = reader.info();
            let w = info.width as usize;
            let h = info.height as usize;

            if w < 1000 || h < 1000 {
                continue; // Not a BBO screenshot
            }

            match anonymize_png(&png_data, &names, &font) {
                Ok(new_png) => {
                    if let Some((_, data)) = entries.iter_mut().find(|(name, _)| *name == zip_path)
                    {
                        *data = new_png;
                    }
                    println!(
                        "  {} ({}x{}): [N]{} [S]{} [W]{} [E]{}",
                        media_path, w, h, names[2], names[0], names[1], names[3]
                    );
                    modified += 1;
                }
                Err(e) => {
                    eprintln!("  Warning: failed to anonymize {}: {}", zip_path, e);
                }
            }
        }
        println!("Modified {} BBO screenshot images", modified);
    }

    // 8. Blank specified images (replace with solid black)
    if let Some(ref blank_list) = cli.blank_images {
        let names: Vec<&str> = blank_list.split(',').map(str::trim).collect();
        for img_name in &names {
            let zip_path = format!("word/media/{}", img_name);
            let png_data = match entries.iter().find(|(n, _)| *n == zip_path) {
                Some((_, data)) => data.clone(),
                None => {
                    eprintln!("  Warning: {} not found in DOCX", zip_path);
                    continue;
                }
            };
            match create_blank_png(&png_data) {
                Ok(blank) => {
                    if let Some((_, data)) = entries.iter_mut().find(|(n, _)| *n == zip_path) {
                        *data = blank;
                    }
                    println!("Blanked image: {}", img_name);
                }
                Err(e) => eprintln!("  Warning: failed to blank {}: {}", img_name, e),
            }
        }
    }

    // 9. Redact convention card player names
    if let Some(ref cc_name) = cli.cc_redact {
        let replacement = cli.cc_names.as_deref().unwrap_or("Bob & Sally");
        let zip_path = format!("word/media/{}", cc_name);
        let png_data = match entries.iter().find(|(n, _)| *n == zip_path) {
            Some((_, data)) => data.clone(),
            None => anyhow::bail!("Convention card image {} not found", zip_path),
        };
        let font = load_system_font()?;
        let new_png = redact_cc_names(&png_data, replacement, &font)?;
        if let Some((_, data)) = entries.iter_mut().find(|(n, _)| *n == zip_path) {
            *data = new_png;
        }
        println!(
            "Redacted convention card names in {} → \"{}\"",
            cc_name, replacement
        );
    }

    // 10. Write output DOCX
    write_docx(&cli.output, &entries)?;
    println!("\nSaved to: {}", cli.output.display());
    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    run(&cli)
}
