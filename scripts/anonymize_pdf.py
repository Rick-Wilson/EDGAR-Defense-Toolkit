#!/usr/bin/env python3
"""Anonymize a PDF by redacting names in text and within embedded images.

Uses PyMuPDF for text-layer redaction and Tesseract OCR + Pillow for
detecting and replacing names baked into raster images (e.g. BBO hand
viewer screenshots).

Usage:
    python scripts/anonymize_pdf.py \
        -i input.pdf \
        -o output_anon.pdf \
        -m "Spwilliams=Bob,Adwilliams=Sally,Alan Williams=Bob Williams" \
        --extra-redact "R003134,3273 Streamside Cir"

Requires:
    pip install PyMuPDF Pillow pytesseract
    brew install tesseract
"""

from __future__ import annotations

import argparse
import io
import re
import shutil
import sys
from collections import Counter
from pathlib import Path

try:
    import fitz  # PyMuPDF
except ImportError:
    sys.exit("PyMuPDF is required: pip install PyMuPDF")

try:
    from PIL import Image, ImageDraw, ImageFont
except ImportError:
    sys.exit("Pillow is required: pip install Pillow")

try:
    import pytesseract
except ImportError:
    sys.exit("pytesseract is required: pip install pytesseract")


# ---------------------------------------------------------------------------
# Name-map parsing
# ---------------------------------------------------------------------------

def parse_name_map(map_str: str) -> dict[str, str]:
    """Parse 'orig1=repl1,orig2=repl2' into {orig: repl} dict.

    Generates case variants automatically.
    """
    mapping: dict[str, str] = {}
    for pair in map_str.split(","):
        pair = pair.strip()
        if "=" not in pair:
            continue
        orig, repl = pair.split("=", 1)
        orig, repl = orig.strip(), repl.strip()
        if not orig:
            continue
        # Add the mapping as given plus case variants
        for o, r in _case_variants(orig, repl):
            mapping[o] = r
    return mapping


def _case_variants(orig: str, repl: str) -> list[tuple[str, str]]:
    """Return case variants: as-is, lower, upper, title."""
    variants = set()
    variants.add((orig, repl))
    variants.add((orig.lower(), repl.lower()))
    variants.add((orig.upper(), repl.upper()))
    variants.add((orig.title(), repl.title()))
    return list(variants)


# ---------------------------------------------------------------------------
# Text-layer redaction
# ---------------------------------------------------------------------------

def redact_text_on_page(page: fitz.Page, name_map: dict[str, str],
                        extra_redact: list[str]) -> int:
    """Search for names/strings in the text layer and apply redactions.

    Returns the number of redactions applied.
    """
    count = 0

    # Redact name mappings (replace with alias)
    # Sort by length descending so longer matches are found first
    for orig in sorted(name_map, key=len, reverse=True):
        rects = page.search_for(orig)
        for rect in rects:
            page.add_redact_annot(
                rect,
                text=name_map[orig],
                fontsize=0,  # auto-fit
                fill=(1, 1, 1),  # white background
            )
            count += 1

    # Redact extra strings (no replacement text — just white box)
    for text in extra_redact:
        text = text.strip()
        if not text:
            continue
        rects = page.search_for(text)
        for rect in rects:
            page.add_redact_annot(rect, text="", fontsize=0,
                                  fill=(1, 1, 1))
            count += 1

    # Redact tinyurl / tinyurl.com links
    for pattern_text in _find_urls(page):
        rects = page.search_for(pattern_text)
        for rect in rects:
            page.add_redact_annot(rect, text="[link]", fontsize=0,
                                  fill=(1, 1, 1))
            count += 1

    if count:
        page.apply_redactions(images=fitz.PDF_REDACT_IMAGE_NONE)

    return count


def _find_urls(page: fitz.Page) -> list[str]:
    """Extract tinyurl.com links from page text."""
    text = page.get_text()
    urls = re.findall(r'https?://(?:www\.)?tinyurl\.com/\S+', text)
    return list(set(urls))


# ---------------------------------------------------------------------------
# Image-layer redaction via OCR
# ---------------------------------------------------------------------------

def sample_bg_color(img: Image.Image, x: int, y: int, w: int, h: int,
                    margin: int = 4) -> tuple[int, ...]:
    """Sample the most common color around the bounding box edges."""
    pixels: list[tuple[int, ...]] = []
    for dx in range(max(0, x - margin), min(img.width, x + w + margin)):
        for dy_val in [max(0, y - margin), min(img.height - 1, y + h + margin)]:
            pixels.append(img.getpixel((dx, dy_val)))
    for dy in range(max(0, y - margin), min(img.height, y + h + margin)):
        for dx_val in [max(0, x - margin), min(img.width - 1, x + w + margin)]:
            pixels.append(img.getpixel((dx_val, dy)))
    if not pixels:
        return (255, 255, 255)
    counter = Counter(pixels)
    return counter.most_common(1)[0][0]


def get_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    """Try to load a sans-serif font at the given size."""
    font_paths = [
        "/System/Library/Fonts/Helvetica.ttc",
        "/System/Library/Fonts/SFNSText.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for fp in font_paths:
        if Path(fp).exists():
            try:
                return ImageFont.truetype(fp, size)
            except Exception:
                continue
    return ImageFont.load_default()


def _fuzzy_match(ocr_word: str, target: str, threshold: float = 0.75) -> bool:
    """Check if OCR word is a fuzzy match for target (handles OCR errors).

    Uses character-level similarity. Threshold of 0.75 means 75% of chars
    must match.
    """
    if not ocr_word or not target:
        return False
    # Exact match
    if ocr_word.lower() == target.lower():
        return True
    # Length must be close (within 2 chars)
    if abs(len(ocr_word) - len(target)) > 2:
        return False
    # For very short words (<=3 chars), require exact match to avoid false positives
    if len(target) <= 3:
        return False
    # Character overlap ratio
    shorter = min(len(ocr_word), len(target))
    matches = sum(1 for a, b in zip(ocr_word.lower(), target.lower()) if a == b)
    return matches / shorter >= threshold


def _ocr_image(img: Image.Image, scale: float = 2.0) -> dict:
    """Run OCR on an image, optionally upscaling for better accuracy."""
    if scale != 1.0:
        new_size = (int(img.width * scale), int(img.height * scale))
        scaled = img.resize(new_size, Image.LANCZOS)
    else:
        scaled = img

    # Try multiple PSM modes and merge results
    configs = [
        "--psm 6",   # Assume uniform block of text
        "--psm 11",  # Sparse text - find as much text as possible
    ]
    best_data = None
    best_words = 0

    for config in configs:
        try:
            data = pytesseract.image_to_data(
                scaled, output_type=pytesseract.Output.DICT, config=config
            )
            # Count non-empty words with positive confidence
            word_count = sum(
                1 for i, w in enumerate(data["text"])
                if w.strip() and data["conf"][i] > 0
            )
            if word_count > best_words:
                best_words = word_count
                best_data = data
        except Exception:
            continue

    if best_data is None:
        return {"text": [], "conf": [], "left": [], "top": [],
                "width": [], "height": []}

    # Adjust coordinates back if we scaled
    if scale != 1.0:
        for key in ("left", "top", "width", "height"):
            best_data[key] = [int(v / scale) for v in best_data[key]]

    return best_data


def redact_image(img: Image.Image, name_map: dict[str, str],
                 extra_redact: list[str] | None = None,
                 ) -> tuple[Image.Image, int]:
    """Use OCR to find names in an image and redact them.

    Returns (modified_image, redaction_count).
    """
    if img.mode != "RGB":
        img = img.convert("RGB")

    try:
        data = _ocr_image(img)
    except pytesseract.TesseractNotFoundError:
        print("ERROR: Tesseract not found. Install with: brew install tesseract",
              file=sys.stderr)
        return img, 0

    words = data["text"]
    n_boxes = len(words)
    count = 0
    draw = ImageDraw.Draw(img)

    # Build a lowercase lookup for case-insensitive matching
    lc_map = {k.lower(): v for k, v in name_map.items()}

    # Also build a list of extra-redact terms for images
    extra_lc = [s.lower() for s in (extra_redact or [])]

    # First pass: try to match multi-word names by joining consecutive words
    used_indices: set[int] = set()
    sorted_names = sorted(lc_map.keys(), key=len, reverse=True)

    for name_lc in sorted_names:
        name_words = name_lc.split()
        if len(name_words) < 2:
            continue
        for i in range(n_boxes - len(name_words) + 1):
            if i in used_indices:
                continue
            # Check if consecutive words match (with fuzzy matching)
            matched = True
            for j, nw in enumerate(name_words):
                idx = i + j
                if idx in used_indices:
                    matched = False
                    break
                w = words[idx].strip()
                if not _fuzzy_match(w, nw):
                    matched = False
                    break
                if data["conf"][idx] < 0:
                    matched = False
                    break
            if not matched:
                continue
            # Found multi-word match — redact the span
            repl = lc_map[name_lc]
            first, last = i, i + len(name_words) - 1
            x = data["left"][first]
            y = min(data["top"][first + k] for k in range(len(name_words)))
            x2 = data["left"][last] + data["width"][last]
            y2 = max(data["top"][first + k] + data["height"][first + k]
                     for k in range(len(name_words)))
            w_box, h_box = x2 - x, y2 - y
            bg = sample_bg_color(img, x, y, w_box, h_box)
            draw.rectangle([x, y, x2, y2], fill=bg)
            font = get_font(max(10, h_box - 4))
            brightness = sum(bg[:3]) / 3 if len(bg) >= 3 else 128
            text_color = (0, 0, 0) if brightness > 128 else (255, 255, 255)
            draw.text((x + 2, y + 1), repl, fill=text_color, font=font)
            for k in range(len(name_words)):
                used_indices.add(i + k)
            count += 1

    # Second pass: single-word matches (with fuzzy matching)
    for i in range(n_boxes):
        if i in used_indices:
            continue
        w = words[i].strip()
        if not w or data["conf"][i] < 0:
            continue
        w_lc = w.lower()
        # Check name map
        matched_repl = None
        for target, repl in lc_map.items():
            if " " in target:
                continue  # multi-word handled above
            if _fuzzy_match(w_lc, target):
                matched_repl = repl
                break
        # Check extra-redact terms
        is_extra = False
        if matched_repl is None:
            for term in extra_lc:
                if " " not in term and _fuzzy_match(w_lc, term):
                    matched_repl = ""
                    is_extra = True
                    break

        if matched_repl is None:
            continue

        x, y = data["left"][i], data["top"][i]
        bw, bh = data["width"][i], data["height"][i]
        bg = sample_bg_color(img, x, y, bw, bh)
        draw.rectangle([x, y, x + bw, y + bh], fill=bg)
        if matched_repl:
            font = get_font(max(10, bh - 4))
            brightness = sum(bg[:3]) / 3 if len(bg) >= 3 else 128
            text_color = (0, 0, 0) if brightness > 128 else (255, 255, 255)
            draw.text((x + 2, y + 1), matched_repl, fill=text_color, font=font)
        used_indices.add(i)
        count += 1

    # Third pass: search for multi-word extra-redact terms
    for term in extra_lc:
        term_words = term.split()
        if len(term_words) < 2:
            continue
        for i in range(n_boxes - len(term_words) + 1):
            if i in used_indices:
                continue
            matched = True
            for j, tw in enumerate(term_words):
                idx = i + j
                if idx in used_indices:
                    matched = False
                    break
                w = words[idx].strip()
                if not _fuzzy_match(w, tw):
                    matched = False
                    break
            if not matched:
                continue
            first, last = i, i + len(term_words) - 1
            x = data["left"][first]
            y = min(data["top"][first + k] for k in range(len(term_words)))
            x2 = data["left"][last] + data["width"][last]
            y2 = max(data["top"][first + k] + data["height"][first + k]
                     for k in range(len(term_words)))
            w_box, h_box = x2 - x, y2 - y
            bg = sample_bg_color(img, x, y, w_box, h_box)
            draw.rectangle([x, y, x2, y2], fill=bg)
            for k in range(len(term_words)):
                used_indices.add(i + k)
            count += 1

    return img, count


def process_page_images(doc: fitz.Document, page: fitz.Page,
                        name_map: dict[str, str],
                        extra_redact: list[str] | None = None) -> int:
    """Extract, OCR, redact, and replace images on a page.

    Returns number of image redactions applied.
    """
    images = page.get_images(full=True)
    if not images:
        return 0

    total = 0
    for img_info in images:
        xref = img_info[0]
        try:
            pix = fitz.Pixmap(doc, xref)
        except Exception:
            continue

        # Skip very small images (icons, decorations)
        if pix.width < 100 or pix.height < 100:
            continue

        # Convert pixmap to PIL Image
        if pix.alpha:
            pix_rgb = fitz.Pixmap(fitz.csRGB, pix)
        else:
            pix_rgb = pix if pix.n == 3 else fitz.Pixmap(fitz.csRGB, pix)

        img = Image.open(io.BytesIO(pix_rgb.tobytes("png")))

        modified_img, count = redact_image(img, name_map, extra_redact)
        if count == 0:
            continue

        total += count

        # Convert back to bytes and replace in PDF
        buf = io.BytesIO()
        modified_img.save(buf, format="PNG")
        buf.seek(0)
        new_pix = fitz.Pixmap(buf)
        page.replace_image(xref, pixmap=new_pix)

    return total


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Anonymize a PDF by redacting names in text and images."
    )
    parser.add_argument("-i", "--input", required=True, help="Input PDF path")
    parser.add_argument("-o", "--output", help="Output PDF path (default: {stem}_anon.pdf)")
    parser.add_argument("-m", "--map", required=True,
                        help="Name mappings: 'orig1=repl1,orig2=repl2'")
    parser.add_argument("--extra-redact", default="",
                        help="Extra strings to redact (comma-separated)")
    parser.add_argument("--no-images", action="store_true",
                        help="Skip image processing (text-only mode)")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        sys.exit(f"Input file not found: {input_path}")

    if args.output:
        output_path = Path(args.output)
    else:
        output_path = input_path.with_stem(input_path.stem + "_anon")

    # Check tesseract is available (unless --no-images)
    if not args.no_images and not shutil.which("tesseract"):
        sys.exit("Tesseract not found. Install with: brew install tesseract")

    name_map = parse_name_map(args.map)
    extra_redact = [s.strip() for s in args.extra_redact.split(",") if s.strip()]

    print(f"Input:  {input_path}")
    print(f"Output: {output_path}")
    print(f"Name mappings ({len(name_map)} variants):")
    shown = set()
    for orig, repl in sorted(name_map.items(), key=lambda x: (-len(x[0]), x[0])):
        key = (orig.lower(), repl.lower())
        if key not in shown:
            print(f"  {orig!r} -> {repl!r}")
            shown.add(key)
    if extra_redact:
        print(f"Extra redact: {extra_redact}")
    print()

    doc = fitz.open(str(input_path))
    total_text = 0
    total_img = 0

    for page_num in range(len(doc)):
        page = doc[page_num]
        print(f"Page {page_num + 1}/{len(doc)}...", end=" ", flush=True)

        # Text-layer redaction
        text_count = redact_text_on_page(page, name_map, extra_redact)
        total_text += text_count

        # Image-layer redaction
        img_count = 0
        if not args.no_images:
            img_count = process_page_images(doc, page, name_map, extra_redact)
            total_img += img_count

        status = []
        if text_count:
            status.append(f"{text_count} text")
        if img_count:
            status.append(f"{img_count} image")
        if status:
            print(", ".join(status))
        else:
            print("(no changes)")

    doc.save(str(output_path), garbage=4, deflate=True)
    doc.close()

    print(f"\nDone! {total_text} text redactions, {total_img} image redactions.")
    print(f"Saved to: {output_path}")


if __name__ == "__main__":
    main()
