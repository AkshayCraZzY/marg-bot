"""
ocr_wcr.py
==========
OCR module for WCR undelivered bills screen — powered by Google Cloud Vision API.

Replaces Tesseract entirely. Sends the screenshot to GCV DOCUMENT_TEXT_DETECTION,
which handles the purple highlighted row, colored backgrounds, and VB6 bitmap fonts
without any image preprocessing.

Bill number format:
  WCR series: WCR/01234  (5 digits)
  WCA series: WCA/01234  (5 digits, new series)
  Both series are captured by typing "wc" in the filter field.

Usage as standalone tester:
  python ocr_wcr.py path/to/screenshot.png
"""

import re
import io
import base64
import logging
import requests
from PIL import Image

log = logging.getLogger("MargMonitor.OCR")

GCV_ENDPOINT = "https://vision.googleapis.com/v1/images:annotate"


# ──────────────────────────────────────────────────────────────
# GOOGLE CLOUD VISION OCR
# ──────────────────────────────────────────────────────────────

def run_ocr(img: Image.Image, gcv_api_key: str) -> str:
    """
    Send a PIL image to Google Cloud Vision DOCUMENT_TEXT_DETECTION.
    Returns the raw extracted text string.

    DOCUMENT_TEXT_DETECTION is used (not TEXT_DETECTION) because it
    preserves layout and line structure — critical for parsing the
    WCR table format correctly.

    Args:
        img:         PIL Image of the WCR screen region
        gcv_api_key: Google Cloud Vision API key from config.ini
    """
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    b64 = base64.b64encode(buf.getvalue()).decode("utf-8")

    body = {
        "requests": [{
            "image":    {"content": b64},
            "features": [{"type": "DOCUMENT_TEXT_DETECTION", "maxResults": 1}],
        }]
    }

    try:
        resp = requests.post(
            GCV_ENDPOINT + f"?key={gcv_api_key}",
            json=body,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
    except requests.RequestException as e:
        log.error("GCV request failed: %s", e)
        return ""

    if resp.status_code != 200:
        log.error("GCV API error %d: %s", resp.status_code, resp.text[:300])
        return ""

    data = resp.json()
    responses = data.get("responses", [{}])

    if "error" in responses[0]:
        err = responses[0]["error"]
        log.error("GCV error %s: %s", err.get("code"), err.get("message"))
        return ""

    full_ann = responses[0].get("fullTextAnnotation", {})
    text = full_ann.get("text", "")
    log.debug("GCV raw text:\n%s", text)
    return text


# ──────────────────────────────────────────────────────────────
# PARSING
# ──────────────────────────────────────────────────────────────

def _strip_date_column(rest: str) -> str:
    """Strip the leading DD-MM date column from text after the bill number."""
    return re.sub(
        r"^[A-Z0-9]{1,3}-[A-Z0-9]{1,3}\s+", "", rest.strip(), flags=re.IGNORECASE
    ).strip()


def _rejoin_split_rows(text: str) -> str:
    """
    GCV's DOCUMENT_TEXT_DETECTION sometimes splits a single WCR table row
    across two lines, for example:

        WCR/00164
        06-04 ADVANCE MEDICO

    instead of the correct single line:

        WCR/00164   06-04 ADVANCE MEDICO

    This happens because the highlighted row has the bill number left-aligned
    and the date+customer further right — GCV treats them as separate text blocks.

    Fix: if line N matches a bare bill number (WCR/NNNNN or WCA/NNNNN with nothing
    after) and line N+1 starts with a DD-MM date pattern, join them with a space.
    Handles both WCR and WCA series.
    """
    lines = text.splitlines()
    out   = []
    i     = 0
    # Pattern: a line that is ONLY a bill number (WCR or WCA, nothing after digits)
    bill_only = re.compile(r"^WC[RA][/\-]\d{4,6}\s*$", re.IGNORECASE)
    # Pattern: a line that starts with a DD-MM date
    date_start = re.compile(r"^\d{2}-\d{2}\s+", re.IGNORECASE)

    while i < len(lines):
        line = lines[i].strip()
        # Check if this line is a bare bill number and the next starts with a date
        if (bill_only.match(line)
                and i + 1 < len(lines)
                and date_start.match(lines[i + 1].strip())):
            # Merge the two lines into one
            out.append(line + "   " + lines[i + 1].strip())
            i += 2
        else:
            out.append(line)
            i += 1

    return "\n".join(out)


def parse_wcr_bills(raw_text: str) -> list:
    """
    Parse GCV text into structured WCR bill rows.

    WC screen column layout (both WCR and WCA series):
      Bill No    | Date  | Customer Name
      WCR/01234  | 09-03 | Raj Traders
      WCA/00056  | 09-03 | ABC Medical

    Bill number format: WCR/ or WCA/ followed by 4-6 digits (5 digits standard).
    Prefix preserved as-is (WCR/ or WCA/), number zero-padded to 5 digits.

    Returns list of dicts: [{"bill_no": "WCR/01234", "customer": "Raj Traders"}, ...]

    Pre-processing:
      _rejoin_split_rows() — fixes GCV splitting one row into two lines
        e.g. "WCR/00164\\n06-04 ADVANCE MEDICO" → "WCR/00164   06-04 ADVANCE MEDICO"

    Three strategies tried in order per line:
      1. Full match  - WCR/NNNNN  DD-MM  Customer
      2. Partial     - WCR/NNNNN then strip date, take customer
      3. Prefix-drop - pure digit run if GCV dropped "WCR/" prefix
    """
    # Pre-process: merge rows that GCV split across two lines
    raw_text = _rejoin_split_rows(raw_text)

    bills = []
    seen  = set()

    for raw_line in raw_text.splitlines():
        line = raw_line.strip()
        if not line or len(line) < 6:
            continue

        # Strategy 1: full three-column match (WCR or WCA)
        m = re.search(
            r"(WC[RA])[/\-](\d{4,6})\s+\d{2}-\d{2}\s{1,8}(.{2,40}?)\s*$",
            line, re.IGNORECASE
        )
        if m:
            bill = m.group(1).upper() + "/" + m.group(2).zfill(5)
            if bill not in seen:
                seen.add(bill)
                bills.append({"bill_no": bill, "customer": m.group(3).strip()})
            continue

        # Strategy 2: bill number + anything after (WCR or WCA)
        m2 = re.search(r"(WC[RA])[/\-](\d{4,6})\s+(.+)", line, re.IGNORECASE)
        if m2:
            bill = m2.group(1).upper() + "/" + m2.group(2).zfill(5)
            rest = _strip_date_column(m2.group(3))
            customer = re.split(r"\s{2,}", rest, maxsplit=1)[0].strip()
            if bill not in seen and customer:
                seen.add(bill)
                bills.append({"bill_no": bill, "customer": customer})
            continue

        # Strategy 3: OCR dropped "WCR/" prefix
        m3 = re.search(r"^(\d{4,6})\s+\d{2}-\d{2}\s+(.{2,40}?)\s*$", line)
        if m3:
            bill = "WCR/" + m3.group(1).zfill(5)
            if bill not in seen:
                seen.add(bill)
                bills.append({"bill_no": bill, "customer": m3.group(2).strip()})

    log.info("Parsed %d bill(s) (WCR+WCA combined)", len(bills))
    return bills


# ──────────────────────────────────────────────────────────────
# TELEGRAM FORMATTING
# ──────────────────────────────────────────────────────────────

def _tg_table(bills: list) -> str:
    """
    Format bills as a fixed-width monospace table for Telegram.
    """
    if not bills:
        return ""
    header  = f"{'No':>2}  {'Bill No':<13}  Customer"
    divider = "-" * max(len(header), 38)
    rows    = [header, divider]
    for i, b in enumerate(bills, 1):
        rows.append(f"{i:>2}  {b['bill_no']:<13}  {b['customer'][:28]}")
    return "\n".join(rows)


def format_wcr_message(bills: list, date_str: str) -> str:
    """Format parsed WCR bills for Telegram — single date."""
    header = f"📋 *Undelivered Bills (WCR + WCA)*\n🗓 {date_str}"
    if not bills:
        return (
            header + "\n\n"
            "✅ No pending deliveries for this date.\n\n"
            "_If bills are visible in the screenshot, run_\n"
            "`python calibrate.py wcr` _to recalibrate the region._"
        )
    table = _tg_table(bills)
    return (
        header + "\n"
        f"📦 *{len(bills)} pending*\n\n"
        f"```\n{table}\n```"
    )


def format_wcr_message_multi(results: list) -> str:
    """
    Format WCR bills for multiple dates into ONE Telegram message.
    results: [(date_display, bills), ...]  — index 0 is always today.
    """
    sections = ["📋 *Undelivered Bills (WCR + WCA)*"]
    total    = sum(len(bills) for _, bills in results)

    for i, (date_display, bills) in enumerate(results):
        is_today = (i == 0)
        if bills:
            table = _tg_table(bills)
            sections.append(
                f"\n🗓 *{date_display}* — 📦 {len(bills)} pending\n"
                f"```\n{table}\n```"
            )
        elif is_today:
            sections.append(f"\n🗓 *{date_display}*\n✅ No pending deliveries")

    if total == 0:
        sections.append(
            "\n_If bills are visible in the screenshots:_\n"
            "• Run `python calibrate.py wcr` to recalibrate\n"
            "• Review the screenshots sent above"
        )

    return "\n".join(sections)


# ──────────────────────────────────────────────────────────────
# STANDALONE TESTER
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys, os, datetime, configparser

    _ini = configparser.ConfigParser()
    _ini.read(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini"), encoding="utf-8")
    GCV_KEY = _ini.get("google", "vision_api_key", fallback="")

    if not GCV_KEY:
        print("ERROR: google.vision_api_key not set in config.ini")
        sys.exit(1)

    if len(sys.argv) < 2:
        print("Usage: python ocr_wcr.py <screenshot.png>")
        sys.exit(0)

    JUNK = [0x202A, 0x202C, 0x200B, 0x200E, 0x200F, 0xFEFF, 0x0027, 0x0022]
    path = "".join(c for c in sys.argv[1] if ord(c) not in JUNK).strip()

    if not os.path.exists(path):
        print(f"File not found: {path}")
        sys.exit(1)

    img   = Image.open(path)
    raw   = run_ocr(img, GCV_KEY)
    bills = parse_wcr_bills(raw)
    today = datetime.date.today().strftime("%d/%m/%Y")
    msg   = format_wcr_message(bills, today)

    print("=" * 50)
    print("GCV RAW TEXT:")
    print("=" * 50)
    print(raw)
    print("=" * 50)
    print(f"PARSED: {len(bills)} bill(s)")
    for b in bills:
        print(b)
    print("=" * 50)
    print("TELEGRAM PREVIEW:")
    print("=" * 50)
    print(msg)
