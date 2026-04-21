"""
whatsapp.py
===========
WhatsApp messaging via Green API (free tier — 500 msg/month, no browser needed).

FREE SETUP (takes ~5 minutes):
  1. Go to https://green-api.com and click "Get Started Free"
  2. Register with your email
  3. Create an instance — you get idInstance + apiTokenInstance
  4. In the instance settings, scan the QR code with your WhatsApp
     (Settings > Scan QR code)
  5. The instance stays connected as long as WhatsApp is on your phone
     (no periodic re-scanning needed)
  6. Fill in the values below

FINDING YOUR GROUP CHAT ID:
  Option A — easiest:
    Run this script:  python whatsapp.py groups
    It lists all your groups with their IDs.

  Option B — manual:
    Open WhatsApp Web, click the group, look at the URL:
    https://web.whatsapp.com/accept?code=...
    OR use the Green API dashboard > API Methods > getChats

  Group IDs look like:  120363XXXXXXXXXX@g.us

FREE TIER LIMITS:
  - 500 messages / month
  - 1 instance (1 connected WhatsApp number)
  - No credit card required
"""

import os
import configparser
import requests
import logging

log = logging.getLogger("MargMonitor.WhatsApp")

# ── Load credentials from config.ini ─────────────────────────────────────────
_cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini")
_ini = configparser.ConfigParser()
_ini.read(_cfg_path, encoding="utf-8")

GREEN_API_ID_INSTANCE    = _ini.get("whatsapp", "id_instance",      fallback="YOUR_ID_INSTANCE")
GREEN_API_TOKEN_INSTANCE = _ini.get("whatsapp", "token_instance",   fallback="YOUR_TOKEN_INSTANCE")
GREEN_API_GROUP_ID       = _ini.get("whatsapp", "group_id",         fallback="120363XXXXXXXXXX@g.us")
WHATSAPP_ENABLED         = _ini.getboolean("whatsapp", "enabled",   fallback=False)

# Individual chat for /party PDFs — separate from the WCR/TFR group
# Format: 91XXXXXXXXXX@c.us  (country code + number, no +, @c.us suffix)
PARTY_PDF_CHAT_ID        = _ini.get("whatsapp", "party_pdf_chat_id", fallback="")
# ─────────────────────────────────────────────────────────────────────────────

_BASE = "https://api.green-api.com"


def _url(method: str) -> str:
    return f"{_BASE}/waInstance{GREEN_API_ID_INSTANCE}/{method}/{GREEN_API_TOKEN_INSTANCE}"


def check_connection() -> bool:
    """Returns True if the WhatsApp instance is connected and authorised."""
    try:
        r = requests.get(_url("getStateInstance"), timeout=10)
        if r.status_code == 200:
            state = r.json().get("stateInstance", "")
            log.info("Green API state: %s", state)
            return state == "authorized"
        return False
    except Exception as e:
        log.error("check_connection error: %s", e)
        return False


def send_text(message: str, group_id: str = None) -> bool:
    """
    Send a plain text message to the WhatsApp group.
    Returns True on success.
    """
    if not WHATSAPP_ENABLED:
        log.debug("WhatsApp disabled — skipping send_text")
        return False

    target = group_id or GREEN_API_GROUP_ID
    try:
        r = requests.post(
            _url("sendMessage"),
            json={"chatId": target, "message": message},
            timeout=15,
        )
        if r.status_code == 200 and r.json().get("idMessage"):
            log.info("WhatsApp message sent to %s", target)
            return True
        else:
            log.error("WhatsApp send failed: %s %s", r.status_code, r.text[:300])
            return False
    except Exception as e:
        log.error("WhatsApp send_text error: %s", e)
        return False


def send_image(img, caption: str = "", group_id: str = None) -> bool:
    """
    Send a PIL Image to the WhatsApp group via Green API.
    Uses multipart/form-data upload — confirmed working (Method 4).
    img: PIL Image object
    Returns True on success.
    """
    if not WHATSAPP_ENABLED:
        log.debug("WhatsApp disabled — skipping send_image")
        return False

    import io
    target = group_id or GREEN_API_GROUP_ID
    try:
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=85)
        buf.seek(0)
        r = requests.post(
            _url("sendFileByUpload"),
            data={"chatId": target, "caption": caption},
            files={"file": ("bills.jpg", buf.getvalue(), "image/jpeg")},
            timeout=30,
        )
        if r.status_code == 200 and r.json().get("idMessage"):
            log.info("WhatsApp image sent to %s", target)
            return True
        else:
            log.error("WhatsApp send_image failed: %s %s", r.status_code, r.text[:300])
            return False
    except Exception as e:
        log.error("WhatsApp send_image error: %s", e)
        return False


def send_pdf(pdf_path: str, caption: str = "", chat_id: str = None) -> bool:
    """
    Send a PDF file to a WhatsApp chat via Green API sendFileByUpload.

    Separate from send_image — PDFs must be sent as application/pdf so
    WhatsApp renders them as documents rather than images.

    Args:
        pdf_path : absolute path to the PDF file on disk
        caption  : optional message caption shown under the document
        chat_id  : target chat ID (individual: 91XXXXXXXXXX@c.us,
                   group: 120363...@g.us). Defaults to PARTY_PDF_CHAT_ID.

    Returns True on success, False on any failure.
    Logs all errors — never raises.
    """
    if not WHATSAPP_ENABLED:
        log.debug("WhatsApp disabled — skipping send_pdf")
        return False

    target = chat_id or PARTY_PDF_CHAT_ID
    if not target:
        log.warning("send_pdf: no target chat_id configured (party_pdf_chat_id empty)")
        return False

    import os
    if not os.path.exists(pdf_path):
        log.error("send_pdf: file not found: %s", pdf_path)
        return False

    filename = os.path.basename(pdf_path)
    try:
        with open(pdf_path, "rb") as f:
            pdf_bytes = f.read()
    except Exception as e:
        log.error("send_pdf: failed to read file %s: %s", pdf_path, e)
        return False

    try:
        r = requests.post(
            _url("sendFileByUpload"),
            data={"chatId": target, "caption": caption},
            files={"file": (filename, pdf_bytes, "application/pdf")},
            timeout=60,
        )
        if r.status_code == 200:
            resp_json = r.json()
            if resp_json.get("idMessage"):
                log.info("WhatsApp PDF sent to %s — idMessage=%s",
                         target, resp_json["idMessage"])
                return True
            else:
                log.error("WhatsApp send_pdf: no idMessage in response: %s", r.text[:300])
                return False
        else:
            log.error("WhatsApp send_pdf failed: HTTP %d — %s",
                      r.status_code, r.text[:300])
            return False
    except requests.Timeout:
        log.error("WhatsApp send_pdf: request timed out sending %s", filename)
        return False
    except Exception as e:
        log.error("WhatsApp send_pdf error: %s", e, exc_info=True)
        return False


def get_groups() -> list:
    """
    Return list of all WhatsApp groups the connected number is in.
    Each item: {"id": "120363...@g.us", "name": "Group Name"}
    """
    try:
        r = requests.get(_url("getChats"), timeout=15)
        if r.status_code != 200:
            log.error("getChats failed: %s", r.status_code)
            return []
        chats  = r.json()
        groups = [
            {"id": c.get("id", ""), "name": c.get("name", "")}
            for c in chats
            if c.get("id", "").endswith("@g.us")
        ]
        return groups
    except Exception as e:
        log.error("get_groups error: %s", e)
        return []


def _wa_bill_no(bill_no: str) -> str:
    """
    Prevent WhatsApp from auto-linking bill numbers as phone numbers.
    Inserts a Unicode word-joiner (U+2060, invisible, zero-width) after
    the slash so the digit string is no longer seen as a standalone number.
    e.g.  WCR/006968  →  WCR/⁠006968  (looks identical, not linkified)
    """
    return bill_no.replace("/", "/\u2060")


def _wa_table(bills: list) -> str:
    """
    Fixed-width table for WhatsApp (monospace via triple-backtick code block).
    WhatsApp renders ```code blocks``` in a monospace font on most platforms.
    """
    header  = f"{'No':>2}  {'Bill No':<12}  Customer"
    divider = "-" * max(len(header), 38)
    rows    = [header, divider]
    for i, b in enumerate(bills, 1):
        bill = _wa_bill_no(b["bill_no"])
        rows.append(f"{i:>2}  {bill:<13}  {b['customer'][:25]}")
    return "\n".join(rows)


def format_wcr_for_whatsapp(bills: list, date_str: str) -> str:
    """
    Format WCR bills for WhatsApp.
    Uses a monospace table inside triple-backtick block for alignment.
    """
    if not bills:
        return (
            f"*Undelivered WCR Bills*\n"
            f"{date_str}\n\n"
            "No pending deliveries for this date."
        )

    table = _wa_table(bills)
    return (
        f"*Undelivered WCR Bills*\n"
        f"{date_str} — *{len(bills)} pending*\n\n"
        f"```\n{table}\n```"
    )


def format_wcr_for_whatsapp_multi(results: list, bill_alert_threshold: int = 10) -> str:
    """
    Format WCR bills for multiple dates into ONE WhatsApp message.
    results: [(date_display, bills), ...]  — index 0 is always today.

    Rules:
    - Today  : always shown, with "No pending deliveries" if empty
    - Previous dates : shown ONLY if they have pending bills; silently skipped if empty
    - High bill alert : prepended if today's pending count >= bill_alert_threshold
    """
    lines = ["*Undelivered WCR Bills*"]

    # High bill count alert — based on today only (index 0)
    today_bills = results[0][1] if results else []
    if len(today_bills) >= bill_alert_threshold:
        lines.append(f"⚠️ *HIGH PENDING ALERT — {len(today_bills)} bills today!*")

    for i, (date_display, bills) in enumerate(results):
        is_today = (i == 0)
        if bills:
            lines.append("")
            lines.append(f"🗓 *{date_display}* — {len(bills)} pending")
            table = _wa_table(bills)
            lines.append(f"```\n{table}\n```")
        elif is_today:
            lines.append("")
            lines.append(f"🗓 *{date_display}* — ✅ No pending deliveries")
        # previous dates with no bills: silently skip

    return "\n".join(lines)


# ── Standalone tool ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys

    if not WHATSAPP_ENABLED:
        print("WHATSAPP_ENABLED is False.")
        print("Fill in GREEN_API_ID_INSTANCE, GREEN_API_TOKEN_INSTANCE,")
        print("and GREEN_API_GROUP_ID in whatsapp.py, then set WHATSAPP_ENABLED = True")
        sys.exit(0)

    if GREEN_API_ID_INSTANCE == "YOUR_ID_INSTANCE":
        print("Fill in your Green API credentials in whatsapp.py first.")
        sys.exit(1)

    cmd = sys.argv[1] if len(sys.argv) > 1 else "test"

    if cmd == "groups":
        print("Fetching your WhatsApp groups...\n")
        groups = get_groups()
        if not groups:
            print("No groups found, or connection failed.")
        else:
            print(f"Found {len(groups)} group(s):\n")
            for g in groups:
                print(f"  ID   : {g['id']}")
                print(f"  Name : {g['name']}")
                print()

    elif cmd == "status":
        connected = check_connection()
        print("WhatsApp connected:", "YES" if connected else "NO (scan QR in Green API dashboard)")

    else:
        # Default: send test message
        print("Checking connection...")
        if not check_connection():
            print("Not connected. Scan QR code in Green API dashboard first.")
            sys.exit(1)

        print(f"Sending test message to: {GREEN_API_GROUP_ID}")
        ok = send_text("Marg ERP Bot — WhatsApp test message. If you see this, integration is working.")
        print("Sent OK" if ok else "Failed — check logs above.")
