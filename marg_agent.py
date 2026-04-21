"""
marg_agent.py  —  Marg ERP Monitor Bot
=======================================
Telegram bot that automates Marg ERP navigation, captures pending bills,
runs OCR on WCR (undelivered only), and sends daily scheduled reports.

File structure:
  marg_agent.py   — main bot, automation, Telegram polling
  ocr_wcr.py      — OCR parsing for WCR undelivered bills
  regions_wcr.py  — screen region coordinates for WCR capture
  regions_tfr.py  — screen region coordinates for TFR capture

Commands:
  /wcr   — WCR godown undelivered bills (screenshot + parsed table)
  /tfr   — TFR series bills (screenshot only, color-coded)
  /all   — WCR + TFR full report
  /help  — Help menu

Requirements:
  pip install pywin32 pillow requests
"""

import os
import sys
import time
import logging
import logging.handlers
import datetime
import configparser
import threading
import io

import requests
import subprocess
import ctypes
import win32gui
import win32ui
import numpy as np
from PIL import Image

from ocr_wcr    import run_ocr as run_ocr_wcr, parse_wcr_bills, format_wcr_message, format_wcr_message_multi
# Regions loaded from config.ini — replaces regions_wcr/tfr/party.py
# Use: python calibrate.py wcr/tfr/party  to calibrate and update config.ini
import whatsapp

# ──────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────

# ── Load config.ini ──────────────────────────────────────────
_cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini")
if not os.path.exists(_cfg_path):
    print(f"ERROR: config.ini not found at {_cfg_path}")
    print("Create config.ini next to marg_agent.py before running.")
    sys.exit(1)

_ini = configparser.ConfigParser()
_ini.read(_cfg_path, encoding="utf-8")

CONFIG = {
    # Telegram
    "telegram_bot_token":     _ini.get("telegram", "bot_token"),
    "telegram_chat_id":       _ini.get("telegram", "chat_id"),
    "telegram_poll_interval": _ini.getint("telegram", "poll_interval", fallback=3),

    # Marg ERP
    "marg_main_window_title": _ini.get("marg", "window_title", fallback="MARG ERP"),

    # Google Cloud Vision API key (replaces Tesseract)
    "gcv_api_key":            _ini.get("google", "vision_api_key", fallback=""),
    # Keystroke delays
    "delay_enter_select":     _ini.getfloat("delays", "enter_select",    fallback=1.5),
    "delay_after_filter":     _ini.getfloat("delays", "after_filter",    fallback=2.0),
    "delay_before_capture":   _ini.getfloat("delays", "before_capture",  fallback=1.5),
    "delay_after_esc":        _ini.getfloat("delays", "after_esc",       fallback=1.5),

    # Schedule
    "daily_report_time":      _ini.get("schedule", "daily_report_time",  fallback="20:00"),

    # Files
    "log_file":               _ini.get("files", "log_file"),
    "screenshot_dir":         _ini.get("files", "screenshot_dir"),

    # Storage limits (MB) — size-based pruning, oldest-first
    "max_logs_mb":            _ini.getfloat("files", "max_logs_mb",        fallback=10.0),
    "max_screenshots_mb":     _ini.getfloat("files", "max_screenshots_mb", fallback=50.0),
    "max_ledger_temp_mb":     _ini.getfloat("files", "max_ledger_temp_mb", fallback=100.0),

    # Marg ERP restart
    "marg_shortcut":          _ini.get("marg", "shortcut", fallback=r"C:\Users\autobot\Desktop\MARG.lnk"),
    "marg_restart_time":      _ini.get("schedule", "marg_restart_time", fallback="07:45"),

    # Health ping — Telegram "bot alive" message sent every hour at this minute past the hour
    "health_ping_minute":     _ini.getint("schedule", "health_ping_minute", fallback=0),

    # High bill count alert threshold (WhatsApp alert if today's pending count >= this)
    "bill_alert_threshold":   _ini.getint("alerts", "bill_alert_threshold", fallback=10),

    # WCR date range: how many previous days to check in addition to today
    # e.g. 2 → today + last 2 working days (default). Sundays always skipped.
    "undelivered_check_days": _ini.getint("alerts", "undelivered_check_days", fallback=2),

    # Maximum bills visible per page in the WCR list screen
    # When the first OCR pass returns exactly this many bills, pagination is triggered
    "wcr_page_limit":         _ini.getint("alerts", "wcr_page_limit", fallback=14),


    # Party ledger — PDF output folder ID
    # PDF is always saved to: C:/Users/Public/MARG/{marg_folder_id}/ledger.PDF
    "marg_folder_id": _ini.get("party", "marg_folder_id", fallback=""),

    # Backup
    "backup_schedule_time": _ini.get    ("schedule", "backup_schedule_time", fallback="07:50"),
    "backup_wait_time":     _ini.getint ("delays",   "backup_wait_time",     fallback=25),

    # Marg ERP login
    "login_user":           _ini.get("marg", "login_user",     fallback="BOT"),
    "login_password":       _ini.get("marg", "login_password", fallback="BOT"),
    "login_wait_s":         _ini.getfloat("marg", "login_wait_s", fallback=15.0),

    # Marg ERP navigation filter strings
    "bill_filter_prefix":   _ini.get("marg_nav", "bill_filter_prefix",  fallback="wc"),
    "tfr_store_filter":     _ini.get("marg_nav", "tfr_store_filter",    fallback="store"),
    "tfr_branch_filter":    _ini.get("marg_nav", "tfr_branch_filter",   fallback="ganga"),

    # Ledger temp directory (configurable path)
    "ledger_temp_dir":      _ini.get("files", "ledger_temp_dir",
                                     fallback=r"C:\MargMonitor\ledger_temp"),

    # Sunday skip flags per scheduler
    "report_skip_sunday":   _ini.getboolean("schedule", "report_skip_sunday",  fallback=True),
    "restart_skip_sunday":  _ini.getboolean("schedule", "restart_skip_sunday", fallback=False),
    "backup_skip_sunday":   _ini.getboolean("schedule", "backup_skip_sunday",  fallback=True),
}

def _load_region(section: str) -> tuple:
    """
    Load a screen capture region and its calibrated flag from config.ini.
    Returns (region_dict, calibrated_bool).
    Region dict has keys: left, top, right, bottom.
    """
    calibrated = _ini.getboolean(section, "calibrated", fallback=False)
    region = {
        "left":   _ini.getint(section, "left",   fallback=0),
        "top":    _ini.getint(section, "top",    fallback=0),
        "right":  _ini.getint(section, "right",  fallback=1920),
        "bottom": _ini.getint(section, "bottom", fallback=1080),
    }
    return region, calibrated

WCR_REGION,   WCR_REGION_CALIBRATED   = _load_region("region_wcr")
TFR_REGION,   TFR_REGION_CALIBRATED   = _load_region("region_tfr")
PARTY_REGION, PARTY_REGION_CALIBRATED = _load_region("region_party")

# ──────────────────────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────────────────────

os.makedirs(os.path.dirname(CONFIG["log_file"]), exist_ok=True)
_log_handler = logging.handlers.RotatingFileHandler(
    CONFIG["log_file"],
    maxBytes=int(CONFIG["max_logs_mb"] * 1024 * 1024),
    backupCount=1,
    encoding="utf-8",
)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        _log_handler,
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("MargMonitor")

# ──────────────────────────────────────────────────────────────
# TELEGRAM HELPERS
# ──────────────────────────────────────────────────────────────

BASE_URL        = f"https://api.telegram.org/bot{CONFIG['telegram_bot_token']}"
ALLOWED_CHAT_ID = str(CONFIG["telegram_chat_id"])
_last_update_id = None


def tg_get_updates(offset=None, timeout=2):
    try:
        params = {"timeout": timeout, "allowed_updates": ["message"]}
        if offset is not None:
            params["offset"] = offset
        r = requests.get(f"{BASE_URL}/getUpdates", params=params, timeout=timeout + 5)
        if r.status_code == 200:
            return r.json().get("result", [])
    except Exception as e:
        log.warning("getUpdates error: %s", e)
    return []


def tg_send_message(chat_id, text):
    try:
        requests.post(
            f"{BASE_URL}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )
    except Exception as e:
        log.warning("sendMessage error: %s", e)


def tg_send_photo(chat_id, img: Image.Image, caption=""):
    try:
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        buf.seek(0)
        requests.post(
            f"{BASE_URL}/sendPhoto",
            data={"chat_id": chat_id, "caption": caption},
            files={"photo": ("bills.png", buf, "image/png")},
            timeout=30,
        )
        log.info("Photo sent: %s", caption)
    except Exception as e:
        log.error("sendPhoto error: %s", e)


# ──────────────────────────────────────────────────────────────
# POSTMESSAGE KEYSTROKES  (works with RDP disconnected)
# ──────────────────────────────────────────────────────────────

WM_KEYDOWN = 0x0100
WM_KEYUP   = 0x0101
WM_CHAR    = 0x0102
VK_DOWN    = 0x28
VK_UP      = 0x26
VK_RIGHT   = 0x27
VK_LEFT    = 0x25
VK_RETURN  = 0x0D
VK_ESCAPE  = 0x1B
VK_P       = 0x50


def _post(hwnd, msg, vk, lparam):
    win32gui.PostMessage(hwnd, msg, vk, lparam)
    time.sleep(0.05)


def send_regular_key(hwnd, vk, delay=0.15):
    """Non-extended keys: Enter, Esc."""
    _post(hwnd, WM_KEYDOWN, vk, 0x00000001)
    time.sleep(delay)
    _post(hwnd, WM_KEYUP,   vk, 0xC0000001)
    time.sleep(delay)


def send_extended_key(hwnd, vk, scan, delay=0.15):
    """
    Extended keys (arrows, Insert, Delete, etc.) need the extended-key
    flag (bit 24) and correct hardware scan code in lparam bits 16-23.
    Scan codes confirmed via key_spy2.py:
      Down Arrow = scan 0x50
    """
    lp_down = 0x00000001 | (scan << 16) | 0x01000000
    lp_up   = 0xC0000001 | (scan << 16) | 0x01000000
    _post(hwnd, WM_KEYDOWN, vk, lp_down)
    time.sleep(delay)
    _post(hwnd, WM_KEYUP,   vk, lp_up)
    time.sleep(delay)


def send_enter(hwnd):
    send_regular_key(hwnd, VK_RETURN)


def send_escape(hwnd):
    send_regular_key(hwnd, VK_ESCAPE)


def send_down_arrow(hwnd):
    """Must target the margwin9c000000 child control, not the parent."""
    send_extended_key(hwnd, VK_DOWN, scan=0x50)


def send_up_arrow(hwnd):
    """Must target the margwin9c000000 child control, not the parent."""
    send_extended_key(hwnd, VK_UP, scan=0x48)


def send_alt_p(hwnd):
    """
    Send Alt+P to trigger Marg ERP print/preview action.
    Uses WM_SYSKEYDOWN/WM_SYSKEYUP — the correct Windows messages for
    keys pressed while Alt is held. No separate WM_KEYDOWN for Alt needed;
    that would spuriously activate the menu bar.
    Works headlessly without focus.
    """
    scan_p = ctypes.windll.user32.MapVirtualKeyW(VK_P, 0)
    # bit 29 (0x20000000) = context code, meaning Alt is held
    lp_p_dn = 0x00000001 | (scan_p << 16) | 0x20000000
    lp_p_up = 0xC0000001 | (scan_p << 16) | 0x20000000
    win32gui.PostMessage(hwnd, 0x0104, VK_P, lp_p_dn)  # WM_SYSKEYDOWN
    time.sleep(0.1)
    win32gui.PostMessage(hwnd, 0x0105, VK_P, lp_p_up)  # WM_SYSKEYUP
    time.sleep(0.3)


def send_alt_f(hwnd):
    """
    Send Alt+F — used after Alt+P to trigger the PDF save/export action.
    Same pattern as send_alt_p: WM_SYSKEYDOWN with bit 29 set.
    """
    VK_F = 0x46
    scan_f = ctypes.windll.user32.MapVirtualKeyW(VK_F, 0)
    lp_f_dn = 0x00000001 | (scan_f << 16) | 0x20000000
    lp_f_up = 0xC0000001 | (scan_f << 16) | 0x20000000
    win32gui.PostMessage(hwnd, 0x0104, VK_F, lp_f_dn)  # WM_SYSKEYDOWN
    time.sleep(0.1)
    win32gui.PostMessage(hwnd, 0x0105, VK_F, lp_f_up)  # WM_SYSKEYUP
    time.sleep(0.3)



def send_string(hwnd, s, interval=0.15):
    """Post WM_CHAR per character — works with RDP disconnected."""
    for ch in s:
        win32gui.PostMessage(hwnd, WM_CHAR, ord(ch), 0x00000001)
        time.sleep(interval)


# ──────────────────────────────────────────────────────────────
# WINDOW UTILITIES
# ──────────────────────────────────────────────────────────────

def find_window(title_fragment):
    frag  = title_fragment.lower()
    found = []
    def cb(hwnd, _):
        if win32gui.IsWindowVisible(hwnd) and frag in win32gui.GetWindowText(hwnd).lower():
            found.append(hwnd)
    win32gui.EnumWindows(cb, None)
    return found[0] if found else None


def find_marg_child(parent_hwnd):
    """
    Arrow keys must go to the VB6 child control inside Marg ERP.
    Class name 'margwin9c000000' confirmed by child_arrow_test.py.
    Falls back to parent if class not found.
    """
    TARGET = "margwin9c000000"
    found  = []
    def cb(hwnd, _):
        if win32gui.GetClassName(hwnd) == TARGET:
            found.append(hwnd)
    try:
        win32gui.EnumChildWindows(parent_hwnd, cb, None)
    except Exception:
        pass
    if found:
        log.debug("Marg child found: [%d]", found[0])
        return found[0]
    log.warning("Child class '%s' not found — falling back to parent", TARGET)
    return parent_hwnd


# ──────────────────────────────────────────────────────────────
# SCREENSHOT  (BitBlt-first, works with RDP disconnected)
# ──────────────────────────────────────────────────────────────

def is_blank(img, threshold=10):
    return np.array(img).mean() < threshold


def grab_window(hwnd) -> Image.Image | None:
    """
    Capture full window via PrintWindow(flag=2).
    Flag 2 = PW_RENDERFULLCONTENT — captures the window's own rendering buffer,
    works when RDP is disconnected and no display is attached.
    Does NOT require the window to be visible or in the foreground.
    """
    try:
        l, t, r, b = win32gui.GetWindowRect(hwnd)
        w, h = r - l, b - t
        if w <= 0 or h <= 0:
            return None
        hdc     = win32gui.GetWindowDC(hwnd)
        mfc_dc  = win32ui.CreateDCFromHandle(hdc)
        save_dc = mfc_dc.CreateCompatibleDC()
        bmp     = win32ui.CreateBitmap()
        bmp.CreateCompatibleBitmap(mfc_dc, w, h)
        save_dc.SelectObject(bmp)
        ctypes.windll.user32.PrintWindow(hwnd, save_dc.GetSafeHdc(), 2)
        info = bmp.GetInfo()
        bits = bmp.GetBitmapBits(True)
        img  = Image.frombuffer("RGB", (info["bmWidth"], info["bmHeight"]),
                                bits, "raw", "BGRX", 0, 1)
        win32gui.DeleteObject(bmp.GetHandle())
        save_dc.DeleteDC(); mfc_dc.DeleteDC()
        win32gui.ReleaseDC(hwnd, hdc)
        return img if not is_blank(img) else None
    except Exception as e:
        log.error("grab_window failed: %s", e)
        return None


def crop_to_region(img: Image.Image, region: dict) -> Image.Image:
    """Crop full-window image to a calibrated region dict (left/top/right/bottom)."""
    return img.crop((region["left"], region["top"], region["right"], region["bottom"]))


def _prune_dir(directory: str, max_mb: float, label: str = "file") -> None:
    """
    Delete oldest files in directory when total size exceeds max_mb.
    Deletes oldest-first (by mtime) until folder is under the limit.
    Logs every deletion and the total space reclaimed.
    Non-blocking: called after each write, runs in microseconds when under limit.
    """
    if not directory or not os.path.isdir(directory):
        return
    max_bytes = max_mb * 1024 * 1024
    try:
        files = [
            os.path.join(directory, f)
            for f in os.listdir(directory)
            if os.path.isfile(os.path.join(directory, f))
        ]
        total = sum(os.path.getsize(f) for f in files)
        if total <= max_bytes:
            return
        files.sort(key=os.path.getmtime)   # oldest first
        reclaimed = 0
        deleted   = 0
        for f in files:
            if total <= max_bytes:
                break
            try:
                size = os.path.getsize(f)
                os.remove(f)
                total     -= size
                reclaimed += size
                deleted   += 1
                log.info("[PRUNE] Deleted %s: %s (%.1f KB)",
                         label, os.path.basename(f), size / 1024)
            except Exception as e:
                log.warning("[PRUNE] Could not delete %s: %s", f, e)
        if deleted:
            log.info("[PRUNE] %s: removed %d file(s), reclaimed %.2f MB — "
                     "folder now %.2f MB (limit %.1f MB)",
                     label, deleted, reclaimed / 1048576,
                     total / 1048576, max_mb)
    except Exception as e:
        log.warning("[PRUNE] %s pruning error: %s", label, e)




def save_shot(img, prefix):
    if not (CONFIG["screenshot_dir"] and img):
        return
    os.makedirs(CONFIG["screenshot_dir"], exist_ok=True)
    ts   = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(CONFIG["screenshot_dir"], f"{prefix}_{ts}.png")
    img.save(path)
    log.info("Saved: %s", path)
    _prune_dir(CONFIG["screenshot_dir"], CONFIG["max_screenshots_mb"], "screenshot")


# ──────────────────────────────────────────────────────────────
# AUTOMATION SEQUENCES
# ──────────────────────────────────────────────────────────────

_automation_lock = threading.Lock()

# ── Party ledger search state ─────────────────────────────────────────────────
# The /party command is a two-step interaction:
#   Step 1: user sends /party → bot asks for party name
#   Step 2: user sends party name → bot searches, sends screenshot
#   Step 3: user sends a number → bot selects that ledger and opens it
# State is stored here (single-user bot, no concurrency needed for UI state).
_party_state = None   # None | "awaiting_name" | "awaiting_selection" | "awaiting_output_type"
_party_hwnd  = None   # hwnd captured when /party was first called
_party_name  = None   # exact party name typed by the user (used in file naming)

# ── Backup confirmation state ─────────────────────────────────────────────────
_backup_state = None  # None | "awaiting_confirm"

def restart_marg_erp(chat_id):
    """
    Restart Marg ERP and log in as user 'bot'.

    Sequence:
      1. Down Arrow x2 + Enter  → exit Marg ERP via menu
      2. Launch .lnk shortcut from config
      3. Wait 15s for login window
      4. Type 'bot' + Enter  (username)
      5. Type 'bot' + Enter  (password)
      6. Esc x10 with 2s gaps  → clear random pop-ups
      7. Screenshot → send to Telegram for confirmation
    """
    tg_send_message(chat_id, "🔄 Restarting Marg ERP...")
    log.info("[RESTART] Starting Marg ERP restart sequence")

    # Step 1: Close Marg ERP — Down Arrow x2 then Enter
    hwnd = find_window(CONFIG["marg_main_window_title"])
    if hwnd:
        log.info("[RESTART] Step 1: Down Arrow x2 + Enter to exit Marg ERP")
        child = find_marg_child(hwnd)
        for _ in range(2):
            send_down_arrow(child)
            time.sleep(0.4)
        send_enter(hwnd)
        time.sleep(3.0)   # wait for Marg to close
    else:
        log.warning("[RESTART] Marg ERP window not found — skipping close step")

    # Step 2: Launch via shortcut using ShellExecuteW.
    # subprocess / cmd /c start requires an active desktop session.
    # ShellExecuteW resolves .lnk files and works headlessly via the service desktop.
    shortcut = CONFIG["marg_shortcut"]
    log.info("[RESTART] Step 2: launching shortcut via ShellExecuteW: %s", shortcut)
    try:
        import os as _os
        if not _os.path.exists(shortcut):
            tg_send_message(chat_id,
                "⚠️ Shortcut not found: `" + shortcut + "`\n"
                "Update `marg_shortcut` in config.ini")
            return
        # ShellExecuteW(hwnd, op, file, params, dir, show)
        # show=1 (SW_SHOWNORMAL) — works even without a display
        ret = ctypes.windll.shell32.ShellExecuteW(None, "open", shortcut, None, None, 1)
        if ret <= 32:
            raise RuntimeError("ShellExecuteW returned " + str(ret))
        log.info("[RESTART] ShellExecuteW succeeded (ret=%d)", ret)
    except Exception as e:
        log.error("[RESTART] Launch failed: %s", e)
        tg_send_message(chat_id, "❌ Failed to launch Marg ERP: " + str(e))
        return

    # Step 3: Wait 15s for Marg ERP login window
    log.info("[RESTART] Step 3: waiting %ds for Marg ERP login window",
             int(CONFIG["login_wait_s"]))
    tg_send_message(chat_id, "⏳ Waiting for Marg ERP login window...")
    time.sleep(CONFIG["login_wait_s"])

    # Find newly launched window
    hwnd = find_window(CONFIG["marg_main_window_title"])
    if hwnd is None:
        tg_send_message(chat_id, "⚠️ Marg ERP window not found after launch. "
                                 "It may need more time — try /restart_margerp again.")
        return

    # Step 5 & 6: Type 'bot' + Enter twice (username then password)
    log.info("[RESTART] Step 4-5: login as bot/bot")
    send_string(hwnd, CONFIG["login_user"], interval=0.15)
    time.sleep(0.3)
    send_enter(hwnd)
    time.sleep(1.5)
    send_string(hwnd, CONFIG["login_password"], interval=0.15)
    time.sleep(0.3)
    send_enter(hwnd)
    time.sleep(2.0)

    # Step 6: Esc x10 with 2s gaps to clear pop-ups
    log.info("[RESTART] Step 6: Esc x10 to clear pop-ups")
    tg_send_message(chat_id, "🔄 Clearing any pop-ups...")
    for i in range(10):
        send_escape(hwnd)
        log.debug("[RESTART] Esc %d/10", i + 1)
        time.sleep(2.0)

    # Step 7: Screenshot for confirmation
    log.info("[RESTART] Step 7: confirmation screenshot")
    time.sleep(1.0)
    hwnd = find_window(CONFIG["marg_main_window_title"])
    if hwnd:
        img = grab_window(hwnd)
        if img:
            save_shot(img, "restart_confirm")
            ts = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")
            tg_send_photo(chat_id, img, "Marg ERP after restart — " + ts)
        
        tg_send_message(chat_id,
            "✅ *Marg ERP restarted*\n"
            "Logged in as *bot*"
        )
        log.info("[RESTART] Complete.")
    else:
        tg_send_message(chat_id, "⚠️ Could not find Marg ERP after restart. "
                                 "Check the screenshot manually.")


def handle_restart_margerp(chat_id):
    if not _automation_lock.acquire(blocking=False):
        tg_send_message(chat_id, "⏳ Already running a command, please wait..."); return
    try:
        restart_marg_erp(chat_id)
    except Exception as e:
        log.error("handle_restart_margerp: %s", e, exc_info=True)
        tg_send_message(chat_id, "❌ Restart error: " + str(e))
    finally:
        _automation_lock.release()


def get_hwnd(chat_id):
    hwnd = find_window(CONFIG["marg_main_window_title"])
    if hwnd is None:
        tg_send_message(chat_id,
            "❌ *Marg ERP window not found*\n"
            "Make sure *" + CONFIG["marg_main_window_title"] + "* is open."
        )
    return hwnd


def capture_region(hwnd, region, calibrated, prefix):
    """
    Take screenshot, crop to region if calibrated, save, return image.
    """
    time.sleep(CONFIG["delay_before_capture"])
    img = grab_window(hwnd)

    if img is None:
        log.error("Screenshot returned None")
        return None

    if is_blank(img):
        log.warning("Screenshot is blank")
        return img

    if calibrated:
        img = crop_to_region(img, region)

    save_shot(img, prefix)
    return img


def _wcr_dates() -> list:
    """
    Return today + N previous working days, where N = config undelivered_check_days.

    Today is always included regardless of weekday.
    Previous days skip Sundays (holiday — no Marg ERP records).

    date_display : DD/MM/YYYY  — used in messages
    date_marg    : DD-MM-YY    — typed into Marg ERP date field

    Uses timedelta for correct month/year boundary handling.
    weekday(): Monday=0 … Saturday=5 … Sunday=6
    """
    n_prev = CONFIG["undelivered_check_days"]   # how many previous days to check
    today  = datetime.date.today()
    dates  = [today]
    d      = today - datetime.timedelta(days=1)
    while len(dates) < 1 + n_prev:
        if d.weekday() != 6:          # skip Sunday
            dates.append(d)
        d -= datetime.timedelta(days=1)
    return [
        (dt, dt.strftime("%d/%m/%Y"), dt.strftime("%d-%m-%y"))
        for dt in dates
    ]


def _wcr_capture_and_ocr(hwnd, child, prefix: str) -> list:
    """
    Capture the WCR region, OCR it, and handle pagination.

    If the first OCR pass returns exactly wcr_page_limit bills (default 14),
    the list may be truncated at the visible page boundary.  In that case:
      - Press Down Arrow rapidly for ~3 seconds to scroll to the bottom
      - Capture and OCR a second screenshot
      - Merge both bill lists, removing duplicates (keyed on bill_no)

    Deduplication preserves insertion order: first-page bills appear first,
    additional bills from the second page are appended.

    Returns the deduplicated combined bill list.
    Sends screenshots to Telegram as part of the normal capture flow.
    """
    PAGE_LIMIT    = CONFIG["wcr_page_limit"]
    SCROLL_SECS   = 3.0    # seconds of continuous Down Arrow presses
    SCROLL_INTERVAL = 0.08  # seconds between each Down Arrow keypress

    # ── First capture ────────────────────────────────────────────────────────
    img1 = capture_region(hwnd, WCR_REGION, WCR_REGION_CALIBRATED, prefix + "_p1")
    bills: list = []

    if not img1:
        log.warning("[WCR-OCR] First capture failed for prefix=%s", prefix)
        return bills

    if is_blank(img1):
        log.info("[WCR-OCR] First capture is blank — no bills")
        return bills

    raw1  = run_ocr_wcr(img1, CONFIG["gcv_api_key"])
    bills = parse_wcr_bills(raw1)
    log.info("[WCR-OCR] First pass: %d bill(s) from %s", len(bills), prefix)

    # ── Pagination check ─────────────────────────────────────────────────────
    if len(bills) < PAGE_LIMIT:
        # Fewer than a full page — no overflow possible
        return bills

    log.info("[WCR-OCR] Hit page limit (%d bills) — scrolling to check for more",
             PAGE_LIMIT)

    # Rapid Down Arrow presses to scroll to the bottom of the list
    scroll_start = time.time()
    presses = 0
    while time.time() - scroll_start < SCROLL_SECS:
        send_down_arrow(child)
        presses += 1
        time.sleep(SCROLL_INTERVAL)
    log.info("[WCR-OCR] Scrolled: %d Down Arrow presses over %.1fs",
             presses, SCROLL_SECS)

    time.sleep(CONFIG["delay_before_capture"])   # let list settle after scroll

    # ── Second capture ───────────────────────────────────────────────────────
    img2 = capture_region(hwnd, WCR_REGION, WCR_REGION_CALIBRATED, prefix + "_p2")

    if not img2 or is_blank(img2):
        log.info("[WCR-OCR] Second capture blank or failed — returning first-page bills only")
        return bills

    raw2        = run_ocr_wcr(img2, CONFIG["gcv_api_key"])
    bills_page2 = parse_wcr_bills(raw2)
    log.info("[WCR-OCR] Second pass: %d bill(s) (may include duplicates)", len(bills_page2))

    # ── Deduplicate: merge page 2 into page 1, skip known bill_no values ─────
    seen = {b["bill_no"] for b in bills}
    added = 0
    for b in bills_page2:
        if b["bill_no"] not in seen:
            seen.add(b["bill_no"])
            bills.append(b)
            added += 1

    log.info("[WCR-OCR] After dedup: %d total bill(s) (%d new from page 2)",
             len(bills), added)
    return bills


def _wcr_fetch_today(chat_id, hwnd, date_display, ts, suppress_whatsapp):
    """
    Fetch WCR for today using the existing workflow (Enter → wcr → capture).
    Returns list of bills.
    """
    log.info("[WCR-TODAY] Enter")
    send_enter(hwnd)
    time.sleep(CONFIG["delay_enter_select"])

    log.info("[WCR-TODAY] type %s (bill filter prefix)", CONFIG["bill_filter_prefix"])
    send_string(hwnd, CONFIG["bill_filter_prefix"], interval=0.2)
    time.sleep(CONFIG["delay_after_filter"])

    child = find_marg_child(hwnd)

    log.info("[WCR-TODAY] capture + OCR (with pagination)")
    bills = _wcr_capture_and_ocr(hwnd, child, "wcr_undelivered")

    # Send the first captured screenshot to Telegram for visual confirmation
    img = capture_region(hwnd, WCR_REGION, WCR_REGION_CALIBRATED, "wcr_undelivered_final")
    if img:
        tg_send_photo(chat_id, img, f"WCR/WCA Undelivered Bills — {date_display} {ts}")
    elif not bills:
        tg_send_message(chat_id, "⚠️ Screenshot failed for today.")

    # Return to main UI
    log.info("[WCR-TODAY] Esc → Y")
    send_escape(hwnd)
    time.sleep(1.5)
    send_string(hwnd, "y", interval=0.1)
    time.sleep(CONFIG["delay_after_esc"])

    return bills


def _wcr_fetch_previous(chat_id, hwnd, date_display, date_marg, ts):
    """
    Fetch WCR for a previous date. Correct sequence:
      1. Down Arrow x3  → navigate to date-filter option in menu
      2. Enter          → open date filter screen
      3. Type DD-MM-YY  → enter the date
      4. Enter          → confirm date
      5. Type wc        → filter to WCR + WCA series
      6. Capture + OCR
      7. Esc → Y        → back to main UI
    Returns list of bills.
    """
    child = find_marg_child(hwnd)

    log.info("[WCR-PREV] Down Arrow x3")
    for _ in range(3):
        send_down_arrow(child)
        time.sleep(0.4)

    log.info("[WCR-PREV] Enter → open date filter")
    send_enter(hwnd)
    time.sleep(CONFIG["delay_enter_select"])

    log.info("[WCR-PREV] Type date: %s", date_marg)
    send_string(hwnd, date_marg, interval=0.2)
    time.sleep(0.3)

    log.info("[WCR-PREV] Enter → confirm date")
    send_enter(hwnd)
    time.sleep(CONFIG["delay_after_filter"])

    log.info("[WCR-PREV] Type %s (bill filter prefix)", CONFIG["bill_filter_prefix"])
    send_string(hwnd, CONFIG["bill_filter_prefix"], interval=0.2)
    time.sleep(CONFIG["delay_after_filter"])

    prefix = "wcr_prev_" + date_marg.replace("-", "")

    log.info("[WCR-PREV] Capture + OCR (with pagination)")
    bills = _wcr_capture_and_ocr(hwnd, child, prefix)

    # Send the current (post-scroll) screenshot to Telegram for visual confirmation
    img = capture_region(hwnd, WCR_REGION, WCR_REGION_CALIBRATED, prefix + "_final")
    if img:
        tg_send_photo(chat_id, img, f"WCR/WCA — {date_display} {ts}")
    elif not bills:
        tg_send_message(chat_id, f"⚠️ Screenshot failed for {date_display}.")

    log.info("[WCR-PREV] Esc → Y")
    send_escape(hwnd)
    time.sleep(1.5)
    send_string(hwnd, "y", interval=0.1)
    time.sleep(CONFIG["delay_after_esc"])

    return bills


def run_wcr(chat_id, hwnd, notify_start=True, suppress_whatsapp=False):
    """
    WCR undelivered bills — checks today + last 2 working days (skips Sundays).

    Today workflow     : Enter → wc → Enter → capture → Esc → Y
    Previous date flow : Enter → DD-MM-YY → Enter → wc → Enter → capture → Esc → Y

    Results from ALL dates are collected first, then sent as ONE combined
    message to both Telegram and WhatsApp.
    """
    ts    = datetime.datetime.now().strftime("%H:%M")
    dates = _wcr_dates()   # [today, last_working_day]

    if notify_start:
        date_labels = " + ".join(disp for _, disp, _ in dates)
        tg_send_message(chat_id, f"🔍 Fetching WCR bills — today + last 2 working days...")

    # ── Collect results for all dates ─────────────────────────
    results = []   # list of (date_display, bills, img)
    for i, (date_obj, date_display, date_marg) in enumerate(dates):
        log.info("[WCR] Checking %s (i=%d)", date_display, i)
        if i == 0:
            bills = _wcr_fetch_today(chat_id, hwnd, date_display, ts, suppress_whatsapp)
        else:
            bills = _wcr_fetch_previous(chat_id, hwnd, date_display, date_marg, ts)
        results.append((date_display, bills))
        time.sleep(0.5)

    # ── Send ONE combined Telegram message ────────────────────
    tg_msg  = format_wcr_message_multi(results)
    tg_send_message(chat_id, tg_msg)

    # ── Send ONE combined WhatsApp message ────────────────────
    if whatsapp.WHATSAPP_ENABLED and not suppress_whatsapp:
        wa_text = whatsapp.format_wcr_for_whatsapp_multi(
            results,
            bill_alert_threshold=CONFIG["bill_alert_threshold"]
        )
        whatsapp.send_text(wa_text)

    # ── Main UI screenshot ────────────────────────────────────
    ts_full = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")
    img_main = grab_window(hwnd)
    if img_main:
        save_shot(img_main, "mainui_after_wcr")
        tg_send_photo(chat_id, img_main, "✅ WCR complete — Main UI — " + ts_full)

    log.info("[WCR] Done.")


def run_tfr(chat_id, hwnd, notify_start=True, suppress_whatsapp=False):
    """
    TFR Gangapur transfers — image only, no OCR.

    Navigation sequence:
      1.  Down Arrow x1      (child control)
      2.  Enter x5
      3.  Up Arrow x1        (child control)
      4.  type "store"
      5.  Enter x2
      6.  type "ganga"
      7.  Enter x1
      8.  type "*"
      9.  Enter x1
      10. Screenshot
      Esc → back to main UI
    """
    ts = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")

    if notify_start:
        tg_send_message(chat_id, "🔍 Fetching TFR Gangapur transfers...")

    child = find_marg_child(hwnd)

    log.info("[TFR] Step 1: Down Arrow x1")
    send_down_arrow(child)
    time.sleep(0.5)

    log.info("[TFR] Step 2: Enter x5")
    for i in range(5):
        send_enter(hwnd)
        time.sleep(CONFIG["delay_enter_select"])

    log.info("[TFR] Step 3: Up Arrow x1")
    send_up_arrow(child)
    time.sleep(0.5)

    log.info("[TFR] Step 4: type 'store'")
    send_string(hwnd, CONFIG["tfr_store_filter"], interval=0.2)
    time.sleep(0.5)

    log.info("[TFR] Step 5: Enter x2")
    for i in range(2):
        send_enter(hwnd)
        time.sleep(CONFIG["delay_enter_select"])

    log.info("[TFR] Step 6: type 'ganga'")
    send_string(hwnd, CONFIG["tfr_branch_filter"], interval=0.2)
    time.sleep(0.5)

    log.info("[TFR] Step 7: Enter x1")
    send_enter(hwnd)
    time.sleep(CONFIG["delay_enter_select"])

    log.info("[TFR] Step 8: type '*'")
    win32gui.PostMessage(hwnd, WM_CHAR, ord("*"), 0x00000001)
    time.sleep(0.4)

    log.info("[TFR] Step 9: Enter x1")
    send_enter(hwnd)
    time.sleep(CONFIG["delay_after_filter"])

    log.info("[TFR] Step 10: screenshot")
    img = capture_region(hwnd, TFR_REGION, TFR_REGION_CALIBRATED, "tfr_gangapur")
    if img:
        tg_send_photo(chat_id, img, "TFR Gangapur Transfers " + ts)
        if whatsapp.WHATSAPP_ENABLED and not suppress_whatsapp:
            log.info("[TFR] Sending image to WhatsApp...")
            whatsapp.send_image(img, "TFR Gangapur Transfers " + ts)
    else:
        tg_send_message(chat_id, "⚠️ Screenshot failed.")

    log.info("[TFR] Esc → main UI")
    send_escape(hwnd)
    time.sleep(CONFIG["delay_after_esc"])

    log.info("[TFR] Done.")


# ──────────────────────────────────────────────────────────────
# COMMAND HANDLERS
# ──────────────────────────────────────────────────────────────

def _run_locked(chat_id, name, fn):
    """
    DRY wrapper for all single-step Marg ERP command handlers.
    Acquires the automation lock, finds the window, calls fn(chat_id, hwnd),
    then releases. Any exception is caught, logged, and sent to Telegram.
    """
    if not _automation_lock.acquire(blocking=False):
        tg_send_message(chat_id, "⏳ Already running a command, please wait..."); return
    try:
        hwnd = get_hwnd(chat_id)
        if hwnd:
            fn(chat_id, hwnd)
    except Exception as e:
        log.error("%s: %s", name, e, exc_info=True)
        tg_send_message(chat_id, f"❌ {name} error: {e}")
    finally:
        _automation_lock.release()


def handle_wcr(chat_id):
    _run_locked(chat_id, "WCR", run_wcr)


def handle_tfr(chat_id):
    _run_locked(chat_id, "TFR", run_tfr)


def handle_all(chat_id, suppress_whatsapp=False):
    if not _automation_lock.acquire(blocking=False):
        tg_send_message(chat_id, "⏳ Already running a command, please wait..."); return
    try:
        hwnd = get_hwnd(chat_id)
        if hwnd is None:
            return
        label = "📊 Running full report (WhatsApp suppressed)..." if suppress_whatsapp else "📊 Running full report — WCR + TFR..."
        tg_send_message(chat_id, label)
        run_wcr(chat_id, hwnd, notify_start=False, suppress_whatsapp=suppress_whatsapp)
        time.sleep(1.5)
        run_tfr(chat_id, hwnd, notify_start=False, suppress_whatsapp=suppress_whatsapp)
        tg_send_message(chat_id, "✅ Full report complete.")
    except Exception as e:
        log.error("handle_all: %s", e, exc_info=True)
        tg_send_message(chat_id, "❌ Error: " + str(e))
    finally:
        _automation_lock.release()


# ──────────────────────────────────────────────────────────────
# DAILY SCHEDULERS
# ──────────────────────────────────────────────────────────────

_daily_fired_date   = None   # date the daily report last fired
_daily_restart_date = None   # date the auto-restart last fired
_last_ping_hour     = None   # hour of the last health ping sent

# On startup these are pre-marked so times that have already
# passed today do NOT fire immediately — see _init_schedulers().


def _past_or_at(hhmm: str) -> bool:
    """True if current wall-clock time >= hhmm today."""
    now = datetime.datetime.now()
    h, m = int(hhmm.split(":")[0]), int(hhmm.split(":")[1])
    return now >= now.replace(hour=h, minute=m, second=0, microsecond=0)


def _init_schedulers():
    """
    Called once at startup.
    Pre-mark any schedule whose time has already passed today so the
    bot does not fire them immediately on launch.
    Only leave them un-marked if we are BEFORE the scheduled time,
    meaning the bot started early enough to catch the window today.
    """
    global _daily_fired_date, _daily_restart_date, _last_ping_hour
    # Pre-set ping hour so bot doesn't ping immediately on startup
    now = datetime.datetime.now()
    _last_ping_hour = (now.date(), now.hour)
    today = datetime.datetime.now().date()
    _is_sunday = datetime.datetime.now().weekday() == 6
    if _past_or_at(CONFIG["daily_report_time"]) or (CONFIG["report_skip_sunday"] and _is_sunday):
        _daily_fired_date = today
        reason = "Sunday" if _is_sunday else f"time {CONFIG['daily_report_time']} already passed"
        log.info("[SCHEDULER] Daily report skipped today (%s)", reason)
    if _past_or_at(CONFIG["marg_restart_time"]):
        _daily_restart_date = today
        log.info("[SCHEDULER] Restart time %s already passed — will fire tomorrow",
                 CONFIG["marg_restart_time"])



def check_daily_report():
    """
    Fire the daily /all report once per day at or after daily_report_time.
    Skips Sundays automatically (weekday 6).
    """
    global _daily_fired_date
    now   = datetime.datetime.now()
    today = now.date()
    if _daily_fired_date == today:
        return
    if CONFIG["report_skip_sunday"] and now.weekday() == 6:
        return
    if _past_or_at(CONFIG["daily_report_time"]):
        _daily_fired_date = today
        day_name = now.strftime("%A")
        log.info("[SCHEDULER] Daily report firing — %s %s",
                 day_name, CONFIG["daily_report_time"])
        threading.Thread(target=handle_all, args=(ALLOWED_CHAT_ID,), daemon=True).start()


def check_daily_restart():
    """
    Restart Marg ERP once per day at or after marg_restart_time.
    Sunday behaviour controlled by restart_skip_sunday in config.
    """
    global _daily_restart_date
    now   = datetime.datetime.now()
    today = now.date()
    if _daily_restart_date == today:
        return
    if CONFIG["restart_skip_sunday"] and now.weekday() == 6:
        return
    if _past_or_at(CONFIG["marg_restart_time"]):
        _daily_restart_date = today
        log.info("[SCHEDULER] Auto-restart firing at %s", CONFIG["marg_restart_time"])
        threading.Thread(
            target=handle_restart_margerp, args=(ALLOWED_CHAT_ID,), daemon=True
        ).start()


def check_health_ping():
    """
    Send a Telegram "bot alive" ping once per hour at health_ping_minute past the hour.
    Example: health_ping_minute=0 → pings at 08:00, 09:00, 10:00 ...
    Uses the current hour as dedup key so it fires exactly once per hour.
    """
    global _last_ping_hour
    now = datetime.datetime.now()
    if now.minute != CONFIG["health_ping_minute"]:
        return
    hour_key = (now.date(), now.hour)
    if _last_ping_hour == hour_key:
        return
    _last_ping_hour = hour_key
    ts = now.strftime("%d/%m/%Y %H:%M")
    log.info("[PING] Health ping at %s", ts)
    tg_send_message(ALLOWED_CHAT_ID, f"✅ Bot alive — {ts}")


# ──────────────────────────────────────────────────────────────
# TELEGRAM MENU
# ──────────────────────────────────────────────────────────────

def help_text():
    cal_wcr    = "✅ calibrated" if WCR_REGION_CALIBRATED   else "⚠️ not calibrated"
    cal_tfr    = "✅ calibrated" if TFR_REGION_CALIBRATED   else "⚠️ not calibrated"
    cal_party  = "✅ calibrated" if PARTY_REGION_CALIBRATED else "⚠️ not calibrated"
    wa_status  = "✅ enabled"    if whatsapp.WHATSAPP_ENABLED else "⚠️ disabled"
    today      = datetime.date.today().strftime("%d/%m/%Y")

    # WhatsApp PDF recipient — show number or "not configured"
    wa_pdf_chat = whatsapp.PARTY_PDF_CHAT_ID.strip()
    if wa_pdf_chat:
        # Format: 919876543210@c.us → show as +91 98765 43210
        num = wa_pdf_chat.replace("@c.us", "").replace("@g.us", "")
        wa_pdf_display = "+" + num
    else:
        wa_pdf_display = "not configured"

    check_days = CONFIG["undelivered_check_days"]
    days_label = f"today + {check_days} previous working day{'s' if check_days != 1 else ''}"

    return (
        "🤖 *Marg ERP Monitor Bot*\n"
        "─────────────────────────\n\n"

        "🔍 *Party Ledger Search*\n"
        "/party [name] — Search party ledger\n"
        "  _Type name inline or enter after prompt_\n"
        "  _Pick entry number → choose output type_\n"
        "  *1* — 📄 Ledger PDF\n"
        "  *2* — 📊 Outstanding PDF\n"
        "  _PDF sent to Telegram + WhatsApp individual_\n\n"

        "📦 *Bill Reports*\n"
        "/wcr — WCR undelivered bills\n"
        "  _Checks: " + days_label + "_\n"
        "  _Screenshot + OCR → Telegram + WhatsApp group_\n\n"
        "/tfr — TFR Gangapur transfers\n"
        "  _Screenshot only (color-coded) → Telegram + WhatsApp group_\n\n"
        "/all — Full report: WCR + TFR\n"
        "/all\\_no\\_whatsapp — Full report, Telegram only\n\n"

        "🔄 *Marg ERP Control*\n"
        "/restart\\_margerp — Restart Marg ERP & log in\n"
        "/backup — Run full Marg ERP backup\n\n"

        "🖨️ *Printer*\n"
        "/restart\\_printer\\_spooler — Restart Print Spooler\n\n"

        "─────────────────────────\n"
        "📊 *Current Status*\n"
        "Today            : " + today + "\n"
        "WhatsApp group   : " + wa_status + "\n"
        "Party PDF → WA   : " + wa_pdf_display + "\n"
        "WCR date range   : " + days_label + "\n"
        "Bill alert at    : ≥ " + str(CONFIG["bill_alert_threshold"]) + " pending\n"
        "WCR region       : " + cal_wcr + "\n"
        "TFR region       : " + cal_tfr + "\n"
        "Party region     : " + cal_party + "\n\n"

        "🕗 *Schedule*\n"
        "Daily report  : " + CONFIG["daily_report_time"] + "\n"
        "Auto-restart  : " + CONFIG["marg_restart_time"] + "\n"
        "Daily backup  : " + CONFIG["backup_schedule_time"] + "\n"
        "Health ping   : every hour at :" + str(CONFIG["health_ping_minute"]).zfill(2)
    )




# ──────────────────────────────────────────────────────────────
# PARTY LEDGER SEARCH
# ──────────────────────────────────────────────────────────────

def _party_done(hwnd=None):
    """
    End the party flow: press Esc x2 to return to Marg ERP main UI,
    then clear the party state globals.

    Esc is only sent when we have actually navigated into Marg ERP
    (i.e. state is NOT awaiting_name — if still awaiting_name we haven't
    pressed any keys in Marg yet so no navigation to undo).
    """
    global _party_state, _party_hwnd
    target = hwnd or _party_hwnd
    # Only send Esc if we navigated past the name prompt
    if target and _party_state != "awaiting_name":
        try:
            for _ in range(3):
                send_escape(target)
                time.sleep(0.5)
            log.info("[PARTY] Esc x3 sent — back at main UI")
        except Exception as e:
            log.warning("[PARTY] Esc x3 failed: %s", e)
    _party_state = None
    _party_hwnd  = None
    _party_name  = None


def run_party_search(chat_id, hwnd, party_name: str):
    """
    Step 1 of party search: navigate to ledger list and capture screenshot.

    Navigation:
      1. Down Arrow x4  → child control
      2. Enter          → child control (open party search screen)
      3. Type party name → parent (WM_CHAR)
      4. Capture PARTY_REGION screenshot → send to Telegram
      5. Ask user to reply with a number (0 = first match, 1 = second, etc.)

    NOTE: All keystrokes go to the child control to avoid stray messages
    reaching the parent's menu system (which can interpret WM_KEYDOWN for
    VK_RETURN/VK_MENU as menu activation).
    """
    global _party_state, _party_hwnd, _party_name
    child = find_marg_child(hwnd)
    ts = datetime.datetime.now().strftime("%H:%M")
    _party_name = party_name.strip()   # store exact name for file naming

    log.info("[PARTY] Step 1: Down Arrow x4 → child")
    for _ in range(4):
        send_down_arrow(child)
        time.sleep(0.4)

    log.info("[PARTY] Step 2: Enter → child")
    send_enter(child)   # send to child, not parent
    time.sleep(CONFIG["delay_enter_select"])

    log.info("[PARTY] Step 3: type party name: %s", party_name)
    send_string(hwnd, party_name, interval=0.15)
    time.sleep(CONFIG["delay_after_filter"])

    log.info("[PARTY] Step 3b: Right Arrow x2 → load full ledger row data")
    for _ in range(2):
        send_extended_key(child, VK_RIGHT, scan=0x4D)   # scan 0x4D = Right Arrow
        time.sleep(0.3)

    log.info("[PARTY] Step 4: capture ledger list region")
    img = capture_region(hwnd, PARTY_REGION, PARTY_REGION_CALIBRATED, "party_search")

    if img is None:
        tg_send_message(chat_id, "⚠️ Screenshot failed during party search.")
        send_escape(child)
        time.sleep(1.0)
        _party_done(hwnd)
        return

    tg_send_photo(chat_id, img, f"Party search: '{party_name}' — {ts}")
    tg_send_message(
        chat_id,
        "👆 Reply with the *number* of the ledger you want to open:\n"
        "`0` = first entry (already selected)\n"
        "`1` = second entry\n"
        "`2` = third entry\n"
        "...and so on.\n\n"
        "Send /cancel to go back to main UI."
    )

    # Advance state — next message should be the user's number choice
    _party_state = "awaiting_selection"
    _party_hwnd  = hwnd
    log.info("[PARTY] Waiting for user selection")


def run_party_open(chat_id, hwnd, selection: int):
    """
    Step 2 of party search: navigate to selected ledger and open it.

    Navigation:
      1. Down Arrow x{selection}  → move to selected entry
      2. Enter x4                  → open the ledger
      3. Screenshot → send to Telegram
      4. Prompt user for action (email / PDF)
    """
    global _party_state, _party_hwnd
    child = find_marg_child(hwnd)
    ts = datetime.datetime.now().strftime("%H:%M")

    log.info("[PARTY] Step 1: Down Arrow x%d to reach selection", selection)
    for _ in range(selection):
        send_down_arrow(child)
        time.sleep(0.4)

    log.info("[PARTY] Step 2: Enter x4 → child to open ledger")
    for _ in range(4):
        send_enter(child)   # child, not parent
        time.sleep(CONFIG["delay_enter_select"])
    time.sleep(1.0)   # extra wait for ledger to fully load

    log.info("[PARTY] Step 3: capture ledger view")
    time.sleep(CONFIG["delay_before_capture"])
    img = grab_window(hwnd)
    if img:
        save_shot(img, "party_ledger")
        tg_send_photo(chat_id, img, f"Ledger opened — {ts}")
    else:
        tg_send_message(chat_id, "⚠️ Could not capture ledger screenshot.")

    # Step 4: proceed directly to PDF generation
    log.info("[PARTY] Proceeding directly to PDF generation")
    # Step 4: ask user which output they want
    tg_send_message(
        chat_id,
        "📂 *Select output type:*\n\n"
        "*1* — 📄 Ledger PDF\n"
        "*2* — 📊 Outstanding PDF\n\n"
        "Reply with `1` or `2`, or /cancel to go back."
    )
    _party_state = "awaiting_output_type"
    log.info("[PARTY] Waiting for output type selection")




LEDGER_TEMP_DIR = CONFIG["ledger_temp_dir"]


def _send_pdf_file(chat_id, pdf_path, caption):
    """
    Send a party PDF to Telegram and forward to WhatsApp individual chat.

    Two independent delivery channels — a failure in one does not affect
    the other. WhatsApp forwarding uses whatsapp.send_pdf() which targets
    PARTY_PDF_CHAT_ID (individual chat), completely separate from the
    WCR/TFR group messaging flows.

    Retries file open for up to 15s to handle PermissionError (file still
    being written by Marg ERP).
    """
    log.info("[PARTY] Sending PDF: %s", pdf_path)

    # ── Wait for file to be readable ──────────────────────────────────────────
    pdf_bytes = None
    for attempt in range(15):
        try:
            with open(pdf_path, "rb") as f:
                pdf_bytes = f.read()
            log.info("[PARTY] File readable after %d attempt(s)", attempt + 1)
            break
        except PermissionError:
            log.info("[PARTY] File locked, retry %d/15", attempt + 1)
            time.sleep(1)

    if pdf_bytes is None:
        log.error("[PARTY] File still locked after 15s — aborting send")
        tg_send_message(chat_id, "\u274c File still locked after 15s \u2014 could not send.")
        return

    # ── Telegram delivery ─────────────────────────────────────────────────────
    filename = os.path.basename(pdf_path)
    tg_ok = False
    try:
        buf  = io.BytesIO(pdf_bytes)
        resp = requests.post(
            f"https://api.telegram.org/bot{CONFIG['telegram_bot_token']}/sendDocument",
            data={"chat_id": chat_id, "caption": caption},
            files={"document": (filename, buf, "application/pdf")},
            timeout=60,
        )
        if resp.status_code == 200:
            log.info("[PARTY] Telegram PDF sent OK")
            tg_send_message(chat_id, "\u2705 *PDF sent.*")
            tg_ok = True
        else:
            log.error("[PARTY] Telegram sendDocument failed HTTP %d: %s",
                      resp.status_code, resp.text[:300])
            tg_send_message(chat_id,
                "\u274c Telegram upload failed.\nFile: `" + pdf_path + "`")
    except Exception as e:
        log.error("[PARTY] Telegram send error: %s", e, exc_info=True)
        tg_send_message(chat_id, "\u274c Telegram send error: " + str(e))

    # ── WhatsApp delivery (individual chat — separate from group flows) ────────
    if whatsapp.WHATSAPP_ENABLED and whatsapp.PARTY_PDF_CHAT_ID:
        log.info("[PARTY] Forwarding PDF to WhatsApp: %s → %s",
                 filename, whatsapp.PARTY_PDF_CHAT_ID)
        wa_ok = whatsapp.send_pdf(pdf_path, caption=caption)
        if wa_ok:
            log.info("[PARTY] WhatsApp PDF forwarded OK")
        else:
            log.error("[PARTY] WhatsApp PDF forward failed — see whatsapp log above")
            # Notify on Telegram so user knows WA delivery failed
            tg_send_message(chat_id,
                "\u26a0\ufe0f PDF sent to Telegram but *WhatsApp delivery failed*.\n"
                "Check the log for details.")
    elif whatsapp.WHATSAPP_ENABLED and not whatsapp.PARTY_PDF_CHAT_ID:
        log.info("[PARTY] WhatsApp enabled but party_pdf_chat_id not configured — skipping WA")
    else:
        log.debug("[PARTY] WhatsApp disabled — skipping WA forward")


def _outstanding_filename():
    """
    Build filename: {party_name}_outstanding_{DD-MM-YY}_{HHMMSS}
    e.g. revive hea_outstanding_08-04-26_133855
    """
    name = (_party_name or "party").strip().lower()
    now  = datetime.datetime.now()
    return f"{name}_outstanding_{now.strftime('%d-%m-%y')}_{now.strftime('%H%M%S')}"


def _type_into_save_dialog(filename: str):
    """
    Set filename in Windows Save As dialog and click Save.

    Primary strategy (headless-safe):
      1. EnumWindows to find the common Save dialog (#32770)
      2. EnumChildWindows to find the filename Edit control
      3. SendMessage(edit, WM_SETTEXT) — sets text without needing focus
      4. Find the Save button by caption and SendMessage BM_CLICK

    Fallback (requires dialog to have focus):
      Ctrl+A, type filename char-by-char via keybd_event, then Alt+S.
    """
    WM_SETTEXT = 0x000C
    BM_CLICK   = 0x00F5

    # ── Primary: find dialog and set text directly ────────────────────────
    dialog_hwnd = None

    def _find_dialog(h, _):
        nonlocal dialog_hwnd
        try:
            if win32gui.GetClassName(h) == "#32770" and win32gui.IsWindowVisible(h):
                if dialog_hwnd is None:
                    dialog_hwnd = h
        except Exception:
            pass

    win32gui.EnumWindows(_find_dialog, None)

    if dialog_hwnd:
        log.info("[OUTSTANDING] Save dialog found hwnd=%d title=%r",
                 dialog_hwnd, win32gui.GetWindowText(dialog_hwnd))

        # Find filename Edit control
        edit_hwnd = None

        def _find_edit(h, _):
            nonlocal edit_hwnd
            try:
                if win32gui.GetClassName(h) == "Edit" and win32gui.IsWindowVisible(h):
                    if edit_hwnd is None:
                        edit_hwnd = h
            except Exception:
                pass

        win32gui.EnumChildWindows(dialog_hwnd, _find_edit, None)

        if edit_hwnd:
            log.info("[OUTSTANDING] Edit control hwnd=%d, setting text: %s",
                     edit_hwnd, filename)
            # WM_SETTEXT works without focus — headless-safe
            win32gui.SendMessage(edit_hwnd, WM_SETTEXT, 0, filename)
            time.sleep(0.3)

            # Find Save button
            save_btn = None

            def _find_save_btn(h, _):
                nonlocal save_btn
                try:
                    cls = win32gui.GetClassName(h)
                    txt = win32gui.GetWindowText(h).lower().replace("&", "").strip()
                    if cls == "Button" and txt in ("save", "open") and win32gui.IsWindowVisible(h):
                        if save_btn is None:
                            save_btn = h
                except Exception:
                    pass

            win32gui.EnumChildWindows(dialog_hwnd, _find_save_btn, None)

            if save_btn:
                log.info("[OUTSTANDING] Clicking Save button hwnd=%d", save_btn)
                win32gui.SendMessage(save_btn, BM_CLICK, 0, 0)
                log.info("[OUTSTANDING] Save button clicked — dialog done")
                return
            else:
                # No button found — send Enter to dialog
                log.warning("[OUTSTANDING] Save button not found, sending Enter")
                win32gui.SendMessage(dialog_hwnd, WM_KEYDOWN, 0x0D, 0x00000001)
                time.sleep(0.1)
                win32gui.SendMessage(dialog_hwnd, WM_KEYUP, 0x0D, 0xC0000001)
                return
        else:
            log.warning("[OUTSTANDING] Edit control not found in Save dialog")
    else:
        log.warning("[OUTSTANDING] Save dialog (#32770) not found — using keybd_event fallback")

    # ── Fallback: keybd_event (requires dialog to have focus) ────────────
    KEYEVENTF_KEYUP = 0x0002
    ctypes.windll.user32.keybd_event(0x11, 0, 0, 0)           # Ctrl down
    ctypes.windll.user32.keybd_event(0x41, 0, 0, 0)           # A
    time.sleep(0.05)
    ctypes.windll.user32.keybd_event(0x41, 0, KEYEVENTF_KEYUP, 0)
    ctypes.windll.user32.keybd_event(0x11, 0, KEYEVENTF_KEYUP, 0)
    time.sleep(0.1)
    for ch in filename:
        vk = ctypes.windll.user32.VkKeyScanW(ch)
        if vk == -1:
            continue
        lo = vk & 0xFF
        hi = (vk >> 8) & 0xFF
        if hi & 1:
            ctypes.windll.user32.keybd_event(0x10, 0, 0, 0)
        ctypes.windll.user32.keybd_event(lo, 0, 0, 0)
        time.sleep(0.03)
        ctypes.windll.user32.keybd_event(lo, 0, KEYEVENTF_KEYUP, 0)
        if hi & 1:
            ctypes.windll.user32.keybd_event(0x10, 0, KEYEVENTF_KEYUP, 0)
        time.sleep(0.05)
    time.sleep(0.3)
    ctypes.windll.user32.keybd_event(0x12, 0, 0, 0)           # Alt down
    ctypes.windll.user32.keybd_event(0x53, 0, 0, 0)           # S
    time.sleep(0.05)
    ctypes.windll.user32.keybd_event(0x53, 0, KEYEVENTF_KEYUP, 0)
    ctypes.windll.user32.keybd_event(0x12, 0, KEYEVENTF_KEYUP, 0)
    log.info("[OUTSTANDING] Fallback: typed via keybd_event + Alt+S")


def run_party_outstanding_pdf(chat_id, hwnd):
    """
    Generate outstanding report PDF.

    Marg ERP keystrokes (all posted to Marg hwnd):
      1. F5       - refresh / open outstanding view
      2. Alt+O    - open outstanding option
      3. Alt+P    - print/preview
      4. type A   - select option A
      5. Alt+P    - confirm print
      6. Enter x2 - confirm dialogs → triggers Windows Save As dialog

    Windows Save As dialog (standard OS dialog, NOT Marg ERP):
      - Type generated filename
      - Alt+S to save

    File saved to: C:\\MargMonitor\\ledger_temp\\{filename}.pdf
    After sending: Esc x3 to return to Marg ERP main UI.
    """
    global _party_state, _party_hwnd

    os.makedirs(LEDGER_TEMP_DIR, exist_ok=True)
    _prune_dir(LEDGER_TEMP_DIR, CONFIG["max_ledger_temp_mb"], "ledger PDF")
    filename = _outstanding_filename()
    pdf_path = os.path.join(LEDGER_TEMP_DIR, filename + ".pdf")
    t0 = time.time()

    log.info("[OUTSTANDING] Target file: %s", pdf_path)
    tg_send_message(chat_id, "\u23f3 Generating outstanding report PDF...")

    VK_F5 = 0x74
    VK_O  = 0x4F

    # Step 1: F5 — function keys need scan code + extended-key flag in lparam
    log.info("[OUTSTANDING] Step 1: F5 (scan + extended-key flag)")
    _f5_scan = ctypes.windll.user32.MapVirtualKeyW(VK_F5, 0)
    _f5_dn   = 0x00000001 | (_f5_scan << 16) | 0x01000000   # repeat=1, scan, extended
    _f5_up   = 0xC0000001 | (_f5_scan << 16) | 0x01000000   # transition+prev+extended
    win32gui.PostMessage(hwnd, WM_KEYDOWN, VK_F5, _f5_dn)
    time.sleep(0.1)
    win32gui.PostMessage(hwnd, WM_KEYUP,   VK_F5, _f5_up)
    time.sleep(CONFIG["delay_enter_select"])

    # Step 2: Alt+O
    log.info("[OUTSTANDING] Step 2: Alt+O")
    scan_o = ctypes.windll.user32.MapVirtualKeyW(VK_O, 0)
    win32gui.PostMessage(hwnd, 0x0104, VK_O, 0x00000001 | (scan_o << 16) | 0x20000000)
    time.sleep(0.1)
    win32gui.PostMessage(hwnd, 0x0105, VK_O, 0xC0000001 | (scan_o << 16) | 0x20000000)
    time.sleep(CONFIG["delay_enter_select"])

    # Step 3: Alt+P
    log.info("[OUTSTANDING] Step 3: Alt+P")
    send_alt_p(hwnd)
    time.sleep(CONFIG["delay_enter_select"])

    # Step 4: type A
    log.info("[OUTSTANDING] Step 4: type A")
    win32gui.PostMessage(hwnd, WM_CHAR, ord("A"), 0x00000001)
    time.sleep(0.5)

    # Step 5: Alt+P
    log.info("[OUTSTANDING] Step 5: Alt+P")
    send_alt_p(hwnd)
    time.sleep(CONFIG["delay_enter_select"])

    # Step 6: Enter x2 — triggers Save As dialog
    log.info("[OUTSTANDING] Step 6: Enter x2 to trigger Save As dialog")
    send_enter(hwnd)
    time.sleep(CONFIG["delay_enter_select"])
    send_enter(hwnd)
    time.sleep(2.5)   # wait for Windows Save As dialog to open

    # ── Windows Save As dialog ───────────────────────────────────────────────
    log.info("[OUTSTANDING] Typing filename into Save As dialog: %s", filename)
    _type_into_save_dialog(filename)
    time.sleep(2.0)   # wait for save to complete

    # ── Poll for saved file ───────────────────────────────────────────────────
    log.info("[OUTSTANDING] Polling for saved file at %s", pdf_path)
    POLL = 2
    MAX  = 60
    elapsed = 0
    while elapsed < MAX:
        time.sleep(POLL)
        elapsed += POLL
        if os.path.exists(pdf_path):
            mtime = os.path.getmtime(pdf_path)
            if mtime > t0:
                log.info("[OUTSTANDING] File found after %ds", elapsed)
                break
            log.info("[OUTSTANDING] File exists but stale (%ds)", elapsed)
        else:
            log.info("[OUTSTANDING] Not found yet (%ds)", elapsed)
    else:
        tg_send_message(chat_id,
            "\u26a0\ufe0f Outstanding PDF not found at expected path after 60s.\n"
            "Expected: `" + pdf_path + "`"
        )
        _party_done(hwnd)
        return

    # ── Send file ─────────────────────────────────────────────────────────────
    _send_pdf_file(chat_id, pdf_path,
                   "\U0001f4ca Outstanding Report — " + (_party_name or "") + " — "
                   + datetime.datetime.now().strftime("%d/%m/%Y %H:%M"))

    log.info("[OUTSTANDING] Done.")
    _party_done(hwnd)


def _party_pdf_path() -> str:
    """
    Build the expected PDF file path from config.
    Path pattern: C:/Users/Public/MARG/{marg_folder_id}/ledger.PDF
    """
    folder_id = CONFIG["marg_folder_id"].strip()
    if not folder_id:
        return ""
    return os.path.join(r"C:\Users\Public\MARG", folder_id, "ledger.PDF")


def run_party_pdf(chat_id, hwnd):
    """
    Action: Generate PDF of ledger.

    Sequence:
      1. Record command start time (t0) BEFORE triggering anything
      2. Alt+P → Alt+F  (trigger PDF generation)
      3. Poll every 2s for up to 60s:
           - File doesn't exist yet → keep waiting
           - File exists but mtime <= t0 → old file, keep waiting
           - File exists and mtime > t0  → fresh file, send it immediately
      4. Send the PDF to Telegram via sendDocument
    """
    global _party_state, _party_hwnd

    pdf_path = _party_pdf_path()
    if not pdf_path:
        tg_send_message(
            chat_id,
            "⚠️ `marg_folder_id` not set in config.ini.\n"
            "Add under `[party]`:\n"
            "`marg_folder_id = YOUR_FOLDER_ID`"
        )
        _party_done(hwnd)
        return

    # t0: exact moment command was executed — any file with mtime <= t0 is stale
    t0 = time.time()
    log.info("[PARTY-PDF] t0=%.3f — recording command start time", t0)

    if os.path.exists(pdf_path):
        existing_mtime = os.path.getmtime(pdf_path)
        log.info("[PARTY-PDF] Existing file mtime=%.3f (%s) — will wait for newer",
                 existing_mtime,
                 datetime.datetime.fromtimestamp(existing_mtime).strftime("%H:%M:%S"))
    else:
        log.info("[PARTY-PDF] No existing file — will wait for creation")

    # Trigger PDF generation
    log.info("[PARTY-PDF] Alt+P")
    tg_send_message(chat_id, "📄 Generating PDF...")
    send_alt_p(hwnd)
    time.sleep(1.0)

    log.info("[PARTY-PDF] Alt+F")
    send_alt_f(hwnd)

    # Poll until a file newer than t0 appears
    log.info("[PARTY-PDF] Polling for fresh PDF at %s", pdf_path)
    POLL_INTERVAL = 2     # seconds between checks
    MAX_WAIT      = 120   # give up after 2 minutes
    elapsed       = 0

    while elapsed < MAX_WAIT:
        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL

        if not os.path.exists(pdf_path):
            log.info("[PARTY-PDF] Not found yet (%ds elapsed)", elapsed)
            continue

        mtime = os.path.getmtime(pdf_path)
        if mtime <= t0:
            log.info("[PARTY-PDF] File exists but mtime %.3f <= t0 %.3f — still old (%ds)",
                     mtime, t0, elapsed)
            continue

        # Fresh file detected — wait until Marg releases the file handle
        # by retrying the open until it succeeds (PermissionError = still writing)
        log.info("[PARTY-PDF] Fresh PDF detected after %ds — waiting for file to be released",
                 elapsed)
        for attempt in range(15):          # retry for up to 15 seconds
            time.sleep(1)
            try:
                with open(pdf_path, "rb") as _test:
                    _test.read(1)          # try reading 1 byte
                log.info("[PARTY-PDF] File released after %d extra second(s)", attempt + 1)
                break
            except PermissionError:
                log.info("[PARTY-PDF] Still locked by Marg, retrying... (%ds)", attempt + 1)
        else:
            log.warning("[PARTY-PDF] File still locked after 15s")
        break
    else:
        log.warning("[PARTY-PDF] Timed out after %ds waiting for fresh PDF", MAX_WAIT)
        tg_send_message(
            chat_id,
            "⚠️ PDF generation timed out after 2 minutes.\n"
            "Expected: `" + pdf_path + "`\n"
            "_Check if `marg_folder_id` is correct in config.ini._"
        )
        _party_done(hwnd)
        return

    # Send via shared helper (handles PermissionError retry)
    caption = ("📄 Ledger PDF — " + (_party_name or "")
               + " — " + datetime.datetime.now().strftime("%d/%m/%Y %H:%M"))
    _send_pdf_file(chat_id, pdf_path, caption)
    log.info("[PARTY-PDF] Done.")
    _party_done(hwnd)


def handle_party(chat_id, party_name: str = ""):
    """
    Handle /party command.
    If party_name is provided (e.g. /party revive), skip the name prompt
    and go straight to searching. Otherwise prompt the user for a name.
    """
    global _party_state, _party_hwnd

    if not _automation_lock.acquire(blocking=False):
        tg_send_message(chat_id, "⏳ Already running a command, please wait.")
        return

    hwnd = get_hwnd(chat_id)
    if hwnd is None:
        _automation_lock.release()
        return

    _party_hwnd = hwnd

    if party_name:
        # Name provided inline — go straight to search
        _party_state = "awaiting_name"   # set so _party_done knows no Esc needed yet
        _automation_lock.release()
        log.info("[PARTY] Inline name provided: %s", party_name)
        # Feed directly into the input handler as if the user typed it
        handle_party_input(chat_id, party_name)
    else:
        # No name — prompt the user
        _party_state = "awaiting_name"
        _automation_lock.release()
        tg_send_message(
            chat_id,
            "🔍 *Party Ledger Search*\n\n"
            "Type the party name to search for:\n"
            "_Example: shri sai medical_\n\n"
            "Or use: `/party shri sai medical`\n\n"
            "Send /cancel to abort."
        )
        log.info("[PARTY] Waiting for party name from user")


def handle_party_input(chat_id, text: str):
    """
    Handle non-command messages when party state machine is active.
    Called from process_updates when _party_state is not None.
    """
    global _party_state, _party_hwnd

    if text.strip().lower() == "/cancel":
        tg_send_message(chat_id, "❌ Party search cancelled.")
        _party_done(_party_hwnd)
        return

    if _party_state == "awaiting_name":
        party_name = text.strip()
        if not party_name:
            tg_send_message(chat_id, "⚠️ Please enter a party name.")
            return
        hwnd = _party_hwnd
        if hwnd is None or not win32gui.IsWindow(hwnd):
            tg_send_message(chat_id, "❌ Marg ERP window lost. Please try /party again.")
            _party_done(hwnd)
            return
        if not _automation_lock.acquire(blocking=False):
            tg_send_message(chat_id, "⏳ Already running a command, please wait.")
            return
        try:
            run_party_search(chat_id, hwnd, party_name)
        except Exception as e:
            log.error("run_party_search: %s", e, exc_info=True)
            tg_send_message(chat_id, "❌ Party search error: " + str(e))
            _party_done(hwnd)
        finally:
            _automation_lock.release()

    elif _party_state == "awaiting_selection":
        try:
            selection = int(text.strip())
            if selection < 0:
                raise ValueError("negative")
        except ValueError:
            tg_send_message(
                chat_id,
                "⚠️ Please reply with a number (0, 1, 2, ...) or /cancel."
            )
            return
        hwnd = _party_hwnd
        if hwnd is None or not win32gui.IsWindow(hwnd):
            tg_send_message(chat_id, "❌ Marg ERP window lost. Please try /party again.")
            _party_done(hwnd)
            return
        if not _automation_lock.acquire(blocking=False):
            tg_send_message(chat_id, "⏳ Already running a command, please wait.")
            return
        try:
            run_party_open(chat_id, hwnd, selection)
        except Exception as e:
            log.error("run_party_open: %s", e, exc_info=True)
            tg_send_message(chat_id, "❌ Error opening ledger: " + str(e))
            _party_done(hwnd)
        finally:
            _automation_lock.release()

    elif _party_state == "awaiting_output_type":
        choice = text.strip()
        if choice not in ("1", "2"):
            tg_send_message(
                chat_id,
                "⚠️ Please reply with:\n"
                "*1* — 📄 Ledger PDF\n"
                "*2* — 📊 Outstanding PDF\n"
                "or /cancel"
            )
            return
        hwnd = _party_hwnd
        if hwnd is None or not win32gui.IsWindow(hwnd):
            tg_send_message(chat_id, "❌ Marg ERP window lost. Please try /party again.")
            _party_done(hwnd)
            return
        if not _automation_lock.acquire(blocking=False):
            tg_send_message(chat_id, "⏳ Already running a command, please wait.")
            return
        try:
            if choice == "1":
                run_party_pdf(chat_id, hwnd)
            else:
                run_party_outstanding_pdf(chat_id, hwnd)
        except Exception as e:
            log.error("party output: %s", e, exc_info=True)
            tg_send_message(chat_id, "❌ Error generating PDF: " + str(e))
            _party_done(hwnd)
        finally:
            _automation_lock.release()




# ──────────────────────────────────────────────────────────────
# BACKUP
# ──────────────────────────────────────────────────────────────

def run_backup(chat_id, hwnd):
    """
    Execute Marg ERP backup sequence from the main UI.

    Navigation:
      1. Down Arrow x5     → navigate to backup menu item (child)
      2. Enter             → open backup screen (child)
      3. Type 'c'          → select company (hwnd)
      4. Enter             → confirm selection (child)
      5. Enter x8          → step through backup confirmation screens (child)
      6. Screenshot        → send to Telegram so user can see backup is running
      7. Wait backup_wait_time seconds (from [delays] in config)
      8. Esc               → dismiss backup complete dialog
      9. Type 'a'          → acknowledge any prompt
     10. Enter             → confirm (child)
     11. Esc x10 (2s gap) → clear remaining popups (same pattern as restart)
     12. Screenshot        → final confirmation backup is done
    """
    child = find_marg_child(hwnd)
    wait  = CONFIG["backup_wait_time"]
    ts    = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")

    log.info("[BACKUP] Step 1: Down Arrow x5")
    for _ in range(5):
        send_down_arrow(child)
        time.sleep(0.4)

    log.info("[BACKUP] Step 2: Enter → open backup screen")
    send_enter(child)
    time.sleep(CONFIG["delay_enter_select"])

    log.info("[BACKUP] Step 3: type 'c'")
    send_string(hwnd, "c", interval=0.15)
    time.sleep(0.5)

    log.info("[BACKUP] Step 4: Enter → confirm selection")
    send_enter(child)
    time.sleep(CONFIG["delay_enter_select"])

    log.info("[BACKUP] Step 5: Enter x8 → step through backup screens")
    for i in range(8):
        send_enter(child)
        log.debug("[BACKUP] Enter %d/8", i + 1)
        time.sleep(CONFIG["delay_enter_select"])

    log.info("[BACKUP] Step 6: mid-backup screenshot")
    img = grab_window(hwnd)
    if img:
        save_shot(img, "backup_running")
        tg_send_photo(chat_id, img, f"⏳ Backup running — {ts}")
    tg_send_message(chat_id, f"⏳ Waiting {wait}s for backup to complete...")

    log.info("[BACKUP] Step 7: waiting %ds", wait)
    time.sleep(wait)

    log.info("[BACKUP] Step 8: Esc → dismiss backup complete dialog")
    send_escape(hwnd)
    time.sleep(1.0)

    log.info("[BACKUP] Step 9: type 'a'")
    send_string(hwnd, "a", interval=0.15)
    time.sleep(0.5)

    log.info("[BACKUP] Step 10: Enter → confirm")
    send_enter(child)
    time.sleep(CONFIG["delay_enter_select"])

    log.info("[BACKUP] Step 11: Esc x10 to clear remaining popups")
    for i in range(10):
        send_escape(hwnd)
        log.debug("[BACKUP] Esc %d/10", i + 1)
        time.sleep(2.0)

    log.info("[BACKUP] Step 12: final confirmation screenshot")
    hwnd2 = find_window(CONFIG["marg_main_window_title"])
    img2 = grab_window(hwnd2 or hwnd)
    if img2:
        save_shot(img2, "backup_done")
        ts2 = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")
        tg_send_photo(chat_id, img2, f"✅ Backup complete — {ts2}")
    tg_send_message(chat_id, "✅ *Backup complete.* Marg ERP is back at main UI.")
    log.info("[BACKUP] Done.")


def handle_backup(chat_id):
    """
    Handle /backup command — prompt for confirmation before running.
    Uses _backup_state to track the yes/no reply.
    """
    global _backup_state
    if not _automation_lock.acquire(blocking=False):
        tg_send_message(chat_id, "⏳ Already running a command, please wait.")
        return
    _automation_lock.release()

    _backup_state = "awaiting_confirm"
    tg_send_message(
        chat_id,
        "💾 *Marg ERP Backup*\n\n"
        "This will navigate the main UI and run the full backup sequence.\n"
        "Estimated time: ~" + str(CONFIG["backup_wait_time"] + 40) + "s\n\n"
        "Reply *yes* to proceed or *no* to cancel."
    )
    log.info("[BACKUP] Waiting for user confirmation")


def handle_backup_input(chat_id, text: str):
    """
    Handle yes/no confirmation reply for /backup.
    Called from process_updates when _backup_state is not None.
    """
    global _backup_state
    reply = text.strip().lower()

    if reply in ("no", "n", "cancel", "/cancel"):
        tg_send_message(chat_id, "❌ Backup cancelled.")
        _backup_state = None
        return

    if reply not in ("yes", "y"):
        tg_send_message(chat_id, "Please reply *yes* to proceed or *no* to cancel.")
        return

    # User confirmed — proceed
    _backup_state = None

    if not _automation_lock.acquire(blocking=False):
        tg_send_message(chat_id, "⏳ Already running a command, please wait.")
        return

    hwnd = get_hwnd(chat_id)
    if hwnd is None:
        _automation_lock.release()
        return

    tg_send_message(chat_id, "💾 Starting backup sequence...")
    try:
        run_backup(chat_id, hwnd)
    except Exception as e:
        log.error("[BACKUP] %s", e, exc_info=True)
        tg_send_message(chat_id, "❌ Backup error: " + str(e))
    finally:
        _automation_lock.release()


def check_scheduled_backup():
    """
    Fire the backup at backup_schedule_time. Fires every day the time
    matches the current HH:MM — no "already ran today" dedup.
    Deduplication is handled naturally by the 1-minute window: the main
    loop polls every 3s, so this fires ~20 times per minute but the
    lock prevents concurrent runs.

    Skips Sunday. Does NOT fire on bot startup even if time has passed
    (no pre-marking in _init_schedulers).
    """
    now = datetime.datetime.now()
    if CONFIG["backup_skip_sunday"] and now.weekday() == 6:
        return
    sched_h, sched_m = map(int, CONFIG["backup_schedule_time"].split(":"))
    if now.hour != sched_h or now.minute != sched_m:
        return
    # Only acquire if not already running — lock doubles as dedup within the minute
    if not _automation_lock.acquire(blocking=False):
        return   # already running something — silently skip, try again next poll
    def _scheduled_backup():
        try:
            hwnd = find_window(CONFIG["marg_main_window_title"])
            if hwnd is None:
                tg_send_message(ALLOWED_CHAT_ID, "⚠️ Scheduled backup: Marg ERP not found.")
                return
            log.info("[SCHEDULER] Auto-backup firing at %s", CONFIG["backup_schedule_time"])
            tg_send_message(ALLOWED_CHAT_ID, "💾 *Scheduled backup starting...*")
            run_backup(ALLOWED_CHAT_ID, hwnd)
        except Exception as e:
            log.error("[BACKUP] Scheduled: %s", e, exc_info=True)
            tg_send_message(ALLOWED_CHAT_ID, "❌ Scheduled backup error: " + str(e))
        finally:
            _automation_lock.release()
    threading.Thread(target=_scheduled_backup, daemon=True).start()

def handle_restart_printer_spooler(chat_id):
    """
    Restart the Windows Print Spooler via a Task Scheduler task.

    Task Scheduler runs the task under SYSTEM or a privileged account,
    so no elevation of the bot process itself is needed.

    Setup (one-time, in Task Scheduler):
      Name   : RestartPrintSpooler
      User   : SYSTEM
      Run    : Whether user is logged on or not
      Highest: ✅ Run with highest privileges
      Action : cmd.exe  /c net stop spooler & net start spooler
      Trigger: none (on-demand only)

    schtasks /run exits 0 immediately (fire-and-forget), so we wait
    a few seconds then use schtasks /query to read the Last Run Result.
    0x0 = success, anything else = failure.
    """
    TASK_NAME = "RestartPrintSpooler"
    tg_send_message(chat_id, "🖨️ Restarting Print Spooler...")
    log.info("[SPOOLER] schtasks /run /tn %s", TASK_NAME)

    try:
        # Trigger the task
        run = subprocess.run(
            ["schtasks", "/run", "/tn", TASK_NAME],
            capture_output=True, text=True, timeout=15
        )
        run_out = (run.stdout + run.stderr).strip()
        log.info("[SPOOLER] /run rc=%d out=%r", run.returncode, run_out)

        # schtasks /run returns non-zero if the task doesn't exist
        if run.returncode != 0 or "ERROR" in run_out.upper():
            log.error("[SPOOLER] Task trigger failed: %s", run_out)
            tg_send_message(chat_id,
                "❌ *Could not trigger task*\n"
                "```\n" + run_out + "\n```\n\n"
                "*Task not set up yet? Create it in Task Scheduler:*\n"
                "• Name: `RestartPrintSpooler`\n"
                "• User: `SYSTEM`  ✅ Run with highest privileges\n"
                "• Action: `cmd.exe /c net stop spooler & net start spooler`\n"
                "• Trigger: none (on demand only)"
            )
            return

        # Wait for the task to complete (net stop + net start takes ~3-5s)
        log.info("[SPOOLER] Task triggered, waiting 8s for completion...")
        time.sleep(8)

        # Query last run result
        query = subprocess.run(
            ["schtasks", "/query", "/tn", TASK_NAME, "/fo", "LIST", "/v"],
            capture_output=True, text=True, timeout=15
        )
        query_out = (query.stdout + query.stderr).strip()
        log.info("[SPOOLER] /query rc=%d out=%r", query.returncode, query_out)

        # Extract "Last Result" line (schtasks uses "Last Result", not "Last Run Result")
        last_result = ""
        for line in query_out.splitlines():
            if "last result" in line.lower():
                last_result = line.strip()
                break

        log.info("[SPOOLER] %s", last_result)

        # Task Scheduler reports 0 (decimal) for success, not 0x0
        result_val = last_result.split(":")[-1].strip() if ":" in last_result else ""
        if result_val in ("0", "0x0"):
            tg_send_message(chat_id,
                "✅ *Print Spooler restarted successfully*\n"
                "`" + last_result + "`"
            )
        else:
            tg_send_message(chat_id,
                "⚠️ *Spooler task ran but check result*\n"
                "`" + (last_result or "Could not read Last Result") + "`\n\n"
                "```\n" + query_out[:800] + "\n```"
            )

    except subprocess.TimeoutExpired:
        log.error("[SPOOLER] schtasks timed out")
        tg_send_message(chat_id, "❌ Timed out waiting for spooler task.")
    except Exception as e:
        log.error("[SPOOLER] %s", e, exc_info=True)
        tg_send_message(chat_id, "❌ Error: " + str(e))


def dispatch_command(text, chat_id):
    cmd = text.strip().lower().split()[0] if text.strip() else ""
    if cmd == "/wcr":
        threading.Thread(target=handle_wcr, args=(chat_id,), daemon=True).start()
    elif cmd == "/tfr":
        threading.Thread(target=handle_tfr, args=(chat_id,), daemon=True).start()
    elif cmd == "/all":
        threading.Thread(target=handle_all, args=(chat_id,), daemon=True).start()
    elif cmd == "/all_no_whatsapp":
        threading.Thread(target=handle_all, args=(chat_id, True), daemon=True).start()
    elif cmd == "/restart_margerp":
        threading.Thread(target=handle_restart_margerp, args=(chat_id,), daemon=True).start()
    elif cmd == "/restart_printer_spooler":
        threading.Thread(target=handle_restart_printer_spooler, args=(chat_id,), daemon=True).start()
    elif cmd == "/party":
        # Support inline party name: /party revive  or  /party shri sai medical
        inline_name = text.strip()[len("/party"):].strip()
        threading.Thread(target=handle_party, args=(chat_id, inline_name), daemon=True).start()
    elif cmd == "/backup":
        threading.Thread(target=handle_backup, args=(chat_id,), daemon=True).start()
    elif cmd in ("/help", "/start"):
        tg_send_message(chat_id, help_text())
    else:
        tg_send_message(chat_id,
            "❓ Unknown: `" + text + "`\nSend /help to see available commands."
        )


def process_updates(updates):
    global _last_update_id
    for update in updates:
        _last_update_id = update.get("update_id", 0) + 1
        msg      = update.get("message", {})
        if not msg:
            continue
        chat_id  = str(msg.get("chat", {}).get("id", ""))
        text     = msg.get("text", "").strip()
        username = msg.get("from", {}).get("username", "?")
        log.info("Msg from %s (@%s): %s", chat_id, username, text)
        if chat_id != ALLOWED_CHAT_ID:
            tg_send_message(chat_id, "🚫 Unauthorised.")
            continue
        if text.startswith("/"):
            cmd_lower = text.strip().lower()
            # Commands that belong to the party state machine
            _party_cmds = {"/cancel"}
            if _party_state is not None and cmd_lower in _party_cmds:
                threading.Thread(
                    target=handle_party_input, args=(chat_id, text), daemon=True
                ).start()
            elif _backup_state is not None and cmd_lower in ("/cancel", "/no"):
                threading.Thread(
                    target=handle_backup_input, args=(chat_id, "no"), daemon=True
                ).start()
            else:
                dispatch_command(text, chat_id)
        elif _party_state is not None:
            threading.Thread(
                target=handle_party_input, args=(chat_id, text), daemon=True
            ).start()
        elif _backup_state is not None:
            threading.Thread(
                target=handle_backup_input, args=(chat_id, text), daemon=True
            ).start()


# ──────────────────────────────────────────────────────────────
# MAIN LOOP
# ──────────────────────────────────────────────────────────────


# ──────────────────────────────────────────────────────────────
# STARTUP VALIDATION
# ──────────────────────────────────────────────────────────────

def _validate_config() -> None:
    """
    Strict startup validation of all required configuration values.
    Runs before the main loop. On any failure:
      - Logs a specific error message
      - Attempts to send a Telegram notification
      - Exits the process immediately (fail-fast)

    Checks:
      - Required keys are present and non-empty
      - Numeric keys are within sane bounds
      - Directory paths are accessible or creatable
    """
    errors = []

    # ── Required: non-empty strings ───────────────────────────────────────────
    required_nonempty = [
        ("telegram_bot_token", "telegram.bot_token"),
        ("telegram_chat_id",   "telegram.chat_id"),
        ("log_file",           "files.log_file"),
        ("screenshot_dir",     "files.screenshot_dir"),
    ]
    for key, ini_path in required_nonempty:
        val = CONFIG.get(key, "")
        if not val or not str(val).strip():
            errors.append(f"[{ini_path}] is required but missing or empty")

    # ── GCV API key: required for OCR ─────────────────────────────────────────
    if not CONFIG.get("gcv_api_key", "").strip():
        errors.append(
            "[google.vision_api_key] is required for OCR but not set. "
            "Get a key at https://console.cloud.google.com → Vision API → Credentials"
        )

    # ── Marg folder ID: required for /party ledger PDF ────────────────────────
    if not CONFIG.get("marg_folder_id", "").strip():
        # Warn only — bot can run without it but /party PDF will fail
        log.warning("[VALIDATE] [party.marg_folder_id] not set — /party ledger PDF will fail")

    # ── Numeric sanity checks ─────────────────────────────────────────────────
    numeric_bounds = [
        ("telegram_poll_interval", 1,   60,    "telegram.poll_interval"),
        ("delay_enter_select",     0.1, 10.0,  "delays.enter_select"),
        ("delay_after_filter",     0.1, 10.0,  "delays.after_filter"),
        ("delay_before_capture",   0.1, 10.0,  "delays.before_capture"),
        ("delay_after_esc",        0.1, 10.0,  "delays.after_esc"),
        ("backup_wait_time",       5,   300,   "delays.backup_wait_time"),
        ("bill_alert_threshold",   1,   9999,  "alerts.bill_alert_threshold"),
        ("undelivered_check_days", 0,   30,    "alerts.undelivered_check_days"),
        ("health_ping_minute",     0,   59,    "schedule.health_ping_minute"),
        ("max_logs_mb",            1.0, 500.0, "files.max_logs_mb"),
        ("max_screenshots_mb",     1.0, 500.0, "files.max_screenshots_mb"),
        ("max_ledger_temp_mb",     1.0, 2000.0,"files.max_ledger_temp_mb"),
    ]
    for key, lo, hi, ini_path in numeric_bounds:
        val = CONFIG.get(key)
        if val is None:
            errors.append(f"[{ini_path}] is missing")
        elif not (lo <= val <= hi):
            errors.append(f"[{ini_path}] = {val} is out of valid range [{lo}, {hi}]")

    # ── Time format checks (HH:MM) ────────────────────────────────────────────
    import re as _re
    time_keys = [
        ("daily_report_time",   "schedule.daily_report_time"),
        ("marg_restart_time",   "schedule.marg_restart_time"),
        ("backup_schedule_time","schedule.backup_schedule_time"),
    ]
    for key, ini_path in time_keys:
        val = CONFIG.get(key, "")
        if not _re.match(r"^\d{2}:\d{2}$", val):
            errors.append(f"[{ini_path}] = {val!r} must be HH:MM format (e.g. 07:50)")

    # ── Directory writability ─────────────────────────────────────────────────
    dir_keys = [
        ("screenshot_dir", "files.screenshot_dir"),
    ]
    for key, ini_path in dir_keys:
        path = CONFIG.get(key, "")
        if path:
            try:
                os.makedirs(path, exist_ok=True)
                test = os.path.join(path, ".write_test")
                with open(test, "w") as _f:
                    _f.write("ok")
                os.remove(test)
            except Exception as e:
                errors.append(f"[{ini_path}] = {path!r} is not writable: {e}")

    # Log directory
    log_dir = os.path.dirname(CONFIG.get("log_file", ""))
    if log_dir:
        try:
            os.makedirs(log_dir, exist_ok=True)
        except Exception as e:
            errors.append(f"[files.log_file] directory not creatable: {e}")

    # ── Report results ────────────────────────────────────────────────────────
    if not errors:
        log.info("[VALIDATE] All configuration checks passed.")
        return

    # Build the failure message
    msg_lines = ["❌ *Bot startup failed — configuration errors:*\n"]
    for i, err in enumerate(errors, 1):
        log.error("[VALIDATE] #%d: %s", i, err)
        msg_lines.append(f"{i}. {err}")
    full_msg = "\n".join(msg_lines)

    # Try to send Telegram notification before exiting
    token = CONFIG.get("telegram_bot_token", "")
    chat  = CONFIG.get("telegram_chat_id",   "")
    if token and chat:
        try:
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat, "text": full_msg, "parse_mode": "Markdown"},
                timeout=10,
            )
            log.info("[VALIDATE] Failure notification sent to Telegram")
        except Exception as e:
            log.warning("[VALIDATE] Could not send Telegram notification: %s", e)

    log.error("[VALIDATE] %d error(s) found — terminating.", len(errors))
    sys.exit(1)


def main():
    global _last_update_id

    # ── Validate config before anything else ─────────────────────────────
    _validate_config()

    # ── Startup pruning — run once before main loop ───────────────────────
    _prune_dir(os.path.dirname(CONFIG["log_file"]),
               CONFIG["max_logs_mb"], "log")
    _prune_dir(CONFIG["screenshot_dir"],
               CONFIG["max_screenshots_mb"], "screenshot")
    _prune_dir(LEDGER_TEMP_DIR,
               CONFIG["max_ledger_temp_mb"], "ledger PDF")

    log.info("=" * 60)
    log.info("Marg ERP Monitor Bot starting")
    log.info("Chat ID      : %s", ALLOWED_CHAT_ID)
    log.info("Daily report : %s", CONFIG["daily_report_time"])
    log.info("Auto-restart : %s", CONFIG["marg_restart_time"])
    log.info("WCR region   : %s", "calibrated" if WCR_REGION_CALIBRATED else "NOT calibrated")
    log.info("TFR region   : %s", "calibrated" if TFR_REGION_CALIBRATED else "NOT calibrated")
    log.info("=" * 60)

    _init_schedulers()   # pre-mark past schedules so they don't fire on startup

    wa_status  = "✅ enabled"  if whatsapp.WHATSAPP_ENABLED else "⚠️ disabled"
    cal_wcr    = "✅ calibrated" if WCR_REGION_CALIBRATED   else "⚠️ not calibrated"
    cal_tfr    = "✅ calibrated" if TFR_REGION_CALIBRATED   else "⚠️ not calibrated"
    cal_party  = "✅ calibrated" if PARTY_REGION_CALIBRATED else "⚠️ not calibrated"

    startup = (
        "🟢 *Marg ERP Bot is online*\n"
        "─────────────────────────\n\n"

        "🔍 *Party Ledger Search*\n"
        "/party — Search party ledger by name\n"
        "  _name → pick entry → /ledger\\_pdf or /outstanding\\_pdf_\n\n"

        "📦 *Bill Reports*\n"
        "/wcr — WCR undelivered bills\n"
        "/tfr — TFR Gangapur transfers\n"
        "/all — Full report: WCR + TFR\n"
        "/all\\_no\\_whatsapp — Telegram only\n\n"

        "🔄 *Marg ERP Control*\n"
        "/restart\\_margerp — Restart & log in\n"
        "/backup — Run full Marg ERP backup\n\n"

        "🖨️ *Printer*\n"
        "/restart\\_printer\\_spooler — Restart Print Spooler\n\n"

        "ℹ️ /help — Full status & command list\n\n"

        "─────────────────────────\n"
        "🕗 Daily report:    *" + CONFIG["daily_report_time"] + "*\n"
        "🔄 Auto-restart:    *" + CONFIG["marg_restart_time"] + "*\n"
        "💾 Daily backup:    *" + CONFIG["backup_schedule_time"] + "*\n"
        "💓 Health ping:     every hour at :*" + str(CONFIG["health_ping_minute"]).zfill(2) + "*\n"
        "🔔 Bill alert:      ≥ *" + str(CONFIG["bill_alert_threshold"]) + "* pending\n"
        "📲 WhatsApp:        " + wa_status + "\n"
        "📐 WCR region:      " + cal_wcr + "\n"
        "📐 TFR region:      " + cal_tfr + "\n"
        "📐 Party region:    " + cal_party
    )
    if not WCR_REGION_CALIBRATED:
        startup += "\n\n⚠️ Run `python calibrate.py wcr` to calibrate WCR region"
    if not TFR_REGION_CALIBRATED:
        startup += "\n⚠️ Run `python calibrate.py tfr` to calibrate TFR region"
    if not PARTY_REGION_CALIBRATED:
        startup += "\n⚠️ Run `python calibrate.py party` to calibrate Party region"
    tg_send_message(ALLOWED_CHAT_ID, startup)

    while True:
        try:
            updates = tg_get_updates(offset=_last_update_id, timeout=2)
            if updates:
                process_updates(updates)
            check_daily_report()
            check_daily_restart()
            check_scheduled_backup()
            check_health_ping()
        except Exception as e:
            log.error("Main loop error: %s", e)
        time.sleep(CONFIG["telegram_poll_interval"])


if __name__ == "__main__":
    main()
