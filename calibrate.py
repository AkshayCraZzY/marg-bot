"""
calibrate.py
============
Unified screen region calibration tool for Marg ERP Monitor Bot.
Replaces regions_wcr.py, regions_tfr.py, and regions_party.py.

Reads/writes region coordinates directly to config.ini.

Usage:
    python calibrate.py wcr        Calibrate WCR undelivered bills region
    python calibrate.py tfr        Calibrate TFR Gangapur transfers region
    python calibrate.py party      Calibrate Party ledger search region
    python calibrate.py all        Calibrate all three in sequence
    python calibrate.py status     Show current calibration status

After running, open the saved screenshot, find the four corners of the
data area, then enter the coordinates when prompted.
The tool writes them directly into config.ini — no manual editing needed.
"""

import sys
import os
import time
import datetime
import configparser

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH  = os.path.join(SCRIPT_DIR, "config.ini")
SCREENSHOT_DIR = r"C:\MargMonitor\screenshots"

REGIONS = {
    "wcr": {
        "section":     "region_wcr",
        "label":       "WCR Undelivered Bills",
        "instruction": "Navigate to the WCR undelivered bills screen in Marg ERP.",
        "prefix":      "wcr_calibrate",
        "grid_color":  (220, 0, 0),    # red
    },
    "tfr": {
        "section":     "region_tfr",
        "label":       "TFR Gangapur Transfers",
        "instruction": "Navigate to the TFR Gangapur bill list screen in Marg ERP.",
        "prefix":      "tfr_calibrate",
        "grid_color":  (0, 0, 220),    # blue
    },
    "party": {
        "section":     "region_party",
        "label":       "Party Ledger Search Results",
        "instruction": (
            "Navigate Marg ERP to the party search results screen.\n"
            "   (Main UI → Down Arrow x4 → Enter → type a party name)"
        ),
        "prefix":      "party_calibrate",
        "grid_color":  (0, 160, 0),    # green
    },
}


def load_config() -> configparser.ConfigParser:
    ini = configparser.ConfigParser()
    ini.read(CONFIG_PATH, encoding="utf-8")
    return ini


def save_config(ini: configparser.ConfigParser):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        ini.write(f)


def take_screenshot():
    """Capture full screen via mss (calibration is run while RDP is connected)."""
    import mss
    from PIL import Image
    with mss.mss() as sct:
        raw = sct.grab(sct.monitors[1])
    return Image.frombytes("RGB", (raw.width, raw.height), raw.bgra, "raw", "BGRX")


def draw_grid(img, color):
    """Overlay a coordinate grid on the image for easy pixel identification."""
    from PIL import ImageDraw
    draw = ImageDraw.Draw(img)
    for x in range(0, img.width, 100):
        draw.line([(x, 0), (x, img.height)], fill=color, width=1)
        draw.text((x + 2, 4), str(x), fill=color)
    for y in range(0, img.height, 100):
        draw.line([(0, y), (img.width, y)], fill=color, width=1)
        draw.text((4, y + 2), str(y), fill=color)
    return img


def save_screenshot(img, prefix) -> str:
    os.makedirs(SCREENSHOT_DIR, exist_ok=True)
    ts   = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(SCREENSHOT_DIR, f"{prefix}_{ts}.png")
    img.save(path)
    return path


def get_int(prompt, default=None):
    while True:
        raw = input(prompt).strip()
        if raw == "" and default is not None:
            return default
        try:
            return int(raw)
        except ValueError:
            print("  Please enter a whole number.")


def calibrate_region(key: str):
    """Run the full calibration flow for one region and write to config.ini."""
    cfg_entry = REGIONS[key]
    section   = cfg_entry["section"]
    label     = cfg_entry["label"]

    print()
    print(f"━━━  Calibrating: {label}  ━━━")
    print(f"1. {cfg_entry['instruction']}")
    print("2. Press ENTER here — screenshot taken after 2-second countdown.")
    input()

    for i in (2, 1):
        print(f"  {i}..."); time.sleep(1)

    print("  Taking screenshot...")
    img = take_screenshot()
    img = draw_grid(img.copy(), cfg_entry["grid_color"])
    path = save_screenshot(img, cfg_entry["prefix"])

    print(f"\n  Screen   : {img.width} x {img.height}")
    print(f"  Saved    : {path}")
    print("\n  Open the image and find the four pixel coordinates of the data area.")
    print("  The grid lines are every 100px — use them to estimate coordinates.")
    print()

    left   = get_int("  left   (X of left edge)  : ")
    top    = get_int("  top    (Y of top edge)   : ")
    right  = get_int("  right  (X of right edge) : ")
    bottom = get_int("  bottom (Y of bot edge)   : ")

    # Save preview crop
    from PIL import Image as PILImage
    original = take_screenshot()   # clean version without grid
    # Actually use the grid image for preview (user already has it open)
    preview = img.crop((left, top, right, bottom))
    preview_path = save_screenshot(preview, cfg_entry["prefix"] + "_preview")
    print(f"\n  Preview  : {preview_path}")

    # Write to config.ini
    ini = load_config()
    if not ini.has_section(section):
        ini.add_section(section)
    ini.set(section, "calibrated", "true")
    ini.set(section, "left",       str(left))
    ini.set(section, "top",        str(top))
    ini.set(section, "right",      str(right))
    ini.set(section, "bottom",     str(bottom))
    save_config(ini)

    print(f"\n  ✅ [{section}] written to config.ini")
    print(f"     calibrated = true")
    print(f"     left={left}  top={top}  right={right}  bottom={bottom}")


def show_status():
    """Print current calibration status for all regions."""
    ini = load_config()
    print()
    print("Current region calibration status:")
    print(f"{'Region':<12}  {'Calibrated':<12}  Coordinates")
    print("─" * 60)
    for key, cfg_entry in REGIONS.items():
        section = cfg_entry["section"]
        if ini.has_section(section):
            cal   = ini.getboolean(section, "calibrated", fallback=False)
            left  = ini.getint(section, "left",   fallback=0)
            top   = ini.getint(section, "top",    fallback=0)
            right = ini.getint(section, "right",  fallback=1920)
            bot   = ini.getint(section, "bottom", fallback=1080)
            status = "✅ YES" if cal else "⚠️  NO"
            coords = f"L={left} T={top} R={right} B={bot}"
        else:
            status = "⚠️  NO (section missing)"
            coords = "—"
        print(f"{key:<12}  {status:<12}  {coords}")
    print()


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if not os.path.exists(CONFIG_PATH):
        print(f"ERROR: config.ini not found at {CONFIG_PATH}")
        print("Place calibrate.py in the same folder as config.ini.")
        sys.exit(1)

    if len(sys.argv) < 2:
        print(__doc__)
        print("Usage: python calibrate.py <wcr|tfr|party|all|status>")
        sys.exit(0)

    cmd = sys.argv[1].lower()

    if cmd == "status":
        show_status()
    elif cmd == "all":
        for key in REGIONS:
            calibrate_region(key)
        show_status()
    elif cmd in REGIONS:
        calibrate_region(cmd)
        show_status()
    else:
        print(f"Unknown target: {cmd!r}")
        print("Valid options: wcr, tfr, party, all, status")
        sys.exit(1)
