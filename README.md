<div align="center">

<br/>

```
███╗   ███╗ █████╗ ██████╗  ██████╗     ██████╗  ██████╗ ████████╗
████╗ ████║██╔══██╗██╔══██╗██╔════╝     ██╔══██╗██╔═══██╗╚══██╔══╝
██╔████╔██║███████║██████╔╝██║  ███╗    ██████╔╝██║   ██║   ██║   
██║╚██╔╝██║██╔══██║██╔══██╗██║   ██║    ██╔══██╗██║   ██║   ██║   
██║ ╚═╝ ██║██║  ██║██║  ██║╚██████╔╝    ██████╔╝╚██████╔╝   ██║   
╚═╝     ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝     ╚═════╝  ╚═════╝    ╚═╝  
```

### Your ERP. Your Phone. Zero Compromise.

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Platform](https://img.shields.io/badge/Windows%20Server-2022-0078D6?style=for-the-badge&logo=windows&logoColor=white)]()
[![Telegram](https://img.shields.io/badge/Telegram-Bot%20API-26A5E4?style=for-the-badge&logo=telegram&logoColor=white)](https://core.telegram.org/bots)
[![WhatsApp](https://img.shields.io/badge/WhatsApp-Green%20API-25D366?style=for-the-badge&logo=whatsapp&logoColor=white)](https://green-api.com)
[![GCV](https://img.shields.io/badge/OCR-Google%20Vision-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)](https://cloud.google.com/vision)

[![Status](https://img.shields.io/badge/Status-Production%20Ready-brightgreen?style=flat-square)]()
[![License](https://img.shields.io/badge/License-MIT-blue?style=flat-square)]()
[![Architecture](https://img.shields.io/badge/Architecture-Headless%20Win32-orange?style=flat-square)]()
[![Automation](https://img.shields.io/badge/Automation-PostMessage%20Only-red?style=flat-square)]()

<br/>

> **A production-grade Telegram bot that fully automates Marg ERP — undelivered bills, party ledgers,
> PDF generation, backups, and system control — operating completely headlessly on Windows Server
> with zero RDP session or screen required.**

<br/>

[📦 Features](#-features) · [⚡ Quick Start](#-quick-start) · [🔧 Configuration](#-configuration-reference) · [💬 Commands](#-commands) · [🐛 Debugging](#-debugging-tools) · [❓ FAQ](#-faq)

---

</div>

## 🧠 What Problem Does This Solve?

Managing a **Marg ERP installation** on a Windows Server typically means:

- Physically accessing the server or maintaining an RDP session
- Manually navigating through VB6 screens to pull bill reports
- Downloading and forwarding PDFs by hand
- Hoping someone remembers the daily backup

**Marg Bot eliminates all of that.** It runs as a background Windows Service, accepts commands over Telegram from any device anywhere in the world, and drives the Marg ERP application through Win32 message APIs — the same channel Windows itself uses, making it immune to session disconnection.

Send `/wcr` from your phone. Get a formatted bill table in 8 seconds. Done.

---

## ✨ Features

<table>
<thead>
<tr><th width="50%">📦 Bill Intelligence</th><th width="50%">📄 Party Ledger Engine</th></tr>
</thead>
<tbody>
<tr>
<td>

- **WCR + WCA series** captured in one pass (`wc` filter)
- **Multi-date lookback** — today + N previous working days
- **Paginated OCR** — scrolls past the 14-entry screen limit and deduplicates automatically
- **Formatted tables** in Telegram monospace code blocks
- **High-pending alerts** to WhatsApp when threshold exceeded
- **Sunday skip** configurable per task

</td>
<td>

- **Fuzzy name search** — partial match navigates the full ledger list
- **Interactive selection** — screenshot → pick entry number
- **Dual PDF modes** — Ledger PDF or Outstanding Report
- **Auto-forwarded** to individual WhatsApp contact (separate from group)
- **Deterministic filenames** — `revive_outstanding_08-04-26_133855.pdf`
- **Windows Save As automation** via `WM_SETTEXT` + `BM_CLICK` (headless-safe)

</td>
</tr>
</tbody>
<thead>
<tr><th>⚙️ ERP System Control</th><th>🏗️ Infrastructure & Reliability</th></tr>
</thead>
<tbody>
<tr>
<td>

- **Remote restart** — closes Marg ERP, relaunches via shortcut, auto-logs in
- **Backup automation** — full sequence with configurable wait + Telegram progress
- **Scheduled backup** — fires at configured time, lock-protected against concurrency
- **Print Spooler restart** — via Windows Task Scheduler (no elevation needed)
- **All credentials** in `config.ini` — no hardcoded values

</td>
<td>

- **Windows Service** via NSSM — auto-start on boot, auto-restart on crash
- **Strict startup validation** — fail-fast with specific Telegram notification on bad config
- **Size-based pruning** — logs, screenshots, PDFs all auto-cleaned oldest-first
- **Rotating log** with configurable MB limit
- **Hourly health ping** — Telegram confirmation the bot is alive
- **Single automation lock** — concurrent commands queued, never collide

</td>
</tr>
</tbody>
</table>

---

## 🛠 Tech Stack

| Layer | Technology | Why |
|---|---|---|
| **Language** | Python 3.11+ | Mature `pywin32` bindings, stdlib threading |
| **ERP Automation** | `pywin32` — `PostMessage` / `SendMessage` | Headless-safe; works without desktop session |
| **OCR** | Google Cloud Vision API (`DOCUMENT_TEXT_DETECTION`) | Superior accuracy on colored VB6 UI vs Tesseract |
| **Messaging** | Telegram Bot API (long-polling) | Zero infrastructure — Telegram handles routing |
| **WhatsApp** | Green API `sendFileByUpload` | PDF delivery as `application/pdf` document |
| **Screen Capture** | `PrintWindow(flag=2)` + Pillow | Works headlessly without BitBlt |
| **Service Host** | NSSM (Non-Sucking Service Manager) | Reliable Windows Service wrapping for Python |
| **Config** | `configparser` — `config.ini` | Single source of truth, no env var fragmentation |
| **OS Target** | Windows Server 2022 | Marg ERP VB6 runtime requirement |

---

## 📁 Project Structure

```
marg-erp-bot/
│
├── 🤖 Core
│   ├── marg_agent.py          # Main bot — Telegram polling, dispatch, all automation
│   ├── ocr_wcr.py             # Google Vision OCR pipeline + WCR/WCA bill parser
│   ├── whatsapp.py            # Green API WhatsApp integration (text, image, PDF)
│   ├── calibrate.py           # Interactive screen region calibration tool
│   └── config.ini             # All configuration — single source of truth
│
├── 🔬 Testers & Debuggers (manual use during setup)
│   ├── f5_tester.py           # Test F5 keypress methods A–H against live Marg ERP
│   ├── gcv_ocr_test.py        # Standalone GCV OCR tester with optional Tesseract compare
│   ├── click_tester.py        # Headless click method tester (methods A–Q)
│   ├── key_combo_tester.py    # Keyboard shortcut tester
│   ├── arrow_test.py          # Arrow key send method tester
│   ├── find_coords.py         # Mouse coordinate finder (hover + print)
│   └── vb6_control_finder.py  # VB6 child window enumeration tool
│
├── 🚀 Service
│   └── install_service.bat    # One-shot NSSM installer (run as Administrator)
│
└── 📂 Runtime Output (auto-created)
    └── C:\MargMonitor\
        ├── marg_agent.log     # Rotating log (size-limited, configurable)
        ├── screenshots\       # Automation debug screenshots (auto-pruned)
        └── ledger_temp\       # Outstanding PDF storage (auto-pruned)
```

---

## ⚡ Quick Start

### Prerequisites

Ensure the following are available before beginning:

| Requirement | Notes |
|---|---|
| Windows Server 2022 | With Marg ERP installed and running |
| Python 3.11+ | [python.org/downloads](https://www.python.org/downloads/) |
| NSSM | [nssm.cc/download](https://nssm.cc/download) — for service installation |
| Telegram Bot Token | From [@BotFather](https://t.me/BotFather) → `/newbot` |
| Google Cloud Vision API Key | [console.cloud.google.com](https://console.cloud.google.com) → Enable *Cloud Vision API* → Create API Key |
| Green API account | Optional — [app.green-api.com](https://app.green-api.com) for WhatsApp |

---

### Step 1 — Install

```bat
git clone https://github.com/yourorg/marg-erp-bot.git C:\MargMonitor\bot
cd C:\MargMonitor\bot
pip install -r requirements.txt
```

---

### Step 2 — Configure

Open `config.ini` and fill in at minimum these required values:

```ini
[telegram]
bot_token           = 7xxxxxxxxx:AAxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
chat_id             = 518725669          # Your Telegram user ID (get from @userinfobot)

[google]
vision_api_key      = AIzaSyXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

[marg]
shortcut            = C:\Users\autobot\Desktop\MARG.lnk
marg_folder_id      = 12345              # Numeric folder ID from C:/Users/Public/MARG/

[files]
log_file            = C:\MargMonitor\marg_agent.log
screenshot_dir      = C:\MargMonitor\screenshots
```

> **The bot validates every required key at startup.** If anything is missing or malformed, it logs the
> specific error, sends a Telegram notification with the failure reason, and exits immediately.
> No silent failures.

---

### Step 3 — Calibrate Screen Regions

With Marg ERP open and visible, calibrate each screen region:

```bat
# Navigate to WCR undelivered bills list first, then:
python calibrate.py wcr

# Navigate to TFR Gangapur screen first, then:
python calibrate.py tfr

# Navigate to party search results screen first, then:
python calibrate.py party

# Verify all regions:
python calibrate.py status
```

Calibration saves pixel coordinates directly to `config.ini`. Re-run if the server resolution changes.

---

### Step 4 — Smoke Test

```bat
python marg_agent.py
```

Open Telegram and send `/help` to your bot. You should receive a full status message with schedule times, WhatsApp status, region calibration status, and the party PDF WhatsApp number.

---

### Step 5 — Install as Windows Service

```bat
# Copy nssm.exe to C:\MargMonitor\bot\ first, then:
install_service.bat
```

> **Run as Administrator.** The service auto-starts on boot and restarts automatically on crash.

```bat
sc start MargSaleBookMonitor     # Start
sc stop  MargSaleBookMonitor     # Stop
sc query MargSaleBookMonitor     # Status
```

---

## 💬 Commands

### Full Command Reference

| Command | Description | Interactive? |
|---|---|---|
| `/wcr` | Undelivered WCR + WCA bills — today + N previous working days with paginated OCR | No |
| `/tfr` | TFR Gangapur transfers screenshot | No |
| `/all` | Full combined report — WCR + TFR | No |
| `/all_no_whatsapp` | Full report — Telegram only, suppresses WhatsApp | No |
| `/party [name]` | Party ledger search, PDF generation | **Yes — 3 steps** |
| `/backup` | Marg ERP backup with confirmation prompt | **Yes — confirm** |
| `/restart_margerp` | Close Marg ERP, relaunch shortcut, auto-login | No |
| `/restart_printer_spooler` | Restart Windows Print Spooler via Task Scheduler | No |
| `/help` | Full status panel — schedules, config values, calibration state | No |

---

### `/party` — Interactive Flow

```
You  →  /party revive

Bot  →  📸 [search results screenshot]
        Reply with the number of the entry:
        0 = first (already selected)
        1 = second  ...

You  →  0

Bot  →  📸 [ledger opened screenshot]
        📂 Select output type:
        1 — 📄 Ledger PDF
        2 — 📊 Outstanding PDF
        Reply with 1 or 2, or /cancel

You  →  1

Bot  →  📄 Generating PDF...
        ✅ PDF sent.   ← Telegram document
                       ← Also forwarded to WhatsApp (if party_pdf_chat_id set)
```

### `/backup` — Confirmation Flow

```
You  →  /backup

Bot  →  💾 Marg ERP Backup
        Estimated time: ~55s
        Reply yes to proceed or no to cancel.

You  →  yes

Bot  →  💾 Starting backup sequence...
        📸 [mid-backup screenshot]  ← confirms it's running
        ⏳ Waiting 25s...
        📸 [completion screenshot]
        ✅ Backup complete.
```

---

## 🔧 Configuration Reference

All configuration is in a single `config.ini`. Sections are organized by concern:

```ini
# ── Telegram ──────────────────────────────────────────────────────────────────
[telegram]
bot_token           = YOUR_BOT_TOKEN
chat_id             = YOUR_CHAT_ID         # Only this ID can issue commands
poll_interval       = 3                    # Seconds between update polls (1–60)

# ── WhatsApp (Green API) ──────────────────────────────────────────────────────
[whatsapp]
enabled             = false
id_instance         = YOUR_ID_INSTANCE
token_instance      = YOUR_TOKEN_INSTANCE
group_id            = 120363XXXXXXXXXX@g.us   # WCR/TFR group chat
party_pdf_chat_id   =                         # Individual chat for /party PDFs
                                              # Format: 919876543210@c.us

# ── Google Cloud Vision ───────────────────────────────────────────────────────
[google]
vision_api_key      =                      # Required for OCR — free tier: 1000/month

# ── Marg ERP ─────────────────────────────────────────────────────────────────
[marg]
window_title        = MARG ERP
shortcut            = C:\Users\autobot\Desktop\MARG.lnk
login_user          = BOT
login_password      = BOT
login_wait_s        = 15                   # Seconds to wait after launching Marg ERP
marg_folder_id      =                      # Required for /party ledger PDF

# ── Navigation Strings ────────────────────────────────────────────────────────
[marg_nav]
bill_filter_prefix  = wc                   # Shows both WCR and WCA series
tfr_store_filter    = store                # Typed during TFR navigation
tfr_branch_filter   = ganga               # Branch name for Gangapur TFR

# ── Delays (seconds) ─────────────────────────────────────────────────────────
[delays]
enter_select        = 1.5                  # After pressing Enter to open a screen
after_filter        = 2.0                  # After typing a filter string
before_capture      = 1.5                  # Before taking a screenshot
after_esc           = 1.5                  # After pressing Escape
backup_wait_time    = 25                   # Wait during backup execution

# ── Schedule ─────────────────────────────────────────────────────────────────
[schedule]
daily_report_time       = 20:00            # Automated WCR+TFR report (HH:MM)
marg_restart_time       = 07:45            # Daily Marg ERP restart
backup_schedule_time    = 07:50            # Daily automated backup
health_ping_minute      = 0               # Minute past each hour for alive ping
report_skip_sunday      = true            # Skip Sunday for daily report
restart_skip_sunday     = false           # Run restart even on Sunday
backup_skip_sunday      = true            # Skip Sunday for backup

# ── Alerts ───────────────────────────────────────────────────────────────────
[alerts]
bill_alert_threshold    = 10              # WhatsApp alert if pending bills >= this
undelivered_check_days  = 2               # Today + N previous working days
wcr_page_limit          = 14             # Triggers second OCR pass at page boundary

# ── Files & Storage ──────────────────────────────────────────────────────────
[files]
log_file            = C:\MargMonitor\marg_agent.log
screenshot_dir      = C:\MargMonitor\screenshots
ledger_temp_dir     = C:\MargMonitor\ledger_temp
max_logs_mb         = 10                  # Oldest log files deleted beyond this
max_screenshots_mb  = 50                  # Oldest screenshots deleted beyond this
max_ledger_temp_mb  = 100                 # Oldest PDFs deleted beyond this

# ── Screen Capture Regions (set by calibrate.py) ──────────────────────────────
[region_wcr]
calibrated = false
left = 0 / top = 0 / right = 1920 / bottom = 1080

[region_tfr]
calibrated = false
left = 0 / top = 0 / right = 1920 / bottom = 1080

[region_party]
calibrated = false
left = 0 / top = 0 / right = 1920 / bottom = 1080
```

---

## 🏗 Service Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   Windows Server 2022                    │
│                                                          │
│  ┌─────────────────┐      ┌──────────────────────────┐  │
│  │   Marg ERP      │◄─────│   marg_agent.py          │  │
│  │  (VB6 Process)  │      │   (Windows Service)       │  │
│  │                 │      │                           │  │
│  │  WM_KEYDOWN     │      │  PostMessage / SendMsg    │  │
│  │  WM_CHAR        │◄─────│  PrintWindow(flag=2)      │  │
│  │  WM_SETTEXT     │      │  EnumWindows              │  │
│  └─────────────────┘      └──────────┬────────────────┘  │
│                                       │                   │
└───────────────────────────────────────┼───────────────────┘
                                        │  HTTPS
              ┌─────────────────────────┤
              ▼                         ▼
      ┌───────────────┐        ┌────────────────┐
      │  Telegram API │        │  Green API     │
      │  Bot polling  │        │  WhatsApp      │
      └───────┬───────┘        └────────┬───────┘
              │                          │
              ▼                          ▼
         👤 Operator                👤 Recipient
         (any device)              (WhatsApp contact)
```

**Key design constraint:** Every Win32 call uses `PostMessage` or `SendMessage` — never `SetForegroundWindow`, `keybd_event` (except as fallback), or `SendInput`. This ensures the bot works in a service context without a desktop session.

---

## 🐛 Debugging Tools

Each automation primitive has a dedicated tester. Run these against a live Marg ERP window during setup or when something stops working:

```bat
# F5 key — use when outstanding PDF generation fails at Step 1
# Tests 8 different send methods and reports which one works
python f5_tester.py                    # Run all headless-safe methods
python f5_tester.py --method B         # Test specific method only
python f5_tester.py --delay 3          # Slow mode — easier to observe

# Google Vision OCR — test your API key and parse accuracy on real screenshots
python gcv_ocr_test.py screenshot.png --api-key YOUR_KEY
python gcv_ocr_test.py screenshot.png --compare  # Side-by-side vs Tesseract
python gcv_ocr_test.py screenshot.png --debug    # Show block/confidence detail
python gcv_ocr_test.py screenshot.png --dump-json # Save raw API response

# UI coordinate discovery
python find_coords.py                  # Hover over any UI element to print coords

# Click method tester
python click_tester.py                 # Tests A–Q click methods, reports which work

# VB6 window structure
python vb6_control_finder.py           # Enumerate all child windows and classes
```

---

## ❓ FAQ

<details>
<summary><strong>The bot starts but all screenshots are blank.</strong></summary>

The screen capture region is not calibrated, or Marg ERP is iconified (minimised to taskbar). Marg ERP must be in a **non-minimised state** even when running headlessly — it can be off-screen but not iconified.

Run `python calibrate.py wcr` to set the correct pixel region for your screen resolution.

</details>

<details>
<summary><strong>OCR returns 0 bills but the screenshot clearly shows bills.</strong></summary>

1. Verify `vision_api_key` is set in `config.ini`
2. Confirm the Cloud Vision API is **enabled** in your Google Cloud project (not just created)
3. Test in isolation: `python gcv_ocr_test.py the_screenshot.png --api-key YOUR_KEY`
4. Check the log for `GCV API error` lines — quota exceeded or billing not enabled are common causes

</details>

<details>
<summary><strong>Only 14 bills appear even though there are more.</strong></summary>

Pagination is triggered automatically when exactly `wcr_page_limit` (default `14`) bills are returned. If you're seeing exactly 14 and suspect more exist, check the log for `[WCR-OCR] Scrolled:` entries. If absent, lower `wcr_page_limit` to `13` in `config.ini` to force the second pass.

</details>

<details>
<summary><strong>The /party command opens the wrong ledger entry.</strong></summary>

The first entry (index 0) in the list is already selected when the screenshot is taken. Replying `0` navigates with 0 Down Arrow presses and opens the currently highlighted entry. Replying `1` presses Down once to move to the second entry. Count from the **top of the screenshot**, starting at 0.

</details>

<details>
<summary><strong>WhatsApp messages arrive but PDFs do not.</strong></summary>

PDF delivery uses `party_pdf_chat_id` (individual chat) — a separate field from `group_id` (used for WCR/TFR). Ensure `party_pdf_chat_id` is set in `config.ini` in the exact format `919876543210@c.us` — no `+`, no spaces, no country code prefix other than digits.

</details>

<details>
<summary><strong>The service starts but exits immediately.</strong></summary>

A startup validation failure occurred. Check two places:
1. `C:\MargMonitor\marg_agent.log` — every validation error is logged with `[VALIDATE] #N:`
2. The Telegram chat — the bot sends a notification listing all config failures before exiting

Common causes: empty `vision_api_key`, missing `log_file` path, or a schedule time not in `HH:MM` format.

</details>

<details>
<summary><strong>Can the bot handle multiple users simultaneously?</strong></summary>

No — by design. The bot controls a single Marg ERP UI instance. A global `threading.Lock` serialises all automation. If a second command arrives while one is running, the user receives "⏳ Already running a command, please wait." All commands are idempotent and safe to retry.

</details>

<details>
<summary><strong>The backup fires immediately when the bot starts.</strong></summary>

The backup scheduler (`check_scheduled_backup`) does **not** pre-mark itself on startup — it fires at the configured minute every day. If the bot starts at exactly `07:50`, the backup will fire within that minute. This is intentional — the backup lock prevents duplicate runs within the same minute window. The daily report and restart **do** pre-mark on startup to prevent immediate firing.

</details>

---

## 🤝 Contributing

Contributions that improve headless reliability, OCR accuracy, or configuration coverage are welcome.

```bash
# Fork → branch → change → PR
git checkout -b feat/your-improvement

# Commit format
git commit -m "feat: add configurable scroll speed for WCR pagination"
git commit -m "fix: handle PermissionError on locked PDF files > 15s"
git commit -m "docs: document new marg_nav config keys"
```

**Required standards for any PR:**

| Rule | Detail |
|---|---|
| **Headless-safe Win32 only** | `PostMessage` / `SendMessage` exclusively — no `SetForegroundWindow`, no `mouse_event` |
| **Config-driven** | New tunable values go in `config.ini` with a sensible `fallback=` — never hardcoded |
| **Validated at startup** | Required config keys must be added to `_validate_config()` |
| **Tested headlessly** | Verify against Marg ERP with RDP disconnected, not just in a visible session |
| **Single responsibility** | Each automation function does one navigable action and returns cleanly |

---

<div align="center">

<br/>

**Built for headless Windows · Driven by Win32 · Delivered by Telegram**

<br/>

```
If it saves you a trip to the server room, it's doing its job.
```

<br/>

[![Made with Python](https://img.shields.io/badge/Made%20with-Python-3776AB?style=flat-square&logo=python&logoColor=white)]()
[![Runs Headless](https://img.shields.io/badge/Runs-Headless-black?style=flat-square&logo=windows-terminal&logoColor=white)]()
[![Zero RDP Required](https://img.shields.io/badge/Zero-RDP%20Required-success?style=flat-square)]()

</div>
