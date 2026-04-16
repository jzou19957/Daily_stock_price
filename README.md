# Finviz Daily Scraper → Google Drive

Scrapes all 6 Finviz screener views every weekday, auto-detects the trading-day
date, saves `finviz_MASTER_YYYYMMDD.csv` with a `Data_Date` column, and uploads
it to Google Drive — retrying every 15 minutes until it succeeds.

---

## Retry Schedule

| Slot | UTC | ET | Purpose |
|---|---|---|---|
| 1 | 21:05 | 5:05 PM | First attempt |
| 2 | 21:20 | 5:20 PM | Retry 1 |
| 3 | 21:35 | 5:35 PM | Retry 2 |
| 4 | 21:50 | 5:50 PM | Retry 3 |
| 5 | 22:05 | 6:05 PM | Retry 4 |
| 6 | 22:20 | 6:20 PM | Retry 5 — if this fails, a GitHub Issue is opened |

Each slot checks Drive first. If today's file is already there, it exits immediately without scraping again. So retries are free — they only do real work if the previous attempt failed.

---

## File Structure

```
your-repo/
├── finviz_scraper.py          # Scrapes all 6 Finviz views → CSV
├── run_daily.py               # Orchestrator: check Drive → scrape → upload
├── gdrive_upload.py           # Google Drive upload helper
├── date_detective.py          # Auto-detects the data's trading-day date
├── credentials.json           # ← YOUR SERVICE ACCOUNT KEY (never commit this!)
├── .gitignore
└── .github/
    └── workflows/
        └── daily_finviz.yml   # GitHub Actions schedule + retry logic
```

---

## One-Time Setup

### Step 1 — Google Cloud service account

1. Go to [console.cloud.google.com](https://console.cloud.google.com)
2. Create a project → **APIs & Services → Enable APIs → Google Drive API → Enable**
3. **Credentials → Create Credentials → Service Account** → give it any name → Done
4. Click the service account → **Keys → Add Key → JSON** → download
5. Rename the downloaded file to `credentials.json` and place it in the repo root

### Step 2 — Share your Drive folder

1. In Google Drive, right-click your target folder → **Share**
2. Paste the service account email (from `credentials.json`, field `"client_email"`)
3. Set permission to **Editor** → Share
4. Copy your **folder ID** from the URL:
   `https://drive.google.com/drive/folders/`**`1AbCdEfGhIjKlMnOpQrStUvWxYz`** ← this part

### Step 3 — Add GitHub Secrets

Go to your repo → **Settings → Secrets and variables → Actions → New repository secret**

| Secret name | Value |
|---|---|
| `GDRIVE_FOLDER_ID` | Your Drive folder ID string |
| `GDRIVE_CREDENTIALS` | The **entire text content** of `credentials.json` (paste the whole JSON) |

### Step 4 — Add .gitignore

Make sure `credentials.json` is never committed:

```
credentials.json
*.log
.progress/
```

### Step 5 — Create the `scrape-failure` label

Go to **Issues → Labels → New label**, name it `scrape-failure` (red).
This is used by the auto-issue alert when all retries fail.

### Step 6 — Push and you're done

```bash
git add .
git commit -m "Add Finviz daily scraper"
git push
```

The workflow will fire automatically Mon–Fri starting at 5:05 PM ET.

---

## Local Testing

```bash
# Install dependencies
pip install requests beautifulsoup4 lxml google-api-python-client google-auth

# Quick test — 3 pages per view (~60 tickers, ~2 minutes)
python run_daily.py --pages 3 --folder-id YOUR_FOLDER_ID

# Full run (~10,000 tickers, ~45–60 minutes)
python run_daily.py --folder-id YOUR_FOLDER_ID

# Just check if today's file already exists on Drive
python run_daily.py --check-only --folder-id YOUR_FOLDER_ID

# Resume an interrupted run
python run_daily.py --resume --folder-id YOUR_FOLDER_ID
```

---

## What's in the CSV

Output: `finviz_MASTER_YYYYMMDD.csv` — one row per ticker, ~86 columns.

Key columns:

| Column | Source | Notes |
|---|---|---|
| `Ticker` | Overview | Stock symbol |
| `Data_Date` | Auto-detected | YYYY-MM-DD of the trading day |
| `Company` | Overview | Full company name |
| `Sector` / `Industry` | Overview | Classification |
| `Market Cap` | Overview | e.g. `32.63B` |
| `Price` / `Change` / `Volume` | Overview | Day's closing data |
| `Fwd P/E`, `PEG`, `P/S` … | Valuation view | Valuation metrics |
| `Insider Own`, `Inst Own` … | Ownership view | Ownership data |
| `Perf Week` … `Perf 10Y` | Performance view | Return history |
| `ROE`, `ROA`, `Profit M` … | Financial view | Fundamentals |
| `Earnings` | Financial view | e.g. `Apr 16/a` |

### Date detection priority

1. **HTTP `Last-Modified` header** — server timestamp, most reliable
2. **HTML text scan** — looks for "as of April 16, 2026" type strings
3. **Earnings cross-check** — most recent `/a` earnings date = lower bound
4. **Fallback** — last NYSE trading day

---

## Failure Alerts

If all 6 retry slots fail, the workflow automatically opens a GitHub Issue
titled `🚨 Finviz scrape FAILED — YYYY-MM-DD` with a link to the run log
and troubleshooting steps. Since you're watching the repo you'll get an email.

To re-run manually after fixing the problem:
**Actions → Daily Finviz Scrape → Run workflow**
