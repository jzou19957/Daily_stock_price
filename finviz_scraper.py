"""
╔══════════════════════════════════════════════════════════════════════════════╗
║         FINVIZ FULL SCRAPER  —  All Views → Master CSV                     ║
║                                                                              ║
║  Automatically detects the actual trading-day date of the data using:       ║
║    1. HTTP Last-Modified / Date response headers  (most reliable)           ║
║    2. HTML page text  ("as of …" markers)                                   ║
║    3. Earnings date cross-check  (lower-bound pin from /a entries)          ║
║    4. Fallback: last NYSE trading day                                        ║
║                                                                              ║
║  The detected date is:                                                       ║
║    • Embedded in filename:  finviz_MASTER_YYYYMMDD.csv                      ║
║    • Added as  Data_Date  column in every row (YYYY-MM-DD)                  ║
║    • Logged clearly so you always know which signal won                      ║
║                                                                              ║
║  Usage:                                                                      ║
║    python finviz_scraper.py                  # full run                     ║
║    python finviz_scraper.py --pages 3        # local test (60 tickers/view) ║
║    python finviz_scraper.py --resume         # continue interrupted run     ║
║    python finviz_scraper.py --delay 3        # slower = safer               ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import argparse
import csv
import email.utils
import json
import logging
import random
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("finviz")

# ── Constants ─────────────────────────────────────────────────────────────────

BASE_URL      = "https://finviz.com/screener.ashx"
ROWS_PER_PAGE = 20

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]

RETRYABLE_CODES  = {429, 500, 502, 503, 504}
BACKOFF_SCHEDULE = [15, 30, 60, 120, 300]
MAX_CONSEC_FAIL  = 3

# Views to scrape — (view_id, column_suffix_for_repeated_cols)
VIEWS = [
    (111, ""),             # Overview
    (121, "_valuation"),
    (131, "_ownership"),
    (141, "_performance"),
    (161, "_financial"),
    (171, "_technical"),
]

# Columns Finviz repeats in every view; we suffix them to keep the master schema clean
REPEATED_COLS = {"Ticker", "Company", "Sector", "Industry", "Country",
                 "Market Cap", "P/E", "Price", "Change", "Volume"}

# ── NYSE Holiday Calendar ─────────────────────────────────────────────────────
# Source: NYSE Holidays & Trading Hours (verified 2026–2028)

NYSE_HOLIDAYS = {
    # 2026
    date(2026, 1,  1),   # New Year's Day
    date(2026, 1,  19),  # Martin Luther King Jr. Day
    date(2026, 2,  16),  # Washington's Birthday
    date(2026, 4,  3),   # Good Friday
    date(2026, 5,  25),  # Memorial Day
    date(2026, 6,  19),  # Juneteenth
    date(2026, 7,  3),   # Independence Day (observed — July 4 is Saturday)
    date(2026, 9,  7),   # Labor Day
    date(2026, 11, 26),  # Thanksgiving Day
    date(2026, 12, 25),  # Christmas Day

    # 2027
    date(2027, 1,  1),   # New Year's Day
    date(2027, 1,  18),  # Martin Luther King Jr. Day
    date(2027, 2,  15),  # Washington's Birthday
    date(2027, 3,  26),  # Good Friday
    date(2027, 5,  31),  # Memorial Day
    date(2027, 6,  18),  # Juneteenth (observed — June 19 is Saturday)
    date(2027, 7,  5),   # Independence Day (observed — July 4 is Sunday)
    date(2027, 9,  6),   # Labor Day
    date(2027, 11, 25),  # Thanksgiving Day
    date(2027, 12, 24),  # Christmas Day (observed — Dec 25 is Saturday)

    # 2028
    date(2028, 1,  17),  # Martin Luther King Jr. Day  (Jan 1 falls on weekend, not observed)
    date(2028, 2,  21),  # Washington's Birthday
    date(2028, 4,  14),  # Good Friday
    date(2028, 5,  29),  # Memorial Day
    date(2028, 6,  19),  # Juneteenth
    date(2028, 7,  4),   # Independence Day
    date(2028, 9,  4),   # Labor Day
    date(2028, 11, 23),  # Thanksgiving Day
    date(2028, 12, 25),  # Christmas Day
}

MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "may": 5, "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


# ════════════════════════════════════════════════════════════════════════════
#  DATE DETECTION
# ════════════════════════════════════════════════════════════════════════════

def last_trading_day(ref: date = None) -> date:
    """Most recent NYSE trading day on or before `ref` (default: today)."""
    d = ref or date.today()
    while d.weekday() >= 5 or d in NYSE_HOLIDAYS:
        d -= timedelta(days=1)
    return d


def _parse_finviz_earnings_date(raw: str) -> date | None:
    """
    Parse Finviz earnings string like 'Apr 16/a' or 'Jan 29/b' into a date.
    The year is inferred: if the month is more than 2 months in the future,
    it belongs to the prior year.
    """
    m = re.match(
        r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2})/[ab]',
        raw.strip(), re.IGNORECASE
    )
    if not m:
        return None
    month = MONTH_MAP[m.group(1).lower()]
    day   = int(m.group(2))
    today = date.today()
    year  = today.year
    if month > today.month + 2:
        year -= 1
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _date_from_http_headers(resp: requests.Response) -> date | None:
    """
    Check HTTP Last-Modified then Date header.
    These are the most reliable signal — the server stamps exactly when
    the page was generated (= close-of-business on the data date).
    """
    for hdr in ("Last-Modified", "Date"):
        val = resp.headers.get(hdr, "")
        if val:
            try:
                return email.utils.parsedate_to_datetime(val).date()
            except Exception:
                pass
    return None


def _date_from_html_text(soup: BeautifulSoup) -> date | None:
    """
    Scan visible page text for explicit date strings like:
      'as of April 16, 2026'  |  'Apr 16, 2026'  |  '2026-04-16'
    """
    text = soup.get_text(" ", strip=True)
    patterns = [
        r'as\s+of\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\.?\s+(\d{1,2}),?\s+(20\d\d)',
        r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\.?\s+(\d{1,2}),?\s+(20\d\d)',
        r'\b(20\d\d)-(\d{2})-(\d{2})\b',
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if not m:
            continue
        try:
            g = m.groups()
            if re.match(r'20\d\d', g[0]):                       # ISO
                return date(int(g[0]), int(g[1]), int(g[2]))
            else:                                                 # Month Day Year
                return date(int(g[2]), MONTH_MAP[g[0][:3].lower()], int(g[1]))
        except Exception:
            pass
    return None


def _date_from_earnings(rows: list[dict]) -> date | None:
    """
    Find the most recent after-close earnings date (/a) across all rows.
    A company that reported after-hours on date D means:
      → the data was scraped on day D or later.
    """
    latest: date | None = None
    for row in rows:
        raw = row.get("Earnings", "").strip()
        if not raw.endswith("/a"):
            continue
        d = _parse_finviz_earnings_date(raw)
        if d and (latest is None or d > latest):
            latest = d
    return latest


def determine_data_date(
    merged_rows: list[dict],
    first_resp: "requests.Response | None",
    first_soup: "BeautifulSoup | None",
) -> date:
    """
    Try all detection signals in priority order. Logs which one was used.

    Priority:
      1. HTTP headers          — server timestamp, exact
      2. HTML page text        — explicit 'as of' dates
      3. Earnings lower-bound  — cross-checks recent /a earnings dates
      4. Fallback              — last NYSE trading day
    """
    # 1. HTTP headers
    if first_resp is not None:
        d = _date_from_http_headers(first_resp)
        if d:
            result = last_trading_day(d)
            log.info(f"  ✓ Data date from HTTP headers   → {result}  "
                     f"(raw header date: {d})")
            return result

    # 2. HTML text
    if first_soup is not None:
        d = _date_from_html_text(first_soup)
        if d:
            result = last_trading_day(d)
            log.info(f"  ✓ Data date from HTML text      → {result}")
            return result

    # 3. Earnings cross-check
    earnings_lower = _date_from_earnings(merged_rows)
    if earnings_lower:
        candidate = last_trading_day()
        if candidate >= earnings_lower:
            log.info(f"  ✓ Data date from earnings bound → {candidate}  "
                     f"(most recent /a earnings: {earnings_lower})")
            return candidate
        else:
            result = last_trading_day(earnings_lower)
            log.info(f"  ✓ Data date pinned to earnings  → {result}")
            return result

    # 4. Fallback
    result = last_trading_day()
    log.warning(f"  ⚠ Data date: fallback to last trading day → {result}")
    return result


# ════════════════════════════════════════════════════════════════════════════
#  HTTP
# ════════════════════════════════════════════════════════════════════════════

def fetch_page(session, view: int, row_start: int, filters: str,
               delay: float, jitter: float
               ) -> "tuple[BeautifulSoup | None, requests.Response | None]":
    params = f"v={view}&r={row_start}" + (f"&{filters}" if filters else "")
    url    = f"{BASE_URL}?{params}"

    for attempt, wait in enumerate(BACKOFF_SCHEDULE, start=1):
        hdrs = {
            "User-Agent":      random.choice(USER_AGENTS),
            "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection":      "keep-alive",
            "Referer":         "https://finviz.com/",
        }
        try:
            resp = session.get(url, headers=hdrs, timeout=25)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                if soup.select_one("table.styled-table-new.screener_table"):
                    return soup, resp
                if "captcha" in resp.text.lower():
                    log.warning(f"  CAPTCHA on attempt {attempt}.")
                else:
                    log.warning(f"  Table missing on attempt {attempt} (r={row_start}).")
            elif resp.status_code == 403:
                log.error("HTTP 403 blocked. Raise --delay/--jitter, then --resume.")
                return None, None
            elif resp.status_code in RETRYABLE_CODES:
                log.warning(f"  HTTP {resp.status_code} on attempt {attempt}.")
            else:
                log.warning(f"  HTTP {resp.status_code} on attempt {attempt}.")
        except requests.exceptions.Timeout:
            log.warning(f"  Timeout on attempt {attempt}.")
        except requests.exceptions.RequestException as e:
            log.warning(f"  Request error on attempt {attempt}: {e}")

        if attempt < len(BACKOFF_SCHEDULE):
            log.info(f"  Backing off {wait}s…")
            time.sleep(wait)

    log.error(f"All attempts failed for view={view} r={row_start}.")
    return None, None


# ════════════════════════════════════════════════════════════════════════════
#  HTML PARSING
# ════════════════════════════════════════════════════════════════════════════

def parse_headers(soup) -> list:
    """
    Extract column headers from the screener table.

    Finviz uses different header row classes across views:
      v121+  →  tr.table-top          (has this class)
      v111   →  first tr that is NOT a data row (no styled-row class)

    We try selectors in order and use whichever finds a non-empty header row.
    A valid header row must have >= 5 cells AND the cells must look like
    column names (not numeric ticker/price values).
    """
    table = soup.select_one("table.styled-table-new.screener_table")
    if not table:
        return []

    # Selectors tried in priority order
    candidates = [
        table.select_one("tr.table-top"),          # v121, v131, v141, v161, v171
        table.select_one("tr.table-header"),        # possible alt class
        table.select_one("tr.header-row"),          # possible alt class
    ]

    # Also try: first tr that has no 'styled-row' in its classes
    for tr in table.find_all("tr"):
        classes = tr.get("class", [])
        if "styled-row" not in classes and "table-top" not in classes:
            candidates.append(tr)
            break

    # Last resort: very first tr
    first_tr = table.find("tr")
    if first_tr:
        candidates.append(first_tr)

    for candidate in candidates:
        if candidate is None:
            continue
        cells = candidate.find_all(["td", "th"])
        if len(cells) < 5:
            continue
        texts = [c.get_text(strip=True) for c in cells]
        # Sanity check: header rows contain label words, not just numbers/tickers
        # At least 3 cells should be non-numeric non-empty strings
        label_count = sum(
            1 for t in texts
            if t and not re.match(r'^-?[\d,.%]+$', t)
        )
        if label_count >= 3:
            log.debug(f"  parse_headers: found {len(texts)} headers via candidate {candidates.index(candidate)}")
            return texts

    return []


def parse_rows(soup) -> list:
    """
    Extract all data rows from the screener table.

    Tries multiple row selectors to handle differences across views:
      - tr.styled-row          (most views)
      - tr.is-bordered         (alt class)
      - any tr with >= 5 tds that isn't the header row
    """
    table = soup.select_one("table.styled-table-new.screener_table")
    if not table:
        return []

    # Try specific class first
    rows = table.select("tr.styled-row")
    if not rows:
        rows = table.select("tr.is-bordered")
    if not rows:
        # Fallback: any tr with enough cells, skipping the first (header) tr
        all_trs = table.find_all("tr")
        rows = [tr for tr in all_trs[1:] if len(tr.find_all("td")) >= 5]

    return [
        [td.get_text(strip=True) for td in tr.find_all("td")]
        for tr in rows
        if tr.find_all("td")
    ]


def parse_total(soup) -> int:
    m = re.search(r"#\d+\s*/\s*([\d,]+)\s*Total", soup.get_text(" ", strip=True))
    return int(m.group(1).replace(",", "")) if m else 0


def get_next_row_start(soup) -> "int | None":
    nxt = soup.select_one("a.screener-pages.is-next")
    if not nxt:
        return None
    m = re.search(r"r=(\d+)", nxt.get("href", ""))
    return int(m.group(1)) if m else None


# ════════════════════════════════════════════════════════════════════════════
#  SCRAPE ONE VIEW
# ════════════════════════════════════════════════════════════════════════════

def scrape_view(session, view_id: int, suffix: str, args, progress_dir: Path):
    pfile    = progress_dir / f".progress_v{view_id}.json"
    progress = {}
    if args.resume and pfile.exists():
        try:
            progress = json.loads(pfile.read_text())
            log.info(f"  Resuming view {view_id} from page {progress.get('last_page', 0)+1}")
        except Exception:
            pass

    next_row   = progress.get("next_row", 1)
    pages_done = progress.get("last_page", 0)
    data: dict = progress.get("data", {})

    hdrs_known = []
    consec_fail = 0
    first_resp = None
    first_soup = None

    while True:
        log.info(f"  [v{view_id}] page {pages_done+1}  r={next_row}")
        soup, resp = fetch_page(session, view_id, next_row, args.filters, args.delay, args.jitter)

        if soup is None:
            consec_fail += 1
            if consec_fail >= MAX_CONSEC_FAIL:
                log.error(f"  View {view_id}: too many failures — saving progress.")
                _save(pfile, next_row, pages_done, data)
                break
            next_row += ROWS_PER_PAGE
            time.sleep(args.delay + random.uniform(0, args.jitter))
            continue

        consec_fail = 0
        if first_resp is None:
            first_resp, first_soup = resp, soup

        if not hdrs_known:
            hdrs_known = parse_headers(soup)
            if hdrs_known:
                log.info(f"  View {view_id}: {len(hdrs_known)} columns found — {hdrs_known[:5]}…")
            else:
                log.warning(f"  View {view_id}: could not parse headers on page {pages_done+1} — will retry next page.")

        if pages_done == 0:
            total = parse_total(soup)
            if total:
                log.info(f"  View {view_id}: {total:,} tickers total")

        # Don't try to store rows if we still have no headers — column names would be wrong
        if not hdrs_known:
            pages_done += 1
            next_page = get_next_row_start(soup)
            if not next_page or (args.pages and pages_done >= args.pages):
                break
            next_row = next_page
            time.sleep(args.delay + random.uniform(0, args.jitter))
            continue

        rows = parse_rows(soup)
        if not rows:
            log.info(f"  View {view_id}: no rows — done.")
            break

        for cells in rows:
            if len(cells) < len(hdrs_known):
                cells += [""] * (len(hdrs_known) - len(cells))
            row_dict = {}
            for col, val in zip(hdrs_known, cells):
                val = val.strip()
                if val == "-":
                    val = ""
                key = f"{col}{suffix}" if (suffix and col in REPEATED_COLS and col != "Ticker") else col
                row_dict[key] = val
            ticker = row_dict.get("Ticker", "").strip()
            if ticker:
                data[ticker] = {**data.get(ticker, {}), **row_dict}

        pages_done += 1
        if args.pages and pages_done >= args.pages:
            log.info(f"  View {view_id}: --pages limit reached.")
            break

        next_page = get_next_row_start(soup)
        if not next_page:
            log.info(f"  View {view_id}: last page.")
            break

        next_row = next_page
        _save(pfile, next_row, pages_done, data)
        sleep_for = args.delay + random.uniform(0, args.jitter)
        log.info(f"  Sleeping {sleep_for:.1f}s…")
        time.sleep(sleep_for)

    if pfile.exists():
        pfile.unlink()
    return data, first_resp, first_soup


def _save(pfile, next_row, last_page, data):
    pfile.write_text(json.dumps({
        "next_row": next_row, "last_page": last_page,
        "data": data, "updated": datetime.now().isoformat()
    }, indent=2))


# ════════════════════════════════════════════════════════════════════════════
#  MERGE + CSV
# ════════════════════════════════════════════════════════════════════════════

def merge_views(all_data: list) -> list:
    merged = {}
    for vd in all_data:
        for ticker, row in vd.items():
            if ticker not in merged:
                merged[ticker] = {}
            merged[ticker].update(row)
    rows = list(merged.values())
    try:
        rows.sort(key=lambda r: int(r.get("No.", 0)))
    except Exception:
        pass
    return rows


def write_master_csv(rows: list, out_path: Path):
    if not rows:
        log.warning("No rows to write.")
        return
    seen, all_keys = set(), []
    # Data_Date right after Ticker so it's always prominent
    for k in ["No.", "Ticker", "Data_Date", "Company", "Sector", "Industry",
              "Country", "Market Cap", "P/E", "Price", "Change", "Volume"]:
        if k not in seen:
            all_keys.append(k); seen.add(k)
    for row in rows:
        for k in row:
            if k not in seen:
                all_keys.append(k); seen.add(k)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    log.info(f"CSV: {out_path}  ({len(rows):,} rows, {len(all_keys)} cols)")


# ════════════════════════════════════════════════════════════════════════════
#  MAIN
# ════════════════════════════════════════════════════════════════════════════

def run(args):
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    prog_dir = out_dir / ".progress"
    prog_dir.mkdir(exist_ok=True)

    log.info("=" * 62)
    log.info(f"  Finviz Scraper  |  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"  Delay: {args.delay}s + jitter 0–{args.jitter}s")
    if args.pages:
        log.info(f"  Test mode: {args.pages} pages per view")
    log.info("=" * 62)

    session   = requests.Session()
    all_data  = []
    first_resp_global = None
    first_soup_global = None

    for view_id, suffix in VIEWS:
        log.info(f"\n{'─'*50}")
        log.info(f"  View {view_id}{suffix or ' (overview)'}…")
        log.info(f"{'─'*50}")

        vdata, resp, soup = scrape_view(session, view_id, suffix, args, prog_dir)
        all_data.append(vdata)
        log.info(f"  View {view_id}: {len(vdata):,} tickers")

        if first_resp_global is None and resp:
            first_resp_global = resp
            first_soup_global = soup

        if (view_id, suffix) != VIEWS[-1]:
            p = args.delay * 3 + random.uniform(0, args.jitter * 2)
            log.info(f"  Between-view pause: {p:.1f}s")
            time.sleep(p)

    log.info("\nMerging views…")
    merged = merge_views(all_data)

    # ── Auto-detect data date ─────────────────────────────────────────────────
    log.info("\nDetecting data date…")
    data_date     = determine_data_date(merged, first_resp_global, first_soup_global)
    data_date_str = data_date.strftime("%Y-%m-%d")
    log.info(f"  ► Confirmed data date: {data_date_str}")

    # Stamp every row
    for row in merged:
        row["Data_Date"] = data_date_str

    # Output filename uses the confirmed data date
    out_csv = out_dir / f"finviz_MASTER_{data_date.strftime('%Y%m%d')}.csv"

    fh = logging.FileHandler(out_dir / f"scraper_{data_date.strftime('%Y%m%d')}.log", encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s"))
    log.addHandler(fh)

    write_master_csv(merged, out_csv)

    log.info("=" * 62)
    log.info(f"  DONE  |  {len(merged):,} tickers")
    log.info(f"  Data date : {data_date_str}")
    log.info(f"  File      : {out_csv.name}")
    log.info("=" * 62)

    return out_csv


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape all Finviz views → Master CSV with auto-detected data date")
    parser.add_argument("--pages",   type=int,   default=None)
    parser.add_argument("--filters", type=str,   default="")
    parser.add_argument("--resume",  action="store_true")
    parser.add_argument("--delay",   type=float, default=2.5)
    parser.add_argument("--jitter",  type=float, default=1.5)
    parser.add_argument("--out-dir", type=str,   default=".", dest="out_dir")
    run(parser.parse_args())
