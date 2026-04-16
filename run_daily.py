"""
run_daily.py  —  Scrape Finviz + Upload to Google Drive
========================================================

Exit codes (used by the GitHub Actions retry loop):
    0  = success  (CSV scraped and uploaded)
    2  = already done today  (Drive already has today's file — skip cleanly)
    1  = failure  (scrape or upload error — next hourly slot will retry)

Usage:
    python run_daily.py                          # full run
    python run_daily.py --pages 3               # local test (~60 tickers/view)
    python run_daily.py --resume                # continue interrupted scrape
    python run_daily.py --check-only            # just check if today's file exists on Drive

Environment variables (set as GitHub Actions secrets):
    GDRIVE_FOLDER_ID    — Google Drive folder ID
    GDRIVE_CREDENTIALS  — Full JSON text of your service account key file
"""

import argparse
import logging
import os
import sys
from argparse import Namespace
from datetime import date, timedelta
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("runner")

# ── NYSE Holiday Calendar ─────────────────────────────────────────────────────
# Source: NYSE Holidays & Trading Hours (verified from screenshot)

NYSE_HOLIDAYS = {
    # 2026
    date(2026, 1,  1),   # New Year's Day
    date(2026, 1,  19),  # Martin Luther King Jr. Day
    date(2026, 2,  16),  # Washington's Birthday
    date(2026, 4,  3),   # Good Friday
    date(2026, 5,  25),  # Memorial Day
    date(2026, 6,  19),  # Juneteenth
    date(2026, 7,  3),   # Independence Day (observed, July 4 is Saturday)
    date(2026, 9,  7),   # Labor Day
    date(2026, 11, 26),  # Thanksgiving Day
    date(2026, 12, 25),  # Christmas Day

    # 2027
    date(2027, 1,  1),   # New Year's Day
    date(2027, 1,  18),  # Martin Luther King Jr. Day
    date(2027, 2,  15),  # Washington's Birthday
    date(2027, 3,  26),  # Good Friday
    date(2027, 5,  31),  # Memorial Day
    date(2027, 6,  18),  # Juneteenth (observed, June 19 is Saturday)
    date(2027, 7,  5),   # Independence Day (observed, July 4 is Sunday)
    date(2027, 9,  6),   # Labor Day
    date(2027, 11, 25),  # Thanksgiving Day
    date(2027, 12, 24),  # Christmas Day (observed, Dec 25 is Saturday)

    # 2028
    date(2028, 1,  17),  # Martin Luther King Jr. Day  (Jan 1 not observed — falls on weekend)
    date(2028, 2,  21),  # Washington's Birthday
    date(2028, 4,  14),  # Good Friday
    date(2028, 5,  29),  # Memorial Day
    date(2028, 6,  19),  # Juneteenth
    date(2028, 7,  4),   # Independence Day
    date(2028, 9,  4),   # Labor Day
    date(2028, 11, 23),  # Thanksgiving Day
    date(2028, 12, 25),  # Christmas Day
}


def last_trading_day(ref: date = None) -> date:
    """Most recent NYSE trading day on or before ref (default: today)."""
    d = ref or date.today()
    while d.weekday() >= 5 or d in NYSE_HOLIDAYS:
        d -= timedelta(days=1)
    return d


def is_trading_day(d: date = None) -> bool:
    d = d or date.today()
    return d.weekday() < 5 and d not in NYSE_HOLIDAYS


# ── Credentials helper ────────────────────────────────────────────────────────

def resolve_credentials() -> str:
    """
    Returns path to credentials.json.
    In GitHub Actions the GDRIVE_CREDENTIALS secret holds the full JSON string.
    Locally the file should already exist next to the scripts.
    """
    creds_json = os.environ.get("GDRIVE_CREDENTIALS", "")
    if creds_json:
        p = Path("credentials.json")
        p.write_text(creds_json)
        log.info("credentials.json written from GDRIVE_CREDENTIALS env var.")
        return str(p)
    if Path("credentials.json").exists():
        log.info("Using local credentials.json")
        return "credentials.json"
    log.error("No credentials! Set GDRIVE_CREDENTIALS env var or place credentials.json here.")
    sys.exit(1)


# ── Google Drive helpers ──────────────────────────────────────────────────────

def build_drive_service(creds_path: str):
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    creds = service_account.Credentials.from_service_account_file(
        creds_path, scopes=["https://www.googleapis.com/auth/drive.file"]
    )
    return build("drive", "v3", credentials=creds)


def file_exists_on_drive(service, filename: str, folder_id: str) -> "str | None":
    q = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    r = service.files().list(q=q, fields="files(id,name)").execute()
    files = r.get("files", [])
    return files[0]["id"] if files else None


def upload_to_drive(service, local_path: Path, folder_id: str) -> str:
    from googleapiclient.http import MediaFileUpload
    media       = MediaFileUpload(str(local_path), mimetype="text/csv", resumable=True)
    existing_id = file_exists_on_drive(service, local_path.name, folder_id)
    if existing_id:
        log.info(f"  Updating existing Drive file: {local_path.name}")
        f = service.files().update(fileId=existing_id, media_body=media).execute()
    else:
        log.info(f"  Creating new Drive file: {local_path.name}")
        meta = {"name": local_path.name, "parents": [folder_id]}
        f = service.files().create(body=meta, media_body=media, fields="id").execute()
    return f["id"]


# ── Already-done check ────────────────────────────────────────────────────────

def check_already_done(service, folder_id: str) -> bool:
    """Return True if today's trading-day CSV already exists on Drive."""
    today         = last_trading_day()
    expected_name = f"finviz_MASTER_{today.strftime('%Y%m%d')}.csv"
    existing      = file_exists_on_drive(service, expected_name, folder_id)
    if existing:
        log.info(f"  ✓ Already on Drive: {expected_name}  (id={existing})")
        return True
    log.info(f"  Not yet on Drive: {expected_name}")
    return False


# ── Main ──────────────────────────────────────────────────────────────────────

def run(args):
    log.info("=" * 62)
    log.info("  Finviz Daily Runner")
    log.info("=" * 62)

    folder_id = args.folder_id or os.environ.get("GDRIVE_FOLDER_ID", "")
    if not folder_id:
        log.error("No folder ID — set --folder-id or GDRIVE_FOLDER_ID env var.")
        sys.exit(1)

    if not is_trading_day() and not args.force:
        log.info("Today is not a trading day (weekend or NYSE holiday). Nothing to do.")
        log.info("Use --force to run anyway.")
        sys.exit(0)

    creds_path = resolve_credentials()
    log.info("Connecting to Google Drive…")
    service = build_drive_service(creds_path)

    # ── Already done? ─────────────────────────────────────────────────────────
    if check_already_done(service, folder_id):
        log.info("Today's file is already on Drive — nothing to do.")
        sys.exit(2)   # clean skip, not a failure

    if args.check_only:
        log.info("--check-only: file not found, would proceed with scrape.")
        sys.exit(0)

    # ── Step 1: Scrape ────────────────────────────────────────────────────────
    log.info("\n[1/2]  Scraping Finviz…\n")
    import finviz_scraper
    scrape_args = Namespace(
        pages=args.pages,
        filters=args.filters,
        resume=args.resume,
        delay=args.delay,
        jitter=args.jitter,
        out_dir=args.out_dir,
    )
    try:
        csv_path = finviz_scraper.run(scrape_args)
    except Exception as e:
        log.error(f"Scrape failed: {e}")
        sys.exit(1)

    if not csv_path or not Path(csv_path).exists():
        log.error("Scraper returned no output file.")
        sys.exit(1)

    log.info(f"Scrape complete → {csv_path}  ({Path(csv_path).stat().st_size/1024:.0f} KB)")

    # ── Step 2: Upload ────────────────────────────────────────────────────────
    log.info("\n[2/2]  Uploading to Google Drive…\n")
    try:
        file_id = upload_to_drive(service, Path(csv_path), folder_id)
        log.info(f"  ✓ Uploaded!  Drive file ID: {file_id}")
        log.info(f"  View: https://drive.google.com/file/d/{file_id}/view")
    except Exception as e:
        log.error(f"Upload failed: {e}")
        sys.exit(1)

    log.info("\n" + "=" * 62)
    log.info("  ALL DONE ✓")
    log.info("=" * 62)
    sys.exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape Finviz + Upload to Google Drive",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exit codes:  0=success  2=already done today  1=failure (retry next hour)

Local test (quick):
  python run_daily.py --pages 3 --folder-id 1AQFN2b6iReA2jCr31pqWXlnTpvfffdqx

Full run:
  python run_daily.py --folder-id 1AQFN2b6iReA2jCr31pqWXlnTpvfffdqx

Check if today's file already on Drive:
  python run_daily.py --check-only --folder-id 1AQFN2b6iReA2jCr31pqWXlnTpvfffdqx
        """,
    )
    parser.add_argument("--pages",      type=int,   default=None)
    parser.add_argument("--filters",    type=str,   default="")
    parser.add_argument("--resume",     action="store_true")
    parser.add_argument("--delay",      type=float, default=2.5)
    parser.add_argument("--jitter",     type=float, default=1.5)
    parser.add_argument("--folder-id",  type=str,   default="",  dest="folder_id")
    parser.add_argument("--out-dir",    type=str,   default=".", dest="out_dir")
    parser.add_argument("--force",      action="store_true",     help="Run even on non-trading days")
    parser.add_argument("--check-only", action="store_true",     dest="check_only")
    run(parser.parse_args())