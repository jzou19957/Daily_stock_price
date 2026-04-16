"""
run_daily.py  —  Scrape Finviz + Upload to Google Drive
========================================================

How it works:
  1. Connects to Google Drive
  2. Checks if today's finviz_MASTER_YYYYMMDD.csv already exists
     → YES: logs "already done", exits 0  (GitHub Actions sees success)
     → NO:  scrapes all 6 Finviz views, uploads the CSV, exits 0
  3. Any error during scrape or upload → exits 1
     (GitHub Actions sees failure, next hourly slot retries)

Run locally:
    python run_daily.py                     # full run
    python run_daily.py --pages 3           # quick test, ~60 tickers/view
    python run_daily.py --resume            # continue interrupted scrape
    python run_daily.py --force             # re-run even if today's file exists

Credentials:
    Locally    → place credentials.json next to this script
    GitHub     → set GDRIVE_CREDENTIALS secret (full JSON text)
    Folder ID  → set GDRIVE_FOLDER_ID secret (or use --folder-id flag)
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

NYSE_HOLIDAYS = {
    # 2026
    date(2026, 1,  1),  date(2026, 1,  19), date(2026, 2,  16),
    date(2026, 4,  3),  date(2026, 5,  25), date(2026, 6,  19),
    date(2026, 7,  3),  date(2026, 9,  7),  date(2026, 11, 26),
    date(2026, 12, 25),
    # 2027
    date(2027, 1,  1),  date(2027, 1,  18), date(2027, 2,  15),
    date(2027, 3,  26), date(2027, 5,  31), date(2027, 6,  18),
    date(2027, 7,  5),  date(2027, 9,  6),  date(2027, 11, 25),
    date(2027, 12, 24),
    # 2028
    date(2028, 1,  17), date(2028, 2,  21), date(2028, 4,  14),
    date(2028, 5,  29), date(2028, 6,  19), date(2028, 7,  4),
    date(2028, 9,  4),  date(2028, 11, 23), date(2028, 12, 25),
}


def last_trading_day(ref: date = None) -> date:
    d = ref or date.today()
    while d.weekday() >= 5 or d in NYSE_HOLIDAYS:
        d -= timedelta(days=1)
    return d


def is_trading_day(d: date = None) -> bool:
    d = d or date.today()
    return d.weekday() < 5 and d not in NYSE_HOLIDAYS


# ── Credentials ───────────────────────────────────────────────────────────────

def resolve_credentials() -> str:
    """
    Find credentials.json.
    GitHub Actions: reads from GDRIVE_CREDENTIALS secret (full JSON string).
    Local:          looks for credentials.json next to this script.
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
    log.error("No credentials found.")
    log.error("  Local:  place credentials.json next to this script")
    log.error("  GitHub: set the GDRIVE_CREDENTIALS secret")
    sys.exit(1)


# ── Google Drive ──────────────────────────────────────────────────────────────

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


# ── Main ──────────────────────────────────────────────────────────────────────

def run(args):
    log.info("=" * 62)
    log.info("  Finviz Daily Runner")
    log.info("=" * 62)

    # ── Resolve folder ID ─────────────────────────────────────────────────────
    folder_id = args.folder_id or os.environ.get("GDRIVE_FOLDER_ID", "")
    if not folder_id:
        log.error("No Google Drive folder ID.")
        log.error("  Local:  python run_daily.py --folder-id YOUR_FOLDER_ID")
        log.error("  GitHub: set the GDRIVE_FOLDER_ID secret")
        sys.exit(1)

    # ── Skip on non-trading days (unless forced) ──────────────────────────────
    if not is_trading_day() and not args.force:
        log.info("Today is not a trading day — nothing to do.")
        log.info("Use --force to run anyway.")
        sys.exit(0)

    # ── Connect to Drive ──────────────────────────────────────────────────────
    creds_path = resolve_credentials()
    log.info("Connecting to Google Drive…")
    try:
        service = build_drive_service(creds_path)
    except Exception as e:
        log.error(f"Could not connect to Google Drive: {e}")
        sys.exit(1)

    # ── Check if today's file already exists ──────────────────────────────────
    today         = last_trading_day()
    expected_name = f"finviz_MASTER_{today.strftime('%Y%m%d')}.csv"
    existing_id   = file_exists_on_drive(service, expected_name, folder_id)

    if existing_id and not args.force:
        log.info(f"  ✓ Already on Drive: {expected_name}")
        log.info("  Nothing to do — exiting cleanly.")
        sys.exit(0)   # success — not an error, just already done

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

    log.info(f"Scrape complete → {csv_path}  ({Path(csv_path).stat().st_size / 1024:.0f} KB)")

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
Examples:
  python run_daily.py                                      # full run
  python run_daily.py --pages 3                           # quick test
  python run_daily.py --resume                            # continue interrupted
  python run_daily.py --force                             # re-run even if done today
  python run_daily.py --folder-id 1AQFN2b6iReA2jCr31pqWXlnTpvfffdqx
        """,
    )
    parser.add_argument("--pages",     type=int,   default=None,  help="Pages per view (None=all, 3=quick test)")
    parser.add_argument("--filters",   type=str,   default="",    help="Finviz filter params")
    parser.add_argument("--resume",    action="store_true",       help="Resume interrupted scrape")
    parser.add_argument("--delay",     type=float, default=2.5,   help="Base delay between requests (s)")
    parser.add_argument("--jitter",    type=float, default=1.5,   help="Max random extra delay (s)")
    parser.add_argument("--folder-id", type=str,   default="",    dest="folder_id", help="Google Drive folder ID")
    parser.add_argument("--out-dir",   type=str,   default=".",   dest="out_dir",   help="Local output folder")
    parser.add_argument("--force",     action="store_true",       help="Run even if today's file already on Drive")
    run(parser.parse_args())