"""
epstein_downloader.py  (v3 — modular)

============================================================================
AI / LLM ASSISTANT PROMPT — READ THIS FIRST
============================================================================

This script bulk-downloads paired files (typically PDF + video) from a
government website that requires cookie-based authentication.  It was
originally built for the DOJ Epstein Library "No Images Produced" collection,
but is designed to work with ANY source that follows the same pattern:

    1.  A Google Sheet (or local CSV) with two columns of URLs.
    2.  A website that requires browser cookies to download files.
    3.  An output directory (optionally on a Google-Drive-mounted drive).

HOW IT WORKS (5 phases):
    Phase 1 — Read a list of URL pairs from a Google Sheet or local CSV.
    Phase 2 — Authenticate with the target site via session cookies.
    Phase 3 — Download every pair (PDF + video) into per-item folders,
              using parallel threads with resume support.
    Phase 4 — Extract a short text summary from the first page of each PDF
              and rename the folder to include the summary.
    Phase 5 — (Optional) "Dehydrate" files on a Google-Drive-mounted drive
              so they become online-only and free local disk space.
    Cleanup — Remove any empty directories left behind by failed downloads.

WHAT THE USER NEEDS TO PROVIDE:
    • A Google Sheet (shared publicly, or "anyone with the link") whose first
      row has column headers including "PDF URL" and "MOV URL" (configurable
      via --pdf-col and --video-col).  Pass the sheet ID with --sheet-id or
      the full URL with --sheet-url.
      ALTERNATIVELY, a local CSV file (--csv path/to/file.csv) with the same
      column headers.

    • Session cookies for the download site.  Three ways to get them:
        a) AUTOMATIC (Selenium): the script opens Chrome, the user passes
           any challenges (CAPTCHA, age verification), then presses ENTER.
           Requires: pip install selenium, Chrome + chromedriver installed.
        b) MANUAL (--manual): the user copies the cookie string from their
           browser DevTools (F12 → Console → document.cookie) and pastes it.
        c) CACHED (--no-prompt): reuse a previously saved session from
           session_cookies.json — no browser or interaction required.

    • An output directory (--output).  If the drive is a Google Drive mount,
      the --dehydrate flag will mark files as online-only after upload.

WALK-THROUGH FOR A NEW USER:
    1. Install dependencies:
           pip install requests pdfplumber
           pip install selenium          # optional, for automatic cookie grab
    2. Scrape URLs: Install the Claude browser extension from the Chrome Web
       Store (search "Claude" by Anthropic), navigate to the target site, and
       use Claude to help automate the JavaScript scraping — or run the JS
       snippets manually in the browser console (F12 → Console).  See README
       for the full scraping strategy.
    3. Share your Google Sheet so "anyone with the link" can view it.
    4. Run a small test:
           python epstein_downloader.py --sheet-id YOUR_SHEET_ID \
               --output ./test_downloads --limit 5 --manual
    5. The script will ask you to paste cookies from your browser.
       → Open the target site in Chrome, pass any challenges, then:
         F12 → Console → type:  document.cookie  → copy the output.
    6. Once the test works, run the full download:
           python epstein_downloader.py --sheet-id YOUR_SHEET_ID \
               --output ./MyLibrary
    7. The script saves progress; if interrupted, just re-run — it resumes.
    8. To free local disk space on a Google Drive mount:
           python epstein_downloader.py --dehydrate-only \
               --output ./MyLibrary

NOTE:
    You must provide a URL source via --sheet-id, --sheet-url, or --csv.
    For the Epstein Library use case, create a Google Sheet with PDF/video
    URL pairs (see README) and pass its ID:
        python epstein_downloader.py --sheet-id YOUR_SHEET_ID --manual --output ./Epstein_Library

============================================================================

SETUP:
    pip install requests pdfplumber
    pip install selenium   # optional — only needed for --auto cookie grab

USAGE EXAMPLES:
    # Epstein Library with manual cookie entry
    python epstein_downloader.py --sheet-id YOUR_SHEET_ID --manual

    # Custom sheet, custom output
    python epstein_downloader.py --sheet-id ABCDEF123 --output ./downloads

    # Local CSV instead of Google Sheet
    python epstein_downloader.py --csv urls.csv --output ./downloads

    # Re-generate summaries only (no downloading)
    python epstein_downloader.py --resummarize --output ./downloads

    # Free local space on Google Drive mount
    python epstein_downloader.py --dehydrate-only --output ./downloads
"""

import os
import sys
import time
import re
import json
import csv
import io
import logging
import argparse
import subprocess
import threading
import shutil
import requests
from urllib.parse import unquote, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed


# ============================================================
# DEFAULTS (Epstein Library — overridable via CLI)
# ============================================================

DEFAULT_SPREADSHEET_ID = None  # Must be provided via --sheet-id, --sheet-url, or --csv
DEFAULT_AUTH_URL = "https://www.justice.gov/epstein"
DEFAULT_OUTPUT_DIR = "./Epstein_Library"
DEFAULT_COOKIE_DOMAIN = ".justice.gov"
DEFAULT_REQUIRED_COOKIES = [
    "QueueITAccepted-SDFrts345E-V3_usdojsearch",
    "justiceGovAgeVerified",
]
DEFAULT_PDF_COLUMN = "PDF URL"
DEFAULT_VIDEO_COLUMN = "MOV URL"
DEFAULT_VIDEO_EXTENSIONS = [
    # Video formats
    "mov", "mp4", "wmv", "avi", "3gp", "3g2", "m4v", "mpg", "flv", "webm",
    # Image formats
    "jpg", "jpeg", "png", "gif", "bmp", "tiff", "tif", "webp", "heic", "svg",
]

MAX_DOWNLOAD_WORKERS = 5
MAX_RETRIES = 3
RETRY_DELAY = 2
REQUEST_TIMEOUT = 120
CHUNK_SIZE = 8192

COOKIE_CACHE_FILE = "session_cookies.json"
PROGRESS_FILE = "download_progress.json"
LOCAL_CSV_CACHE = "urls_cache.csv"

MAX_SUMMARY_IN_FOLDER_NAME = 60
DEHYDRATE_WAIT_SECONDS = 30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("epstein_downloader.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

progress_lock = threading.Lock()


# ============================================================
# GOOGLE SHEET / CSV — FETCH URL PAIRS
# ============================================================

def fetch_url_pairs(sheet_id=None, csv_path=None,
                    pdf_col=DEFAULT_PDF_COLUMN,
                    video_col=DEFAULT_VIDEO_COLUMN):
    """Get URL pairs from a Google Sheet or local CSV.

    Priority: csv_path > LOCAL_CSV_CACHE > Google Sheet (sheet_id).
    """
    # 1) Explicit local CSV supplied via --csv
    if csv_path:
        log.info(f"Reading URL pairs from local CSV: {csv_path}")
        return _read_csv_pairs(csv_path, pdf_col, video_col)

    # 2) Cached CSV from a previous Google Sheet fetch
    if os.path.exists(LOCAL_CSV_CACHE):
        log.info(f"Using cached URL list: {LOCAL_CSV_CACHE}")
        return _read_csv_pairs(LOCAL_CSV_CACHE, pdf_col, video_col)

    # 3) Fetch from Google Sheet
    if not sheet_id:
        log.error(
            "No URL source provided. Use --sheet-id, --sheet-url, or --csv."
        )
        sys.exit(1)

    sheet_csv_url = (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
    )
    log.info(f"Fetching URL list from Google Sheet ({sheet_id})...")
    try:
        resp = requests.get(sheet_csv_url, timeout=30, allow_redirects=True)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.error(f"Could not fetch Google Sheet: {e}")
        log.error("Make sure the sheet is shared (anyone with link).")
        log.error(
            f"Or place a CSV with '{pdf_col}' and '{video_col}' columns "
            f"at: {LOCAL_CSV_CACHE}"
        )
        sys.exit(1)

    with open(LOCAL_CSV_CACHE, "w", encoding="utf-8", newline="") as f:
        f.write(resp.text)

    pairs = _parse_csv_text(resp.text, pdf_col, video_col)
    log.info(
        f"Loaded {len(pairs)} pairs from Google Sheet "
        f"(cached to {LOCAL_CSV_CACHE})"
    )
    return pairs


def _read_csv_pairs(path, pdf_col, video_col):
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    pairs = _parse_csv_text(text, pdf_col, video_col)
    log.info(f"Loaded {len(pairs)} pairs from {path}")
    return pairs


def _parse_csv_text(text, pdf_col, video_col):
    reader = csv.DictReader(io.StringIO(text))
    pairs = []
    for row in reader:
        pdf = row.get(pdf_col, "").strip()
        vid = row.get(video_col, "").strip()
        if pdf and vid:
            pairs.append((pdf, vid))
    return pairs


# ============================================================
# SESSION / COOKIE HANDLING
# ============================================================

def get_session_auto(auth_url, required_cookies, cookie_domain):
    """Open Chrome, let user pass site challenges, extract cookies."""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
    except ImportError:
        log.error("Selenium not installed.  Run: pip install selenium")
        log.error("Or use --manual mode to paste cookies instead.")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("BROWSER SESSION — PASS THE SITE CHALLENGES")
    print("=" * 60)
    print(f"""
A Chrome browser will open to:
  {auth_url}

Complete any challenges (CAPTCHA, age verification, etc.)
then come back here and press ENTER.
""")

    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    try:
        driver = webdriver.Chrome(options=options)
    except Exception as e:
        log.error(f"Could not start Chrome: {e}")
        log.error("Make sure Chrome and chromedriver are installed, or use --manual.")
        sys.exit(1)

    driver.get(auth_url)
    input("\n>>> Press ENTER after passing all challenges... ")

    browser_cookies = driver.get_cookies()
    driver.quit()

    session = _build_session()
    cookie_dict = {}
    for c in browser_cookies:
        session.cookies.set(
            c["name"], c["value"],
            domain=c.get("domain", ""), path=c.get("path", "/"),
        )
        cookie_dict[c["name"]] = c["value"]

    missing = [c for c in required_cookies if c not in cookie_dict]
    if missing:
        log.warning(f"Missing expected cookies: {missing}")
    else:
        log.info("All required cookies captured!")

    _save_cookie_cache(cookie_dict)
    return session


def get_session_manual(required_cookies, cookie_domain):
    """Let user paste cookies from browser DevTools."""
    print("\n" + "=" * 60)
    print("MANUAL COOKIE ENTRY")
    print("=" * 60)
    print("""
To get cookies:

  1. Open the target site in Chrome and pass any challenges.
  2. DevTools (F12) -> Console tab
  3. Type:  document.cookie  -> Enter
  4. Copy the entire output and paste below
""")

    cookie_string = input("Paste cookie string:\n> ").strip()

    session = _build_session()
    cookie_dict = {}
    for pair in cookie_string.split(";"):
        pair = pair.strip()
        if "=" in pair:
            idx = pair.index("=")
            name, value = pair[:idx].strip(), pair[idx + 1:].strip()
            session.cookies.set(name, value, domain=cookie_domain, path="/")
            cookie_dict[name] = value

    missing = [c for c in required_cookies if c not in cookie_dict]
    if missing:
        log.warning(f"Missing expected cookies: {missing}")
    else:
        log.info("All required cookies found!")

    _save_cookie_cache(cookie_dict)
    return session


def load_session_from_cache(cookie_domain):
    """Load cached cookies into a session WITHOUT validating them."""
    if not os.path.exists(COOKIE_CACHE_FILE):
        return None
    try:
        with open(COOKIE_CACHE_FILE, "r") as f:
            cookie_dict = json.load(f)
        session = _build_session()
        for name, value in cookie_dict.items():
            session.cookies.set(name, value, domain=cookie_domain, path="/")
        log.info("Loaded cookies from cache (skipping validation).")
        return session
    except Exception:
        return None


def get_session_cached(cookie_domain):
    """Try to reuse cached cookies (with a lightweight validation probe)."""
    if not os.path.exists(COOKIE_CACHE_FILE):
        return None
    try:
        with open(COOKIE_CACHE_FILE, "r") as f:
            cookie_dict = json.load(f)
        session = _build_session()
        for name, value in cookie_dict.items():
            session.cookies.set(name, value, domain=cookie_domain, path="/")
        # Quick validation: try a lightweight request
        resp = session.get(
            "https://www.justice.gov/multimedia-search",
            params={"keys": "no images produced", "page": 1},
            timeout=15,
        )
        if resp.status_code == 200:
            try:
                if "hits" in resp.json():
                    log.info("Cached session still valid!")
                    return session
            except (json.JSONDecodeError, KeyError):
                pass
        log.info("Cached session expired.")
        return None
    except Exception:
        return None


def get_session(manual=False, auto_use_cache=False,
                auth_url=DEFAULT_AUTH_URL,
                required_cookies=None,
                cookie_domain=DEFAULT_COOKIE_DOMAIN):
    """Get authenticated session, trying cache first."""
    if required_cookies is None:
        required_cookies = DEFAULT_REQUIRED_COOKIES

    if auto_use_cache:
        session = load_session_from_cache(cookie_domain)
        if session:
            return session
        log.error(
            "No cached cookies found and --no-prompt set. Cannot authenticate."
        )
        log.error(
            "Run once without --no-prompt to set up cookies, "
            "or manually create session_cookies.json"
        )
        sys.exit(1)

    session = get_session_cached(cookie_domain)
    if session:
        use_it = input(
            "Found working cached session. Use it? (Y/n): "
        ).strip().lower()
        if use_it != "n":
            return session

    if manual:
        return get_session_manual(required_cookies, cookie_domain)
    return get_session_auto(auth_url, required_cookies, cookie_domain)


def _build_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        )
    })
    return session


def _save_cookie_cache(cookie_dict):
    with open(COOKIE_CACHE_FILE, "w") as f:
        json.dump(cookie_dict, f, indent=2)


# ============================================================
# URL PARSING & FILE DOWNLOAD
# ============================================================

def parse_url_info(url):
    """Extract a group name and file ID from a URL.

    For DOJ Epstein URLs: returns (DataSet_N, EFTA_ID).
    For other URLs: returns (domain, filename_stem).
    """
    decoded = unquote(url)

    # DOJ Epstein pattern: .../DataSet N/filename.ext
    ds_match = re.search(r"DataSet\s*(\d+)", decoded)
    if ds_match:
        dataset = f"DataSet_{ds_match.group(1)}"
        filename = decoded.split("/")[-1]
        file_id = os.path.splitext(filename)[0]
        return dataset, file_id

    # Generic: use domain as group, filename stem as ID
    parsed = urlparse(url)
    domain = parsed.netloc.replace("www.", "").replace(".", "_")
    filename = decoded.split("/")[-1]
    file_id = os.path.splitext(filename)[0]
    if not file_id:
        file_id = "unknown"
    return domain, file_id


def download_file(session, url, local_path):
    """Download a single file with retries. Returns True on success."""
    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        return True

    encoded_url = url.replace(" ", "%20")
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(encoded_url, timeout=REQUEST_TIMEOUT, stream=True)
            if resp.status_code in (403, 404):
                log.warning(f"{resp.status_code}: {url}")
                return False
            resp.raise_for_status()

            temp_path = local_path + ".tmp"
            with open(temp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                    f.write(chunk)

            if os.path.getsize(temp_path) > 0:
                os.replace(temp_path, local_path)
                return True
            else:
                os.remove(temp_path)
                log.warning(f"Empty response: {url}")
                return False

        except requests.RequestException as e:
            log.warning(f"Attempt {attempt}/{MAX_RETRIES} for {url}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY * attempt)
            for p in [local_path + ".tmp", local_path]:
                if os.path.exists(p):
                    try:
                        os.remove(p)
                    except OSError:
                        pass

    log.error(f"FAILED after {MAX_RETRIES} retries: {url}")
    return False


# ============================================================
# DEHYDRATION (Google Drive online-only)
# ============================================================

def dehydrate_single_file(fpath):
    """Mark one file as online-only (best-effort, Windows only)."""
    try:
        subprocess.run(
            ["attrib", "+U", "-P", fpath],
            capture_output=True, timeout=10,
        )
    except Exception:
        pass


def dehydrate_files(base_dir):
    """Mark all files under base_dir as online-only for Google Drive."""
    log.info(f"Dehydrating files in {base_dir} ...")
    count = errors = 0
    for root, _dirs, files in os.walk(base_dir):
        for fname in files:
            fpath = os.path.join(root, fname)
            try:
                result = subprocess.run(
                    ["attrib", "+U", "-P", fpath],
                    capture_output=True, timeout=10,
                )
                if result.returncode == 0:
                    count += 1
                else:
                    errors += 1
            except Exception:
                errors += 1

    log.info(f"Dehydrated {count} files ({errors} errors)")
    if errors > 0:
        log.info("If automatic dehydration did not work, manually free space:")
        log.info(f"  Right-click '{base_dir}' in Explorer -> 'Free up space'")
    return count


# ============================================================
# DOWNLOAD PAIR (PDF + VIDEO)
# ============================================================

def download_pair(session, pdf_url, video_url, base_dir,
                  video_extensions=None, immediate_dehydrate=False):
    """Download a PDF/video pair into a named folder.

    Tries each video extension in order (e.g. .mov then .mp4).
    If no video is retrieved, the folder is removed entirely.
    """
    if video_extensions is None:
        video_extensions = DEFAULT_VIDEO_EXTENSIONS

    group, file_id = parse_url_info(pdf_url)

    # Check if folder already exists (possibly with a summary suffix)
    group_dir = os.path.join(base_dir, group)
    folder = None
    if os.path.isdir(group_dir):
        try:
            for entry in os.listdir(group_dir):
                if entry == file_id or entry.startswith(file_id + " - "):
                    folder = os.path.join(group_dir, entry)
                    break
        except PermissionError:
            pass

    if folder is None:
        folder = os.path.join(group_dir, file_id)
    os.makedirs(folder, exist_ok=True)

    # Download PDF
    pdf_path = os.path.join(folder, f"{file_id}.pdf")
    pdf_ok = download_file(session, pdf_url, pdf_path)

    # Try companion file extensions in order (.mov, .mp4, .jpg, etc.)
    vid_ok = False
    actual_vid_path = None
    base_video_url = video_url.rsplit(".", 1)[0] if "." in video_url else video_url

    for ext in video_extensions:
        # First attempt: use the original URL for the first extension
        if ext == video_extensions[0]:
            vid_path = os.path.join(folder, f"{file_id}.{ext}")
            vid_ok = download_file(session, video_url, vid_path)
        else:
            alt_url = base_video_url + "." + ext
            vid_path = os.path.join(folder, f"{file_id}.{ext}")
            vid_ok = download_file(session, alt_url, vid_path)

        if vid_ok:
            actual_vid_path = vid_path
            break

    # If no companion file was retrieved, skip this record entirely
    if not vid_ok:
        log.info(f"No companion file found for {file_id} — removing folder")
        shutil.rmtree(folder, ignore_errors=True)
        return False, False, group, file_id

    # Immediately dehydrate to free local space
    if immediate_dehydrate:
        if pdf_ok and os.path.exists(pdf_path):
            dehydrate_single_file(pdf_path)
        if actual_vid_path and os.path.exists(actual_vid_path):
            dehydrate_single_file(actual_vid_path)

    return pdf_ok, vid_ok, group, file_id


# ============================================================
# SUMMARY GENERATION
# ============================================================

def extract_pdf_summary(pdf_path):
    """Extract text from first 1-2 pages of a PDF.

    Returns (short_summary, full_summary).
    """
    try:
        import pdfplumber
    except ImportError:
        return "", ""

    try:
        with pdfplumber.open(pdf_path) as pdf:
            if not pdf.pages:
                return "", ""

            text = ""
            for page in pdf.pages[:2]:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
                if len(text) > 500:
                    break

            if not text.strip():
                return "", ""

            clean = re.sub(r"\s+", " ", text).strip()

            short = clean[:MAX_SUMMARY_IN_FOLDER_NAME].strip()
            if len(clean) > MAX_SUMMARY_IN_FOLDER_NAME:
                last_space = short.rfind(" ")
                if last_space > 20:
                    short = short[:last_space]

            return short, clean[:2000]

    except Exception as e:
        log.debug(f"Summary extraction failed for {pdf_path}: {e}")
        return "", ""


def sanitize_for_filename(text):
    """Remove characters that are invalid in Windows filenames."""
    text = re.sub(r'[\\/:*?"<>|\r\n\t]', "", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = text.rstrip(". ")
    return text


def generate_all_summaries(base_dir, force=False):
    """Walk downloaded folders, extract PDF summaries, rename folders."""
    log.info("Generating summaries from downloaded PDFs...")

    try:
        import pdfplumber  # noqa: F401
    except ImportError:
        log.warning("pdfplumber not installed — skipping summaries.")
        log.warning("Install with:  pip install pdfplumber")
        return

    processed = 0
    for group_name in sorted(os.listdir(base_dir)):
        group_dir = os.path.join(base_dir, group_name)
        if not os.path.isdir(group_dir):
            continue

        for folder_name in sorted(os.listdir(group_dir)):
            folder = os.path.join(group_dir, folder_name)
            if not os.path.isdir(folder):
                continue

            summary_file = os.path.join(folder, "summary.txt")
            if os.path.exists(summary_file) and not force:
                continue

            try:
                pdfs = [
                    f for f in os.listdir(folder)
                    if f.lower().endswith(".pdf")
                ]
            except PermissionError:
                log.debug(
                    f"Permission denied listing {folder} (may be dehydrated)"
                )
                continue
            if not pdfs:
                continue

            pdf_path = os.path.join(folder, pdfs[0])
            file_id = os.path.splitext(pdfs[0])[0]

            short_summary, full_summary = extract_pdf_summary(pdf_path)

            try:
                with open(summary_file, "w", encoding="utf-8") as f:
                    f.write(f"File ID: {file_id}\n")
                    f.write(f"Group: {group_name}\n\n")
                    if full_summary:
                        f.write(full_summary)
                    else:
                        f.write("(No extractable text found in PDF)")
            except PermissionError:
                log.debug(f"Permission denied writing summary in {folder}")
                continue

            if short_summary:
                bare_id = file_id
                if folder_name == bare_id or (force and " - " in folder_name):
                    safe = sanitize_for_filename(short_summary)
                    if safe:
                        new_name = f"{bare_id} - {safe}"
                        new_path = os.path.join(group_dir, new_name)
                        if new_path != folder:
                            try:
                                os.rename(folder, new_path)
                            except OSError as e:
                                log.debug(f"Rename failed for {folder}: {e}")

            processed += 1
            if processed % 50 == 0:
                log.info(f"Summaries: {processed} processed...")

    log.info(f"Summary generation complete: {processed} folders")


# ============================================================
# CLEANUP — REMOVE EMPTY DIRECTORIES
# ============================================================

def cleanup_empty_dirs(base_dir):
    """Remove empty directories under base_dir (bottom-up).

    Walks the tree in reverse depth order so that nested empty dirs
    are removed before their parents are checked.
    """
    removed = 0
    # Walk bottom-up: topdown=False ensures children are visited first
    for root, _dirs, _files in os.walk(base_dir, topdown=False):
        # Don't remove the base_dir itself
        if os.path.normpath(root) == os.path.normpath(base_dir):
            continue
        try:
            if not os.listdir(root):
                os.rmdir(root)
                removed += 1
                log.debug(f"Removed empty directory: {root}")
        except (PermissionError, OSError) as e:
            log.debug(f"Could not remove {root}: {e}")

    if removed > 0:
        log.info(f"Cleaned up {removed} empty directories")
    return removed


# ============================================================
# PROGRESS TRACKING & DELTA DETECTION
# ============================================================

def load_progress():
    """Load set of completed pair keys from disk."""
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r") as f:
                data = json.load(f)
                return set(data.get("completed", []))
        except (json.JSONDecodeError, KeyError):
            pass
    return set()


def save_progress(completed_set):
    """Save completed pair keys to disk."""
    with progress_lock:
        with open(PROGRESS_FILE, "w") as f:
            json.dump({"completed": sorted(completed_set)}, f)


def folder_exists_for_pair(pdf_url, output_dir):
    """Check if a download folder already exists for this URL pair.

    This provides a disk-based delta check: if the folder is on disk,
    the pair was successfully downloaded (even across progress file resets).
    Returns True if a matching folder exists.
    """
    group, file_id = parse_url_info(pdf_url)
    group_dir = os.path.join(output_dir, group)

    if not os.path.isdir(group_dir):
        return False

    try:
        for entry in os.listdir(group_dir):
            if entry == file_id or entry.startswith(file_id + " - "):
                return True
    except PermissionError:
        # Dehydrated folder — assume it exists
        return True

    return False


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Bulk downloader for paired PDF/video files from a "
            "cookie-authenticated website.  Reads URL pairs from a "
            "Google Sheet or CSV, downloads into organized folders, "
            "and extracts PDF summaries."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  %(prog)s --manual\n"
            "  %(prog)s --sheet-id ABCDEF123 --output ./downloads\n"
            "  %(prog)s --csv urls.csv --output ./downloads --manual\n"
            "  %(prog)s --resummarize --output ./downloads\n"
            "  %(prog)s --dehydrate-only --output ./downloads\n"
        ),
    )

    # ── URL source ─────────────────────────────────────────
    src = parser.add_argument_group("URL source (pick one)")
    src.add_argument(
        "--sheet-id", type=str, default=None,
        help="Google Sheet ID to fetch URL pairs from",
    )
    src.add_argument(
        "--sheet-url", type=str, default=None,
        help="Full Google Sheet URL (sheet ID is extracted automatically)",
    )
    src.add_argument(
        "--csv", type=str, default=None, dest="csv_file",
        help="Path to a local CSV file with URL pairs",
    )
    src.add_argument(
        "--pdf-col", type=str, default=DEFAULT_PDF_COLUMN,
        help=f'CSV column name for PDF URLs (default: "{DEFAULT_PDF_COLUMN}")',
    )
    src.add_argument(
        "--video-col", type=str, default=DEFAULT_VIDEO_COLUMN,
        help=f'CSV column name for video URLs (default: "{DEFAULT_VIDEO_COLUMN}")',
    )

    # ── Authentication ─────────────────────────────────────
    auth = parser.add_argument_group("Authentication")
    auth.add_argument(
        "--manual", action="store_true",
        help="Paste cookies manually (no Selenium needed)",
    )
    auth.add_argument(
        "--no-prompt", action="store_true",
        help="Run non-interactively (auto-use cached session)",
    )
    auth.add_argument(
        "--auth-url", type=str, default=DEFAULT_AUTH_URL,
        help=f"URL to open for authentication (default: {DEFAULT_AUTH_URL})",
    )
    auth.add_argument(
        "--cookie-domain", type=str, default=DEFAULT_COOKIE_DOMAIN,
        help=f"Cookie domain for auth (default: {DEFAULT_COOKIE_DOMAIN})",
    )
    auth.add_argument(
        "--cookie-names", type=str, default=None,
        help=(
            "Comma-separated required cookie names "
            "(default: DOJ Epstein cookies)"
        ),
    )

    # ── Download options ───────────────────────────────────
    dl = parser.add_argument_group("Download options")
    dl.add_argument(
        "--output", type=str, default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    dl.add_argument(
        "--workers", type=int, default=MAX_DOWNLOAD_WORKERS,
        help=f"Parallel download threads (default: {MAX_DOWNLOAD_WORKERS})",
    )
    dl.add_argument(
        "--limit", type=int, default=0,
        help="Process only the first N pairs (0 = all)",
    )
    dl.add_argument(
        "--force", action="store_true",
        help="Force re-download of all pairs (ignore progress and existing folders)",
    )
    dl.add_argument(
        "--video-extensions", type=str, default=None,
        help=(
            "Comma-separated companion file extensions to try in order "
            "(default: mov,mp4,wmv,avi,3gp,3g2,m4v,mpg,flv,webm,"
            "jpg,jpeg,png,gif,bmp,tiff,tif,webp,heic,svg)"
        ),
    )

    # ── Post-processing ────────────────────────────────────
    post = parser.add_argument_group("Post-processing")
    post.add_argument(
        "--no-summary", action="store_true",
        help="Skip PDF summary extraction (download only)",
    )
    post.add_argument(
        "--resummarize", action="store_true",
        help="Re-generate summaries for all folders (no downloading)",
    )
    post.add_argument(
        "--dehydrate", action="store_true",
        help="Mark files as online-only after download to free local space",
    )
    post.add_argument(
        "--dehydrate-only", action="store_true",
        help="Only dehydrate existing files (no downloading)",
    )
    post.add_argument(
        "--refresh-urls", action="store_true",
        help="Re-fetch URL list from Google Sheet (ignore local cache)",
    )

    args = parser.parse_args()

    # ── Resolve sheet ID from URL if needed ────────────────
    sheet_id = args.sheet_id
    if not sheet_id and args.sheet_url:
        m = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", args.sheet_url)
        if m:
            sheet_id = m.group(1)
        else:
            log.error(f"Could not extract sheet ID from URL: {args.sheet_url}")
            sys.exit(1)

    # Require a URL source
    if not sheet_id and not args.csv_file:
        log.error(
            "No URL source provided. Use --sheet-id, --sheet-url, or --csv."
        )
        sys.exit(1)

    # ── Resolve required cookies ───────────────────────────
    required_cookies = DEFAULT_REQUIRED_COOKIES
    if args.cookie_names:
        required_cookies = [
            c.strip() for c in args.cookie_names.split(",") if c.strip()
        ]

    # ── Resolve video extensions ───────────────────────────
    video_extensions = DEFAULT_VIDEO_EXTENSIONS
    if args.video_extensions:
        video_extensions = [
            e.strip().lstrip(".")
            for e in args.video_extensions.split(",")
            if e.strip()
        ]

    output_dir = args.output

    print("=" * 60)
    print("BULK FILE DOWNLOADER v3")
    print(f"Output: {output_dir}")
    print("=" * 60)

    # ── Special modes (no downloading) ─────────────────────

    if args.dehydrate_only:
        dehydrate_files(output_dir)
        return

    if args.resummarize:
        generate_all_summaries(output_dir, force=True)
        return

    # ── Phase 1: Get URL pairs ─────────────────────────────

    if args.refresh_urls and os.path.exists(LOCAL_CSV_CACHE):
        os.remove(LOCAL_CSV_CACHE)
        log.info("Cleared URL cache — will re-fetch from Google Sheet")

    pairs = fetch_url_pairs(
        sheet_id=sheet_id,
        csv_path=args.csv_file,
        pdf_col=args.pdf_col,
        video_col=args.video_col,
    )
    if not pairs:
        log.error("No URL pairs found. Check the Google Sheet or CSV file.")
        sys.exit(1)

    if args.limit > 0:
        pairs = pairs[: args.limit]
        log.info(f"Limited to first {args.limit} pairs")

    # ── Phase 2: Authenticate ──────────────────────────────

    session = get_session(
        manual=args.manual,
        auto_use_cache=args.no_prompt,
        auth_url=args.auth_url,
        required_cookies=required_cookies,
        cookie_domain=args.cookie_domain,
    )

    # ── Phase 3: Download all pairs (parallel) ─────────────

    completed = load_progress()

    if args.force:
        log.info("--force: ignoring progress file and existing folders")
        remaining = list(pairs)
        completed.clear()
        save_progress(completed)
    else:
        # Skip pairs that are in the progress file OR have a folder on disk.
        # The folder check acts as an automatic delta — if the folder exists
        # from a previous run (even if progress.json was reset), we skip it.
        remaining = []
        for p, m in pairs:
            pair_key = f"{parse_url_info(p)[0]}/{parse_url_info(p)[1]}"
            if pair_key in completed:
                continue
            if folder_exists_for_pair(p, output_dir):
                # Folder exists on disk but wasn't in progress file — sync it
                completed.add(pair_key)
                continue
            remaining.append((p, m))
        # Persist any newly-discovered completions
        if len(completed) > len(load_progress()):
            save_progress(completed)

    already_done = len(pairs) - len(remaining)
    log.info(
        f"Total: {len(pairs)} | Already done: {already_done} | "
        f"To download: {len(remaining)}"
    )

    if remaining:
        succeeded = failed = 0
        save_every = 25

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {}
            for pdf_url, vid_url in remaining:
                future = executor.submit(
                    download_pair, session, pdf_url, vid_url, output_dir,
                    video_extensions=video_extensions,
                    immediate_dehydrate=args.dehydrate,
                )
                futures[future] = (pdf_url, vid_url)

            for i, future in enumerate(as_completed(futures), 1):
                pdf_url, vid_url = futures[future]
                try:
                    pdf_ok, vid_ok, group, file_id = future.result()
                    pair_key = f"{group}/{file_id}"
                    if pdf_ok or vid_ok:
                        with progress_lock:
                            completed.add(pair_key)
                        succeeded += 1
                    else:
                        failed += 1
                except Exception as e:
                    log.error(f"Exception: {e}")
                    failed += 1

                if i % 10 == 0 or i == len(futures):
                    log.info(
                        f"Downloads: {i}/{len(futures)} | "
                        f"OK={succeeded} Failed={failed}"
                    )
                if i % save_every == 0:
                    save_progress(completed)

        save_progress(completed)
        log.info(f"Download phase complete.  OK={succeeded}  Failed={failed}")

    # ── Phase 4: Generate summaries ────────────────────────

    if not args.no_summary:
        generate_all_summaries(output_dir)

    # ── Phase 5: Dehydrate (optional) ──────────────────────

    if args.dehydrate:
        log.info(
            f"Waiting {DEHYDRATE_WAIT_SECONDS}s for Google Drive to sync "
            "before dehydrating..."
        )
        time.sleep(DEHYDRATE_WAIT_SECONDS)
        dehydrate_files(output_dir)

    # ── Cleanup: Remove empty directories ──────────────────

    cleanup_empty_dirs(output_dir)

    # ── Done ───────────────────────────────────────────────

    print("\n" + "=" * 60)
    print("DONE!")
    print(f"Files saved to: {output_dir}")
    if not args.dehydrate:
        print(
            "Tip: run with --dehydrate to free local disk space after sync"
        )
    print(
        "Tip: run with --resummarize to regenerate folder summaries later"
    )
    print("=" * 60)


if __name__ == "__main__":
    main()
