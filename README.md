# Epstein Library Scraper & Downloader

A toolkit for scraping and downloading files from the DOJ Epstein Library — the public repository of documents released by the U.S. Department of Justice related to the Jeffrey Epstein case.

## Prerequisites

### Required

| Dependency | Version | Purpose |
|------------|---------|---------|
| **Python** | 3.8+ | Runtime for all scripts |
| **Google Chrome** | Latest stable | Browser-based downloads bypass DOJ's Akamai bot detection |
| **Selenium** | 4.x+ | Controls Chrome via Chrome DevTools Protocol (CDP) |

### Python Packages

```bash
pip install -r requirements.txt
```

Or install manually:

```bash
pip install requests pdfplumber selenium
```

| Package | Required? | Purpose |
|---------|-----------|---------|
| `requests` | Yes | HTTP client (fallback download mode) |
| `pdfplumber` | Yes | PDF text extraction for folder summaries |
| `selenium` | Yes | Chrome automation via CDP for browser-based downloads |

### Why Chrome Is Required

The DOJ Epstein Library uses **Akamai Bot Manager** which blocks all non-browser HTTP clients (`requests`, `curl`, `wget`, etc.) with HTTP 403 responses. The only way to download files is through a real browser session.

This project uses **Chrome DevTools Protocol (CDP)** to:
1. Launch Chrome with remote debugging enabled (`--remote-debugging-port=9222`)
2. Connect via Selenium WebDriver
3. Execute `fetch()` API calls inside the browser context
4. Transfer file data back to Python for saving to disk

This bypasses Akamai because all HTTP requests originate from a genuine Chrome browser with a real fingerprint.

### Optional

| Dependency | Purpose |
|------------|---------|
| **Google Drive for Desktop** | Mount Google Drive as a local drive letter for direct file saving + dehydration |

## Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/YOUR_USERNAME/EpsteinScraper.git
cd EpsteinScraper

# 2. Install dependencies
pip install -r requirements.txt

# 3. Verify Chrome CDP connection works
python refresh_cookies.py

# 4. Run the scraper to collect all URLs (creates all_urls_YYYYMMDD.csv)
python scrape_all_urls.py --query "*" --output all_urls.csv

# 5. Download all files
python epstein_downloader.py --csv all_urls.csv --output ./Epstein_Library

# 6. (Optional) Free local disk space on a Google Drive mount
python epstein_downloader.py --dehydrate-only --output ./Epstein_Library
```

## How It Works

### Phase 1: Scraping URLs

`scrape_all_urls.py` connects to Chrome via CDP and uses the DOJ's internal Elasticsearch API (`/multimedia-search`) to collect all PDF/video URL pairs. It runs 20 broad search queries to maximize coverage:

```bash
# Scrape all URLs (wildcard mode — 20 queries covering the full library)
python scrape_all_urls.py --query "*" --output all_urls.csv

# Scrape a specific search term
python scrape_all_urls.py --query "no images produced" --output nip_urls.csv

# Delta scrape: merge new results with an existing CSV
python scrape_all_urls.py --query "*" --output all_urls_20260301.csv --merge all_urls.csv
```

**Note:** Chrome must be running or the script will launch it automatically. If the DOJ site presents a Queue-IT challenge page, the API calls still work — Queue-IT only blocks page navigation, not same-origin `fetch()` requests.

### Phase 2: Downloading Files

`epstein_downloader.py` downloads each PDF + companion media file pair into organized folders:

```bash
# Download all files using browser-based fetch (default)
python epstein_downloader.py --csv all_urls.csv --output ./Epstein_Library

# Download to a Google Drive mount with auto-dehydration
python epstein_downloader.py --csv all_urls.csv --output "I:/My Drive/Epstein_Library" --dehydrate

# Resume an interrupted download
python epstein_downloader.py --csv all_urls.csv --output ./Epstein_Library

# Fallback: use requests library (only works if Akamai is not blocking)
python epstein_downloader.py --csv all_urls.csv --output ./Epstein_Library --no-browser --manual
```

The downloader runs in 5 phases:

1. **URL Pairs** — Reads PDF/video URL pairs from a local CSV or Google Sheet
2. **Authentication** — Connects to Chrome via CDP (or establishes a cookie session with `--no-browser`)
3. **Download** — Downloads each PDF + companion media. In browser mode, downloads are sequential (CDP is single-threaded). In `--no-browser` mode, uses 5 parallel threads.
4. **Summaries** — Extracts text from the first page of each PDF, renames the folder to include a short summary
5. **Dehydration** — (Optional) Marks files as online-only on Google Drive to free local disk space

### Output Structure

```
Epstein_Library/
    DataSet_8/
        EFTA00033009 - INTERVIEW OF John Doe conducted/
            EFTA00033009.pdf
            EFTA00033009.mov
            summary.txt
    DataSet_9/
        ...
    DataSet_10/
        ...
    Court Records/
        Giuffre v. Maxwell, No. 115-cv-07433 (S.D.N.Y. 2015)/
            EFTA02731957/
                EFTA02731957.pdf
                summary.txt
        United States v. Maxwell, No. 120-cr-00330 (S.D.N.Y. 2020)/
            ...
```

## Features

- **Browser-based downloads** — Bypasses Akamai bot detection using Chrome CDP
- **Resume support** — Progress saved to `download_progress.json`; re-running skips completed pairs
- **Delta detection** — Scans existing folders on disk for fast skip (works even after progress file reset)
- **Multiple video formats** — Tries 20 extensions: mov, mp4, wmv, avi, 3gp, 3g2, m4v, mpg, flv, webm, jpg, jpeg, png, gif, bmp, tiff, tif, webp, heic, svg
- **PDF summaries** — First-page text extraction for folder naming and `summary.txt` files
- **Google Drive dehydration** — `attrib +U -P` to mark files as online-only after upload
- **Versioned delta scrapes** — Scraper supports `--merge` to combine new results with existing data

## Dataset Statistics

| Metric | Count |
|--------|-------|
| Total URL pairs (as of Mar 2026) | 104,049 |
| DataSet files | ~80,000 |
| Court Records files | ~24,000 |
| Distinct court cases | 89 |
| Distinct DataSets | 8, 9, 10, 11 |

## Files

| File | Description |
|------|-------------|
| `epstein_downloader.py` | Main downloader (v4 — browser-based) |
| `scrape_all_urls.py` | URL scraper using DOJ Elasticsearch API via CDP |
| `browser_utils.py` | Shared Chrome CDP launch/connect module |
| `refresh_cookies.py` | Chrome CDP connection verifier |
| `all_urls.csv` | URL dataset (80K pairs, Feb 2026 baseline) |
| `all_urls_20260301.csv` | URL dataset (104K pairs, Mar 2026 delta) |
| `requirements.txt` | Python dependencies |

Auto-generated files (gitignored):

| File | Description |
|------|-------------|
| `session_cookies.json` | Cached auth cookies |
| `download_progress.json` | Download progress tracker |
| `urls_cache.csv` | Cached URL list from Google Sheet |
| `epstein_downloader.log` | Log file |

## Full CLI Reference

### epstein_downloader.py

```
URL source:
  --csv FILE             Local CSV file path
  --sheet-id ID          Google Sheet ID
  --sheet-url URL        Full Google Sheet URL
  --pdf-col NAME         PDF column name (default: "PDF URL")
  --video-col NAME       Video column name (default: "MOV URL")

Authentication:
  --no-browser           Use requests library instead of Chrome CDP
  --manual               Paste cookies manually (--no-browser mode)
  --no-prompt            Use cached session, non-interactive (--no-browser mode)
  --auth-url URL         Auth page URL
  --cookie-domain DOMAIN Cookie domain
  --cookie-names LIST    Required cookie names (comma-separated)

Download options:
  --output DIR           Output directory (default: ./Epstein_Library)
  --workers N            Parallel threads for --no-browser mode (default: 5)
  --limit N              Process only first N pairs
  --force                Ignore progress, re-download everything
  --video-extensions EXT Comma-separated extensions to try

Post-processing:
  --no-summary           Skip PDF summary extraction
  --resummarize          Re-generate summaries only
  --dehydrate            Mark files online-only after download
  --dehydrate-only       Only dehydrate (no downloading)
  --refresh-urls         Re-fetch URL list from Google Sheet
```

### scrape_all_urls.py

```
  --query QUERY          Search query (use "*" for wildcard/all)
  --output FILE          Output CSV file path
  --merge FILE           Merge with existing CSV (for delta scrapes)
```

## Troubleshooting

### "Chrome not found" error
Install Google Chrome from https://www.google.com/chrome/. The script looks for Chrome at standard Windows installation paths.

### Chrome opens but nothing happens
Chrome must be launched with remote debugging enabled. The scripts handle this automatically via `browser_utils.py`. If Chrome is already running normally, the script will close it first and relaunch with debugging flags.

### HTTP 403 errors on downloads
This means Akamai is blocking the request. Make sure you're using the default browser mode (not `--no-browser`). The browser-based `fetch()` approach bypasses Akamai.

### Queue-IT challenge page
The DOJ site uses Queue-IT for bot detection on page navigation. However, API `fetch()` calls bypass Queue-IT entirely. You may see the Queue-IT page in Chrome, but downloads will still work.

### Slow folder scanning on Google Drive
The initial scan of existing folders can be slow on Google Drive mounts. The script caches directory listings to minimize this. Subsequent runs use the progress file for instant skip.

## License

MIT
