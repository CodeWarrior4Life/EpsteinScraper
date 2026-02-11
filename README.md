# Epstein Library Scraper & Downloader

A toolkit for scraping and downloading files from the DOJ Epstein Library — the public repository of documents released by the U.S. Department of Justice related to the Jeffrey Epstein case.

This project consists of two phases:

1. **Scraping** — Collecting PDF and video file URLs from the DOJ search interface into a Google Sheet
2. **Downloading** — Bulk-downloading paired PDF + video files into organized, summarized folders

## Background

The DOJ Epstein Library is hosted at [justice.gov/epstein](https://www.justice.gov/epstein). It contains thousands of documents across multiple DataSets (8, 9, 10, etc.). The "No Images Produced" collection alone has ~2,760 unique document pairs. Each pair consists of a PDF file and a corresponding video file (MOV, MP4, 3GP, or other formats).

## Prerequisites

### Python (for Phase 2 — Downloading)

- Python 3.8+
- `pip install -r requirements.txt` (installs `requests` and `pdfplumber`)
- Optionally: `pip install selenium` for automatic cookie capture

### Claude Browser Extension (for Phase 1 — Scraping)

The scraping phase uses an AI assistant to automate collecting URLs from the DOJ search interface. Install the **Claude** browser extension:

1. Open **Google Chrome** (or any Chromium-based browser)
2. Go to the [Chrome Web Store](https://chromewebstore.google.com/) and search for **"Claude"** by Anthropic
3. Click **"Add to Chrome"** → **"Add extension"**
4. Click the Extensions puzzle icon in the toolbar and pin Claude for easy access
5. Click the Claude icon and sign in with your Anthropic account (create one at [claude.ai](https://claude.ai) if needed)
6. Navigate to the DOJ Epstein Library page and use Claude to help you run the scraping JavaScript described below

Alternatively, you can run the JavaScript snippets manually in the browser console (`F12` → Console tab).

## Phase 1: Scraping URLs into a Google Sheet

### How the DOJ Search Works

- The Epstein Library is a Drupal-based site with an age verification gate
- The search box is labeled "Search Full Epstein Library" (input `#searchInput`, button `#searchButton`)
- This is separate from the top-of-page DOJ site search bar
- Results are JavaScript-rendered, paginated 10 per page
- Each result links to a `.pdf` file like: `https://www.justice.gov/epstein/files/DataSet 8/EFTA00024813.pdf`

### Scraping Strategy

Use the Claude browser extension (or JavaScript in the browser console) to:

1. Navigate to [justice.gov/epstein](https://www.justice.gov/epstein) and pass the age verification gate
2. Enter search query (e.g. "no images produced") in `#searchInput` and click `#searchButton`
3. Collect all PDF link `href` attributes from the `#results` div
4. Click the "Next" pagination button to advance
5. Repeat until all pages are scraped
6. Deduplicate using a Set (~3,797 raw results → ~2,760 unique)
7. Generate video URLs by replacing `.pdf` with `.mov`
8. Paste the URL pairs into a Google Sheet

### Google Sheet Format

| Column A (Header: `PDF URL`) | Column B (Header: `MOV URL`) |
|------|------|
| `https://www.justice.gov/epstein/files/DataSet 8/EFTA00024813.pdf` | `https://www.justice.gov/epstein/files/DataSet 8/EFTA00024813.mov` |

Share the sheet as "Anyone with the link" so the downloader can export it as CSV without API credentials.

### Expected Data Summary

| Item | Details |
|------|---------|
| Search query | "no images produced" |
| Raw results | ~3,797 |
| Unique PDF URLs | ~2,760 |
| DataSets covered | 2, 8, 9, 10, 11 |
| DataSet breakdown | DS2: 1, DS8: 340, DS9: 1,749, DS10: 666, DS11: 4 |

## Phase 2: Downloading Files

### Authentication

The DOJ site requires passing a Queue-IT bot challenge and age verification gate before downloads work. The script needs the resulting session cookies. Three options:

1. **Automatic (Selenium):** Script opens Chrome, you pass the challenges, press ENTER
2. **Manual (`--manual`):** Copy cookies from browser DevTools (`F12 → Console → document.cookie`) and paste them
3. **Cached (`--no-prompt`):** Reuse previously saved cookies from `session_cookies.json`

### Quick Start

```bash
# Test with 5 pairs using manual cookie entry
python epstein_downloader.py --sheet-id YOUR_SHEET_ID --output ./test --limit 5 --manual

# Full download with dehydration (Google Drive mount)
python epstein_downloader.py --sheet-id YOUR_SHEET_ID --output ./Epstein_Library --dehydrate

# Resume an interrupted download (non-interactive)
python epstein_downloader.py --no-prompt

# Re-generate folder summaries
python epstein_downloader.py --resummarize

# Free disk space on Google Drive mount
python epstein_downloader.py --dehydrate-only
```

### How It Works

The downloader runs in 5 phases:

1. **URL Pairs** — Reads PDF/video URL pairs from a Google Sheet (exported as CSV) or a local CSV file
2. **Authentication** — Establishes a session with the DOJ site using cookies
3. **Parallel Download** — Downloads each PDF + video pair into its own folder, trying multiple video extensions (mov, mp4, wmv, avi, 3gp, 3g2, m4v, mpg, flv, webm). If no video is found in any format, the folder is removed.
4. **Summaries** — Extracts text from the first page of each PDF and renames the folder to include a short summary
5. **Dehydration** — (Optional) Marks files as online-only on Google Drive to free local disk space
6. **Cleanup** — Removes any empty directories left behind

### Output Structure

```
Epstein_Library/
    DataSet_8/
        EFTA00033009 - INTERVIEW OF John Doe conducted/
            EFTA00033009.pdf
            EFTA00033009.mov
            summary.txt
        EFTA00033010 - FEDERAL BUREAU OF INVESTIGATION/
            EFTA00033010.pdf
            EFTA00033010.mp4
            summary.txt
    DataSet_9/
        ...
    DataSet_10/
        ...
```

### Features

- **Resume support** — Progress is saved to `download_progress.json`; re-running skips completed pairs
- **Delta detection** — Checks for existing folders on disk, so it works even after a progress file reset
- **Multiple video formats** — Tries 10 extensions before giving up (mov, mp4, wmv, avi, 3gp, 3g2, m4v, mpg, flv, webm)
- **PDF summaries** — First-page text extraction for folder naming and `summary.txt` files
- **Google Drive dehydration** — `attrib +U -P` to mark files as online-only after upload
- **Modular CLI** — All parameters are configurable; works with any Google Sheet or CSV, any cookie-auth site
- **Force re-download** — `--force` flag to re-download everything from scratch

### Retry Script

For pairs that were skipped (no video found), `retry_skipped.py` compares the spreadsheet against existing folders and retries just the missing ones:

```bash
# Dry run — see what would be retried
python retry_skipped.py --dry-run

# Retry with cached session
python retry_skipped.py --no-prompt --dehydrate

# Retry with custom video extensions
python retry_skipped.py --no-prompt --video-extensions "mov,mp4,wmv,3gp,avi"
```

### Full CLI Reference

Run `python epstein_downloader.py --help` for all options:

```
URL source:
  --sheet-id ID          Google Sheet ID
  --sheet-url URL        Full Google Sheet URL
  --csv FILE             Local CSV file path
  --pdf-col NAME         PDF column name (default: "PDF URL")
  --video-col NAME       Video column name (default: "MOV URL")

Authentication:
  --manual               Paste cookies manually
  --no-prompt            Use cached session (non-interactive)
  --auth-url URL         Auth page URL
  --cookie-domain DOMAIN Cookie domain
  --cookie-names LIST    Required cookie names (comma-separated)

Download options:
  --output DIR           Output directory
  --workers N            Parallel threads (default: 5)
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

## Using With Other Data Sources

The downloader is generic enough to work with any paired-file download scenario:

```bash
# Custom spreadsheet with different column names
python epstein_downloader.py \
    --sheet-id ABCDEF123 \
    --pdf-col "Document URL" \
    --video-col "Recording URL" \
    --output ./my_downloads \
    --auth-url "https://example.com/login" \
    --cookie-domain ".example.com" \
    --manual

# Local CSV file, no auth needed (just remove cookie requirements)
python epstein_downloader.py \
    --csv my_urls.csv \
    --output ./downloads \
    --no-prompt
```

## Files

| File | Description |
|------|-------------|
| `epstein_downloader.py` | Main downloader script (v3) |
| `retry_skipped.py` | Retry script for previously skipped pairs |
| `requirements.txt` | Python dependencies |
| `session_cookies.json` | Cached auth cookies (auto-generated, gitignored) |
| `download_progress.json` | Download progress tracker (auto-generated, gitignored) |
| `urls_cache.csv` | Cached URL list from Google Sheet (auto-generated, gitignored) |
| `epstein_downloader.log` | Log file (auto-generated, gitignored) |

## License

MIT
