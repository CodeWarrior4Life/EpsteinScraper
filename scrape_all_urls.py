"""
scrape_all_urls.py — Enumerate ALL files in the DOJ Epstein Library

Uses Selenium to drive the DOJ search interface and paginate through
every result, collecting all PDF URLs.  Supports filtering by search
query or scraping the entire library.

Requires: pip install selenium
          Chrome browser + chromedriver

Usage:
    # Scrape EVERYTHING in the library
    python scrape_all_urls.py --query "*"

    # Scrape only media-related results
    python scrape_all_urls.py --query "no images produced"

    # Merge with existing CSV (deduplicates)
    python scrape_all_urls.py --query "*" --merge urls_cache.csv

    # Media-only mode (skip PDFs without companion files)
    python scrape_all_urls.py --query "*" --media-only
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
import time

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────
COOKIE_CACHE_FILE = "session_cookies.json"
EPSTEIN_URL = "https://www.justice.gov/epstein"
SEARCH_API = "https://www.justice.gov/multimedia-search"


def load_cookies():
    """Load cached cookies dict from session_cookies.json."""
    if not os.path.exists(COOKIE_CACHE_FILE):
        return None
    with open(COOKIE_CACHE_FILE, "r") as f:
        return json.load(f)


def setup_driver(headless=False):
    """Create a Selenium Chrome driver."""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
    except ImportError:
        log.error("Selenium not installed. Run: pip install selenium")
        sys.exit(1)

    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    if headless:
        options.add_argument("--headless=new")

    try:
        driver = webdriver.Chrome(options=options)
    except Exception as e:
        log.error(f"Could not start Chrome: {e}")
        sys.exit(1)

    return driver


def authenticate(driver):
    """
    Navigate to the Epstein Library, inject cached cookies if available,
    or let the user pass challenges manually.
    """
    driver.get(EPSTEIN_URL)
    time.sleep(2)

    # Try injecting cached cookies
    cached = load_cookies()
    if cached:
        log.info("Injecting cached cookies...")
        for name, value in cached.items():
            try:
                driver.add_cookie({
                    "name": name,
                    "value": value,
                    "domain": ".justice.gov",
                    "path": "/",
                })
            except Exception:
                pass
        driver.refresh()
        time.sleep(2)

    # Check if we need manual intervention (age gate, Queue-IT, etc.)
    # The search input should be visible if auth is good
    from selenium.webdriver.common.by import By
    try:
        driver.find_element(By.CSS_SELECTOR, "#searchInput")
        log.info("Search interface accessible — authentication OK")
        return True
    except Exception:
        pass

    # Need manual auth
    print("\n" + "=" * 60)
    print("BROWSER SESSION — PASS THE SITE CHALLENGES")
    print("=" * 60)
    print(f"""
The DOJ Epstein Library requires:
  1. Queue-IT bot challenge (wait in queue)
  2. Age verification (click "Yes")

Complete these in the Chrome window, then come back
here and press ENTER once you see the search box.
""")
    input(">>> Press ENTER after passing all challenges... ")

    # Save the new cookies
    browser_cookies = driver.get_cookies()
    cookie_dict = {}
    for c in browser_cookies:
        cookie_dict[c["name"]] = c["value"]
    with open(COOKIE_CACHE_FILE, "w") as f:
        json.dump(cookie_dict, f, indent=2)
    log.info(f"Saved {len(cookie_dict)} cookies to {COOKIE_CACHE_FILE}")

    return True


def scrape_with_api(driver, query="*"):
    """
    Use the DOJ multimedia-search JSON API by executing fetch() inside
    the Selenium browser.  This keeps the browser's full cookie jar
    intact (avoids 403 from incomplete cookie transfer to requests).
    """
    all_urls = set()
    page = 0
    total_pages = None
    MAX_PAGES = 5000  # Safety limit to prevent infinite loops
    consecutive_empty = 0  # Track pages with no new URLs

    log.info(f"Querying DOJ API via browser fetch (query={query!r})...")

    # Make sure we're on the justice.gov domain so fetch is same-origin
    if "justice.gov" not in driver.current_url:
        driver.get(EPSTEIN_URL)
        time.sleep(2)

    # URL-encode the query for use in fetch()
    from urllib.parse import quote
    encoded_query = quote(query, safe="")

    while True:
        try:
            raw = driver.execute_async_script(f"""
                const callback = arguments[arguments.length - 1];
                (async () => {{
                    try {{
                        const resp = await fetch(
                            "/multimedia-search?keys={encoded_query}&page={page}",
                            {{ credentials: "same-origin" }}
                        );
                        if (!resp.ok) {{
                            callback(JSON.stringify({{error: resp.status}}));
                            return;
                        }}
                        callback(await resp.text());
                    }} catch(e) {{
                        callback(JSON.stringify({{error: e.message}}));
                    }}
                }})();
            """)
        except Exception as e:
            log.error(f"Browser fetch failed on page {page}: {e}")
            return None

        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            log.warning(f"Non-JSON response on page {page}")
            return None

        if "error" in data:
            log.warning(f"API error on page {page}: {data['error']}")
            return None

        # Elasticsearch response: {"hits": {"total": {"value": N}, "hits": [{"_source": {...}}]}}
        outer_hits = data.get("hits", data)
        if isinstance(outer_hits, dict):
            total_obj = outer_hits.get("total", {})
            total = total_obj.get("value", 0) if isinstance(total_obj, dict) else total_obj
            hits = outer_hits.get("hits", [])
        else:
            total = data.get("total", 0)
            hits = outer_hits if isinstance(outer_hits, list) else []

        if total_pages is None:
            total_pages = (int(total) + 9) // 10
            log.info(f"Total results: {total} ({total_pages} pages)")

            # Diagnostic: log the structure of the first hit
            if hits:
                first_hit = hits[0]
                if isinstance(first_hit, dict):
                    source = first_hit.get("_source", first_hit)
                    if isinstance(source, dict):
                        log.info(f"First hit _source keys: {sorted(source.keys())}")
                        uri = source.get("ORIGIN_FILE_URI", "<missing>")
                        name = source.get("ORIGIN_FILE_NAME", "<missing>")
                        log.info(f"First hit: NAME={name!r}, URI={uri!r}")
                    else:
                        log.info(f"First hit _source is {type(source).__name__}: {str(source)[:200]}")
                else:
                    log.info(f"First hit is {type(first_hit).__name__}: {str(first_hit)[:200]}")

        if not hits:
            log.info(f"No hits on page {page} — stopping pagination")
            break

        before = len(all_urls)
        for hit in hits:
            source = hit.get("_source", hit) if isinstance(hit, dict) else {}
            if isinstance(source, dict):
                url = (
                    source.get("ORIGIN_FILE_URI", "")
                    or source.get("url", "")
                    or source.get("href", "")
                )
                if url:
                    all_urls.add(url)
            elif isinstance(hit, str) and hit.startswith("http"):
                all_urls.add(hit)

        new_this_page = len(all_urls) - before
        if new_this_page == 0:
            consecutive_empty += 1
        else:
            consecutive_empty = 0

        page += 1
        if page % 25 == 0:
            log.info(f"  Page {page}/{total_pages} — {len(all_urls)} unique URLs")

        # Break conditions
        if total_pages is not None and total_pages > 0 and page >= total_pages:
            break

        if page >= MAX_PAGES:
            log.warning(f"Reached max page limit ({MAX_PAGES})")
            break

        # If we've gone 50+ pages with no new URLs, something is wrong
        if consecutive_empty >= 50:
            log.warning(f"50 consecutive pages with no new URLs — aborting query")
            break

        time.sleep(0.2)

    if not all_urls:
        log.warning("API returned 0 results — will fall back to Selenium")
        return None

    log.info(f"API scrape complete: {len(all_urls)} unique URLs")
    return all_urls


def scrape_with_selenium(driver, query="*"):
    """
    Fall back to scraping via the browser DOM if the API fails.
    Slower but works if API returns 403.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    all_urls = set()

    # Enter search query
    search_input = driver.find_element(By.CSS_SELECTOR, "#searchInput")
    search_input.clear()
    search_input.send_keys(query)
    driver.find_element(By.CSS_SELECTOR, "#searchButton").click()

    time.sleep(3)  # Wait for initial results

    page = 0
    while True:
        page += 1

        # Collect all links from results
        try:
            results_div = driver.find_element(By.CSS_SELECTOR, "#results")
            links = results_div.find_elements(By.TAG_NAME, "a")
            for link in links:
                href = link.get_attribute("href") or ""
                if href and "/epstein/files/" in href:
                    all_urls.add(href)
        except Exception as e:
            log.warning(f"Error reading results on page {page}: {e}")

        if page % 25 == 0:
            log.info(f"  Page {page} — {len(all_urls)} unique URLs so far")

        # Try to click Next
        try:
            next_btn = driver.find_element(By.LINK_TEXT, "Next")
            driver.execute_script("arguments[0].click();", next_btn)
            time.sleep(1.5)
        except Exception:
            log.info(f"No more pages after page {page}")
            break

    log.info(f"Selenium scrape complete: {len(all_urls)} unique URLs")
    return all_urls


def generate_companion_url(url):
    """Generate the default companion file URL (.mov) from any URL."""
    base = url.rsplit(".", 1)[0] if "." in url else url
    return base + ".mov"


def classify_url(url):
    """Determine file type from URL."""
    lower = url.lower()
    if lower.endswith(".pdf"):
        return "pdf"
    for ext in ["mov", "mp4", "wmv", "avi", "3gp", "3g2", "m4v", "mpg", "flv", "webm"]:
        if lower.endswith(f".{ext}"):
            return "video"
    for ext in ["jpg", "jpeg", "png", "gif", "bmp", "tiff", "tif", "webp", "heic", "svg"]:
        if lower.endswith(f".{ext}"):
            return "image"
    return "other"


def write_csv(url_pairs, output_path, pdf_col="PDF URL", video_col="MOV URL"):
    """Write URL pairs to CSV."""
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([pdf_col, video_col])
        for pdf_url, companion_url in sorted(url_pairs):
            writer.writerow([pdf_url, companion_url])

    log.info(f"Wrote {len(url_pairs)} pairs to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Scrape all file URLs from the DOJ Epstein Library",
    )
    parser.add_argument(
        "-o", "--output", type=str, default="all_urls.csv",
        help="Output CSV path (default: all_urls.csv)",
    )
    parser.add_argument(
        "--query", type=str, default="*",
        help='Search query (default: "*" = everything)',
    )
    parser.add_argument(
        "--merge", type=str, default=None,
        help="Merge results with an existing CSV (deduplicates)",
    )
    parser.add_argument(
        "--media-only", action="store_true",
        help="Only include entries that have media companion files (video/image)",
    )
    parser.add_argument(
        "--headless", action="store_true",
        help="Run Chrome in headless mode (no visible browser window)",
    )
    args = parser.parse_args()

    # Launch browser and authenticate
    driver = setup_driver(headless=args.headless)

    try:
        authenticate(driver)

        # If query is "*", try multiple broad queries to cover the full library
        if args.query == "*":
            queries = [
                "EFTA",          # Most files have EFTA prefix — proven to return results
                "DataSet",       # Category prefix
                "no images produced",
                "deposition",
                "transcript",
                "exhibit",
                "document",
                "photograph",
                "video",
                "image",
                "record",
                "report",
                "letter",
                "email",
                "statement",
                "testimony",
                "interview",
                "subpoena",
                "motion",
                "filing",
            ]
            log.info(f"Wildcard mode: running {len(queries)} broad queries...")
            raw_urls = set()
            rate_limited = False
            for i, q in enumerate(queries):
                if rate_limited:
                    log.info(f"Rate limited — waiting 60s before query {q!r}...")
                    time.sleep(60)
                    # Reload the page to reset any block
                    driver.get(EPSTEIN_URL)
                    time.sleep(3)
                    rate_limited = False

                log.info(f"--- Query {i+1}/{len(queries)}: {q!r} ---")
                result = scrape_with_api(driver, query=q)
                if result is None:
                    # API failed (likely 403 rate limit)
                    rate_limited = True
                    log.warning(f"  API failed for {q!r} — will retry after cooldown")
                    # Wait and retry this one query
                    log.info("  Waiting 60s for rate limit cooldown...")
                    time.sleep(60)
                    driver.get(EPSTEIN_URL)
                    time.sleep(3)
                    result = scrape_with_api(driver, query=q)
                    if result is None:
                        log.warning(f"  Retry also failed for {q!r} — skipping")
                        continue
                    rate_limited = False

                if result:
                    before = len(raw_urls)
                    raw_urls |= result
                    log.info(f"  +{len(raw_urls) - before} new (total: {len(raw_urls)})")

                # Polite delay between queries to avoid rate limiting
                if i < len(queries) - 1:
                    time.sleep(5)
        else:
            # Single query
            raw_urls = scrape_with_api(driver, query=args.query)
            if raw_urls is None:
                raw_urls = scrape_with_selenium(driver, query=args.query)
    finally:
        driver.quit()

    if not raw_urls:
        log.warning("No new URLs collected from scraping.")
        # Still proceed — the merge step may add existing URLs

    # Classify URLs and build pairs
    # The DOJ returns PDF URLs in search results. For each PDF, we generate
    # a companion URL. For non-PDF URLs, we pair them with themselves.
    pdf_urls = set()
    media_urls = set()
    other_urls = set()

    for url in raw_urls:
        ftype = classify_url(url)
        if ftype == "pdf":
            pdf_urls.add(url)
        elif ftype in ("video", "image"):
            media_urls.add(url)
        else:
            other_urls.add(url)

    log.info(
        f"Classified: {len(pdf_urls)} PDFs, {len(media_urls)} media, "
        f"{len(other_urls)} other"
    )

    # Build pairs: PDF → companion (.mov default, downloader tries all extensions)
    pairs = set()
    for pdf_url in pdf_urls:
        pairs.add((pdf_url, generate_companion_url(pdf_url)))

    # For media files found directly, try to find their PDF counterpart
    for media_url in media_urls:
        base = media_url.rsplit(".", 1)[0]
        pdf_version = base + ".pdf"
        pairs.add((pdf_version, media_url))

    log.info(f"Generated {len(pairs)} unique pairs")

    # Merge with existing CSV if requested
    if args.merge and os.path.exists(args.merge):
        log.info(f"Merging with existing CSV: {args.merge}")
        existing_pdfs = set()
        with open(args.merge, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                pdf = row.get("PDF URL", "").strip()
                vid = row.get("MOV URL", "").strip()
                if pdf:
                    existing_pdfs.add(pdf)
                    pairs.add((pdf, vid if vid else generate_companion_url(pdf)))
        log.info(f"After merge: {len(pairs)} unique pairs")

    # Write output
    write_csv(pairs, args.output)

    # Summary
    print(f"\n{'=' * 60}")
    print(f"SCRAPE COMPLETE")
    print(f"{'=' * 60}")
    print(f"  Total pairs:  {len(pairs)}")
    print(f"  Output file:  {args.output}")
    print(f"\nTo download everything:")
    print(f"  python epstein_downloader.py --csv {args.output} \\")
    print(f"      --output ./Epstein_Library --manual --dehydrate")


if __name__ == "__main__":
    main()
