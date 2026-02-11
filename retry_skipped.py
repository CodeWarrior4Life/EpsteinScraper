"""
retry_skipped.py

Finds URL pairs from the spreadsheet/CSV that were skipped (no folder exists
in the output directory) and retries downloading them with an extended list
of video format extensions.

This is useful after the main downloader ran and skipped records where
neither .mov nor .mp4 was found — this script tries additional formats
like .wmv, .avi, .3gp, .3g2, .m4v, .mpg, .flv, .webm, etc.

USAGE:
    # Retry with defaults (reads urls_cache.csv, outputs to H: drive)
    python retry_skipped.py --no-prompt

    # Retry with custom CSV and output
    python retry_skipped.py --csv urls.csv --output ./downloads --manual

    # Dry-run: just list what would be retried
    python retry_skipped.py --dry-run
"""

import os
import sys
import argparse
import logging

# Import everything we need from the main downloader
from epstein_downloader import (
    DEFAULT_AUTH_URL,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_COOKIE_DOMAIN,
    DEFAULT_REQUIRED_COOKIES,
    DEFAULT_PDF_COLUMN,
    DEFAULT_VIDEO_COLUMN,
    DEFAULT_VIDEO_EXTENSIONS,
    MAX_DOWNLOAD_WORKERS,
    fetch_url_pairs,
    get_session,
    parse_url_info,
    download_pair,
    generate_all_summaries,
    cleanup_empty_dirs,
    folder_exists_for_pair,
    save_progress,
    load_progress,
    log,
)


def find_skipped_pairs(pairs, output_dir):
    """Compare spreadsheet pairs against existing folders on disk.

    Returns a list of (pdf_url, video_url) tuples for pairs whose
    folder does NOT exist in the output directory.
    """
    return [
        (pdf_url, vid_url)
        for pdf_url, vid_url in pairs
        if not folder_exists_for_pair(pdf_url, output_dir)
    ]


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Retry downloading skipped URL pairs with extended video "
            "format extensions. Compares the spreadsheet against existing "
            "folders and retries any that are missing."
        ),
    )

    parser.add_argument(
        "--sheet-id", type=str, default=None,
        help="Google Sheet ID (default: Epstein Library sheet)",
    )
    parser.add_argument(
        "--csv", type=str, default=None, dest="csv_file",
        help="Path to a local CSV file with URL pairs",
    )
    parser.add_argument(
        "--pdf-col", type=str, default=DEFAULT_PDF_COLUMN,
        help=f'CSV column name for PDF URLs (default: "{DEFAULT_PDF_COLUMN}")',
    )
    parser.add_argument(
        "--video-col", type=str, default=DEFAULT_VIDEO_COLUMN,
        help=f'CSV column name for video URLs (default: "{DEFAULT_VIDEO_COLUMN}")',
    )
    parser.add_argument(
        "--output", type=str, default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--manual", action="store_true",
        help="Paste cookies manually",
    )
    parser.add_argument(
        "--no-prompt", action="store_true",
        help="Run non-interactively (use cached session)",
    )
    parser.add_argument(
        "--auth-url", type=str, default=DEFAULT_AUTH_URL,
        help=f"Auth URL (default: {DEFAULT_AUTH_URL})",
    )
    parser.add_argument(
        "--cookie-domain", type=str, default=DEFAULT_COOKIE_DOMAIN,
        help=f"Cookie domain (default: {DEFAULT_COOKIE_DOMAIN})",
    )
    parser.add_argument(
        "--workers", type=int, default=MAX_DOWNLOAD_WORKERS,
        help=f"Parallel download threads (default: {MAX_DOWNLOAD_WORKERS})",
    )
    parser.add_argument(
        "--video-extensions", type=str, default=None,
        help=(
            "Comma-separated video extensions to try "
            "(default: mov,mp4,wmv,avi,3gp,3g2,m4v,mpg,flv,webm)"
        ),
    )
    parser.add_argument(
        "--dehydrate", action="store_true",
        help="Mark files as online-only after download",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Just list skipped pairs without downloading",
    )
    parser.add_argument(
        "--no-summary", action="store_true",
        help="Skip PDF summary extraction",
    )

    args = parser.parse_args()

    sheet_id = args.sheet_id
    if not sheet_id and not args.csv_file:
        log.error(
            "No URL source provided. Use --sheet-id or --csv."
        )
        sys.exit(1)

    video_extensions = DEFAULT_VIDEO_EXTENSIONS
    if args.video_extensions:
        video_extensions = [
            e.strip().lstrip(".")
            for e in args.video_extensions.split(",")
            if e.strip()
        ]

    output_dir = args.output

    print("=" * 60)
    print("RETRY SKIPPED PAIRS")
    print(f"Output: {output_dir}")
    print(f"Video extensions to try: {', '.join(video_extensions)}")
    print("=" * 60)

    # Phase 1: Load all URL pairs
    pairs = fetch_url_pairs(
        sheet_id=sheet_id,
        csv_path=args.csv_file,
        pdf_col=args.pdf_col,
        video_col=args.video_col,
    )
    if not pairs:
        log.error("No URL pairs found.")
        sys.exit(1)

    log.info(f"Total pairs in spreadsheet: {len(pairs)}")

    # Phase 2: Find which ones are missing from disk
    skipped = find_skipped_pairs(pairs, output_dir)
    log.info(f"Skipped pairs (no folder on disk): {len(skipped)}")

    if not skipped:
        print("\nAll pairs already have folders. Nothing to retry!")
        return

    # Also remove these from the progress file so they get re-attempted
    completed = load_progress()
    cleared = 0
    for pdf_url, _ in skipped:
        group, file_id = parse_url_info(pdf_url)
        pair_key = f"{group}/{file_id}"
        if pair_key in completed:
            completed.discard(pair_key)
            cleared += 1
    if cleared > 0:
        save_progress(completed)
        log.info(f"Cleared {cleared} entries from progress file for retry")

    if args.dry_run:
        print(f"\n--- DRY RUN: {len(skipped)} pairs would be retried ---\n")
        for i, (pdf_url, vid_url) in enumerate(skipped[:50], 1):
            group, file_id = parse_url_info(pdf_url)
            print(f"  {i:4d}. {group}/{file_id}")
        if len(skipped) > 50:
            print(f"  ... and {len(skipped) - 50} more")
        return

    # Phase 3: Authenticate
    session = get_session(
        manual=args.manual,
        auto_use_cache=args.no_prompt,
        auth_url=args.auth_url,
        required_cookies=DEFAULT_REQUIRED_COOKIES,
        cookie_domain=args.cookie_domain,
    )

    # Phase 4: Download skipped pairs with extended extensions
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from epstein_downloader import progress_lock

    succeeded = failed = 0
    save_every = 25

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {}
        for pdf_url, vid_url in skipped:
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
                    f"Retry progress: {i}/{len(futures)} | "
                    f"OK={succeeded} Failed={failed}"
                )
            if i % save_every == 0:
                save_progress(completed)

    save_progress(completed)
    log.info(f"Retry complete.  OK={succeeded}  Failed={failed}")

    # Phase 5: Summaries for newly downloaded folders
    if not args.no_summary and succeeded > 0:
        generate_all_summaries(output_dir)

    # Cleanup empty dirs
    cleanup_empty_dirs(output_dir)

    print("\n" + "=" * 60)
    print("RETRY DONE!")
    print(f"  Retried: {len(skipped)}")
    print(f"  Succeeded: {succeeded}")
    print(f"  Still failed: {failed}")
    print("=" * 60)


if __name__ == "__main__":
    main()
