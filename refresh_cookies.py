# -*- coding: utf-8 -*-
"""Refresh / verify Chrome CDP connection for DOJ Epstein Library downloads.

Launches Chrome with remote debugging (or connects to an existing instance),
navigates to the DOJ site, and verifies the browser fetch() API works.
Once this succeeds, epstein_downloader.py can use the same Chrome instance.
"""
import json
import sys
import time

from browser_utils import launch_chrome, connect_to_chrome

TIMEOUT = 300  # 5 minutes


def main():
    print("=" * 60)
    print("CHROME CDP CONNECTION SETUP")
    print("=" * 60)

    # Launch or connect to Chrome
    print("\nLaunching Chrome with remote debugging...")
    launch_chrome(port=9222)
    driver = connect_to_chrome(port=9222)

    # Navigate to DOJ
    print("Navigating to DOJ Epstein Library...")
    try:
        driver.get("https://www.justice.gov/epstein")
    except Exception:
        pass  # May hit Queue-IT, that's fine
    time.sleep(3)

    # Test browser fetch
    print("\nTesting browser fetch() API...")
    js = (
        "const callback = arguments[arguments.length - 1];"
        "(async () => {"
        "  try {"
        "    const resp = await fetch("
        "      '/multimedia-search?keys=EFTA&page=0',"
        "      {credentials: 'same-origin'}"
        "    );"
        "    if (!resp.ok) {"
        "      callback(JSON.stringify({status: resp.status, ok: false}));"
        "      return;"
        "    }"
        "    const data = await resp.json();"
        "    const total = data.hits?.total?.value || data.hits?.total || 0;"
        "    callback(JSON.stringify({ok: true, total: total}));"
        "  } catch(e) {"
        "    callback(JSON.stringify({error: e.message}));"
        "  }"
        "})();"
    )

    try:
        raw = driver.execute_async_script(js)
        result = json.loads(raw)
    except Exception as e:
        print(f"  ERROR: {e}")
        print("\nBrowser fetch failed. The DOJ site may require you to")
        print("complete the Queue-IT challenge first. Check the Chrome window.")
        sys.exit(1)

    if result.get("ok"):
        total = result.get("total", "?")
        print(f"  SUCCESS — fetch API working! ({total} results in index)")
        print("\nChrome is ready. You can now run:")
        print("  python epstein_downloader.py --csv all_urls_20260301.csv \\")
        print('      --output "H:/My Drive/Epstein_Library"')
    else:
        print(f"  WARNING — fetch returned: {result}")
        print("\nThe browser may need Queue-IT challenge completion.")
        print("Check the Chrome window, complete any challenges, then re-run.")
        sys.exit(1)

    # Also test a file download to make sure that works
    print("\nTesting file download via fetch()...")
    test_js = (
        "const callback = arguments[arguments.length - 1];"
        "(async () => {"
        "  try {"
        "    const resp = await fetch("
        "      '/multimedia-search?keys=EFTA&page=0',"
        "      {credentials: 'same-origin'}"
        "    );"
        "    const data = await resp.json();"
        "    const hits = data.hits?.hits || [];"
        "    if (hits.length === 0) {"
        "      callback(JSON.stringify({error: 'no hits'}));"
        "      return;"
        "    }"
        "    const src = hits[0]._source || {};"
        "    const url = src.url || '';"
        "    if (!url) {"
        "      callback(JSON.stringify({error: 'no url in first hit'}));"
        "      return;"
        "    }"
        "    const dlResp = await fetch(url, {credentials: 'same-origin'});"
        "    callback(JSON.stringify({"
        "      ok: dlResp.ok,"
        "      status: dlResp.status,"
        "      type: dlResp.headers.get('content-type'),"
        "      url: url"
        "    }));"
        "  } catch(e) {"
        "    callback(JSON.stringify({error: e.message}));"
        "  }"
        "})();"
    )

    try:
        raw2 = driver.execute_async_script(test_js)
        result2 = json.loads(raw2)
        if result2.get("ok"):
            print(f"  File fetch OK — {result2.get('type', '?')} from {result2.get('url', '?')[:60]}")
        else:
            print(f"  File fetch issue: {result2}")
    except Exception as e:
        print(f"  File fetch test error: {e}")

    print("\n" + "=" * 60)
    print("SETUP COMPLETE — Chrome is ready for downloads")
    print("=" * 60)


if __name__ == "__main__":
    main()
