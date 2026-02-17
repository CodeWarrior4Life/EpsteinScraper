# -*- coding: utf-8 -*-
"""Refresh DOJ Epstein Library cookies via Selenium Chrome."""
import json
import sys
import time

try:
    import requests
except ImportError:
    print("ERROR: pip install requests")
    sys.exit(1)

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
except ImportError:
    print("ERROR: pip install selenium")
    sys.exit(1)

COOKIE_FILE = "session_cookies.json"
TIMEOUT = 900  # 15 minutes

print("Opening Chrome...")
options = Options()
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-background-timer-throttling")
options.add_argument("--disable-backgrounding-occluded-windows")
options.add_argument("--disable-renderer-backgrounding")

driver = webdriver.Chrome(options=options)
driver.set_page_load_timeout(60)

try:
    driver.get("https://www.justice.gov/epstein")
    print("Chrome is open at the DOJ Epstein Library page.")
    print("Please complete Queue-IT challenge + age verification.")
    print("Polling for search box (%d min timeout)..." % (TIMEOUT // 60))

    selectors = [
        "input[type=search]",
        "#search-field-en-small-desktop",
        "#searchInput",
        "input[name=query]",
        "input[name=keys]",
    ]

    start = time.time()
    found = False
    while time.time() - start < TIMEOUT:
        try:
            _ = driver.current_url
        except Exception as e:
            print("ERROR: Browser disconnected: %s" % e)
            sys.exit(1)

        for sel in selectors:
            try:
                el = driver.find_element(By.CSS_SELECTOR, sel)
                if el.is_displayed():
                    elapsed = int(time.time() - start)
                    print("  Found search box (%s) after %ds!" % (sel, elapsed))
                    found = True
                    break
            except Exception:
                pass
        if found:
            break

        elapsed = int(time.time() - start)
        if elapsed > 0 and elapsed % 30 == 0:
            print("  Waiting... (%ds)" % elapsed)
        time.sleep(5)

    if not found:
        print("ERROR: Timed out waiting for search box.")
        print("Saving cookies anyway...")

    # Wait for cookies to settle
    print("Auth detected - waiting 5s for cookies to settle...")
    time.sleep(5)

    # Navigate to the search API to trigger any additional cookies
    try:
        driver.get("https://www.justice.gov/multimedia-search?keys=EFTA&page=0")
        time.sleep(3)
    except Exception as e:
        print("Warning: could not navigate to search API: %s" % e)

    # Navigate back to trigger any remaining cookies
    try:
        driver.get("https://www.justice.gov/epstein")
        time.sleep(2)
    except Exception as e:
        print("Warning: could not navigate back: %s" % e)

    # Grab all cookies
    browser_cookies = driver.get_cookies()
    cookie_dict = {}
    for c in browser_cookies:
        cookie_dict[c["name"]] = c["value"]

    print("Captured %d cookies: %s" % (len(cookie_dict), sorted(cookie_dict.keys())))

    # Save to file
    with open(COOKIE_FILE, "w") as f:
        json.dump(cookie_dict, f, indent=2)
    print("Saved to %s" % COOKIE_FILE)

    # Test cookies with requests library
    print("")
    print("Testing cookies with requests library...")
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        )
    })
    for name, value in cookie_dict.items():
        session.cookies.set(name, value, domain=".justice.gov", path="/")

    try:
        resp = session.get(
            "https://www.justice.gov/multimedia-search",
            params={"keys": "EFTA", "page": "0"},
            timeout=15,
        )
        print("  Search API: HTTP %d" % resp.status_code)
        if resp.status_code == 200:
            try:
                data = resp.json()
                if "hits" in data:
                    hits = data["hits"]
                    if isinstance(hits, dict):
                        total = hits.get("total", {})
                        count = total.get("value", 0) if isinstance(total, dict) else total
                        print("  Results: %s hits - COOKIES VALID!" % count)
                    else:
                        print("  Got response - COOKIES VALID!")
                else:
                    print("  Response keys: %s" % list(data.keys()))
            except Exception:
                print("  Non-JSON response: %s" % resp.text[:200])
        else:
            print("  Cookies NOT valid (HTTP %d)" % resp.status_code)
            print("  Response: %s" % resp.text[:200])
    except Exception as e:
        print("  Test failed: %s" % e)

    # Also test via browser fetch
    print("")
    print("Testing via browser fetch...")
    try:
        js = (
            "const callback = arguments[arguments.length - 1];"
            "(async () => {"
            "  try {"
            "    const resp = await fetch("
            "      '/multimedia-search?keys=EFTA&page=0',"
            "      { credentials: 'same-origin' }"
            "    );"
            "    callback(JSON.stringify({status: resp.status, ok: resp.ok}));"
            "  } catch(e) {"
            "    callback(JSON.stringify({error: e.message}));"
            "  }"
            "})();"
        )
        raw = driver.execute_async_script(js)
        result = json.loads(raw)
        print("  Browser fetch: %s" % result)
        if result.get("ok"):
            print("  Browser auth is working!")
    except Exception as e:
        print("  Browser fetch test failed: %s" % e)

finally:
    print("")
    print("Closing Chrome...")
    try:
        driver.quit()
    except Exception:
        pass
    print("Done.")
