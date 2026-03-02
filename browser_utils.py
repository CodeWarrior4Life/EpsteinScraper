"""
browser_utils.py — Shared Chrome browser management via CDP

Launches the user's real Chrome profile with remote debugging enabled,
then connects via Selenium WebDriver over CDP.  This approach:
  - Uses the user's real browsing profile (passes reCAPTCHA/Queue-IT)
  - Provides full Selenium API (find_element, execute_async_script, etc.)
  - Bypasses Akamai bot detection (real browser fingerprint)
"""

import logging
import os
import subprocess
import sys
import time

log = logging.getLogger(__name__)

DEFAULT_PORT = 9222
CHROME_PATHS = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"),
]


def _find_chrome():
    """Find Chrome executable on Windows."""
    for path in CHROME_PATHS:
        if os.path.isfile(path):
            return path
    return None


def _is_chrome_running():
    """Check if any Chrome process is running."""
    try:
        result = subprocess.run(
            ["tasklist", "/FI", "IMAGENAME eq chrome.exe", "/NH"],
            capture_output=True, text=True, timeout=10,
        )
        return "chrome.exe" in result.stdout.lower()
    except Exception:
        return False


def _kill_chrome():
    """Kill all Chrome processes and wait for them to fully exit."""
    # PowerShell is more reliable at killing Chrome on Windows
    try:
        subprocess.run(
            ["powershell", "-Command",
             "Stop-Process -Name chrome -Force -ErrorAction SilentlyContinue;"
             "Stop-Process -Name 'chrome-native-host' -Force -ErrorAction SilentlyContinue"],
            capture_output=True, timeout=30,
        )
    except (subprocess.TimeoutExpired, Exception) as e:
        log.warning("PowerShell kill failed, trying taskkill: %s", e)
        try:
            subprocess.run(
                ["taskkill", "/F", "/IM", "chrome.exe"],
                capture_output=True, timeout=30,
            )
        except Exception:
            pass

    # Wait until all chrome.exe processes are gone
    for _ in range(15):
        if not _is_chrome_running():
            return
        time.sleep(1)
    log.warning("Chrome processes still running after kill attempts")


def launch_chrome(port=DEFAULT_PORT, kill_existing=True):
    """Launch Chrome with the user's real profile and remote debugging.

    Args:
        port: Remote debugging port (default 9222).
        kill_existing: Kill existing Chrome instances first (required to
            reuse the same user profile).

    Returns:
        subprocess.Popen handle for the Chrome process.
    """
    chrome_path = _find_chrome()
    if not chrome_path:
        log.error("Chrome not found. Install Google Chrome.")
        sys.exit(1)

    # Chrome requires a non-default data dir for remote debugging.
    # Use a dedicated debug profile in TEMP.
    debug_profile = os.path.join(
        os.environ.get("TEMP", os.path.expanduser("~")),
        "chrome_debug_profile",
    )
    os.makedirs(debug_profile, exist_ok=True)

    if kill_existing and _is_chrome_running():
        log.info("Closing existing Chrome instances...")
        _kill_chrome()

    cmd = [
        chrome_path,
        f"--remote-debugging-port={port}",
        f"--user-data-dir={debug_profile}",
        "--no-first-run",
        "--no-default-browser-check",
    ]
    log.info("Launching Chrome with remote debugging on port %d...", port)
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # Wait for Chrome to start and open the debugging port
    import urllib.request
    for i in range(60):
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/json/version", timeout=2)
            log.info("Chrome is ready (port %d)", port)
            return proc
        except Exception:
            time.sleep(1)

    log.error("Chrome did not start within 60 seconds")
    sys.exit(1)


def connect_to_chrome(port=DEFAULT_PORT):
    """Connect to a running Chrome instance via CDP.

    Returns a standard Selenium WebDriver with full API support.
    """
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
    except ImportError:
        log.error("Selenium not installed. Run: pip install selenium")
        sys.exit(1)

    options = Options()
    options.debugger_address = f"127.0.0.1:{port}"

    try:
        driver = webdriver.Chrome(options=options)
    except Exception as e:
        log.error("Could not connect to Chrome on port %d: %s", port, e)
        log.error(
            "Make sure Chrome is running with --remote-debugging-port=%d\n"
            "  Or let launch_chrome() start it for you.", port
        )
        sys.exit(1)

    log.info("Connected to Chrome via CDP (port %d)", port)
    return driver
