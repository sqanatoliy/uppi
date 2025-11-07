import os

BOT_NAME = "uppi"

SPIDER_MODULES = ["uppi.spiders"]
NEWSPIDER_MODULE = "uppi.spiders"

# === Scrapy Performance Settings ===
CONCURRENT_REQUESTS = 1
DOWNLOAD_DELAY = 1

USER_AGENT = None

# === Playwright Settings ===
PLAYWRIGHT_BROWSER_TYPE = "chromium"
PLAYWRIGHT_LAUNCH_OPTIONS = {
    # "executable_path": "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "headless": False,
    "args": [
        "--disable-blink-features=AutomationControlled",
        "--disable-gpu",
        "--no-sandbox",
        "--disable-dev-shm-usage",
    ],
}
PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 30_000
PLAYWRIGHT_MAX_CONTEXTS = 3
PLAYWRIGHT_CONTEXTS = {
    "default": {
        "viewport": {"width": 1920, "height": 1080},
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "java_script_enabled": True,
        "ignore_https_errors": True,
        # "proxy": {
        #     "server": "http://myproxy.com:3128",
        #     "username": "user",
        #     "password": "pass",
        # },
        "extra_http_headers": {
            "Accept-Language": "en-US,en;q=0.9,q=0.8",
            "Referer": "https://www.agenziaentrate.gov.it/",
        },
        # "storage_state": "state.json",
    }
}
if os.path.exists("state.json"):
    PLAYWRIGHT_CONTEXTS["default"]["storage_state"] = "state.json"


DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}

# Obey robots.txt rules
ROBOTSTXT_OBEY = True


# Set settings whose default value is deprecated to a future-proof value
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"
