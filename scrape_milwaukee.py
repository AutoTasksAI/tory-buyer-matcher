"""
Scrape Milwaukee open data portal using Playwright.
Downloads: sales_2022.csv, sales_2023.csv, sales_2024.csv, mprop.csv
Run: python scrape_milwaukee.py
"""

import os, time
from playwright.sync_api import sync_playwright

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)

DOWNLOADS = [
    {
        "name": "mprop.csv",
        "url": "https://data.milwaukee.gov/dataset/mprop",
        "description": "MPROP property details",
    },
    {
        "name": "sales_2024.csv",
        "url": "https://data.milwaukee.gov/dataset/property-sales-data",
        "description": "Property sales 2024",
        "year": 2024,
    },
    {
        "name": "sales_2023.csv",
        "url": "https://data.milwaukee.gov/dataset/property-sales-data",
        "description": "Property sales 2023",
        "year": 2023,
    },
    {
        "name": "sales_2022.csv",
        "url": "https://data.milwaukee.gov/dataset/property-sales-data",
        "description": "Property sales 2022",
        "year": 2022,
    },
]

def download_file(page, url, dest_path, year=None):
    print(f"  Navigating to {url}...")
    page.goto(url, wait_until="domcontentloaded", timeout=30000)
    time.sleep(2)

    if year:
        # On property sales page, find the link for the specific year
        # Try clicking on the year-specific resource link
        try:
            # Look for link containing the year
            link = page.locator(f"a:has-text('{year}')").first
            if link.count() == 0:
                link = page.locator(f"[href*='{year}']").first
            link.click()
            time.sleep(2)
        except Exception as e:
            print(f"  Could not find year link for {year}: {e}")
            # Fall through to find CSV download on current page

    # Find and click the CSV download button/link
    with page.expect_download(timeout=60000) as dl_info:
        # Try various selectors for CSV download
        downloaded = False
        selectors = [
            "a[href$='.csv']",
            "a:has-text('CSV')",
            "button:has-text('CSV')",
            "[data-format='csv']",
            "a:has-text('Download')",
        ]
        for sel in selectors:
            try:
                btn = page.locator(sel).first
                if btn.count() > 0:
                    btn.click()
                    downloaded = True
                    break
            except Exception:
                continue
        if not downloaded:
            raise Exception("Could not find CSV download button")

    download = dl_info.value
    download.save_as(dest_path)
    size = os.path.getsize(dest_path)
    print(f"  Saved: {os.path.basename(dest_path)} ({size/1024/1024:.1f} MB)")


def main():
    print("=== Milwaukee Data Scraper ===\n")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # visible so Cloudflare doesn't block
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            accept_downloads=True,
        )
        page = context.new_page()

        # First visit the main site to get a cookie
        print("Warming up session...")
        page.goto("https://data.milwaukee.gov", wait_until="domcontentloaded", timeout=20000)
        time.sleep(3)

        # Download MPROP first
        dest = os.path.join(DATA_DIR, "mprop.csv")
        if os.path.exists(dest):
            print(f"  mprop.csv already exists, skipping")
        else:
            print("\nDownloading MPROP...")
            try:
                page.goto("https://data.milwaukee.gov/dataset/mprop", wait_until="domcontentloaded", timeout=30000)
                time.sleep(2)

                # Look for the CSV resource link
                with page.expect_download(timeout=120000) as dl_info:
                    # Try to find CSV download link
                    csv_link = page.locator("a[href*='download/mprop.csv']").first
                    if csv_link.count() == 0:
                        csv_link = page.locator("a:has-text('CSV')").first
                    if csv_link.count() == 0:
                        csv_link = page.locator("a[href$='.csv']").first
                    csv_link.click()

                dl_info.value.save_as(dest)
                size = os.path.getsize(dest)
                print(f"  Saved mprop.csv ({size/1024/1024:.1f} MB)")
            except Exception as e:
                print(f"  MPROP download failed: {e}")

        # Download property sales by year
        for year in [2024, 2023, 2022]:
            name = f"sales_{year}.csv"
            dest = os.path.join(DATA_DIR, name)
            if os.path.exists(dest):
                print(f"\n  {name} already exists, skipping")
                continue

            print(f"\nDownloading {name}...")
            try:
                page.goto("https://data.milwaukee.gov/dataset/property-sales-data",
                          wait_until="domcontentloaded", timeout=30000)
                time.sleep(2)

                # Find the year-specific resource link and click it
                year_link = page.locator(f"a:has-text('{year}')").first
                if year_link.count() == 0:
                    year_link = page.locator(f"[href*='{year}']").first

                if year_link.count() > 0:
                    year_link.click()
                    time.sleep(2)
                else:
                    print(f"  No direct link found for {year}, trying current page CSV...")

                # Now click CSV download
                with page.expect_download(timeout=120000) as dl_info:
                    for sel in ["a[href$='.csv']", "a:has-text('CSV')", "a:has-text('Download')"]:
                        btn = page.locator(sel).first
                        if btn.count() > 0:
                            btn.click()
                            break

                dl_info.value.save_as(dest)
                size = os.path.getsize(dest)
                print(f"  Saved {name} ({size/1024/1024:.1f} MB)")
                time.sleep(1)

            except Exception as e:
                print(f"  {name} download failed: {e}")
                # Take screenshot for debugging
                page.screenshot(path=os.path.join(DATA_DIR, f"debug_{year}.png"))

        browser.close()

    # Check what we got
    print("\n=== Download Summary ===")
    for fname in ["mprop.csv", "sales_2022.csv", "sales_2023.csv", "sales_2024.csv"]:
        path = os.path.join(DATA_DIR, fname)
        if os.path.exists(path):
            size = os.path.getsize(path) / 1024 / 1024
            print(f"  {fname}: {size:.1f} MB")
        else:
            print(f"  {fname}: MISSING")


if __name__ == "__main__":
    main()
