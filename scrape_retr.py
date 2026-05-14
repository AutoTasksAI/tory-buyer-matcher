"""
Scrape Wisconsin RETR for Milwaukee County cash buyers.
Searches by buyer name pattern to find all recent transactions.
Run with US VPN active: python scrape_retr.py
Output: data/retr_buyers.csv
"""

import os, csv, time
from playwright.sync_api import sync_playwright

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)
OUTPUT = os.path.join(DATA_DIR, "retr_buyers.csv")

BASE = "https://propertyinfo.revenue.wi.gov/WisconsinProd"

# Search A-Z to get all buyers in Milwaukee County last 2 years
SEARCH_LETTERS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")


def accept_disclaimer(page):
    try:
        page.goto(f"{BASE}/Search/Disclaimer.aspx?FromUrl=../search/commonsearch.aspx?mode=owner",
                  wait_until="domcontentloaded", timeout=20000)
        time.sleep(1)
        agree = page.locator("input[value='Agree'], button:has-text('Agree'), a:has-text('Agree')").first
        if agree.count() > 0:
            agree.click()
            time.sleep(1)
        print("  Disclaimer accepted")
    except Exception as e:
        print(f"  Disclaimer step: {e}")


def search_buyers(page, letter):
    results = []
    try:
        # Go to owner search
        page.goto(f"{BASE}/search/commonsearch.aspx?mode=owner",
                  wait_until="domcontentloaded", timeout=20000)
        time.sleep(1)

        # Fill county = Milwaukee (55)
        county = page.locator("select[name*='county'], select[id*='county'], #ddlCounty").first
        if county.count() > 0:
            county.select_option(label="Milwaukee")
            time.sleep(0.5)

        # Fill owner name starts with letter
        name_field = page.locator("input[name*='owner'], input[id*='owner'], input[name*='Name'], #txtOwnerName").first
        if name_field.count() > 0:
            name_field.fill(letter)
        else:
            print(f"  No name field found for {letter}")
            return []

        # Date range -- last 2 years
        try:
            date_from = page.locator("input[name*='DateFrom'], input[id*='DateFrom'], #txtSaleDateFrom").first
            date_to = page.locator("input[name*='DateTo'], input[id*='DateTo'], #txtSaleDateTo").first
            if date_from.count() > 0:
                date_from.fill("01/01/2023")
            if date_to.count() > 0:
                date_to.fill("12/31/2025")
        except Exception:
            pass

        # Submit
        submit = page.locator("input[type='submit'], button[type='submit'], input[value='Search']").first
        submit.click()
        time.sleep(2)

        # Parse results table
        rows = page.locator("table tr").all()
        for row in rows[1:]:  # skip header
            cells = row.locator("td").all()
            if len(cells) >= 3:
                texts = [c.inner_text().strip() for c in cells]
                results.append(texts)

        # Handle pagination
        while True:
            next_btn = page.locator("a:has-text('Next'), input[value='Next >']").first
            if next_btn.count() == 0:
                break
            next_btn.click()
            time.sleep(1.5)
            rows = page.locator("table tr").all()
            for row in rows[1:]:
                cells = row.locator("td").all()
                if len(cells) >= 3:
                    texts = [c.inner_text().strip() for c in cells]
                    results.append(texts)

    except Exception as e:
        print(f"  Error searching {letter}: {e}")
        page.screenshot(path=os.path.join(DATA_DIR, f"debug_retr_{letter}.png"))

    return results


def main():
    print("=== Wisconsin RETR Buyer Scraper ===")
    print("Make sure US VPN is active!\n")

    all_rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        )
        page = context.new_page()

        print("Accepting disclaimer...")
        accept_disclaimer(page)

        # Take screenshot to see what we're working with
        page.screenshot(path=os.path.join(DATA_DIR, "retr_start.png"))

        for letter in SEARCH_LETTERS:
            print(f"Searching buyers starting with '{letter}'...")
            rows = search_buyers(page, letter)
            all_rows.extend(rows)
            print(f"  Found {len(rows)} results")
            time.sleep(0.5)

        browser.close()

    # Save to CSV
    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["buyer_name", "address", "sale_date", "sale_price", "col5", "col6", "col7", "col8"])
        for row in all_rows:
            # Pad to 8 cols
            padded = row + [""] * (8 - len(row))
            writer.writerow(padded[:8])

    print(f"\nSaved {len(all_rows)} records to data/retr_buyers.csv")


if __name__ == "__main__":
    main()
