"""
Downloads Milwaukee MPROP data and builds buyer profiles.
Runs automatically on Railway startup (US servers, no geo-block).
"""

import os, json, re, time, csv, io
import urllib.request, urllib.error
from datetime import date

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
PROFILES_FILE = os.path.join(DATA_DIR, "buyer_profiles.json")
MPROP_FILE = os.path.join(DATA_DIR, "mprop.csv")
os.makedirs(DATA_DIR, exist_ok=True)

# Try multiple endpoints — Cloudflare blocks the direct download but the
# CKAN DataStore dump and API endpoints have different rules
MPROP_URLS = [
    # CKAN DataStore dump (often not Cloudflare-protected)
    "https://data.milwaukee.gov/datastore/dump/0a2c7f31-cd15-4151-8222-09dd57d5f16d?format=csv",
    "https://data.milwaukee.gov/api/3/action/datastore_search?resource_id=0a2c7f31-cd15-4151-8222-09dd57d5f16d&limit=200000",
    # Direct download fallback
    "https://data.milwaukee.gov/dataset/562ab824-48a5-42cd-b714-87e205e489ba/resource/0a2c7f31-cd15-4151-8222-09dd57d5f16d/download/mprop.csv",
]

BLDG_TYPE_MAP = {
    "0": "Vacant Land", "1": "Single Family", "2": "Duplex",
    "3": "Triplex",     "4": "Fourplex",      "5": "5+ Units",
    "6": "Commercial",  "7": "Industrial",     "8": "Exempt", "C": "Condo",
}

SKIP_OWNERS = [
    "CITY OF MILWAUKEE", "COUNTY OF MILWAUKEE", "STATE OF WISCONSIN",
    "HUD", "FANNIE MAE", "FREDDIE MAC", "SECRETARY OF", "DEPARTMENT OF",
    "MILWAUKEE PUBLIC", "SCHOOL DISTRICT", "HOUSING AUTHORITY",
    "REDEVELOPMENT", "WISCONSIN HOUSING",
]


def should_refresh():
    if not os.path.exists(PROFILES_FILE):
        return True
    with open(PROFILES_FILE) as f:
        profiles = json.load(f)
    # Synthetic data has < 200 buyers; real data will have thousands
    if len(profiles) < 200:
        return True
    # Refresh if older than 30 days
    age = date.today().toordinal() - date.fromtimestamp(os.path.getmtime(PROFILES_FILE)).toordinal()
    return age > 30


def download_mprop():
    print("[fetch] Downloading Milwaukee MPROP...")
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/csv,application/json,*/*",
        "Referer": "https://data.milwaukee.gov/",
        "Accept-Language": "en-US,en;q=0.9",
    }
    for url in MPROP_URLS:
        print(f"[fetch] Trying: {url[:70]}...")
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=180) as r:
                data = r.read()
            content_type = r.headers.get("Content-Type", "")
            # If JSON response (CKAN API), convert to CSV
            if "json" in content_type or url.endswith("200000"):
                parsed = json.loads(data)
                records = parsed.get("result", {}).get("records", [])
                if not records:
                    print(f"[fetch] JSON response had 0 records, trying next...")
                    continue
                keys = list(records[0].keys())
                with open(MPROP_FILE, "w", newline="", encoding="utf-8") as f:
                    w = csv.DictWriter(f, fieldnames=keys)
                    w.writeheader()
                    w.writerows(records)
                print(f"[fetch] Got {len(records):,} records via CKAN API")
            else:
                with open(MPROP_FILE, "wb") as f:
                    f.write(data)
            size_mb = os.path.getsize(MPROP_FILE) / 1024 / 1024
            print(f"[fetch] Saved mprop.csv ({size_mb:.1f} MB)")
            return True
        except urllib.error.HTTPError as e:
            print(f"[fetch] HTTP {e.code} -- trying next URL")
        except Exception as e:
            print(f"[fetch] Failed: {e} -- trying next URL")
    print("[fetch] All download attempts failed.")
    return False


def clean_name(*parts):
    name = " ".join(str(x).strip() for x in parts if str(x).strip() not in ("", "nan")).strip().upper()
    if not name or name in ("NAN", "NONE", "N/A"):
        return ""
    for skip in SKIP_OWNERS:
        if skip in name:
            return ""
    return name


def parse_float(val):
    if val is None:
        return None
    try:
        v = float(str(val).replace(",", "").strip())
        return v if v > 0 else None
    except Exception:
        return None


def is_investor(name, deal_count):
    if not name:
        return False
    if deal_count >= 2:
        return True
    keywords = ["LLC", "INC", "CORP", "PROPERTIES", "INVESTMENTS", "REALTY",
                "HOLDINGS", "CAPITAL", "VENTURES", "GROUP", "PARTNERS",
                "REAL ESTATE", "RENTAL", "ACQUISITIONS", "ENTERPRISES",
                "DEVELOPMENT", "MANAGEMENT", "TRUST", "FUND", "SOLUTIONS"]
    return any(k in name for k in keywords)


def build_profiles_from_mprop():
    print("[fetch] Building buyer profiles from MPROP...")
    today = date.today()
    cutoff = today.replace(year=today.year - 2)
    profiles = {}
    processed = skipped = 0

    with open(MPROP_FILE, encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            owner = clean_name(
                row.get("OWNER_NAME_1", ""),
                row.get("OWNER_NAME_2", ""),
                row.get("OWNER_NAME_3", ""),
            )
            if not owner:
                skipped += 1
                continue

            conv_date_raw = str(row.get("CONVEY_DATE", "")).strip()
            if not conv_date_raw or conv_date_raw in ("", "nan", "0"):
                skipped += 1
                continue
            try:
                import datetime
                # Try multiple date formats
                for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%Y%m%d"):
                    try:
                        conv_dt = datetime.datetime.strptime(conv_date_raw[:10], fmt).date()
                        break
                    except ValueError:
                        continue
                else:
                    skipped += 1
                    continue
            except Exception:
                skipped += 1
                continue

            if conv_dt < cutoff:
                skipped += 1
                continue

            fee = parse_float(row.get("CONVEY_FEE"))
            if not fee or fee < 15:
                skipped += 1
                continue
            price = round(fee / 0.003)
            if price < 5000 or price > 5_000_000:
                skipped += 1
                continue

            taxkey      = str(row.get("TAXKEY", "")).strip()
            address     = f"{row.get('HOUSE_NR_LO','')} {row.get('SDIR','')} {row.get('STREET','')} {row.get('STTYPE','')}".strip()
            zip_code    = str(row.get("GEO_ZIP_CODE", "")).strip()
            neighborhood= str(row.get("NEIGHBORHOOD", "")).strip()
            bldg_code   = str(row.get("BLDG_TYPE", "")).strip()
            bldg_type   = BLDG_TYPE_MAP.get(bldg_code, bldg_code or "Unknown")
            bedrooms    = int(parse_float(row.get("BEDROOMS")) or 0)
            units       = int(parse_float(row.get("NR_UNITS")) or 1)
            yr_built    = int(parse_float(row.get("YR_BUILT")) or 0)
            sqft        = int(parse_float(row.get("BLDG_AREA")) or 0)
            assessed    = parse_float(row.get("C_A_TOTAL"))
            owner_addr  = str(row.get("OWNER_MAIL_ADDR", "")).strip()
            owner_city  = str(row.get("OWNER_CITY_STATE", "")).strip()
            owner_zip   = str(row.get("OWNER_ZIP", "")).strip()

            processed += 1
            if owner not in profiles:
                profiles[owner] = {
                    "name": owner,
                    "owner_addr": f"{owner_addr}, {owner_city}".strip(", "),
                    "owner_zip": owner_zip,
                    "purchases": [], "zip_counts": {}, "type_counts": {},
                    "neighborhood_counts": {}, "prices": [], "bedrooms_list": [],
                    "units_list": [], "yr_built_list": [], "sqft_list": [],
                    "assessed_list": [], "dates": [],
                }

            p = profiles[owner]
            p["purchases"].append({
                "address": address, "zip": zip_code, "price": price,
                "sale_date": str(conv_dt), "bedrooms": bedrooms, "units": units,
                "bldg_type": bldg_type, "yr_built": yr_built, "sqft": sqft,
                "assessed": assessed, "neighborhood": neighborhood, "taxkey": taxkey,
            })
            p["prices"].append(price)
            p["dates"].append(str(conv_dt))
            if zip_code and zip_code != "nan":
                p["zip_counts"][zip_code] = p["zip_counts"].get(zip_code, 0) + 1
            if bldg_type and bldg_type != "Unknown":
                p["type_counts"][bldg_type] = p["type_counts"].get(bldg_type, 0) + 1
            if neighborhood and neighborhood != "nan":
                p["neighborhood_counts"][neighborhood] = p["neighborhood_counts"].get(neighborhood, 0) + 1
            if bedrooms:  p["bedrooms_list"].append(bedrooms)
            if yr_built:  p["yr_built_list"].append(yr_built)
            if sqft:      p["sqft_list"].append(sqft)
            if assessed:  p["assessed_list"].append(assessed)

    print(f"[fetch] Processed {processed:,} transactions, {len(profiles):,} unique owners")

    # Summarize into final profiles
    final = {}
    for owner, p in profiles.items():
        deal_count = len(p["purchases"])
        if not is_investor(owner, deal_count):
            continue

        prices = p["prices"]
        dates_sorted = sorted(p["dates"])
        last_purchase = dates_sorted[-1]

        days_inactive = None
        try:
            days_inactive = (today - date.fromisoformat(last_purchase)).days
        except Exception:
            pass

        avg_gap = None
        if len(dates_sorted) >= 2:
            dts = [date.fromisoformat(d) for d in dates_sorted]
            gaps = [(dts[i+1] - dts[i]).days for i in range(len(dts)-1)]
            avg_gap = round(sum(gaps) / len(gaps)) if gaps else None

        discount_pcts = [
            round((pur["price"] / pur["assessed"]) * 100, 1)
            for pur in p["purchases"]
            if pur.get("assessed") and pur["price"] and pur["assessed"] > 0
        ]

        beds  = p["bedrooms_list"]
        sqfts = p["sqft_list"]
        yrs   = p["yr_built_list"]
        top_zips  = sorted(p["zip_counts"].items(), key=lambda x: -x[1])[:5]
        top_types = sorted(p["type_counts"].items(), key=lambda x: -x[1])[:3]
        top_hoods = sorted(p["neighborhood_counts"].items(), key=lambda x: -x[1])[:3]

        final[owner] = {
            "name": owner,
            "owner_addr": p["owner_addr"],
            "owner_zip": p["owner_zip"],
            "deal_count": deal_count,
            "first_purchase": dates_sorted[0],
            "last_purchase": last_purchase,
            "days_since_last_purchase": days_inactive,
            "avg_days_between_purchases": avg_gap,
            "price_min": int(min(prices)),
            "price_max": int(max(prices)),
            "price_avg": int(sum(prices) / len(prices)),
            "top_zips": [z for z, _ in top_zips],
            "zip_counts": dict(top_zips),
            "top_property_types": [t for t, _ in top_types],
            "type_counts": dict(top_types),
            "top_neighborhoods": [n for n, _ in top_hoods],
            "avg_bedrooms": round(sum(beds)/len(beds), 1) if beds else None,
            "max_bedrooms": max(beds) if beds else None,
            "avg_sqft": int(sum(sqfts)/len(sqfts)) if sqfts else None,
            "yr_built_avg": int(sum(yrs)/len(yrs)) if yrs else None,
            "yr_built_min": min(yrs) if yrs else None,
            "avg_price_to_assessed_pct": round(sum(discount_pcts)/len(discount_pcts), 1) if discount_pcts else None,
            "purchases": p["purchases"],
        }

    with open(PROFILES_FILE, "w") as f:
        json.dump(final, f)

    deal_counts = [p["deal_count"] for p in final.values()]
    print(f"[fetch] {len(final):,} investor profiles saved")
    if deal_counts:
        print(f"[fetch] Avg deals/buyer: {sum(deal_counts)/len(deal_counts):.1f}")
        print(f"[fetch] Buyers with 5+ deals: {sum(1 for d in deal_counts if d >= 5)}")
    return len(final)


def run():
    if not should_refresh():
        print("[fetch] Profiles are fresh, skipping download.")
        return
    print("[fetch] Starting Milwaukee data refresh...")
    if download_mprop():
        count = build_profiles_from_mprop()
        print(f"[fetch] Done. {count:,} real buyer profiles ready.")
    else:
        print("[fetch] Download failed. Keeping existing profiles.")


if __name__ == "__main__":
    run()
