"""
Build buyer profiles from Milwaukee property sales + MPROP data.

Data sources (download manually via browser, drop in data/):
  Sales: https://data.milwaukee.gov/dataset/property-sales-data
         Download each year CSV (2022, 2023, 2024) -- save as data/sales_YYYY.csv
  MPROP:  https://data.milwaukee.gov/dataset/mprop
          Download CSV -- save as data/mprop.csv

Run: python build_profiles.py
Output: data/buyer_profiles.json
"""

import os
import json
import re
import glob
import pandas as pd
from datetime import datetime, date

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
OUTPUT_FILE = os.path.join(DATA_DIR, "buyer_profiles.json")

# Milwaukee property type codes from MPROP BLDG_TYPE
BLDG_TYPE_MAP = {
    "0": "Vacant Land",
    "1": "Single Family",
    "2": "Duplex",
    "3": "Triplex",
    "4": "Fourplex",
    "5": "5+ Units",
    "6": "Commercial",
    "7": "Industrial",
    "8": "Exempt",
    "C": "Condo",
}

# Flexible column name aliases (Milwaukee CSVs use inconsistent naming across years)
SALES_COL_ALIASES = {
    "taxkey":    ["taxkey", "tax_key", "parcel_id", "TAXKEY"],
    "address":   ["address", "prop_addr", "situs_address", "ADDRESS"],
    "buyer":     ["buyer1", "buyer_1", "grantee", "buyer_name", "BUYER1", "GRANTEE"],
    "sale_date": ["sale_date", "convey_date", "recording_date", "SALE_DATE"],
    "sale_price":["sale_price", "price", "amount", "SALE_PRICE", "convey_fee_basis"],
    "convey_fee":["convey_fee", "transfer_fee", "CONVEY_FEE"],
    "deed_type": ["deed_type", "instrument_type", "DEED_TYPE"],
    "zip":       ["zip", "zip_code", "prop_zip", "ZIP"],
}

MPROP_COL_ALIASES = {
    "taxkey":    ["taxkey", "TAXKEY"],
    "bedrooms":  ["nr_beds", "nbeds", "bedrooms", "beds", "NR_BEDS", "NBEDS"],
    "units":     ["nr_units", "units", "NR_UNITS"],
    "bldg_type": ["bldg_type", "building_type", "BLDG_TYPE"],
    "yr_built":  ["yr_built", "year_built", "YR_BUILT", "YEAR_BUILT"],
    "lot_size":  ["lotsize", "lot_size", "lot_area", "LOTSIZE"],
    "assessed":  ["c_a_total", "assessed_value", "total_assessed", "C_A_TOTAL"],
    "zip":       ["zip", "owner_zip", "ZIP", "OWNER_ZIP"],
    "sqft":      ["bldg_area", "sqft", "grs_bld_ar", "BLDG_AREA", "GRS_BLD_AR"],
}


def find_col(df, aliases):
    """Return first matching column name from aliases list."""
    cols_lower = {c.lower(): c for c in df.columns}
    for alias in aliases:
        if alias in df.columns:
            return alias
        if alias.lower() in cols_lower:
            return cols_lower[alias.lower()]
    return None


def resolve_cols(df, alias_map):
    """Build a mapping of logical name -> actual column name."""
    resolved = {}
    for logical, aliases in alias_map.items():
        col = find_col(df, aliases)
        if col:
            resolved[logical] = col
    return resolved


def clean_name(name):
    if not isinstance(name, str):
        return ""
    name = name.strip().upper()
    # Remove common suffixes that are noise
    name = re.sub(r'\s+(LLC|INC|CORP|LTD|LP|L\.P\.|L\.L\.C\.)\.?$', r' \1', name)
    return name


def is_investor(buyer_name, num_purchases):
    """Heuristic: LLCs or anyone buying 2+ properties is treated as investor."""
    if not buyer_name:
        return False
    if num_purchases >= 2:
        return True
    name_upper = buyer_name.upper()
    investor_keywords = ["LLC", "INC", "CORP", "PROPERTIES", "INVESTMENTS",
                         "REALTY", "HOLDINGS", "CAPITAL", "VENTURES", "GROUP",
                         "PARTNERS", "REAL ESTATE", "RENTAL", "ACQUISITIONS"]
    return any(k in name_upper for k in investor_keywords)


def parse_price(val):
    if pd.isna(val):
        return None
    if isinstance(val, (int, float)):
        return float(val)
    val = str(val).replace("$", "").replace(",", "").strip()
    try:
        return float(val)
    except ValueError:
        return None


def load_sales(data_dir):
    """Load all sales CSVs from data/ directory."""
    pattern = os.path.join(data_dir, "sales*.csv")
    files = glob.glob(pattern)
    if not files:
        print("No sales CSV files found. Expected: data/sales_2022.csv, data/sales_2023.csv, etc.")
        return pd.DataFrame()

    frames = []
    for f in sorted(files):
        print(f"  Loading {os.path.basename(f)}...")
        try:
            df = pd.read_csv(f, dtype=str, low_memory=False)
            frames.append(df)
        except Exception as e:
            print(f"  Error reading {f}: {e}")

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    print(f"  Total sales rows: {len(combined):,}")
    return combined


def load_mprop(data_dir):
    """Load MPROP CSV for property detail enrichment."""
    path = os.path.join(data_dir, "mprop.csv")
    if not os.path.exists(path):
        print("No mprop.csv found in data/. Buyer profiles will lack bedrooms/sqft/year built.")
        return pd.DataFrame()
    print(f"  Loading mprop.csv...")
    df = pd.read_csv(path, dtype=str, low_memory=False)
    print(f"  MPROP rows: {len(df):,}")
    return df


def build_property_lookup(mprop_df):
    """Build taxkey -> property details dict from MPROP."""
    if mprop_df.empty:
        return {}
    cols = resolve_cols(mprop_df, MPROP_COL_ALIASES)
    lookup = {}
    for _, row in mprop_df.iterrows():
        if "taxkey" not in cols:
            break
        key = str(row[cols["taxkey"]]).strip()
        details = {}
        for logical in ["bedrooms", "units", "bldg_type", "yr_built", "lot_size", "assessed", "sqft"]:
            if logical in cols:
                val = row[cols[logical]]
                if not pd.isna(val) and str(val).strip() not in ("", "0"):
                    details[logical] = str(val).strip()
        if details:
            lookup[key] = details
    return lookup


def build_profiles(sales_df, property_lookup):
    """Aggregate sales into buyer profiles."""
    if sales_df.empty:
        return {}

    cols = resolve_cols(sales_df, SALES_COL_ALIASES)
    if "buyer" not in cols:
        print("ERROR: Could not find buyer column in sales data.")
        print("Available columns:", list(sales_df.columns[:20]))
        return {}

    print("\nBuilding buyer profiles...")
    profiles = {}
    today = date.today()
    cutoff = today.replace(year=today.year - 2)

    for _, row in sales_df.iterrows():
        buyer_raw = row.get(cols.get("buyer", ""), "")
        buyer = clean_name(buyer_raw)
        if not buyer or buyer in ("", "UNKNOWN", "VARIOUS"):
            continue

        price_raw = row.get(cols.get("sale_price", ""), None)
        price = parse_price(price_raw)

        # Skip non-arm's-length or very low price sales
        if price is None or price < 5000:
            continue

        # Parse date
        sale_date_raw = row.get(cols.get("sale_date", ""), "")
        try:
            sale_date = pd.to_datetime(sale_date_raw, errors="coerce")
            if pd.isna(sale_date):
                continue
            sale_dt = sale_date.date()
        except Exception:
            continue

        # Only last 2 years
        if sale_dt < cutoff:
            continue

        taxkey = str(row.get(cols.get("taxkey", ""), "")).strip()
        address = str(row.get(cols.get("address", ""), "")).strip()
        zip_code = str(row.get(cols.get("zip", ""), "")).strip()
        if not zip_code or zip_code == "nan":
            zip_code = ""

        # Enrich from MPROP
        prop = property_lookup.get(taxkey, {})
        bedrooms = int(prop.get("bedrooms", 0) or 0)
        units = int(prop.get("units", 1) or 1)
        bldg_type_code = prop.get("bldg_type", "")
        bldg_type = BLDG_TYPE_MAP.get(bldg_type_code, bldg_type_code or "Unknown")
        yr_built = int(prop.get("yr_built", 0) or 0)
        assessed = parse_price(prop.get("assessed", None))
        sqft = int(prop.get("sqft", 0) or 0)

        if buyer not in profiles:
            profiles[buyer] = {
                "name": buyer,
                "purchases": [],
                "zip_codes": {},
                "property_types": {},
                "prices": [],
                "bedrooms_list": [],
                "units_list": [],
                "yr_built_list": [],
                "sqft_list": [],
                "assessed_list": [],
                "dates": [],
                "addresses": [],
            }

        p = profiles[buyer]
        p["purchases"].append({
            "address": address,
            "zip": zip_code,
            "price": price,
            "sale_date": str(sale_dt),
            "bedrooms": bedrooms,
            "units": units,
            "bldg_type": bldg_type,
            "yr_built": yr_built,
            "sqft": sqft,
            "assessed": assessed,
            "taxkey": taxkey,
        })

        if zip_code:
            p["zip_codes"][zip_code] = p["zip_codes"].get(zip_code, 0) + 1
        if bldg_type and bldg_type != "Unknown":
            p["property_types"][bldg_type] = p["property_types"].get(bldg_type, 0) + 1
        p["prices"].append(price)
        if bedrooms:
            p["bedrooms_list"].append(bedrooms)
        if units:
            p["units_list"].append(units)
        if yr_built:
            p["yr_built_list"].append(yr_built)
        if sqft:
            p["sqft_list"].append(sqft)
        if assessed:
            p["assessed_list"].append(assessed)
        p["dates"].append(str(sale_dt))
        p["addresses"].append(address)

    # Post-process: compute summary stats per buyer
    final = {}
    for buyer, p in profiles.items():
        num = len(p["purchases"])
        if not is_investor(buyer, num):
            continue

        prices = p["prices"]
        dates_sorted = sorted(p["dates"])
        last_purchase = dates_sorted[-1] if dates_sorted else ""
        first_purchase = dates_sorted[0] if dates_sorted else ""

        # Days since last purchase
        days_inactive = None
        if last_purchase:
            try:
                days_inactive = (today - date.fromisoformat(last_purchase)).days
            except Exception:
                pass

        # Average days between purchases
        avg_days_between = None
        if len(dates_sorted) >= 2:
            dts = [date.fromisoformat(d) for d in dates_sorted]
            gaps = [(dts[i+1] - dts[i]).days for i in range(len(dts)-1)]
            avg_days_between = round(sum(gaps) / len(gaps))

        # Discount depth: how far below assessed do they typically buy?
        discount_pcts = []
        for purchase in p["purchases"]:
            if purchase["assessed"] and purchase["price"] and purchase["assessed"] > 0:
                pct = round((purchase["price"] / purchase["assessed"]) * 100, 1)
                discount_pcts.append(pct)

        top_zips = sorted(p["zip_codes"].items(), key=lambda x: -x[1])[:5]
        top_types = sorted(p["property_types"].items(), key=lambda x: -x[1])[:3]

        beds = p["bedrooms_list"]
        sqfts = p["sqft_list"]
        yrs = p["yr_built_list"]

        profile = {
            "name": buyer,
            "deal_count": num,
            "first_purchase": first_purchase,
            "last_purchase": last_purchase,
            "days_since_last_purchase": days_inactive,
            "avg_days_between_purchases": avg_days_between,
            "price_min": int(min(prices)),
            "price_max": int(max(prices)),
            "price_avg": int(sum(prices) / len(prices)),
            "top_zips": [z for z, _ in top_zips],
            "zip_counts": dict(top_zips),
            "top_property_types": [t for t, _ in top_types],
            "type_counts": dict(top_types),
            "avg_bedrooms": round(sum(beds) / len(beds), 1) if beds else None,
            "max_bedrooms": max(beds) if beds else None,
            "avg_sqft": int(sum(sqfts) / len(sqfts)) if sqfts else None,
            "yr_built_avg": int(sum(yrs) / len(yrs)) if yrs else None,
            "yr_built_min": min(yrs) if yrs else None,
            "avg_price_to_assessed_pct": round(sum(discount_pcts)/len(discount_pcts), 1) if discount_pcts else None,
            "purchases": p["purchases"],
        }
        final[buyer] = profile

    return final


def main():
    print("=== Milwaukee Buyer Profile Builder ===\n")

    print("Loading sales data...")
    sales_df = load_sales(DATA_DIR)

    print("\nLoading MPROP property data...")
    mprop_df = load_mprop(DATA_DIR)
    property_lookup = build_property_lookup(mprop_df)
    print(f"  Property lookup built: {len(property_lookup):,} parcels")

    profiles = build_profiles(sales_df, property_lookup)

    if not profiles:
        print("\nNo profiles built. Check that data files are in data/ folder.")
        print("\nTo download data:")
        print("  Sales: https://data.milwaukee.gov/dataset/property-sales-data")
        print("         Save each year CSV as data/sales_2022.csv, data/sales_2023.csv, data/sales_2024.csv")
        print("  MPROP: https://data.milwaukee.gov/dataset/mprop")
        print("         Click 'CSV' download, save as data/mprop.csv")
        return

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(profiles, f, indent=2)

    print(f"\nDone. {len(profiles):,} investor profiles saved to data/buyer_profiles.json")

    # Quick stats
    deal_counts = [p["deal_count"] for p in profiles.values()]
    print(f"  Avg deals per buyer: {sum(deal_counts)/len(deal_counts):.1f}")
    print(f"  Buyers with 5+ deals: {sum(1 for d in deal_counts if d >= 5)}")
    print(f"  Buyers with 10+ deals: {sum(1 for d in deal_counts if d >= 10)}")


if __name__ == "__main__":
    main()
