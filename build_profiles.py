"""
Build buyer profiles purely from Milwaukee MPROP data.

MPROP has everything in one file:
  OWNER_NAME_1  = who owns/bought it
  CONVEY_DATE   = when they bought it
  CONVEY_FEE    = transfer fee -> sale price (price = fee / 0.003)
  GEO_ZIP_CODE  = zip code
  NEIGHBORHOOD  = neighborhood
  BLDG_TYPE     = property type
  BEDROOMS      = bedrooms
  NR_UNITS      = units
  BLDG_AREA     = sqft
  YR_BUILT      = year built
  C_A_TOTAL     = assessed value

Drop mprop.csv in data/ then run: python build_profiles.py
Output: data/buyer_profiles.json
"""

import os, json, re
import pandas as pd
from datetime import date

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
OUTPUT_FILE = os.path.join(DATA_DIR, "buyer_profiles.json")

BLDG_TYPE_MAP = {
    "0": "Vacant Land", "1": "Single Family", "2": "Duplex",
    "3": "Triplex", "4": "Fourplex", "5": "5+ Units",
    "6": "Commercial", "7": "Industrial", "8": "Exempt", "C": "Condo",
}

SKIP_OWNERS = [
    "CITY OF MILWAUKEE", "COUNTY OF MILWAUKEE", "STATE OF WISCONSIN",
    "HUD", "FANNIE MAE", "FREDDIE MAC", "SECRETARY OF", "DEPARTMENT OF",
    "MILWAUKEE PUBLIC", "SCHOOL DISTRICT", "HOUSING AUTHORITY",
    "REDEVELOPMENT AUTH", "WISCONSIN HOUSING",
]


def clean_name(v1, v2="", v3=""):
    name = " ".join(str(x).strip() for x in [v1, v2, v3] if str(x).strip() not in ("", "nan")).strip().upper()
    if not name or name in ("NAN", "NONE", "N/A"):
        return ""
    for skip in SKIP_OWNERS:
        if skip in name:
            return ""
    return name


def parse_float(val):
    if pd.isna(val):
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


def main():
    print("=== Milwaukee Buyer Profile Builder (MPROP only) ===\n")

    path = os.path.join(DATA_DIR, "mprop.csv")
    if not os.path.exists(path):
        print("ERROR: data/mprop.csv not found.")
        return

    print("Loading mprop.csv...")
    df = pd.read_csv(path, dtype=str, low_memory=False)
    print(f"  {len(df):,} properties loaded")

    today = date.today()
    cutoff = today.replace(year=today.year - 2)

    profiles = {}
    skipped_no_owner = 0
    skipped_no_date = 0
    skipped_old = 0
    skipped_price = 0
    processed = 0

    print("Building buyer profiles...")
    for _, row in df.iterrows():
        # Owner name
        owner = clean_name(
            row.get("OWNER_NAME_1", ""),
            row.get("OWNER_NAME_2", ""),
            row.get("OWNER_NAME_3", ""),
        )
        if not owner:
            skipped_no_owner += 1
            continue

        # Conveyance date
        conv_date_raw = str(row.get("CONVEY_DATE", "")).strip()
        if not conv_date_raw or conv_date_raw in ("", "nan", "0"):
            skipped_no_date += 1
            continue
        try:
            conv_dt = pd.to_datetime(conv_date_raw, errors="coerce").date()
            if pd.isna(conv_dt):
                skipped_no_date += 1
                continue
        except Exception:
            skipped_no_date += 1
            continue

        if conv_dt < cutoff:
            skipped_old += 1
            continue

        # Sale price from conveyance fee (WI: $3 per $1,000)
        fee = parse_float(row.get("CONVEY_FEE", None))
        if fee and fee >= 15:  # min fee = $15 = $5,000 property
            price = round(fee / 0.003)
        else:
            skipped_price += 1
            continue

        if price < 5000 or price > 5_000_000:
            skipped_price += 1
            continue

        # Property details
        taxkey  = str(row.get("TAXKEY", "")).strip()
        address = f"{row.get('HOUSE_NR_LO','')} {row.get('SDIR','')} {row.get('STREET','')} {row.get('STTYPE','')}".strip()
        zip_code = str(row.get("GEO_ZIP_CODE", "")).strip()
        neighborhood = str(row.get("NEIGHBORHOOD", "")).strip()
        bldg_code = str(row.get("BLDG_TYPE", "")).strip()
        bldg_type = BLDG_TYPE_MAP.get(bldg_code, bldg_code or "Unknown")
        bedrooms = int(parse_float(row.get("BEDROOMS")) or 0)
        units = int(parse_float(row.get("NR_UNITS")) or 1)
        yr_built = int(parse_float(row.get("YR_BUILT")) or 0)
        sqft = int(parse_float(row.get("BLDG_AREA")) or 0)
        assessed = parse_float(row.get("C_A_TOTAL"))
        owner_addr = str(row.get("OWNER_MAIL_ADDR", "")).strip()
        owner_city = str(row.get("OWNER_CITY_STATE", "")).strip()
        owner_zip = str(row.get("OWNER_ZIP", "")).strip()

        processed += 1

        if owner not in profiles:
            profiles[owner] = {
                "name": owner,
                "owner_addr": f"{owner_addr}, {owner_city}".strip(", "),
                "owner_zip": owner_zip,
                "purchases": [],
                "zip_counts": {},
                "type_counts": {},
                "neighborhood_counts": {},
                "prices": [],
                "bedrooms_list": [],
                "units_list": [],
                "yr_built_list": [],
                "sqft_list": [],
                "assessed_list": [],
                "dates": [],
            }

        p = profiles[owner]
        p["purchases"].append({
            "address": address,
            "zip": zip_code,
            "price": price,
            "sale_date": str(conv_dt),
            "bedrooms": bedrooms,
            "units": units,
            "bldg_type": bldg_type,
            "yr_built": yr_built,
            "sqft": sqft,
            "assessed": assessed,
            "neighborhood": neighborhood,
            "taxkey": taxkey,
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
        if units:     p["units_list"].append(units)
        if yr_built:  p["yr_built_list"].append(yr_built)
        if sqft:      p["sqft_list"].append(sqft)
        if assessed:  p["assessed_list"].append(assessed)

    print(f"  Processed: {processed:,} qualifying transactions")
    print(f"  Skipped (no owner): {skipped_no_owner:,}")
    print(f"  Skipped (no date):  {skipped_no_date:,}")
    print(f"  Skipped (too old):  {skipped_old:,}")
    print(f"  Skipped (no price): {skipped_price:,}")
    print(f"  Unique owners:      {len(profiles):,}")

    # Build final summaries
    final = {}
    for owner, p in profiles.items():
        deal_count = len(p["purchases"])
        if not is_investor(owner, deal_count):
            continue

        prices = p["prices"]
        dates_sorted = sorted(p["dates"])
        last_purchase = dates_sorted[-1]
        first_purchase = dates_sorted[0]

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

        discount_pcts = []
        for pur in p["purchases"]:
            if pur["assessed"] and pur["price"] and pur["assessed"] > 0:
                discount_pcts.append(round((pur["price"] / pur["assessed"]) * 100, 1))

        beds = p["bedrooms_list"]
        sqfts = p["sqft_list"]
        yrs = p["yr_built_list"]
        top_zips  = sorted(p["zip_counts"].items(), key=lambda x: -x[1])[:5]
        top_types = sorted(p["type_counts"].items(), key=lambda x: -x[1])[:3]
        top_hoods = sorted(p["neighborhood_counts"].items(), key=lambda x: -x[1])[:3]

        final[owner] = {
            "name": owner,
            "owner_addr": p["owner_addr"],
            "owner_zip": p["owner_zip"],
            "deal_count": deal_count,
            "first_purchase": first_purchase,
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

    with open(OUTPUT_FILE, "w") as f:
        json.dump(final, f, indent=2)

    deal_counts = [p["deal_count"] for p in final.values()]
    print(f"\nDone. {len(final):,} investor profiles saved to data/buyer_profiles.json")
    if deal_counts:
        print(f"  Avg deals/buyer:    {sum(deal_counts)/len(deal_counts):.1f}")
        print(f"  Buyers with 2+ deals: {sum(1 for d in deal_counts if d >= 2)}")
        print(f"  Buyers with 5+ deals: {sum(1 for d in deal_counts if d >= 5)}")


if __name__ == "__main__":
    main()
