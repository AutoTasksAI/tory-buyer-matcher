"""
Generate realistic synthetic Milwaukee buyer profiles for demo/launch.
Run once: python generate_sample_data.py
Outputs: data/buyer_profiles.json
"""

import json, os, random
from datetime import date, timedelta

random.seed(42)
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)

MKE_ZIPS = ["53202","53203","53204","53205","53206","53207","53208",
            "53209","53210","53211","53212","53213","53214","53215",
            "53216","53218","53219","53220","53221","53222","53223"]

PROP_TYPES = ["Single Family","Duplex","Triplex","Single Family",
              "Single Family","Duplex","Single Family","Fourplex"]

STREETS = ["N 12th St","N 20th St","N 35th St","W Vliet St","W Center St",
           "N Teutonia Ave","W Fond du Lac Ave","S 6th St","W Oklahoma Ave",
           "N Hopkins St","W Burnham St","N 51st Blvd","W Capitol Dr",
           "N Sherman Blvd","W Greenfield Ave","S 27th St","N Port Washington Rd",
           "W Lisbon Ave","N 76th St","W North Ave","S 43rd St"]

LLC_NAMES = [
    "MKE Property Solutions LLC","Cream City Holdings LLC","Brew City Investments LLC",
    "Lakefront Real Estate LLC","Midwest Cash Buyers LLC","Urban Renewal Properties LLC",
    "WI Property Group LLC","Great Lakes Acquisitions LLC","North Shore Holdings LLC",
    "River West Properties LLC","Sherman Park Investments LLC","Bay View Rentals LLC",
    "Third Ward Holdings LLC","Rust Belt Realty LLC","Harambee Homes LLC",
    "Walker's Point Properties LLC","Clarke Square Investments LLC","Brady Street Holdings LLC",
    "Concordia Properties LLC","Merrill Park Holdings LLC","Lincoln Village LLC",
    "Silver Spring Acquisitions LLC","Granville Investments LLC","Menomonee Valley LLC",
    "Kilbourn Park Properties LLC",
]

INDIVIDUAL_NAMES = [
    "Marcus Johnson","Darius Williams","Kevin Smith","Antoine Davis","James Brown",
    "Robert Wilson","Michael Thompson","Carlos Rivera","Steven Anderson","Patrick Moore",
    "Timothy Jackson","Gregory Harris","Raymond White","Kenneth Martin","Donald Taylor",
    "Brian Lee","Christopher Walker","Daniel Hall","Paul Allen","Edward Young",
    "Jason King","Scott Wright","Eric Scott","Jeffrey Green","Ryan Adams",
    "Brandon Baker","Nathan Nelson","Justin Carter","Aaron Mitchell","Jeremy Perez",
]

def random_date(days_back_min, days_back_max):
    days = random.randint(days_back_min, days_back_max)
    return (date.today() - timedelta(days=days)).isoformat()

def make_purchase(zip_code, prop_type, price_min, price_max, bed_range, yr_range, days_back_min, days_back_max):
    beds = random.randint(*bed_range)
    price = random.randint(price_min // 1000, price_max // 1000) * 1000
    assessed = int(price * random.uniform(1.1, 1.6))
    yr = random.randint(*yr_range)
    sqft = random.randint(800, 2200) if "Single" in prop_type else random.randint(1200, 3500)
    units = 1 if "Single" in prop_type else (2 if "Duplex" in prop_type else (3 if "Triplex" in prop_type else 4))
    addr = f"{random.randint(1000,9999)} {random.choice(STREETS)}"
    return {
        "address": addr,
        "zip": zip_code,
        "price": price,
        "sale_date": random_date(days_back_min, days_back_max),
        "bedrooms": beds,
        "units": units,
        "bldg_type": prop_type,
        "yr_built": yr,
        "sqft": sqft,
        "assessed": assessed,
        "taxkey": f"{random.randint(1000000,9999999)}",
    }

def make_profile(name, is_llc, home_zips, prop_prefs, price_range, bed_range, yr_range, deal_count, last_active_days):
    purchases = []
    dates_used = []
    spread = last_active_days + (deal_count * random.randint(20, 60))

    for i in range(deal_count):
        z = random.choices(home_zips, weights=[3 if j == 0 else 1 for j in range(len(home_zips))])[0]
        pt = random.choice(prop_prefs)
        days_min = max(1, last_active_days + i * random.randint(15, 50))
        days_max = min(730, days_min + random.randint(20, 80))
        if days_min >= days_max:
            days_max = days_min + 1
        p = make_purchase(z, pt, price_range[0], price_range[1], bed_range, yr_range, days_min, days_max)
        purchases.append(p)
        dates_used.append(p["sale_date"])

    purchases.sort(key=lambda x: x["sale_date"])
    dates_sorted = sorted(dates_used)
    last_purchase = dates_sorted[-1]
    first_purchase = dates_sorted[0]

    days_inactive = (date.today() - date.fromisoformat(last_purchase)).days

    gaps = []
    if len(dates_sorted) >= 2:
        dts = [date.fromisoformat(d) for d in dates_sorted]
        gaps = [(dts[i+1] - dts[i]).days for i in range(len(dts)-1)]
    avg_gap = round(sum(gaps) / len(gaps)) if gaps else None

    prices = [p["price"] for p in purchases]
    zip_counts = {}
    type_counts = {}
    beds_list = [p["bedrooms"] for p in purchases if p["bedrooms"]]
    sqfts = [p["sqft"] for p in purchases if p["sqft"]]
    yrs = [p["yr_built"] for p in purchases if p["yr_built"]]
    assessed_list = [p["assessed"] for p in purchases if p["assessed"]]

    for p in purchases:
        zip_counts[p["zip"]] = zip_counts.get(p["zip"], 0) + 1
        type_counts[p["bldg_type"]] = type_counts.get(p["bldg_type"], 0) + 1

    top_zips = sorted(zip_counts.items(), key=lambda x: -x[1])
    top_types = sorted(type_counts.items(), key=lambda x: -x[1])

    discount_pcts = []
    for p in purchases:
        if p["assessed"] and p["price"]:
            discount_pcts.append(round((p["price"] / p["assessed"]) * 100, 1))

    return {
        "name": name,
        "deal_count": deal_count,
        "first_purchase": first_purchase,
        "last_purchase": last_purchase,
        "days_since_last_purchase": days_inactive,
        "avg_days_between_purchases": avg_gap,
        "price_min": min(prices),
        "price_max": max(prices),
        "price_avg": int(sum(prices) / len(prices)),
        "top_zips": [z for z, _ in top_zips[:5]],
        "zip_counts": dict(top_zips[:5]),
        "top_property_types": [t for t, _ in top_types[:3]],
        "type_counts": dict(top_types[:3]),
        "avg_bedrooms": round(sum(beds_list)/len(beds_list), 1) if beds_list else None,
        "max_bedrooms": max(beds_list) if beds_list else None,
        "avg_sqft": int(sum(sqfts)/len(sqfts)) if sqfts else None,
        "yr_built_avg": int(sum(yrs)/len(yrs)) if yrs else None,
        "yr_built_min": min(yrs) if yrs else None,
        "avg_price_to_assessed_pct": round(sum(discount_pcts)/len(discount_pcts), 1) if discount_pcts else None,
        "purchases": purchases,
    }

def generate():
    profiles = {}

    # Heavy hitters — LLCs, 8-20 deals, active
    for name in LLC_NAMES[:15]:
        zips = random.sample(MKE_ZIPS, random.randint(2, 4))
        prop_prefs = random.choices(PROP_TYPES, k=random.randint(1, 3))
        prop_prefs = list(set(prop_prefs))
        price_min = random.choice([35000, 40000, 45000, 50000, 55000, 60000])
        price_max = price_min + random.choice([30000, 40000, 50000, 60000, 80000])
        deal_count = random.randint(8, 20)
        last_active = random.randint(5, 90)
        p = make_profile(name, True, zips, prop_prefs, (price_min, price_max),
                         (2, 4), (1920, 1975), deal_count, last_active)
        profiles[name] = p

    # Mid-tier LLCs — 4-8 deals
    for name in LLC_NAMES[15:]:
        zips = random.sample(MKE_ZIPS, random.randint(1, 3))
        prop_prefs = random.choices(PROP_TYPES, k=random.randint(1, 2))
        prop_prefs = list(set(prop_prefs))
        price_min = random.choice([30000, 40000, 50000, 60000, 70000])
        price_max = price_min + random.choice([25000, 35000, 45000])
        deal_count = random.randint(4, 8)
        last_active = random.randint(20, 200)
        p = make_profile(name, True, zips, prop_prefs, (price_min, price_max),
                         (2, 4), (1920, 1980), deal_count, last_active)
        profiles[name] = p

    # Individual investors — 2-6 deals
    for name in INDIVIDUAL_NAMES:
        zips = random.sample(MKE_ZIPS, random.randint(1, 2))
        prop_prefs = [random.choice(PROP_TYPES)]
        price_min = random.choice([30000, 35000, 40000, 45000, 50000])
        price_max = price_min + random.choice([20000, 30000, 40000])
        deal_count = random.randint(2, 6)
        last_active = random.randint(10, 400)
        p = make_profile(name, False, zips, prop_prefs, (price_min, price_max),
                         (2, 3), (1920, 1970), deal_count, last_active)
        profiles[name] = p

    out = os.path.join(DATA_DIR, "buyer_profiles.json")
    with open(out, "w") as f:
        json.dump(profiles, f, indent=2)

    print(f"Generated {len(profiles)} buyer profiles -> data/buyer_profiles.json")
    deal_counts = [p["deal_count"] for p in profiles.values()]
    print(f"  Avg deals/buyer: {sum(deal_counts)/len(deal_counts):.1f}")
    print(f"  Buyers with 8+ deals: {sum(1 for d in deal_counts if d >= 8)}")

if __name__ == "__main__":
    generate()
