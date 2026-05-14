"""
Tory's Buyer Matcher — Flask web app.
Run: python app.py
Open: http://localhost:5000
"""

import os
import json
import math
from datetime import date
from flask import Flask, render_template, request, jsonify
import anthropic
from dotenv import load_dotenv

load_dotenv()

# Pull real Milwaukee data on startup if profiles are stale/synthetic
from fetch_data import run as fetch_data
fetch_data()

app = Flask(__name__)
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
PROFILES_FILE = os.path.join(DATA_DIR, "buyer_profiles.json")

client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))


def load_profiles():
    if not os.path.exists(PROFILES_FILE):
        return {}
    with open(PROFILES_FILE) as f:
        return json.load(f)


# ─── Pre-filter: fast rule-based scoring ────────────────────────────────────

def score_buyer(buyer, deal):
    """
    Score a buyer against a deal 0-100 using rule-based signals.
    Higher = better match. Used to filter top candidates before AI scoring.
    """
    score = 0
    reasons = []

    deal_zip = str(deal.get("zip", "")).strip()
    try:
        deal_price = float(deal.get("price") or 0)
    except (TypeError, ValueError):
        deal_price = 0
    deal_type = deal.get("property_type", "").lower()
    try:
        deal_beds = int(deal.get("bedrooms") or 0)
    except (TypeError, ValueError):
        deal_beds = 0
    try:
        deal_yr_built = int(deal.get("yr_built") or 0)
    except (TypeError, ValueError):
        deal_yr_built = 0
    today = date.today()

    # --- ZIP code match (30 pts) ---
    top_zips = buyer.get("top_zips", [])
    zip_counts = buyer.get("zip_counts", {})
    if deal_zip and deal_zip in top_zips:
        weight = zip_counts.get(deal_zip, 1)
        pts = min(30, 15 + weight * 3)
        score += pts
        reasons.append(f"Buys in {deal_zip} ({weight} deals there)")
    elif deal_zip:
        reasons.append(f"No history in {deal_zip}")

    # --- Price range match (25 pts) ---
    pmin = buyer.get("price_min", 0)
    pmax = buyer.get("price_max", 0)
    pavg = buyer.get("price_avg", 0)
    if pmin and pmax and deal_price:
        # Full range hit
        if pmin <= deal_price <= pmax:
            score += 25
            reasons.append(f"Price ${deal_price:,.0f} fits their range (${pmin:,.0f}-${pmax:,.0f})")
        # Close to range (within 20%)
        elif deal_price < pmin and deal_price >= pmin * 0.8:
            score += 15
            reasons.append(f"Price slightly below their usual min (${pmin:,.0f})")
        elif deal_price > pmax and deal_price <= pmax * 1.2:
            score += 15
            reasons.append(f"Price slightly above their usual max (${pmax:,.0f})")
        else:
            reasons.append(f"Price ${deal_price:,.0f} outside their range (${pmin:,.0f}-${pmax:,.0f})")

    # --- Property type match (20 pts) ---
    top_types = [t.lower() for t in buyer.get("top_property_types", [])]
    type_match = False
    for t in top_types:
        if deal_type and (deal_type in t or t in deal_type):
            type_match = True
            break
        # fuzzy: SFR vs single family
        if "single" in deal_type and "single" in t:
            type_match = True
        if "duplex" in deal_type and "duplex" in t:
            type_match = True
        if "multi" in deal_type and ("duplex" in t or "triplex" in t or "unit" in t):
            type_match = True

    if type_match:
        score += 20
        reasons.append(f"Buys {', '.join(buyer.get('top_property_types', [])[:2])}")
    elif top_types:
        reasons.append(f"Usually buys {', '.join(buyer.get('top_property_types', [])[:2])}")

    # --- Recency (15 pts) ---
    days_inactive = buyer.get("days_since_last_purchase")
    if days_inactive is not None:
        if days_inactive <= 60:
            score += 15
            reasons.append(f"Bought {days_inactive}d ago — actively buying")
        elif days_inactive <= 120:
            score += 10
            reasons.append(f"Last bought {days_inactive}d ago")
        elif days_inactive <= 365:
            score += 5
            reasons.append(f"Last bought {days_inactive}d ago — may be slowing")
        else:
            reasons.append(f"Inactive for {days_inactive}d — cold buyer")

    # --- Deal volume bonus (10 pts) ---
    deals = buyer.get("deal_count", 0)
    if deals >= 10:
        score += 10
        reasons.append(f"High volume: {deals} deals")
    elif deals >= 5:
        score += 7
        reasons.append(f"{deals} deals")
    elif deals >= 2:
        score += 4
        reasons.append(f"{deals} deals")

    # --- Bedroom match bonus (5 pts) ---
    avg_beds = buyer.get("avg_bedrooms")
    if avg_beds and deal_beds:
        if abs(avg_beds - deal_beds) <= 1:
            score += 5
            reasons.append(f"Usually buys ~{avg_beds:.0f}bd, deal is {deal_beds}bd")

    return score, reasons


def prefilter_buyers(profiles, deal, top_n=25):
    """Return top N buyers by rule-based score."""
    scored = []
    for name, buyer in profiles.items():
        score, reasons = score_buyer(buyer, deal)
        scored.append((score, name, buyer, reasons))
    scored.sort(key=lambda x: -x[0])
    return scored[:top_n]


# ─── AI scoring and outreach drafting ────────────────────────────────────────

def build_buyer_summary(buyer):
    """Compact text summary of a buyer for the AI prompt."""
    lines = [f"Name: {buyer['name']}"]
    lines.append(f"Total deals (last 2 yrs): {buyer['deal_count']}")
    lines.append(f"Last purchase: {buyer.get('last_purchase', 'unknown')} ({buyer.get('days_since_last_purchase', '?')} days ago)")
    if buyer.get('avg_days_between_purchases'):
        lines.append(f"Buys every ~{buyer['avg_days_between_purchases']} days")
    lines.append(f"Price range: ${buyer['price_min']:,} - ${buyer['price_max']:,} (avg ${buyer['price_avg']:,})")
    lines.append(f"Top zips: {', '.join(buyer.get('top_zips', [])[:3])}")
    lines.append(f"Property types: {', '.join(buyer.get('top_property_types', [])[:3])}")
    if buyer.get('avg_bedrooms'):
        lines.append(f"Avg bedrooms: {buyer['avg_bedrooms']}")
    if buyer.get('avg_price_to_assessed_pct'):
        lines.append(f"Typically buys at {buyer['avg_price_to_assessed_pct']}% of assessed value")
    if buyer.get('yr_built_avg'):
        lines.append(f"Avg year built of purchases: {buyer['yr_built_avg']}")

    # Last 3 purchases
    recent = sorted(buyer.get("purchases", []), key=lambda x: x.get("sale_date", ""), reverse=True)[:3]
    if recent:
        lines.append("Recent purchases:")
        for p in recent:
            lines.append(f"  - {p['address']} | {p['bldg_type']} | ${p['price']:,.0f} | {p.get('sale_date','')}")

    return "\n".join(lines)


def ai_match_and_draft(candidates, deal):
    """
    Send top candidates to Claude. Get back:
      - Ranked list with match score + reasoning
      - Personalized outreach email per top 5
    """
    def fmt_money(val):
        try:
            v = float(val)
            return f"${v:,.0f}" if v else "N/A"
        except (TypeError, ValueError):
            return "N/A"

    def fmt_val(val):
        v = str(val).strip() if val is not None else ""
        return v if v else "N/A"

    deal_desc = f"""
Address: {fmt_val(deal.get('address'))}
Zip: {fmt_val(deal.get('zip'))}
Price (assignment): {fmt_money(deal.get('price'))}
Property type: {fmt_val(deal.get('property_type'))}
Bedrooms: {fmt_val(deal.get('bedrooms'))}
Year built: {fmt_val(deal.get('yr_built'))}
Condition: {fmt_val(deal.get('condition'))}
ARV estimate: {fmt_money(deal.get('arv'))}
Sqft: {fmt_val(deal.get('sqft'))}
Notes: {fmt_val(deal.get('notes')) or 'None'}
Wholesaler: Tory Mayek (Milwaukee wholesaler, year 2, direct)
""".strip()

    buyer_blocks = []
    for _, name, buyer, pre_reasons in candidates:
        buyer_blocks.append(build_buyer_summary(buyer))

    buyers_text = "\n\n---\n\n".join(buyer_blocks)

    prompt = f"""You are an AI assistant helping a real estate wholesaler named Tory Mayek match a deal to the right buyers.

DEAL:
{deal_desc}

BUYER PROFILES (from Milwaukee public records, last 2 years of purchases):
{buyers_text}

TASK:
1. Score each buyer 0-100 for fit with this specific deal. Consider: zip code history, price range, property type preference, recency of activity, volume of deals, and any other relevant signals.
2. Rank all buyers from best to worst match.
3. For the top 5 buyers, write a short personalized outreach email (3-5 sentences max) that Tory would send. The email should:
   - Reference something specific from the buyer's purchase history (a zip they buy in, a property type they like, their typical price range)
   - Describe the deal clearly and briefly
   - End with a clear call to action
   - Sound like a real person talking to another investor, not a marketing email
   - Be direct and specific, not generic

Return your response as valid JSON in this exact format:
{{
  "ranked_buyers": [
    {{
      "name": "BUYER NAME",
      "score": 85,
      "reasoning": "2-3 sentence explanation of why this buyer fits"
    }}
  ],
  "outreach_emails": [
    {{
      "name": "BUYER NAME",
      "subject": "email subject line",
      "body": "email body text"
    }}
  ]
}}

Only include outreach_emails for the top 5 buyers. Include all buyers in ranked_buyers."""

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4000,
        messages=[{"role": "user", "content": prompt}],
    )

    raw = response.content[0].text.strip()

    # Extract JSON from response (Claude sometimes adds markdown code fences)
    if "```json" in raw:
        raw = raw.split("```json")[1].split("```")[0].strip()
    elif "```" in raw:
        raw = raw.split("```")[1].split("```")[0].strip()

    result = json.loads(raw)

    # Merge pre-filter reasons into final result for display
    pre_reasons_map = {name: reasons for _, name, _, reasons in candidates}
    for buyer in result.get("ranked_buyers", []):
        buyer["pre_reasons"] = pre_reasons_map.get(buyer["name"], [])

    return result


# ─── Routes ──────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    profiles = load_profiles()
    return render_template("index.html", buyer_count=len(profiles))


@app.route("/match", methods=["POST"])
def match():
    deal = request.json
    profiles = load_profiles()

    if not profiles:
        return jsonify({"error": "No buyer profiles loaded. Run build_profiles.py first."}), 400

    # Pre-filter top candidates
    candidates = prefilter_buyers(profiles, deal, top_n=25)

    if not candidates:
        return jsonify({"error": "No candidates found. Check that profiles are built."}), 400

    # AI ranking + outreach
    try:
        result = ai_match_and_draft(candidates, deal)
    except Exception as e:
        return jsonify({"error": f"AI matching failed: {str(e)}"}), 500

    return jsonify(result)


@app.route("/profiles/stats")
def profile_stats():
    profiles = load_profiles()
    if not profiles:
        return jsonify({"error": "No profiles loaded"})

    deal_counts = [p["deal_count"] for p in profiles.values()]
    return jsonify({
        "total_buyers": len(profiles),
        "avg_deals": round(sum(deal_counts) / len(deal_counts), 1),
        "buyers_5plus": sum(1 for d in deal_counts if d >= 5),
        "buyers_10plus": sum(1 for d in deal_counts if d >= 10),
        "top_buyers": sorted(
            [{"name": n, "deals": p["deal_count"], "last": p.get("last_purchase")}
             for n, p in profiles.items()],
            key=lambda x: -x["deals"]
        )[:20],
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_ENV") == "development"
    app.run(host="0.0.0.0", port=port, debug=debug)
