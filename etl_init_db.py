

import os
import sqlite3
import pandas as pd
from datetime import datetime, timedelta

ROOT = os.path.dirname(__file__)
DATA_DIR = os.path.join(ROOT, "data")
DB_PATH = os.path.join(ROOT, "foodwaste.db")
SCHEMA_PATH = os.path.join(ROOT, "schema.sql")

PROVIDERS_CSV = os.path.join(DATA_DIR, "providers_data.csv")
RECEIVERS_CSV = os.path.join(DATA_DIR, "receivers_data.csv")
LISTINGS_CSV  = os.path.join(DATA_DIR, "food_listings_data.csv")
CLAIMS_CSV    = os.path.join(DATA_DIR, "claims_data.csv")

def _ensure_dummy_data_if_missing():
    """Generate a tiny dummy dataset if CSVs aren't present, so the app can run end-to-end."""
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(PROVIDERS_CSV):
        pd.DataFrame([
            {"Provider_ID": 1, "Name": "Green Bites", "Type": "Restaurant", "Address": "12 MG Road", "City": "Bengaluru", "Contact": "99999-11111"},
            {"Provider_ID": 2, "Name": "FreshMart", "Type": "Grocery Store", "Address": "45 Anna Salai", "City": "Chennai", "Contact": "99999-22222"},
        ]).to_csv(PROVIDERS_CSV, index=False)
    if not os.path.exists(RECEIVERS_CSV):
        pd.DataFrame([
            {"Receiver_ID": 1, "Name": "Helping Hands NGO", "Type": "NGO", "City": "Bengaluru", "Contact": "88888-11111"},
            {"Receiver_ID": 2, "Name": "City Shelter", "Type": "Shelter", "City": "Chennai", "Contact": "88888-22222"},
        ]).to_csv(RECEIVERS_CSV, index=False)
    if not os.path.exists(LISTINGS_CSV):
        today = datetime.utcnow().date()
        pd.DataFrame([
            {"Food_ID": 101, "Food_Name": "Veg Biryani", "Quantity": 20, "Expiry_Date": str(today+timedelta(days=1)),
             "Provider_ID": 1, "Provider_Type": "Restaurant", "Location": "Bengaluru", "Food_Type": "Vegetarian", "Meal_Type": "Lunch"},
            {"Food_ID": 102, "Food_Name": "Bread Loaves", "Quantity": 50, "Expiry_Date": str(today+timedelta(days=2)),
             "Provider_ID": 2, "Provider_Type": "Grocery Store", "Location": "Chennai", "Food_Type": "Vegan", "Meal_Type": "Breakfast"},
        ]).to_csv(LISTINGS_CSV, index=False)
    if not os.path.exists(CLAIMS_CSV):
        now = datetime.utcnow()
        pd.DataFrame([
            {"Claim_ID": 1001, "Food_ID": 101, "Receiver_ID": 1, "Status": "Completed", "Timestamp": str(now)},
            {"Claim_ID": 1002, "Food_ID": 101, "Receiver_ID": 1, "Status": "Pending",   "Timestamp": str(now)},
            {"Claim_ID": 1003, "Food_ID": 102, "Receiver_ID": 2, "Status": "Cancelled", "Timestamp": str(now)},
        ]).to_csv(CLAIMS_CSV, index=False)

def _read_csvs():
    providers = pd.read_csv(PROVIDERS_CSV, dtype={"Provider_ID": "Int64"}, keep_default_na=False)
    receivers = pd.read_csv(RECEIVERS_CSV, dtype={"Receiver_ID": "Int64"}, keep_default_na=False)
    listings  = pd.read_csv(LISTINGS_CSV, dtype={"Food_ID": "Int64", "Provider_ID": "Int64"}, keep_default_na=False)
    claims    = pd.read_csv(CLAIMS_CSV, dtype={"Claim_ID": "Int64", "Food_ID": "Int64", "Receiver_ID": "Int64"}, keep_default_na=False)

    # Parse date columns
    if "Expiry_Date" in listings.columns:
        listings["Expiry_Date"] = pd.to_datetime(listings["Expiry_Date"], errors="coerce").dt.date
    if "Timestamp" in claims.columns:
        claims["Timestamp"] = pd.to_datetime(claims["Timestamp"], errors="coerce")

    # Deduplicate on primary keys
    providers = providers.drop_duplicates(subset=["Provider_ID"]).dropna(subset=["Provider_ID"])
    receivers = receivers.drop_duplicates(subset=["Receiver_ID"]).dropna(subset=["Receiver_ID"])
    listings  = listings.drop_duplicates(subset=["Food_ID"]).dropna(subset=["Food_ID"])
    claims    = claims.drop_duplicates(subset=["Claim_ID"]).dropna(subset=["Claim_ID"])

    # Enforce integer types after dropping NA
    providers["Provider_ID"] = providers["Provider_ID"].astype(int)
    receivers["Receiver_ID"] = receivers["Receiver_ID"].astype(int)
    listings["Food_ID"] = listings["Food_ID"].astype(int)
    listings["Provider_ID"] = listings["Provider_ID"].astype(int)
    claims["Claim_ID"] = claims["Claim_ID"].astype(int)
    claims["Food_ID"] = claims["Food_ID"].astype(int)
    claims["Receiver_ID"] = claims["Receiver_ID"].astype(int)

    # Referential integrity: keep only valid links
    valid_provider_ids = set(providers["Provider_ID"])
    listings = listings[listings["Provider_ID"].isin(valid_provider_ids)].copy()

    valid_food_ids = set(listings["Food_ID"])
    valid_receiver_ids = set(receivers["Receiver_ID"])
    claims = claims[claims["Food_ID"].isin(valid_food_ids) & claims["Receiver_ID"].isin(valid_receiver_ids)].copy()

    # Sanitise categorical values to match schema checks
    def normalize(value, allowed, default):
        v = str(value).strip()
        return v if v in allowed else default

    if "Type" in providers.columns:
        providers["Type"] = providers["Type"].map(lambda v: normalize(v, {'Restaurant','Grocery Store','Supermarket','Bakery','Caterer','Other'}, 'Other'))
    if "Type" in receivers.columns:
        receivers["Type"] = receivers["Type"].map(lambda v: normalize(v, {'NGO','Community Center','Individual','Shelter','Other'}, 'Other'))
    if "Food_Type" in listings.columns:
        listings["Food_Type"] = listings["Food_Type"].map(lambda v: normalize(v, {'Vegetarian','Non-Vegetarian','Vegan','Other'}, 'Other'))
    if "Meal_Type" in listings.columns:
        listings["Meal_Type"] = listings["Meal_Type"].map(lambda v: normalize(v, {'Breakfast','Lunch','Dinner','Snacks','Other'}, 'Other'))
    if "Status" in claims.columns:
        claims["Status"] = claims["Status"].map(lambda v: normalize(v, {'Pending','Completed','Cancelled'}, 'Pending'))

    return providers, receivers, listings, claims

def _run_schema(conn):
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        sql = f.read()
    conn.executescript(sql)

def build_database():
    _ensure_dummy_data_if_missing()

    providers, receivers, listings, claims = _read_csvs()

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        _run_schema(conn)

        # Load data
        providers.to_sql("providers", conn, if_exists="append", index=False)
        receivers.to_sql("receivers", conn, if_exists="append", index=False)
        listings.to_sql("food_listings", conn, if_exists="append", index=False)
        claims.to_sql("claims", conn, if_exists="append", index=False)

        # Quick counts
        cur = conn.cursor()
        tables = ["providers","receivers","food_listings","claims"]
        counts = {t: cur.execute(f"SELECT COUNT(*) FROM {t};").fetchone()[0] for t in tables}
        return {"db_path": DB_PATH, "counts": counts}
    finally:
        conn.commit()
        conn.close()

if __name__ == "__main__":
    info = build_database()
    print("Database built:", info)
