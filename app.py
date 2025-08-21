
import os
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

ROOT = os.path.dirname(__file__)
DB_PATH = os.path.join(ROOT, "foodwaste.db")
SCHEMA_PATH = os.path.join(ROOT, "schema.sql")
QUERIES_PATH = os.path.join(ROOT, "queries.sql")

st.set_page_config(page_title="Local Food Wastage Management System", layout="wide")

# ---------- Helpers ----------
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def df_read_sql(query, params=None):
    with closing(get_conn()) as conn:
        return pd.read_sql_query(query, conn, params=params)

def run_query(query, params=None):
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(query, params or {})
        conn.commit()
        return cur

def init_db_if_missing():
    if not os.path.exists(DB_PATH):
        st.warning("Database not found. Click **Build / Refresh DB** to create it from CSVs in ./data.")
    else:
        st.success(f"Connected to database at: {DB_PATH}")

# ---------- Sidebar ----------
st.sidebar.title("Controls")
if st.sidebar.button("Build / Refresh DB"):
    # Run local ETL script
    import importlib.util, sys
    etl_path = os.path.join(ROOT, "etl_init_db.py")
    spec = importlib.util.spec_from_file_location("etl_init_db", etl_path)
    etl = importlib.util.module_from_spec(spec)
    sys.modules["etl_init_db"] = etl
    spec.loader.exec_module(etl)
    info = etl.build_database()
    st.sidebar.success(f"DB built. Counts: {info['counts']}")

init_db_if_missing()

# Dynamic filters
cities = df_read_sql("SELECT DISTINCT City FROM providers WHERE City IS NOT NULL UNION SELECT DISTINCT Location FROM food_listings WHERE Location IS NOT NULL ORDER BY 1;") if os.path.exists(DB_PATH) else pd.DataFrame(columns=["City"])
provider_types = df_read_sql("SELECT DISTINCT Type FROM providers ORDER BY 1;") if os.path.exists(DB_PATH) else pd.DataFrame(columns=["Type"])
food_types = df_read_sql("SELECT DISTINCT Food_Type FROM food_listings ORDER BY 1;") if os.path.exists(DB_PATH) else pd.DataFrame(columns=["Food_Type"])
meal_types = df_read_sql("SELECT DISTINCT Meal_Type FROM food_listings ORDER BY 1;") if os.path.exists(DB_PATH) else pd.DataFrame(columns=["Meal_Type"])

f_city = st.sidebar.multiselect("City", cities["City"].dropna().tolist())
f_provider_type = st.sidebar.multiselect("Provider Type", provider_types["Type"].dropna().tolist())
f_food_type = st.sidebar.multiselect("Food Type", food_types["Food_Type"].dropna().tolist())
f_meal_type = st.sidebar.multiselect("Meal Type", meal_types["Meal_Type"].dropna().tolist())
days_to_expiry = st.sidebar.slider("Expiring within (days)", min_value=1, max_value=30, value=7)

# ---------- Header ----------
st.title("🥗 Local Food Wastage Management System")
st.caption("Connect surplus food providers with receivers, reduce waste, and analyze trends.")

# ---------- Dashboard ----------
st.subheader("📊 Dashboard")

if os.path.exists(DB_PATH):
    # KPIs
    kpi_total_providers = df_read_sql("SELECT COUNT(*) AS c FROM providers;")["c"].iloc[0]
    kpi_total_receivers = df_read_sql("SELECT COUNT(*) AS c FROM receivers;")["c"].iloc[0]
    kpi_total_listings = df_read_sql("SELECT COUNT(*) AS c FROM food_listings;")["c"].iloc[0]
    kpi_total_qty = df_read_sql("SELECT SUM(COALESCE(Quantity,0)) AS qty FROM food_listings;")["qty"].iloc[0] or 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Providers", kpi_total_providers)
    col2.metric("Receivers", kpi_total_receivers)
    col3.metric("Listings", kpi_total_listings)
    col4.metric("Total Qty Available", int(kpi_total_qty))

    # Filtered listings table
    query = "SELECT * FROM food_listings WHERE 1=1"
    params = {}
    if f_city: query += " AND Location IN ({})".format(",".join("?"*len(f_city))); params.update({f"p{i}": v for i,v in enumerate(f_city)})
    if f_provider_type: query += " AND Provider_Type IN ({})".format(",".join("?"*len(f_provider_type))); params.update({f"t{i}": v for i,v in enumerate(f_provider_type)})
    if f_food_type: query += " AND Food_Type IN ({})".format(",".join("?"*len(f_food_type))); params.update({f"ft{i}": v for i,v in enumerate(f_food_type)})
    if f_meal_type: query += " AND Meal_Type IN ({})".format(",".join("?"*len(f_meal_type))); params.update({f"mt{i}": v for i,v in enumerate(f_meal_type)})
    # Workaround for dict->tuple ordering in pandas read_sql_query:
    param_tuple = tuple(params.values()) if params else ()

    listings_df = df_read_sql(query, param_tuple)
    st.markdown("**Filtered Listings**")
    st.dataframe(listings_df, use_container_width=True)

    # Expiring soon
    expiring_df = df_read_sql(
        "SELECT * FROM food_listings WHERE date(Expiry_Date) <= date('now', ?) ORDER BY Expiry_Date;",
        (f"+{days_to_expiry} day",)
    )
    st.markdown(f"**Expiring within {days_to_expiry} days**")
    st.dataframe(expiring_df, use_container_width=True)

# ---------- Tabs for CRUD and Analytics ----------
tabs = st.tabs(["Listings", "Claims", "Providers", "Receivers", "Analytics", "SQL Queries", "Admin"])

# Listings Tab
with tabs[0]:
    st.header("🍽️ Listings")
    if os.path.exists(DB_PATH):
        st.dataframe(df_read_sql("SELECT * FROM food_listings ORDER BY Expiry_Date;"), use_container_width=True)

    with st.form("add_listing"):
        st.subheader("Add Listing")
        colA, colB, colC = st.columns(3)
        with colA:
            Food_ID = st.number_input("Food_ID", min_value=1, step=1)
            Food_Name = st.text_input("Food_Name")
            Quantity = st.number_input("Quantity", min_value=0, step=1)
        with colB:
            Expiry_Date = st.date_input("Expiry_Date")
            Provider_ID = st.number_input("Provider_ID", min_value=1, step=1)
            Provider_Type = st.selectbox("Provider_Type", ["Restaurant", "Grocery Store", "Supermarket", "Bakery", "Caterer", "Other"])
        with colC:
            Location = st.text_input("Location")
            Food_Type = st.selectbox("Food_Type", ["Vegetarian","Non-Vegetarian","Vegan","Other"])
            Meal_Type = st.selectbox("Meal_Type", ["Breakfast","Lunch","Dinner","Snacks","Other"])
        submitted = st.form_submit_button("Create Listing")
        if submitted:
            try:
                run_query("""
                    INSERT INTO food_listings (Food_ID, Food_Name, Quantity, Expiry_Date, Provider_ID, Provider_Type, Location, Food_Type, Meal_Type)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, (int(Food_ID), Food_Name, int(Quantity), str(Expiry_Date), int(Provider_ID), Provider_Type, Location, Food_Type, Meal_Type))
                st.success("Listing created.")
            except Exception as e:
                st.error(f"Error: {e}")

    with st.form("update_listing"):
        st.subheader("Update Listing Quantity / Expiry")
        Food_ID_u = st.number_input("Food_ID to update", min_value=1, step=1, key="u_food_id")
        Quantity_u = st.number_input("New Quantity", min_value=0, step=1, key="u_qty")
        Expiry_Date_u = st.date_input("New Expiry_Date", key="u_exp")
        submitted_u = st.form_submit_button("Update")
        if submitted_u:
            try:
                run_query("UPDATE food_listings SET Quantity=?, Expiry_Date=? WHERE Food_ID=?;", (int(Quantity_u), str(Expiry_Date_u), int(Food_ID_u)))
                st.success("Listing updated.")
            except Exception as e:
                st.error(f"Error: {e}")

    with st.form("delete_listing"):
        st.subheader("Delete Listing")
        Food_ID_d = st.number_input("Food_ID to delete", min_value=1, step=1, key="d_food_id")
        submitted_d = st.form_submit_button("Delete")
        if submitted_d:
            try:
                run_query("DELETE FROM food_listings WHERE Food_ID=?;", (int(Food_ID_d),))
                st.success("Listing deleted.")
            except Exception as e:
                st.error(f"Error: {e}")

# Claims Tab
with tabs[1]:
    st.header("📬 Claims")
    if os.path.exists(DB_PATH):
        st.dataframe(df_read_sql("SELECT * FROM claims ORDER BY Timestamp DESC;"), use_container_width=True)

    with st.form("add_claim"):
        st.subheader("Add Claim")
        Claim_ID = st.number_input("Claim_ID", min_value=1, step=1)
        Food_ID_c = st.number_input("Food_ID", min_value=1, step=1, key="c_food_id")
        Receiver_ID_c = st.number_input("Receiver_ID", min_value=1, step=1)
        Status_c = st.selectbox("Status", ["Pending","Completed","Cancelled"])
        date_part = st.date_input("Date", value=datetime.now().date())
        time_part = st.time_input("Time", value=datetime.now().time())
        Timestamp_c = datetime.combine(date_part, time_part)
        submitted_c = st.form_submit_button("Create Claim")
        if submitted_c:
            try:
                run_query("""
                    INSERT INTO claims (Claim_ID, Food_ID, Receiver_ID, Status, Timestamp)
                    VALUES (?, ?, ?, ?, ?);
                """, (int(Claim_ID), int(Food_ID_c), int(Receiver_ID_c), Status_c, str(Timestamp_c)))
                st.success("Claim created.")
            except Exception as e:
                st.error(f"Error: {e}")

    with st.form("update_claim"):
        st.subheader("Update Claim Status")
        Claim_ID_u = st.number_input("Claim_ID to update", min_value=1, step=1, key="uc_claim_id")
        Status_u = st.selectbox("New Status", ["Pending","Completed","Cancelled"], key="uc_status")
        submitted_uc = st.form_submit_button("Update Claim")
        if submitted_uc:
            try:
                run_query("UPDATE claims SET Status=? WHERE Claim_ID=?;", (Status_u, int(Claim_ID_u)))
                st.success("Claim updated.")
            except Exception as e:
                st.error(f"Error: {e}")

    with st.form("delete_claim"):
        st.subheader("Delete Claim")
        Claim_ID_d = st.number_input("Claim_ID to delete", min_value=1, step=1, key="dc_claim_id")
        submitted_dc = st.form_submit_button("Delete")
        if submitted_dc:
            try:
                run_query("DELETE FROM claims WHERE Claim_ID=?;", (int(Claim_ID_d),))
                st.success("Claim deleted.")
            except Exception as e:
                st.error(f"Error: {e}")

# Providers Tab
with tabs[2]:
    st.header("🏪 Providers")
    if os.path.exists(DB_PATH):
        st.dataframe(df_read_sql("SELECT * FROM providers ORDER BY City, Name;"), use_container_width=True)

    with st.form("add_provider"):
        st.subheader("Add Provider")
        Provider_ID = st.number_input("Provider_ID", min_value=1, step=1)
        Name = st.text_input("Name")
        Type = st.selectbox("Type", ["Restaurant","Grocery Store","Supermarket","Bakery","Caterer","Other"])
        Address = st.text_input("Address")
        City = st.text_input("City")
        Contact = st.text_input("Contact")
        submitted_p = st.form_submit_button("Create Provider")
        if submitted_p:
            try:
                run_query("""
                    INSERT INTO providers (Provider_ID, Name, Type, Address, City, Contact)
                    VALUES (?, ?, ?, ?, ?, ?);
                """, (int(Provider_ID), Name, Type, Address, City, Contact))
                st.success("Provider created.")
            except Exception as e:
                st.error(f"Error: {e}")

# Receivers Tab
with tabs[3]:
    st.header("👤 Receivers")
    if os.path.exists(DB_PATH):
        st.dataframe(df_read_sql("SELECT * FROM receivers ORDER BY City, Name;"), use_container_width=True)

    with st.form("add_receiver"):
        st.subheader("Add Receiver")
        Receiver_ID = st.number_input("Receiver_ID", min_value=1, step=1)
        Name_r = st.text_input("Name", key="r_name")
        Type_r = st.selectbox("Type", ["NGO","Community Center","Individual","Shelter","Other"])
        City_r = st.text_input("City", key="r_city")
        Contact_r = st.text_input("Contact", key="r_contact")
        submitted_r = st.form_submit_button("Create Receiver")
        if submitted_r:
            try:
                run_query("""
                    INSERT INTO receivers (Receiver_ID, Name, Type, City, Contact)
                    VALUES (?, ?, ?, ?, ?);
                """, (int(Receiver_ID), Name_r, Type_r, City_r, Contact_r))
                st.success("Receiver created.")
            except Exception as e:
                st.error(f"Error: {e}")

# Analytics Tab
with tabs[4]:
    st.header("📈 Analytics")
    if os.path.exists(DB_PATH):
        # Top cities by listings
        city_counts = df_read_sql("SELECT Location AS City, COUNT(*) AS Listings FROM food_listings GROUP BY Location ORDER BY Listings DESC;")
        st.bar_chart(city_counts.set_index("City"))

        # Claim status distribution
        status_df = df_read_sql("SELECT Status, COUNT(*) AS Count FROM claims GROUP BY Status;")
        st.dataframe(status_df, use_container_width=True)

        # Meal type claims
        meal_claims = df_read_sql("""
            SELECT fl.Meal_Type, COUNT(*) AS Claim_Count
            FROM claims c JOIN food_listings fl ON fl.Food_ID = c.Food_ID
            GROUP BY fl.Meal_Type ORDER BY Claim_Count DESC;
        """)
        st.bar_chart(meal_claims.set_index("Meal_Type"))

# SQL Queries Tab
with tabs[5]:
    st.header("🧠 Predefined SQL Insights")
    st.caption("These cover 15+ questions from the problem statement.")
    if os.path.exists(DB_PATH):
        # Load and split queries by semicolon pairs considering comments
        with open(QUERIES_PATH, "r", encoding="utf-8") as f:
            raw = f.read()
        # Very simple split: each SELECT/CTE block ends with semicolon
        statements = [s.strip() for s in raw.split(";") if s.strip()]
        for i, stmt in enumerate(statements, start=1):
            # Skip non-SELECT DDL
            if not stmt.lower().startswith(("select","with")):
                continue
            st.markdown(f"**Query {i}**")
            # Provide parameter defaults
            params = ()
            if ":city" in stmt:
                # default to first chosen city or any city present
                city_df = df_read_sql("SELECT City FROM providers WHERE City IS NOT NULL LIMIT 1;")
                default_city = city_df["City"].iloc[0] if not city_df.empty else "Bengaluru"
                stmt = stmt.replace(":city", "?")
                params = (default_city,)
                st.caption(f"Param city = {default_city}")
            if ":days" in stmt:
                stmt = stmt.replace(":days", "?")
                params = (7,)
                st.caption("Param days = 7")
            try:
                df = df_read_sql(stmt, params)
                st.dataframe(df, use_container_width=True)
            except Exception as e:
                st.error(f"Error running query: {e}\n\n{stmt}")

# Admin Tab
with tabs[6]:
    st.header("🛠️ Admin")
    st.markdown("Re-run ETL to rebuild DB from CSVs in `./data` (or use dummy data if missing).")
    if st.button("Rebuild DB Now"):
        import importlib.util, sys
        etl_path = os.path.join(ROOT, "etl_init_db.py")
        spec = importlib.util.spec_from_file_location("etl_init_db", etl_path)
        etl = importlib.util.module_from_spec(spec)
        sys.modules["etl_init_db"] = etl
        spec.loader.exec_module(etl)
        info = etl.build_database()
        st.success(f"Rebuilt. Counts: {info['counts']}")
