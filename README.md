
# Local Food Wastage Management System

End-to-end pipeline (ETL → SQL DB → 15+ queries → Streamlit CRUD+Analytics).

## Project structure

```
food_waste_app/
├─ app.py                # Streamlit app
├─ etl_init_db.py        # Build SQLite DB from CSVs (auto-generates tiny dummy data if missing)
├─ schema.sql            # Relational schema + indexes
├─ queries.sql           # 18 ready-to-run queries
├─ data/                 # Put your 4 CSVs here (names below)
└─ foodwaste.db          # Built database (created after running ETL)
```

## Expected CSV filenames

Place your files in `data/` with exactly these names:
- `providers_data.csv`
- `receivers_data.csv`
- `food_listings_data.csv`
- `claims_data.csv`

## Quick start

1) Create a Python environment and install deps:
```
pip install -U streamlit pandas plotly
```

2) (Optional) Build DB locally (uses your CSVs if present; else makes tiny dummy data):
```
python etl_init_db.py
```

3) Launch the app:
```
streamlit run app.py
```

4) In the sidebar, you can also click **Build / Refresh DB** to rebuild from CSVs at any time.

## Notes

- The schema enforces foreign keys and category checks.
- `queries.sql` implements 18 questions including all required ones.
- The app includes full CRUD for Providers, Receivers, Listings, and Claims.
- Visual analytics: top cities by listings, claim status distribution, and meal-type claims.
