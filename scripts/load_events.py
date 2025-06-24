import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import os
from datetime import datetime

# ----------------------------
# Configuration
# ----------------------------

# Path to your CSV file
FILE_PATH = os.path.join("data", "events.csv")

# PostgreSQL connection parameters
DB_NAME = "national_parks"
DB_USER = "postgres"
DB_PASS = ""
DB_HOST = "localhost"
DB_PORT = "5432"

# ----------------------------
# Connect to PostgreSQL
# ----------------------------

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
conn = engine.connect()

# ----------------------------
# Load CSV
# ----------------------------

df = pd.read_csv(FILE_PATH)
df = df.dropna(how='all')  # Remove fully blank rows that might cause unexpected NaNs

# ----------------------------
# Step 1: Insert event_types if not exists
# ----------------------------

event_types = df["event_type"].dropna().unique()
event_type_id_map = {}

for event_type in event_types:
    # Check if this event_type already exists
    res = conn.execute(
        text("SELECT id FROM event_types WHERE event_type = :etype"),
        {"etype": event_type}
    )
    row = res.fetchone()

    if row:
        event_type_id_map[event_type] = row[0]
    else:
        # Insert new event_type
        res = conn.execute(
            text("INSERT INTO event_types (event_type) VALUES (:etype) RETURNING id"),
            {"etype": event_type}
        )
        event_type_id_map[event_type] = res.fetchone()[0]

# ----------------------------
# Step 2: Insert major_us_events if not exists
# ----------------------------

event_id_map = {}

for idx, row in df.iterrows():
    event_name = row["event_name"]
    event_type_id = event_type_id_map[row["event_type"]]
    start_date = pd.to_datetime(row["start_date"])
    end_date = pd.to_datetime(row["end_date"])
    year = start_date.year
    month = start_date.month
    impact_level = row.get("impact_level", None)
    description = row.get("description", None)
    source_url = row.get("source_url", None)

    # Check if event already exists based on name and start date
    check_res = conn.execute(
        text("SELECT id FROM major_us_events WHERE event_name = :name AND start_date = :sdate"),
        {"name": event_name, "sdate": start_date}
    )
    existing = check_res.fetchone()

    if existing:
        event_id = existing[0]
    else:
        # Insert new event
        res = conn.execute(text("""
            INSERT INTO major_us_events (
                event_name, event_type_id, start_date, end_date, year, month,
                impact_level, description, source_url
            )
            VALUES (
                :event_name, :event_type_id, :start_date, :end_date,
                :year, :month, :impact_level, :description, :source_url
            )
            RETURNING id
        """), {
            "event_name": event_name,
            "event_type_id": event_type_id,
            "start_date": start_date,
            "end_date": end_date,
            "year": year,
            "month": month,
            "impact_level": impact_level,
            "description": description,
            "source_url": source_url
        })
        event_id = res.fetchone()[0]

    event_id_map[idx] = event_id

# ----------------------------
# Step 3: Insert event_locations if not exists
# ----------------------------

for idx, row in df.iterrows():
    event_id = event_id_map[idx]
    state = row.get("state", None)
    region = row.get("region", None)

    # Skip insert if both state and region are blank (national event)
    if pd.isna(state) and pd.isna(region):
        continue

    # Check if this exact location is already linked to this event
    loc_check = conn.execute(text("""
        SELECT id FROM event_locations
        WHERE event_id = :event_id AND
              state IS NOT DISTINCT FROM :state AND
              region IS NOT DISTINCT FROM :region
    """), {
        "event_id": event_id,
        "state": state,
        "region": region
    })

    if not loc_check.fetchone():
        # Insert new location mapping
        conn.execute(text("""
            INSERT INTO event_locations (event_id, state, region)
            VALUES (:event_id, :state, :region)
        """), {
            "event_id": event_id,
            "state": state,
            "region": region
        })

conn.commit()
conn.close()
print("Events loaded successfully.")