import pandas as pd
from sqlalchemy import create_engine
import os

# ------------------------------
# CONFIGURATION
# ------------------------------
# Path to your Excel file
FILE_PATH = os.path.join("data", "NP_Visitor_Stats.xlsx") 

# PostgreSQL connection details
DB_NAME = "national_parks"
DB_USER = "postgres"           
DB_PASS = "Math13Rock$"                   
DB_HOST = "localhost"
DB_PORT = "5432"

# ------------------------------
# STEP 1: Load Excel file
# ------------------------------
print("Loading Excel file...")
df = pd.read_excel(FILE_PATH, engine="openpyxl", header=2)
print("Original columns: ", df.columns.tolist())

# ------------------------------
# STEP 2: Clean column names
# ------------------------------
df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
    .str.replace(r"[^\w_]", "", regex=True)
)

print("Cleaned columns:", df.columns.tolist())

# ------------------------------
# STEP 3: Clean numeric fields
# ------------------------------
# List of numeric columns that may have commas or NaNs
numeric_cols = [
    'recreation_visits', 'nonrecreation_visits',
    'recreation_hours', 'nonrecreation_hours',
    'concessionerlodging', 'concessionercamping',
    'tent_campers', 'rv_campers', 'backcountry_campers',
    'nonrecreation_overnight_stays', 'misc_overnight_stays'
]

for col in numeric_cols:
    df[col] = (
        df[col]
        .astype(str)
        .str.replace(",", "", regex=False)
        .replace("nan", "0")
        .astype(int)
    )

# ------------------------------
# STEP 4: Connect to PostgreSQL
# ------------------------------
print("Connecting to PostgreSQL...")
conn_str = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(conn_str)

# ------------------------------
# STEP 5: Insert data
# ------------------------------
print("Inserting data into database...")
df.to_sql("park_visitation_raw", engine, if_exists="append", index=False)
print("Done!")