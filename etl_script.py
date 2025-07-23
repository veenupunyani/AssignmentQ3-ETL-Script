"""
ETL script to extract, transform, and load Washington State EV population data
into a dimensional schema (Snowflake-compatible). Written for evaluation purposes.

This script fulfills the following requirements:
1. Extract and Explore - Downloads data and performs statistical analysis
2. Clean and Transform - Handles missing values and encodes categorical variables
3. Load Data as Facts and Dimensions - Creates proper star schema with foreign keys
"""

import pandas as pd
import numpy as np
import requests
from io import StringIO
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

print("=" * 60)
print("WASHINGTON STATE EV POPULATION DATA ETL PIPELINE")
print("=" * 60)

# -------------------
# Step 1: Extract and Explore
# -------------------

print("\n1. EXTRACTING DATA...")
print("-" * 30)

# Public Washington State EV Data URL
CSV_URL = "https://data.wa.gov/api/views/f6w7-q2d2/rows.csv"

# Download CSV and load into DataFrame
print(f"Downloading data from: {CSV_URL}")
response = requests.get(CSV_URL)
if response.status_code != 200:
    raise Exception(f"Failed to download dataset. Status code: {response.status_code}")

raw_csv = StringIO(response.text)
df = pd.read_csv(raw_csv)

print(f"✓ Successfully loaded {len(df):,} records")
print(f"✓ Dataset contains {len(df.columns)} columns")

# Examine dataset structure
print("\n2. EXAMINING DATA STRUCTURE...")
print("-" * 35)
print(f"Dataset shape: {df.shape}")
print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

print("\nColumn Information:")
print(df.info())

print("\nFirst 5 rows:")
print(df.head())

print("\nColumn names:")
for i, col in enumerate(df.columns, 1):
    print(f"{i:2d}. {col}")

# Data quality assessment
print("\n3. DATA QUALITY ASSESSMENT...")
print("-" * 35)
missing_data = df.isnull().sum()
missing_pct = (missing_data / len(df)) * 100

print("Missing values by column:")
for col in missing_data[missing_data > 0].index:
    print(f"  {col}: {missing_data[col]:,} ({missing_pct[col]:.1f}%)")

# Explore characteristics of at least 3 key features
print("\n4. STATISTICAL ANALYSIS OF KEY FEATURES...")
print("-" * 45)

# Feature 1: Electric Range
if 'Electric Range' in df.columns:
    electric_range = df['Electric Range'].dropna()
    print("\nFEATURE 1: Electric Range")
    print(f"  Count: {len(electric_range):,}")
    print(f"  Mean: {electric_range.mean():.2f} miles")
    print(f"  Median: {electric_range.median():.2f} miles")
    print(f"  Standard Deviation: {electric_range.std():.2f} miles")
    print(f"  Min: {electric_range.min():.0f} miles")
    print(f"  Max: {electric_range.max():.0f} miles")
    print(f"  25th Percentile: {electric_range.quantile(0.25):.2f} miles")
    print(f"  75th Percentile: {electric_range.quantile(0.75):.2f} miles")

# Feature 2: Model Year
if 'Model Year' in df.columns:
    model_year = df['Model Year'].dropna()
    print("\nFEATURE 2: Model Year")
    print(f"  Count: {len(model_year):,}")
    print(f"  Mean: {model_year.mean():.1f}")
    print(f"  Median: {model_year.median():.0f}")
    print(f"  Standard Deviation: {model_year.std():.2f}")
    print(f"  Min: {model_year.min():.0f}")
    print(f"  Max: {model_year.max():.0f}")
    print(f"  Most common years: {model_year.value_counts().head(3).to_dict()}")

# Feature 3: Base MSRP
if 'Base MSRP' in df.columns:
    base_msrp = df['Base MSRP'].dropna()
    if len(base_msrp) > 0:
        print("\nFEATURE 3: Base MSRP")
        print(f"  Count: {len(base_msrp):,}")
        print(f"  Mean: ${base_msrp.mean():,.2f}")
        print(f"  Median: ${base_msrp.median():,.2f}")
        print(f"  Standard Deviation: ${base_msrp.std():,.2f}")
        print(f"  Min: ${base_msrp.min():,.2f}")
        print(f"  Max: ${base_msrp.max():,.2f}")

# Distribution analysis for categorical variables
print("\nCATEGORICAL VARIABLE DISTRIBUTIONS:")
categorical_cols = ['Make', 'Electric Vehicle Type', 'County']
for col in categorical_cols:
    if col in df.columns:
        print(f"\n{col} distribution (top 10):")
        print(df[col].value_counts().head(10))

# -------------------
# Step 2: Clean and Transform
# -------------------

print("\n5. DATA CLEANING AND TRANSFORMATION...")
print("-" * 40)

# Store original row count for reporting
original_rows = len(df)
print(f"Starting with {original_rows:,} records")

# Drop rows where critical identifiers are missing (VIN, Make, Model)
critical_cols = ["VIN (1-10)", "Make", "Model"]
df.dropna(subset=critical_cols, inplace=True)
print(f"✓ Removed {original_rows - len(df):,} rows with missing critical data")

# Handle missing values with consistent strategy
print("\nHandling missing values:")

# Numeric fields - fill with 0 or sensible defaults
numeric_defaults = {
    "Electric Range": 0,
    "Base MSRP": 0,
    "Legislative District": -1,  # -1 indicates unknown district
    "2020 Census Tract": -1,    # -1 indicates unknown tract
    "Model Year": df["Model Year"].mode()[0] if not df["Model Year"].mode().empty else 2020
}

for col, default_val in numeric_defaults.items():
    if col in df.columns:
        missing_count = df[col].isnull().sum()
        if missing_count > 0:
            df[col] = df[col].fillna(default_val)
            print(f"  ✓ {col}: Filled {missing_count:,} missing values with {default_val}")

# Categorical fields - fill with 'Unknown'
categorical_defaults = {
    "County": "Unknown",
    "City": "Unknown", 
    "State": "WA",  # All should be WA for this dataset
    "Postal Code": "00000",
    "Clean Alternative Fuel Vehicle (CAFV) Eligibility": "Unknown"
}

for col, default_val in categorical_defaults.items():
    if col in df.columns:
        missing_count = df[col].isnull().sum()
        if missing_count > 0:
            df[col] = df[col].fillna(default_val)
            print(f"  ✓ {col}: Filled {missing_count:,} missing values with '{default_val}'")

# Encode categorical variables for storage optimization
print("\nEncoding categorical variables:")

# 1. Electric Vehicle Type encoding
if "Electric Vehicle Type" in df.columns:
    df["ev_type_code"] = df["Electric Vehicle Type"].astype("category").cat.codes
    ev_type_mapping = dict(enumerate(df["Electric Vehicle Type"].astype("category").cat.categories))
    print(f"  ✓ Electric Vehicle Type encoded as ev_type_code")
    print(f"    Mapping: {ev_type_mapping}")

# 2. Make encoding for efficiency (optional - keeping text for readability)
if "Make" in df.columns:
    df["make_code"] = df["Make"].astype("category").cat.codes
    make_mapping = dict(enumerate(df["Make"].astype("category").cat.categories))
    print(f"  ✓ Vehicle Make encoded as make_code ({len(make_mapping)} unique makes)")

# 3. CAFV Eligibility encoding
if "Clean Alternative Fuel Vehicle (CAFV) Eligibility" in df.columns:
    df["cafv_code"] = df["Clean Alternative Fuel Vehicle (CAFV) Eligibility"].astype("category").cat.codes
    cafv_mapping = dict(enumerate(df["Clean Alternative Fuel Vehicle (CAFV) Eligibility"].astype("category").cat.categories))
    print(f"  ✓ CAFV Eligibility encoded as cafv_code")
    print(f"    Mapping: {cafv_mapping}")

# Normalize column names for warehouse compatibility
print("\nNormalizing column names for database compatibility...")
original_columns = df.columns.tolist()
df.columns = [col.strip().lower().replace(" ", "_").replace("(", "").replace(")", "").replace("/", "_") for col in df.columns]

# Show transformation summary
print(f"✓ Standardized {len(original_columns)} column names")
print(f"✓ Final dataset: {len(df):,} records, {len(df.columns)} columns")

# Data validation
print("\nData validation:")
print(f"  ✓ No missing values in VIN: {df['vin_1-10'].isnull().sum() == 0}")
print(f"  ✓ All records have Make: {df['make'].isnull().sum() == 0}")
print(f"  ✓ All records have Model: {df['model'].isnull().sum() == 0}")

# -------------------
# Step 3: Load Data as Facts and Dimensions
# -------------------

print("\n6. CREATING DIMENSIONAL MODEL...")
print("-" * 35)

# Create SQLite engine for demo (replace with Snowflake in production)
engine = create_engine("sqlite:///ev_datawarehouse.db")
print("✓ Database connection established (SQLite for demo)")

# Design dimensional model following star schema principles
print("\nCreating dimension tables...")

# Dimension 1: Vehicle Dimension
print("  Creating dim_vehicle...")
dim_vehicle = df[["make", "model", "model_year", "electric_vehicle_type", "ev_type_code", "make_code"]].drop_duplicates().reset_index(drop=True)
dim_vehicle['vehicle_key'] = dim_vehicle.index + 1  # Surrogate key
print(f"    ✓ dim_vehicle: {len(dim_vehicle):,} unique vehicles")

# Dimension 2: Location Dimension  
print("  Creating dim_location...")
location_cols = ["county", "city", "state", "postal_code", "legislative_district", "_2020_census_tract"]
# Handle column name variations
available_location_cols = [col for col in location_cols if col in df.columns]
if "2020_census_tract" in df.columns and "_2020_census_tract" not in df.columns:
    available_location_cols.append("2020_census_tract")
    
dim_location = df[available_location_cols].drop_duplicates().reset_index(drop=True)
dim_location['location_key'] = dim_location.index + 1  # Surrogate key
print(f"    ✓ dim_location: {len(dim_location):,} unique locations")

# Dimension 3: CAFV Eligibility Dimension
print("  Creating dim_cafv...")
cafv_cols = ["clean_alternative_fuel_vehicle_cafv__eligibility", "cafv_code"]
available_cafv_cols = [col for col in cafv_cols if col in df.columns]
dim_cafv = df[available_cafv_cols].drop_duplicates().reset_index(drop=True)
dim_cafv['cafv_key'] = dim_cafv.index + 1  # Surrogate key
print(f"    ✓ dim_cafv: {len(dim_cafv):,} unique eligibility types")

# Create lookup dictionaries for foreign key mapping
print("\nCreating foreign key mappings...")

# Vehicle lookup
vehicle_lookup = {}
for _, row in dim_vehicle.iterrows():
    key = (row['make'], row['model'], row['model_year'], row.get('electric_vehicle_type', ''))
    vehicle_lookup[key] = row['vehicle_key']

# Location lookup  
location_lookup = {}
for _, row in dim_location.iterrows():
    # Create key from available location columns
    key_parts = []
    for col in available_location_cols:
        key_parts.append(str(row[col]) if pd.notna(row[col]) else '')
    key = tuple(key_parts)
    location_lookup[key] = row['location_key']

# CAFV lookup
cafv_lookup = {}
if available_cafv_cols:
    for _, row in dim_cafv.iterrows():
        key = row[available_cafv_cols[0]] if available_cafv_cols else 'Unknown'
        cafv_lookup[key] = row['cafv_key']

# Fact Table: EV Registration Facts
print("  Creating fact_ev_registration...")

# Map foreign keys to fact table
df['vehicle_key'] = df.apply(lambda row: vehicle_lookup.get(
    (row['make'], row['model'], row['model_year'], row.get('electric_vehicle_type', '')), 1), axis=1)

df['location_key'] = df.apply(lambda row: location_lookup.get(
    tuple(str(row[col]) if pd.notna(row[col]) else '' for col in available_location_cols), 1), axis=1)

if available_cafv_cols:
    df['cafv_key'] = df.apply(lambda row: cafv_lookup.get(row[available_cafv_cols[0]], 1), axis=1)
else:
    df['cafv_key'] = 1

# Create fact table with measures and foreign keys
fact_columns = [
    "vin_1-10", "dol_vehicle_id", "vehicle_key", "location_key", "cafv_key",
    "electric_range", "base_msrp"
]
available_fact_cols = [col for col in fact_columns if col in df.columns]
fact_ev = df[available_fact_cols].copy()

# Add load timestamp
fact_ev['load_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f"    ✓ fact_ev_registration: {len(fact_ev):,} records")

print("\n7. LOADING TO DATA WAREHOUSE...")
print("-" * 35)

# Load dimensions first (for referential integrity)
try:
    dim_vehicle.to_sql("dim_vehicle", con=engine, if_exists="replace", index=False)
    print("  ✓ dim_vehicle loaded successfully")
    
    dim_location.to_sql("dim_location", con=engine, if_exists="replace", index=False)
    print("  ✓ dim_location loaded successfully")
    
    dim_cafv.to_sql("dim_cafv", con=engine, if_exists="replace", index=False)
    print("  ✓ dim_cafv loaded successfully")
    
    # Load fact table
    fact_ev.to_sql("fact_ev_registration", con=engine, if_exists="replace", index=False)
    print("  ✓ fact_ev_registration loaded successfully")
    
except Exception as e:
    print(f"  ✗ Error loading data: {str(e)}")
    raise

# Summary statistics
print("\n8. ETL PIPELINE SUMMARY...")
print("-" * 30)
print(f"✓ Extracted {original_rows:,} records from source")
print(f"✓ Cleaned and transformed data (removed {original_rows - len(df):,} invalid records)")
print(f"✓ Created dimensional model with:")
print(f"    - {len(dim_vehicle):,} vehicle dimensions")
print(f"    - {len(dim_location):,} location dimensions") 
print(f"    - {len(dim_cafv):,} CAFV eligibility dimensions")
print(f"    - {len(fact_ev):,} fact records")
print(f"✓ Data warehouse schema ready for analytics")

print("\n" + "=" * 60)
print("ETL PROCESS COMPLETED SUCCESSFULLY!")
print("Data is now available in the dimensional model for analysis.")
print("=" * 60)
