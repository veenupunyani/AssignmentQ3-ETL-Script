# AssignmentQ3-ETL-Script
ETL pipeline for Washington State EV data with Snowflake-ready schema and Python-based processing.

# Electric Vehicle Population Data ETL

This project extracts, transforms, and loads (ETL) the Washington State Electric Vehicle dataset into a Snowflake-compatible dimensional schema using Python. The ETL script fully satisfies the assignment requirements for data warehousing with proper exploration, cleaning, and dimensional modeling.

## Assignment Requirements Fulfilled

### ✅ 1. Extract and Explore
- **Data Extraction**: Downloads dataset from Washington State's public API
- **Structure Examination**: Analyzes dataset shape, columns, and data types
- **Statistical Analysis**: Performs comprehensive analysis of at least 3 key features:
  - **Electric Range**: Central tendency, distribution, and dispersion analysis
  - **Model Year**: Temporal distribution and trends
  - **Base MSRP**: Price analysis and statistical measures
- **Data Quality Assessment**: Identifies missing values and data quality issues

### ✅ 2. Clean and Transform
- **Missing Value Handling**: Consistent strategy for different data types
  - Numeric fields: Filled with 0 or sensible defaults
  - Categorical fields: Filled with 'Unknown' or appropriate defaults
- **Categorical Encoding**: Multiple variables encoded for storage optimization:
  - Electric Vehicle Type → `ev_type_code`
  - Vehicle Make → `make_code` 
  - CAFV Eligibility → `cafv_code`
- **Data Standardization**: Column names normalized for database compatibility

### ✅ 3. Load Data as Facts and Dimensions
- **Dimensional Model**: Proper star schema implementation
  - `dim_vehicle`: Vehicle attributes (make, model, year, type)
  - `dim_location`: Geographic information (county, city, district)
  - `dim_cafv`: Clean fuel eligibility information
  - `fact_ev_registration`: Central fact table with measures and foreign keys
- **Foreign Key Relationships**: Proper referential integrity with surrogate keys
- **Data Warehouse Ready**: Schema optimized for analytical queries

## Data Source

- **URL**: [Washington State Electric Vehicle Population Data](https://data.wa.gov/api/views/f6w7-q2d2/rows.csv)
- **Format**: CSV via public API
- **Update Frequency**: Regularly updated by Washington State Department of Licensing

## Features

- **Comprehensive Data Exploration**: Statistical analysis and data profiling
- **Robust Data Cleaning**: Handles missing values and data quality issues
- **Optimized Storage**: Categorical encoding reduces storage requirements
- **Dimensional Modeling**: Star schema for efficient analytical queries
- **Production Ready**: Easy to modify for Snowflake or other cloud warehouses
- **Detailed Logging**: Comprehensive progress reporting and validation

## Technical Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────────┐
│   Data Source   │───▶│   ETL Process    │───▶│   Data Warehouse    │
│                 │    │                  │    │                     │
│ • WA State API  │    │ • Extract        │    │ • dim_vehicle       │
│ • CSV Format    │    │ • Explore        │    │ • dim_location      │
│ • 150K+ records │    │ • Clean          │    │ • dim_cafv          │
│                 │    │ • Transform      │    │ • fact_ev_registration │
│                 │    │ • Load           │    │                     │
└─────────────────┘    └──────────────────┘    └─────────────────────┘
```

## Requirements

- Python 3.8+
- Required packages:
  ```
  pandas>=1.3.0
  requests>=2.25.0  
  sqlalchemy>=1.4.0
  numpy>=1.21.0
  matplotlib>=3.3.0
  seaborn>=0.11.0
  ```

## Installation & Usage

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/AssignmentQ3-ETL-Script.git
   cd AssignmentQ3-ETL-Script
   ```

2. **Install dependencies:**
   ```bash
   pip install pandas requests sqlalchemy numpy matplotlib seaborn
   ```

3. **Run the ETL script:**
   ```bash
   python etl_script.py
   ```

4. **Output:**
   - SQLite database: `ev_datawarehouse.db`
   - Comprehensive console logging of all ETL steps
   - Statistical analysis results
   - Data quality reports

## Database Schema

### Dimension Tables
- **dim_vehicle**: Vehicle master data (make, model, year, type)
- **dim_location**: Geographic reference data (county, city, postal code)
- **dim_cafv**: Clean Alternative Fuel Vehicle eligibility types

### Fact Table
- **fact_ev_registration**: Core metrics and foreign key relationships

## Production Deployment

For production use with Snowflake:

1. Replace the SQLite connection string:
   ```python
   # Replace this line:
   engine = create_engine("sqlite:///ev_datawarehouse.db")
   
   # With Snowflake connection:
   engine = create_engine(
       'snowflake://user:password@account/database/schema?warehouse=warehouse_name'
   )
   ```

2. Update table creation for Snowflake-specific data types
3. Implement incremental loading for production data updates

## Data Definitions

- **VIN**: Vehicle Identification Number (first 10 characters)
- **County**: Registration county in Washington State  
- **City**: Registration city
- **State**: State of registration (all "WA")
- **Postal Code**: ZIP code of registration
- **Model Year**: Vehicle model year
- **Make**: Vehicle manufacturer
- **Model**: Vehicle model name
- **Electric Range**: EPA-estimated electric range in miles
- **Base MSRP**: Manufacturer's Suggested Retail Price
- **CAFV Eligibility**: Clean Alternative Fuel Vehicle program eligibility

## Author

Lead Solution Architect - ETL Pipeline Implementation
