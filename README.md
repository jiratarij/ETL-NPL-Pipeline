# NPL Household ETL Pipeline

## Project Overview
This project demonstrates an end-to-end ETL pipeline designed to collect, clean, transform, and prepare household Non-Performing Loan (NPL) data for analytical and forecasting purposes.  

The pipeline integrates macroeconomic indicators and policy-related variables to support structured analysis across different personal consumption segments.

This project was developed as a self-initiated data engineering and analytics exercise to practice ETL workflow design, data validation, and dataset preparation for downstream analysis.

---

## Objectives
- Build a structured ETL pipeline for household NPL data
- Integrate macroeconomic and policy-related datasets
- Prepare analytics-ready datasets for modeling and reporting
- Design the pipeline to support future automation and orchestration

---

## Data Sources
- **Bank of Thailand (BOT)**
  - Gross NPL
  - %NPL
  - Total Loan
  - Flow Rate (S2 → S3)
  - Minimum Retail Rate (MRR)
- **Office of the National Economic and Social Development Council (NESDC)**
  - Quarterly GDP Growth (YoY)
- **Ministry of Commerce, Thailand**
  - Inflation Rate
- **Manual Policy Data**
  - Credit Card Minimum Payment Rate (based on BOT announcements)

Due to data licensing and size constraints, raw data files are excluded from this repository.

---

## ETL Pipeline Design

### 1. Extract
- Load data from Excel, CSV files, and BOT API services
- Handle both automated and manually curated data sources
- Standardize column formats and time periods

### 2. Transform
- Data cleaning and validation
- Handle missing and inconsistent values
- Compute derived metrics:
  - Total Loan (from Gross NPL and %NPL)
  - Flow Rate Ratio (S2 → S3)
- Apply normalization where applicable
- Create a **Macro Shock Index (0/1)** to account for COVID-19 structural breaks
- Organize transformed data by personal consumption sections:
  - Housing
  - Automobile
  - Credit Card

### 3. Load
- Store processed datasets in structured, analytics-ready formats
- Prepare final joined datasets for modeling and reporting

---

## Data Integration Logic
Final datasets are prepared as follows:

- **Housing**
  - NPL + GDP Growth + Inflation Rate + Macro Shock Index + MRR
- **Automobile**
  - NPL + GDP Growth + Inflation Rate + Macro Shock Index
- **Credit Card**
  - NPL + GDP Growth + Inflation Rate + Macro Shock Index + Minimum Payment Rate

---

## Project Structure
```text
NPL-ETL-Pipeline/
│
├─ data/
│   ├─ raw.md
│   ├─ processed.md
│   └─ output.md
│
├─ notebooks/
│   └─ ETL-NPL-Pipeline.ipynb
│
├─ src/
│   └─ ETL-NPL_Pipeline.py
│
├─ README.md
└─ .gitignore
