# Raw Data

This directory contains raw input data used as sources for the ETL pipeline.

## Data Sources
The raw datasets are collected from the following sources:
- Bank of Thailand (BOT) API  
  - Gross NPL  
  - %NPL  
  - Total Loan  
  - Flow Rate (S2 → S3)  
  - Minimum Retail Rate (MRR)
- Office of the National Economic and Social Development Council (NESDC)  
  - Quarterly GDP Growth (YoY)
- Bureau of Trade and Economic Indices, Ministry of Commerce  
  - Inflation Rate
- Manually curated policy data  
  - Credit Card Minimum Payment Rate (based on published BOT announcements)
  - Macro Shock Index

## Notes
- Raw data files are provided in Excel or CSV format.
- Some datasets require manual input due to API or licensing limitations.
- Raw data files are excluded from the repository to comply with data usage policies and file size constraints.
