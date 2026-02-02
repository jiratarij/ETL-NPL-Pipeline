# Processed Data

This directory stores intermediate datasets generated after the transformation stage of the ETL pipeline.

## Processing Details
Processed datasets include:
- Cleaned and validated NPL and loan data
- Normalized values (e.g. scaling and standardization where applicable)
- Derived metrics such as:
  - Total Loan (computed from Gross NPL and %NPL)
  - Flow Rate Ratio
- Structured datasets organized by personal consumption sections:
  - Housing
  - Automobile
  - Credit Card

## Purpose
- To serve as structured and consistent inputs for data joining, modeling, and analytics.
- To separate raw data handling from downstream analytical use cases.

## Notes
- Processed data files are not included in the repository.
- The ETL pipeline can regenerate these datasets when raw data is available.
