## Data Cleaning Automation (Python)

This project demonstrates a **Python-based data cleaning workflow** that automatically transforms messy customer data into a clean, standardized dataset.

----------------------------------------------------------------

## Features
- Cleans encoding issues (e.g. "ZÃ¼rich" → "Zürich")
- Standardizes names, emails, phone numbers, cities, and currencies
- Converts mixed date formats to "YYYY-MM-DD"
- Formats totals as "5204.00 SEK"
- Outputs Excel-ready, semicolon-separated CSV

----------------------------------------------------------------

## Example

| **Before**                  | **After**                  |
|-----------------------------|----------------------------|
| oscar nilsson               | Oscar Nilsson              |
| NILSSON_OSCAR @HOTMAIL.com  | nilsson_oscar@hotmail.com  |
| 0046-14294019               | (+46) 14294019             |
| goteborg                    | Göteborg                   |
| 8/15/25                     | 2025-08-15                 |
| SEK5204                     | 5204.00 SEK                |
-

----------------------------------------------------------------

## How to Run
```bash
pip install pandas
python clean_data.py

----------------------------------------------------------------

## WHY THIS PROJECT?
Data cleaning is one of the most time-consuming steps in analytics.
This project shows how Python automation can replace manual Excel work and ensure consistent, high-quality datasets for analysis or dashboards.

