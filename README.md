# Delinquency Report Generator

## Description
This project generates a delinquency report that lists all active accounts with overdue invoices. The report includes:
- Client name
- Unit number
- Invoice amount
- Due date
- Payment date
- Overdue business days

The script uses SQL queries to fetch data from Athena, processes it in Python, and exports the results to an Excel file.

---

## Features
- Executes complex SQL queries across multiple databases.
- Processes data with Python and Pandas for easy manipulation.
- Generates an Excel report with structured and relevant information.

---

## Workflow
1. **SQL Query**: Queries overdue accounts and relevant details using ANSI-compliant SQL.
2. **Python ETL**: 
   - Reads the query from a file.
   - Executes it via AWS Athena.
   - Processes the result into a Pandas DataFrame.
   - Exports the data to an Excel file.
3. **Output**: The report is saved locally in the specified directory.

---

## Requirements
- **Python Libraries**: 
  - `awswrangler`
  - `pandas`
  - `openpyxl`
- AWS credentials configured for Athena access.

---

## Usage
1. Place the SQL query in the specified directory.
2. Run the Python script to fetch data and generate the report.
3. The Excel file will be saved in the designated folder.

---

## Notes
- Ensure Athena and the database are configured properly.
- Modify file paths and database names as needed for your environment.
