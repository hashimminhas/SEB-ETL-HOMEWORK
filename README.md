# ECB Exchange Rates ETL Pipeline

## Description

This Python ETL (Extract, Transform, Load) pipeline processes European Central Bank (ECB) exchange rate data from CSV files. The script extracts daily and historical exchange rates for four major currencies (USD, SEK, GBP, JPY), calculates statistical means from historical data, and generates a formatted HTML report for easy visualization.

## Features

- ✅ **Strong typing**: Type hints for all variables and function returns (Python 3.12+ compatible)
- ✅ **Robust error handling**: Comprehensive exception handling for file operations and data parsing
- ✅ **Progress tracking**: Clear console output showing each step of the pipeline
- ✅ **Data validation**: Handles missing values and malformed data gracefully
- ✅ **Professional output**: Styled HTML table with modern CSS design

## Requirements

- **Python**: 3.12 or higher
- **Dependencies**: Listed in `requirements.txt`
  - pandas >= 2.2.0

## Installation

1. **Clone or download** the project to your local machine

2. **Create a virtual environment** (recommended):
   ```powershell
   python -m venv .venv
   ```

3. **Activate the virtual environment**:
   ```powershell
   .\.venv\Scripts\Activate.ps1
   ```

4. **Install dependencies**:
   ```powershell
   pip install -r requirements.txt
   ```

## Usage

### Running the Script

Execute the ETL pipeline with:

```powershell
python etl_exchange_rates.py
```

Or with the full path to your virtual environment:

```powershell
& "C:/Users/Hashim Ali/Desktop/seb-etl-homework/.venv/Scripts/python.exe" etl_exchange_rates.py
```

### Input Files

The script expects two CSV files in the project directory:

- `eurofxref.csv` - Daily exchange rates (most recent)
- `eurofxref-hist.csv` - Historical exchange rates

### Pipeline Steps

The script performs the following operations:

1. **Extract** - Reads daily rates from `eurofxref.csv`
2. **Extract** - Reads historical rates from `eurofxref-hist.csv`
3. **Transform** - Filters data for target currencies (USD, SEK, GBP, JPY)
4. **Transform** - Calculates mean historical rates using pandas
5. **Load** - Generates and saves HTML report to `exchange_rates.html`

## Output

The script generates `exchange_rates.html` containing:

- **Currency Code** - The three-letter currency code
- **Rate** - Current exchange rate from the daily file
- **Mean Historical Rate** - Average rate calculated from historical data

The HTML file includes:
- Professional styling with modern CSS
- Responsive table layout
- Hover effects for better readability
- Clean typography optimized for viewing in any browser

Simply open `exchange_rates.html` in your web browser to view the results.

## File Structure

```
seb-etl-homework/
│
├── etl_exchange_rates.py      # Main ETL script
├── requirements.txt            # Python dependencies
├── README.md                   # Project documentation
│
├── eurofxref.csv              # Daily exchange rates (input)
├── eurofxref-hist.csv         # Historical exchange rates (input)
├── exchange_rates.html        # Generated report (output)
│
└── .venv/                     # Virtual environment (recommended)
```

## Code Structure

The script is organized into modular functions:

- `parse_daily_rates()` - Extracts current rates from CSV
- `parse_historical_rates()` - Extracts historical data from CSV
- `calculate_mean_rates()` - Computes statistical means using pandas
- `create_html_table()` - Generates styled HTML output
- `save_html_report()` - Writes HTML file to disk
- `main()` - Orchestrates the entire ETL pipeline

## Error Handling

The script includes comprehensive error handling for:

- Missing or inaccessible files
- Malformed CSV data
- Missing currency columns
- Invalid numeric values
- File write permissions

All errors are logged to the console with clear, descriptive messages.

## AI Tool Usage

This solution was developed with assistance from GitHub Copilot and Claude AI for code structure, XML parsing logic, and documentation.

## License

This project is provided as-is for educational and demonstration purposes.

---

**Author**: Developed as part of SEB ETL homework assignment  
**Date**: February 2026
