"""
ETL script for processing ECB exchange rate data from CSV files.

This script reads daily and historical exchange rate CSV files from the European Central Bank,
extracts rates for specific currencies (USD, SEK, GBP, JPY), calculates historical means,
and generates an HTML report.
"""

from pathlib import Path
from typing import Dict, List, Any
import pandas as pd
import sys


TARGET_CURRENCIES: List[str] = ['USD', 'SEK', 'GBP', 'JPY']


def parse_daily_rates(csv_file: Path) -> Dict[str, float]:
    """
    Parse the daily exchange rates CSV file and extract rates for target currencies.
    
    Args:
        csv_file: Path to the daily rates CSV file
        
    Returns:
        Dictionary mapping currency codes to their daily rates
        
    Raises:
        FileNotFoundError: If the CSV file doesn't exist
        ValueError: If the CSV is malformed, empty, or missing required currencies
    """
    print(f"📖 Reading daily rates from {csv_file.name}...")
    
    # Check if file exists before attempting to parse
    if not csv_file.exists():
        error_msg: str = f"Daily rates file not found: {csv_file}"
        print(f"❌ {error_msg}")
        raise FileNotFoundError(error_msg)
    
    try:
        # Read CSV file with flexible whitespace handling
        df: pd.DataFrame = pd.read_csv(csv_file, skipinitialspace=True)
        
        # Validate CSV is not empty
        if len(df) == 0:
            raise ValueError("CSV file is empty - no data rows found")
        
        daily_rates: Dict[str, float] = {}
        missing_currencies: List[str] = []
        invalid_currencies: List[str] = []
        
        # Extract rates for target currencies from the first row
        currency: str
        for currency in TARGET_CURRENCIES:
            if currency not in df.columns:
                missing_currencies.append(currency)
                continue
            
            rate_value: Any = df[currency].iloc[0]
            
            # Handle N/A or missing values
            if pd.isna(rate_value) or rate_value == '' or rate_value == 'N/A':
                invalid_currencies.append(currency)
                continue
            
            # Validate rate is numeric
            try:
                rate_float: float = float(rate_value)
                if rate_float <= 0:
                    print(f"⚠️ Warning: {currency} has non-positive rate: {rate_float}")
                daily_rates[currency] = rate_float
            except (ValueError, TypeError) as e:
                print(f"⚠️ Warning: Invalid rate value for {currency}: {rate_value}")
                invalid_currencies.append(currency)
        
        # Report issues with missing or invalid currencies
        if missing_currencies:
            print(f"⚠️ Warning: Missing columns in CSV: {', '.join(missing_currencies)}")
        
        if invalid_currencies:
            print(f"⚠️ Warning: Invalid/missing values for: {', '.join(invalid_currencies)}")
        
        # Ensure we have at least some valid data
        if not daily_rates:
            raise ValueError(
                f"No valid rates found for any target currencies. "
                f"Missing: {missing_currencies}, Invalid: {invalid_currencies}"
            )
        
        print(f"✓ Extracted {len(daily_rates)} daily rates")
        return daily_rates
        
    except pd.errors.EmptyDataError:
        error_msg: str = f"CSV file is empty or malformed: {csv_file}"
        print(f"❌ {error_msg}")
        raise ValueError(error_msg)
    except pd.errors.ParserError as e:
        error_msg: str = f"Failed to parse CSV file: {e}"
        print(f"❌ {error_msg}")
        raise ValueError(error_msg)
    except FileNotFoundError:
        raise
    except ValueError:
        raise
    except Exception as e:
        error_msg: str = f"Unexpected error reading daily rates: {type(e).__name__}: {e}"
        print(f"❌ {error_msg}")
        raise RuntimeError(error_msg) from e


def parse_historical_rates(csv_file: Path) -> pd.DataFrame:
    """
    Parse the historical exchange rates CSV file and extract rates for target currencies.
    
    Args:
        csv_file: Path to the historical rates CSV file
        
    Returns:
        DataFrame with columns: date, currency, rate
        
    Raises:
        FileNotFoundError: If the CSV file doesn't exist
        ValueError: If the CSV is malformed, empty, or missing required currencies
    """
    print(f"📖 Reading historical rates from {csv_file.name}...")
    
    # Check if file exists before attempting to parse
    if not csv_file.exists():
        error_msg: str = f"Historical rates file not found: {csv_file}"
        print(f"❌ {error_msg}")
        raise FileNotFoundError(error_msg)
    
    try:
        # Read CSV file with flexible whitespace handling
        df: pd.DataFrame = pd.read_csv(csv_file, skipinitialspace=True)
        
        # Validate CSV is not empty
        if len(df) == 0:
            raise ValueError("CSV file is empty - no data rows found")
        
        # Check for Date column
        if 'Date' not in df.columns:
            raise ValueError("CSV file missing required 'Date' column")
        
        # Melt the DataFrame to convert from wide to long format
        # This transforms columns (currencies) into rows
        historical_data: List[Dict[str, str | float]] = []
        missing_currencies: List[str] = []
        currencies_found: Dict[str, int] = {}
        
        currency: str
        for currency in TARGET_CURRENCIES:
            if currency not in df.columns:
                missing_currencies.append(currency)
                print(f"⚠️ Warning: Currency {currency} not found in historical data")
                continue
            
            # Extract all non-null values for this currency
            valid_count: int = 0
            idx: Any
            row: pd.Series[Any]
            for idx, row in df.iterrows():
                rate_value: Any = row[currency]
                
                # Skip invalid values
                if pd.isna(rate_value) or rate_value == '' or rate_value == 'N/A':
                    continue
                
                # Validate and convert rate
                try:
                    rate_float: float = float(rate_value)
                    date_value: Any = row['Date']
                    
                    if rate_float <= 0:
                        continue  # Skip non-positive rates
                    
                    historical_data.append({
                        'date': str(date_value),
                        'currency': currency,
                        'rate': rate_float
                    })
                    valid_count += 1
                except (ValueError, TypeError):
                    continue  # Skip rows with invalid rate values
            
            currencies_found[currency] = valid_count
        
        # Report missing currencies
        if missing_currencies:
            print(f"⚠️ Warning: Missing currencies in CSV columns: {', '.join(missing_currencies)}")
        
        # Validate we have data
        if not historical_data:
            raise ValueError(
                f"No valid historical rates found for any target currencies. "
                f"Currencies found: {currencies_found}"
            )
        
        result_df: pd.DataFrame = pd.DataFrame(historical_data)
        
        # Report summary of data extracted
        for curr, count in currencies_found.items():
            print(f"  → {curr}: {count} records")
        
        print(f"✓ Extracted {len(result_df)} historical rate records")
        return result_df
        
    except pd.errors.EmptyDataError:
        error_msg: str = f"CSV file is empty or malformed: {csv_file}"
        print(f"❌ {error_msg}")
        raise ValueError(error_msg)
    except pd.errors.ParserError as e:
        error_msg: str = f"Failed to parse CSV file: {e}"
        print(f"❌ {error_msg}")
        raise ValueError(error_msg)
    except FileNotFoundError:
        raise
    except ValueError:
        raise
    except Exception as e:
        error_msg: str = f"Unexpected error reading historical rates: {type(e).__name__}: {e}"
        print(f"❌ {error_msg}")
        raise RuntimeError(error_msg) from e


def calculate_mean_rates(df: pd.DataFrame) -> Dict[str, float]:
    """
    Calculate the mean historical rate for each currency.
    
    Args:
        df: DataFrame containing historical rates with 'currency' and 'rate' columns
        
    Returns:
        Dictionary mapping currency codes to their mean historical rates
        
    Raises:
        ValueError: If DataFrame is empty or missing required columns
    """
    print("📊 Calculating mean historical rates...")
    
    # Validate input DataFrame
    if df.empty:
        raise ValueError("Cannot calculate means: DataFrame is empty")
    
    if 'currency' not in df.columns or 'rate' not in df.columns:
        raise ValueError("DataFrame missing required columns: 'currency' and/or 'rate'")
    
    try:
        mean_rates: Dict[str, float] = df.groupby('currency')['rate'].mean().to_dict()
        
        # Validate all target currencies have means
        missing_means: List[str] = [curr for curr in TARGET_CURRENCIES if curr not in mean_rates]
        if missing_means:
            print(f"⚠️ Warning: No historical data to calculate means for: {', '.join(missing_means)}")
        
        # Report calculated means
        for currency, mean_value in mean_rates.items():
            print(f"  → {currency}: {mean_value:.4f}")
        
        print(f"✓ Calculated means for {len(mean_rates)} currencies")
        return mean_rates
    
    except Exception as e:
        error_msg: str = f"Error calculating mean rates: {type(e).__name__}: {e}"
        print(f"❌ {error_msg}")
        raise RuntimeError(error_msg) from e


def create_html_table(daily_rates: Dict[str, float], mean_rates: Dict[str, float]) -> str:
    """
    Create an HTML table with currency rates.
    
    Args:
        daily_rates: Dictionary of daily exchange rates by currency
        mean_rates: Dictionary of mean historical rates by currency
        
    Returns:
        HTML string containing the formatted table
    """
    print("🔨 Creating HTML table...")
    
    # Create DataFrame for easy table generation
    table_data: List[Dict[str, str | float]] = []
    
    currency: str
    for currency in TARGET_CURRENCIES:
        daily_rate: float | None = daily_rates.get(currency)
        mean_rate: float | None = mean_rates.get(currency)
        
        table_data.append({
            'Currency Code': currency,
            'Rate': f"{daily_rate:.4f}" if daily_rate else "N/A",
            'Mean Historical Rate': f"{mean_rate:.4f}" if mean_rate else "N/A"
        })
    
    df: pd.DataFrame = pd.DataFrame(table_data)
    
    # Generate HTML with styling
    html: str = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Exchange Rates Report</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            max-width: 800px;
            margin: 50px auto;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        h1 {{
            color: #333;
            text-align: center;
            margin-bottom: 30px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background-color: white;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        }}
        th {{
            background-color: #4CAF50;
            color: white;
            padding: 12px;
            text-align: left;
            font-weight: bold;
        }}
        td {{
            padding: 10px 12px;
            border-bottom: 1px solid #ddd;
        }}
        tr:hover {{
            background-color: #f5f5f5;
        }}
        .footer {{
            text-align: center;
            margin-top: 20px;
            color: #666;
            font-size: 14px;
        }}
    </style>
</head>
<body>
    <h1>Exchange Rates Report</h1>
    {df.to_html(index=False, border=0, classes='rate-table')}
    <div class="footer">
        <p>Exchange rates relative to EUR | Source: European Central Bank</p>
    </div>
</body>
</html>
"""
    
    print("✓ HTML table created successfully")
    return html


def save_html_report(html_content: str, output_file: Path) -> None:
    """
    Save the HTML content to a file.
    
    Args:
        html_content: HTML string to save
        output_file: Path where the HTML file should be saved
        
    Raises:
        IOError: If file cannot be written
        PermissionError: If lacking write permissions
    """
    print(f"💾 Saving HTML report to {output_file.name}...")
    
    # Validate HTML content is not empty
    if not html_content or not html_content.strip():
        raise ValueError("Cannot save empty HTML content")
    
    try:
        # Ensure parent directory exists
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        f: Any
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        # Verify file was created
        if not output_file.exists():
            raise IOError(f"File was not created: {output_file}")
        
        file_size: int = output_file.stat().st_size
        print(f"✓ Report saved successfully to {output_file.absolute()} ({file_size:,} bytes)")
        
    except PermissionError as e:
        error_msg: str = f"Permission denied writing to {output_file}: {e}"
        print(f"❌ {error_msg}")
        raise
    except IOError as e:
        error_msg: str = f"I/O error saving HTML report: {e}"
        print(f"❌ {error_msg}")
        raise
    except Exception as e:
        error_msg: str = f"Unexpected error saving HTML report: {type(e).__name__}: {e}"
        print(f"❌ {error_msg}")
        raise RuntimeError(error_msg) from e


def main() -> int:
    """
    Main ETL pipeline execution.
    
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    print("=" * 60)
    print("🚀 Starting Exchange Rate ETL Pipeline")
    print("=" * 60)
    
    try:
        # Define file paths
        current_dir: Path = Path(__file__).parent
        daily_csv: Path = current_dir / 'eurofxref.csv'
        historical_csv: Path = current_dir / 'eurofxref-hist.csv'
        output_html: Path = current_dir / 'exchange_rates.html'
        
        # Verify input files exist upfront
        missing_files: List[str] = []
        if not daily_csv.exists():
            missing_files.append(str(daily_csv))
        if not historical_csv.exists():
            missing_files.append(str(historical_csv))
        
        if missing_files:
            error_msg: str = f"Required input files not found: {', '.join(missing_files)}"
            print(f"❌ {error_msg}")
            raise FileNotFoundError(error_msg)
        
        # Step 1: Parse daily rates
        daily_rates: Dict[str, float] = parse_daily_rates(daily_csv)
        
        # Step 2: Parse historical rates
        historical_df: pd.DataFrame = parse_historical_rates(historical_csv)
        
        # Step 3: Calculate mean historical rates
        mean_rates: Dict[str, float] = calculate_mean_rates(historical_df)
        
        # Validate we have data for all target currencies (at least in one source)
        all_currencies: set[str] = set(daily_rates.keys()) | set(mean_rates.keys())
        missing_all: List[str] = [curr for curr in TARGET_CURRENCIES if curr not in all_currencies]
        
        if missing_all:
            print(f"⚠️ Warning: No data found for currencies: {', '.join(missing_all)}")
        
        if not daily_rates and not mean_rates:
            raise ValueError("No exchange rate data found in either daily or historical files")
        
        # Step 4: Create HTML table
        html_content: str = create_html_table(daily_rates, mean_rates)
        
        # Step 5: Save HTML report
        save_html_report(html_content, output_html)
        
        print("=" * 60)
        print("✅ ETL Pipeline completed successfully!")
        print("=" * 60)
        
        return 0
        
    except Exception as e:
        print("=" * 60)
        print(f"❌ ETL Pipeline failed: {e}")
        print("=" * 60)
        return 1


if __name__ == '__main__':
    sys.exit(main())
