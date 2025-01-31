import sys
import csv
import re
from typing import Tuple, List, Any

def clean_column_name(column: str) -> str:
    """
    Clean column names to be SQL-compatible.
    
    Args:
        column: The original column name
    
    Returns:
        A SQL-safe column name
    """
    # Replace spaces and special characters with underscore
    clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', column.strip())
    # Ensure the column name doesn't start with a number
    if clean_name[0].isdigit():
        clean_name = 'col_' + clean_name
    return clean_name

def infer_sql_type(value: str) -> Tuple[str, str]:
    """
    Infer SQL type from string value.
    
    Args:
        value: String value to analyze
    
    Returns:
        Tuple of (formatted_value, sql_type)
    """
    # Strip whitespace
    value = value.strip()
    
    # Always treat as TEXT if:
    # 1. Empty string
    # 2. Starts with '+' (phone numbers)
    # 3. Starts with leading zeros
    # 4. Contains any non-digit characters except decimal point
    if (not value or 
        value.startswith('+') or 
        value.startswith('0') or 
        any(c not in '0123456789.-' for c in value)):
        return (value.replace("'", "''"), 'TEXT')

    # Try to convert to number
    try:
        float_val = float(value)
        # Check if the number is too large for INTEGER
        if float_val.is_integer() and -2**31 <= float_val <= 2**31-1:
            return (value, 'INTEGER')
        return (value, 'NUMERIC')
    except ValueError:
        # If it's not a number, treat as text
        return (value.replace("'", "''"), 'TEXT')

def get_data(file_path: str) -> Tuple[List[str], List[List[str]]]:
    """
    Read CSV file and return columns and rows.
    
    Args:
        file_path: Path to the CSV file
    
    Returns:
        Tuple of (columns, rows)
    
    Raises:
        FileNotFoundError: If the file doesn't exist
        csv.Error: If there's an issue parsing the CSV
    """
    try:
        with open(file_path, 'r', newline='', encoding='utf-8') as reader:
            csv_reader = csv.reader(reader)
            columns = next(csv_reader)  # Get header row
            if not columns:
                raise ValueError("CSV file appears to be empty")
            
            # Clean column names
            columns = [clean_column_name(col) for col in columns]
            
            # Read all rows
            rows = list(csv_reader)
            
            if not rows:
                raise ValueError("CSV file contains no data rows")
            
            return columns, rows
    except FileNotFoundError:
        raise FileNotFoundError(f"Could not find file: {file_path}")
    except csv.Error as e:
        raise csv.Error(f"Error parsing CSV file: {str(e)}")

def get_column_types(columns: List[str], rows: List[List[str]]) -> dict:
    """
    Determine consistent SQL types for each column by analyzing all values.
    
    Args:
        columns: List of column names
        rows: List of rows
    
    Returns:
        Dictionary mapping column names to their SQL types
    """
    column_types = {}
    
    # Initialize all columns as potential INTEGER type
    for col in columns:
        column_types[col] = 'INTEGER'
    
    # Analyze each value to determine final column types
    for row in rows:
        for col, val in zip(columns, row):
            current_type = column_types[col]
            
            # Skip analysis if column is already TEXT
            if current_type == 'TEXT':
                continue
                
            # Get the type for this value
            _, inferred_type = infer_sql_type(val)
            
            # Upgrade the column type if needed
            if inferred_type == 'TEXT':
                column_types[col] = 'TEXT'
            elif inferred_type == 'NUMERIC' and current_type == 'INTEGER':
                column_types[col] = 'NUMERIC'
    
    return column_types

def row_to_select(columns: List[str], row: List[str], column_types: dict) -> str:
    """
    Convert a row to a SELECT statement.
    
    Args:
        columns: List of column names
        row: List of values
        column_types: Dictionary of column types
    
    Returns:
        SQL SELECT statement for the row
    """
    select_parts = []
    
    for col, val in zip(columns, row):
        formatted_val = val.strip().replace("'", "''")
        sql_type = column_types[col]
        select_parts.append(f"CAST('{formatted_val}' AS {sql_type}) AS {col}")
    
    return "SELECT " + ", ".join(select_parts)

def csv_to_sql(file_path: str) -> str:
    """
    Convert CSV file to SQL query.
    
    Args:
        file_path: Path to the CSV file
    
    Returns:
        SQL query string
    
    Raises:
        Various exceptions for file and parsing errors
    """
    columns, rows = get_data(file_path)
    
    # Determine consistent column types
    column_types = get_column_types(columns, rows)
    
    sql_parts = ['WITH data AS (']
    
    for i, row in enumerate(rows):
        if len(row) != len(columns):
            raise ValueError(f"Row {i+1} has {len(row)} columns, expected {len(columns)}")
        
        sql_parts.append("\t" + row_to_select(columns, row, column_types))
        sql_parts.append(" UNION ALL" if i < len(rows) - 1 else "")
    
    sql_parts.append(") \nSELECT * FROM data;")
    
    return '\n'.join(sql_parts)

def main():
    """Main function to handle command line usage."""
    try:
        if len(sys.argv) != 2:
            print("Usage: python csv2sql.py <csv file>")
            sys.exit(1)
        
        result = csv_to_sql(sys.argv[1])
        print(result)
        
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
