import csv
import os
import argparse
from Tax_AppraisedValue_Score import DelinquencyDataPoint

def process_csv(input_file, output_file):
    """
    Process the input CSV file, apply DelinquencyDataPoint calculations,
    and create a new output CSV with added metrics.
    
    Args:
        input_file (str): Path to the input CSV file
        output_file (str): Path to the output CSV file to create
    """
    print(f"Processing {input_file} to {output_file}...")
    
    # Read the input CSV
    with open(input_file, 'r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        headers = reader.fieldnames.copy()
        
        print(f"Found columns: {headers}")
        
        # Check for the required columns
        balance_col = find_column(headers, ['TOTAL AMOUNT DUE', 'Balance', 'balance', 'Amount Owed', 'amount_owed', 'TotalDue', 'Amount_Due'])
        
        # Look for VALUE and EXEMPTIONS columns to calculate Taxable_value
        value_col = find_column(headers, ['VALUE', 'Value', 'value', 'APPRAISED VALUE', 'Appraised Value'])
        exemptions_col = find_column(headers, ['EXEMPTIONS', 'Exemptions', 'exemptions'])
        
        if not balance_col:
            raise ValueError("Could not find balance column in the input file")
        
        # Decide which columns to use for tax calculations
        calculate_taxable_value = False
        if value_col and exemptions_col:
            calculate_taxable_value = True
            tax_amount_col = 'Taxable_value'  # This will be a new column we'll add
            print(f"Will calculate Taxable_value as {value_col} - {exemptions_col}")
        else:
            tax_amount_col = find_column(headers, ['LAND VALUE', 'tax_amount', 'TaxAmount', 'Tax_Amount', 'tax amount', 'ValueTotal', 'taxable_value', 'Taxable_value', 'Taxable_Total'])
            if not tax_amount_col:
                raise ValueError("Could not find tax_amount column in the input file")
        
        print(f"Using '{balance_col}' as balance (amount owed) column")
        print(f"Using '{tax_amount_col}' as tax amount column")
        
        # Add new columns for the metrics
        new_headers = headers.copy()
        if calculate_taxable_value:
            new_headers.append('Taxable_value')
        
        new_headers.extend([
            'balance_to_tax_ratio', 
            'balance_to_tax_percentage', 
            'balance_risk_category'
        ])
        
        # Prepare to write to output file
        rows_to_write = []
        
        # Process each row
        row_count = 0
        error_count = 0
        for row in reader:
            # Create a copy of the row
            new_row = row.copy()
            row_count += 1
            
            # Calculate Taxable_value if required
            if calculate_taxable_value:
                try:
                    value = float(row[value_col].strip().replace('$', '').replace(',', ''))
                    exemptions = float(row[exemptions_col].strip().replace('$', '').replace(',', ''))
                    taxable_value = max(0, value - exemptions)  # Ensure it's not negative
                    new_row['Taxable_value'] = taxable_value
                except (ValueError, TypeError) as e:
                    print(f"Error calculating Taxable_value for row {row_count}: {e}")
                    new_row['Taxable_value'] = "Error"
                    error_count += 1
            
            # Process the balance to tax amount metrics
            success = process_metric_for_column(new_row, tax_amount_col, balance_col, 
                                      'balance_to_tax_ratio', 'balance_to_tax_percentage', 'balance_risk_category')
            if not success:
                error_count += 1
            
            rows_to_write.append(new_row)
            
            # Print progress periodically
            if row_count % 1000 == 0:
                print(f"Processed {row_count} rows...")
    
    # Write to the output CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=new_headers)
        writer.writeheader()
        writer.writerows(rows_to_write)
    
    print(f"Successfully processed {len(rows_to_write)} rows with {error_count} errors. Output saved to {output_file}")

def find_column(headers, possible_names):
    """
    Find a column in the headers by checking various possible names.
    
    Args:
        headers (list): List of column headers
        possible_names (list): List of possible column names to check
        
    Returns:
        str or None: The matching column name if found, None otherwise
    """
    for header in headers:
        header_stripped = header.strip()
        for name in possible_names:
            if name.lower() == header_stripped.lower():
                return header
    return None

def process_metric_for_column(row, value_col, tax_col, ratio_col, percentage_col, risk_col):
    """
    Process metrics for a given tax column and add the results to the row.
    
    Args:
        row (dict): The row data
        value_col (str): Column name for tax amount value
        tax_col (str): Column name for balance amount
        ratio_col (str): Column name to store the ratio
        percentage_col (str): Column name to store the percentage
        risk_col (str): Column name to store the risk category
        
    Returns:
        bool: True if processing was successful, False if there was an error
    """
    # Get values, handling potential missing data
    tax_amount = row.get(value_col, "")
    balance = row.get(tax_col, "")
    
    # Check for empty values
    if tax_amount == "" or balance == "":
        row[ratio_col] = "N/A"
        row[percentage_col] = "N/A"
        row[risk_col] = "N/A"
        return True
    
    try:
        # Create DelinquencyDataPoint - will automatically handle cleaning the values
        # appraised_value should be the property value (taxable_value)
        # tax_amount should be the amount owed (Balance)
        data_point = DelinquencyDataPoint(
            appraised_value=tax_amount,
            tax_amount=balance
        )
        
        # Add metrics to the row
        if isinstance(data_point.delinquency_ratio, str) and data_point.delinquency_ratio == "Unknown":
            row[ratio_col] = "Unknown"
            row[percentage_col] = "Unknown"
            row[risk_col] = "Unknown"
        else:
            # Format the ratio to have 2 decimal places
            if data_point.delinquency_ratio is not None:
                row[ratio_col] = round(data_point.delinquency_ratio, 2)
            else:
                row[ratio_col] = "N/A"
            
            if data_point.delinquency_percentage is not None:
                row[percentage_col] = f"{data_point.delinquency_percentage:.2f}%"
            else:
                row[percentage_col] = "N/A"
                
            row[risk_col] = data_point.get_risk_category()
        
        return True
        
    except ValueError as e:
        # Handle any errors from DelinquencyDataPoint
        print(f"Error processing row with {value_col}={tax_amount}, {tax_col}={balance}: {e}")
        row[ratio_col] = "Error"
        row[percentage_col] = "Error"
        row[risk_col] = "Error"
        return False

if __name__ == "__main__":
    # Set up command line argument parser
    parser = argparse.ArgumentParser(description='Process tax data CSV files and add risk metrics.')
    parser.add_argument('--input', '-i', dest='input_file', required=False, default="input.csv",
                        help='Input CSV file path (default: input.csv)')
    parser.add_argument('--output', '-o', dest='output_file', required=False, default="output_with_metrics.csv",
                        help='Output CSV file path (default: output_with_metrics.csv)')
    
    args = parser.parse_args()
    
    try:
        process_csv(args.input_file, args.output_file)
    except Exception as e:
        print(f"Error processing file: {e}")
        # If you want to see the full traceback for debugging
        import traceback
        traceback.print_exc() 