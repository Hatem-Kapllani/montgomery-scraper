import csv
import os
from collections import defaultdict
import argparse

def combine_records(input_file, output_file, fields_to_quote=None):
    """
    Process the input CSV file to combine records with the same parcel ID and account number.
    
    The function will:
    1. Group records by parcel ID and account number
    2. For each group, combine years into one field and sum the total due amounts
    3. For other fields, keep the values from the record with the latest year
    4. Optionally add single quotes to specified fields
    5. Clean PropertyAddress field by removing leading zeros and leading quotes from zip codes
    
    Args:
        input_file (str): Path to the input CSV file
        output_file (str): Path to the output CSV file to create
        fields_to_quote (list): Optional list of field names to add single quotes to
    """
    print(f"Processing {input_file} to {output_file}...")
    
    # If fields_to_quote is None, initialize as empty list
    if fields_to_quote is None:
        fields_to_quote = []
    
    # Dictionary to hold grouped records
    # Key: (parcel_id, account_number), Value: list of records
    grouped_records = defaultdict(list)
    
    # Count total records for reporting
    total_records = 0
    
    # Read the input CSV
    with open(input_file, 'r', newline='', encoding='utf-8-sig') as infile:
        reader = csv.DictReader(infile)
        headers = reader.fieldnames.copy()
        
        print(f"Found columns: {headers}")
        
        # Identify the required columns - handle potential BOM character
        parcel_id_col = "ParcelNumber" if "ParcelNumber" in headers else None
        account_number_col = "AccountNumber" if "AccountNumber" in headers else "ACCOUNT NUMBER" if "ACCOUNT NUMBER" in headers else None
        tax_year_col = "TaxYear" if "TaxYear" in headers else None
        total_due_col = " TotalDue " if " TotalDue " in headers else "TOTAL AMOUNT DUE" if "TOTAL AMOUNT DUE" in headers else None
        
        # Handle BOM character if present in column names
        for i, header in enumerate(headers):
            if header.startswith('\ufeff'):
                clean_header = header.replace('\ufeff', '')
                if clean_header == "TaxYear":
                    tax_year_col = header
        
        # Check if we need to combine records or just add quotes
        if parcel_id_col and account_number_col and tax_year_col and total_due_col:
            # Validate that the required columns exist
            for col in [parcel_id_col, account_number_col, tax_year_col, total_due_col]:
                if col not in headers:
                    raise ValueError(f"Required column '{col}' not found in the input file. Available columns: {headers}")
            
            # Group records by parcel ID and account number
            for row in reader:
                total_records += 1
                parcel_id = row.get(parcel_id_col, "").strip()
                account_number = row.get(account_number_col, "").strip()
                
                # Special debugging for 100m properties
                if "100m PROPERTIES" in row.get("AccountName1", ""):
                    account_name = row.get("AccountName1", "")
                    year = row.get(tax_year_col, "")
                    print(f"Found record for {account_name}, Year: {year}, ParcelID: {parcel_id}, Account#: {account_number}")
                
                # If account number is empty, use a unique key to avoid incorrect combining
                if not account_number:
                    # Use a unique identifier (bill number if available, otherwise row count)
                    bill_number = row.get("BillNumber", f"row_{total_records}").strip()
                    key = (parcel_id, f"_unique_{bill_number}")
                # If parcel ID is empty but account number exists, group by account number
                elif not parcel_id:
                    key = ("", account_number)
                # Normal case: both parcel ID and account number exist
                else:
                    key = (parcel_id, account_number)
                    
                # Add the row to the appropriate group
                grouped_records[key].append(row)
            
            # Process each group to combine records
            combined_records = []
            
            # Add a new column for combined years
            new_headers = headers.copy()
            if "CombinedYears" not in new_headers:
                new_headers.append("CombinedYears")
            
            for key, records in grouped_records.items():
                parcel_id, account_number = key
                
                # Debug combined records for 100m properties
                if any("100m PROPERTIES" in record.get("AccountName1", "") for record in records):
                    print(f"Processing group with {len(records)} records for ParcelID: {parcel_id}, Account#: {account_number}")
                    for idx, record in enumerate(records):
                        print(f"  Record {idx+1}: Year: {record.get(tax_year_col, 'N/A')}, Name: {record.get('AccountName1', 'N/A')}")
                
                # If there's only one record in this group, no need to combine
                if len(records) == 1:
                    record = records[0]
                    record["CombinedYears"] = record[tax_year_col]
                    combined_records.append(record)
                    continue
                
                # Sort records by year in descending order (latest first)
                try:
                    records.sort(key=lambda x: int(x[tax_year_col]) if x[tax_year_col].isdigit() else 0, reverse=True)
                except Exception as e:
                    print(f"Warning: Could not sort records by year: {e}. Using original order.")
                
                # Use the record with the latest year as a base
                combined_record = records[0].copy()
                
                # Combine years - extract all years from all records
                all_years = []
                for record in records:
                    year_val = record.get(tax_year_col, "").strip()
                    if year_val and year_val not in all_years:
                        all_years.append(year_val)
                
                # Sort years in descending order (newest first) if they are all digits
                try:
                    all_years.sort(key=lambda y: int(y) if y.isdigit() else 0, reverse=True)
                except Exception as e:
                    print(f"Warning: Could not sort years: {e}")
                
                # Join years with commas
                years_string = ", ".join(all_years)
                
                # Put the combined years in both the TaxYear field and the CombinedYears field
                combined_record[tax_year_col] = years_string
                combined_record["CombinedYears"] = years_string
                
                # For special debugging
                if "100m PROPERTIES" in combined_record.get("AccountName1", ""):
                    print(f"Final combined years for 100m PROPERTIES: {combined_record['CombinedYears']}")
                    print(f"Updated TaxYear field to: {combined_record[tax_year_col]}")
                    print(f"All years found: {all_years}")
                
                # Sum the total due amounts
                total_due_sum = 0
                for record in records:
                    try:
                        # Remove any non-numeric characters (like commas) and convert to float
                        total_due_str = record[total_due_col].strip().replace(',', '').replace('$', '')
                        if total_due_str:
                            total_due_sum += float(total_due_str)
                    except (ValueError, TypeError) as e:
                        print(f"Warning: Could not parse TotalDue value '{record[total_due_col]}': {e}")
                
                # Format the combined total due
                combined_record[total_due_col] = f"{total_due_sum:.2f}"
                
                combined_records.append(combined_record)
            
            records_to_process = combined_records
        else:
            # Simple pass-through when only adding quotes, no record combining
            records_to_process = list(reader)
            new_headers = headers
            total_records = len(records_to_process)
    
        # Add a single quote prefix to specified fields
        for record in records_to_process:
            # Process PropertyAddress field if it exists
            if "PropertyAddress" in record:
                property_address = record["PropertyAddress"]
                
                # Remove leading zeros from property address
                if property_address.startswith('0'):
                    # Count leading zeros and remove them
                    i = 0
                    while i < len(property_address) and property_address[i] == '0':
                        i += 1
                    property_address = property_address[i:]
                
                # Find the zip code at the end of the address and remove any leading quote
                parts = property_address.split()
                if parts and parts[-1].startswith("'"):
                    parts[-1] = parts[-1][1:]
                
                # Reassemble the address
                record["PropertyAddress"] = ' '.join(parts)
            
            # Add quotes to default fields if record combining was done
            if parcel_id_col and account_number_col:
                # Only add the single quote if the value is not empty and doesn't already have one
                if record.get(parcel_id_col) and not record[parcel_id_col].startswith("'"):
                    record[parcel_id_col] = "'" + record[parcel_id_col]
                if record.get(account_number_col) and not record[account_number_col].startswith("'"):
                    record[account_number_col] = "'" + record[account_number_col]
            
            # Add quotes to additional specified fields
            for field in fields_to_quote:
                if field in record and record[field] and not record[field].startswith("'"):
                    record[field] = "'" + record[field]
        
        # Write to the output CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=new_headers)
            writer.writeheader()
            writer.writerows(records_to_process)
        
        print(f"Read {total_records} total records from input file.")
        if parcel_id_col and account_number_col and tax_year_col and total_due_col:
            print(f"Successfully processed {len(records_to_process)} combined records out of {sum(len(records) for records in grouped_records.values())} grouped records.")
        else:
            print(f"Successfully processed {len(records_to_process)} records.")
        if fields_to_quote:
            print(f"Added quotes to fields: {fields_to_quote}")
        print(f"Output saved to {output_file}")

def combine_address_fields(input_file, output_file):
    """
    Process the input CSV file to combine address fields into a single field.
    
    The function will:
    1. Combine ADDRESS 1 with CITY, STATE, ZIP
    2. Combine ADDRESS 2 with CITY, STATE, ZIP when ADDRESS 2 is not empty
    3. Combine STREET NUMBER with STREET NAME
    
    Args:
        input_file (str): Path to the input CSV file
        output_file (str): Path to the output CSV file to create
    """
    print(f"Processing {input_file} to combine address fields...")
    
    # Read the input CSV
    rows = []
    with open(input_file, 'r', newline='', encoding='utf-8-sig') as infile:
        reader = csv.DictReader(infile)
        headers = reader.fieldnames.copy()
        
        print(f"Found columns: {headers}")
        
        # Define the possible column name mappings
        address_mappings = {
            "address1": ["AccountAddress1", "ADDRESS 1"],
            "address2": ["AccountAddress2", "ADDRESS 2"],
            "city": ["AccountCity", "CITY"],
            "state": ["AccountState", "STATE"],
            "zipcode": ["AccountZipCode", "ZIP"],
            "street_number": ["STREET NUMBER"],
            "street_name": ["STREET NAME"]
        }
        
        # Determine which columns exist in the file
        column_map = {}
        for field, possible_names in address_mappings.items():
            for name in possible_names:
                if name in headers:
                    column_map[field] = name
                    break
        
        # Check if we have the minimum required columns
        if "address1" not in column_map or "city" not in column_map or "state" not in column_map or "zipcode" not in column_map:
            missing_fields = []
            for field in ["address1", "city", "state", "zipcode"]:
                if field not in column_map:
                    missing_fields.append(field)
            print(f"Missing required address fields: {missing_fields}")
            print("Will not perform address combining but will continue processing.")
        
        # Add new combined columns to headers
        new_headers = headers.copy()
        if "CombinedAddress1" not in new_headers:
            new_headers.append("CombinedAddress1")
        if "CombinedAddress2" not in new_headers and "address2" in column_map:
            new_headers.append("CombinedAddress2")
        if "CombinedStreetAddress" not in new_headers and "street_number" in column_map and "street_name" in column_map:
            new_headers.append("CombinedStreetAddress")
        
        # Process each row
        for row in reader:
            # Process PropertyAddress field if it exists
            if "PropertyAddress" in row:
                property_address = row["PropertyAddress"]
                
                # Remove leading zeros from property address
                if property_address.startswith('0'):
                    # Count leading zeros and remove them
                    i = 0
                    while i < len(property_address) and property_address[i] == '0':
                        i += 1
                    property_address = property_address[i:]
                
                # Find the zip code at the end of the address and remove any leading quote
                parts = property_address.split()
                if parts and parts[-1].startswith("'"):
                    parts[-1] = parts[-1][1:]
                
                # Reassemble the address
                row["PropertyAddress"] = ' '.join(parts)
            
            # Only combine addresses if we have the required columns
            if all(field in column_map for field in ["address1", "city", "state", "zipcode"]):
                # Create combined address 1
                address1 = row[column_map["address1"]].strip()
                city = row[column_map["city"]].strip()
                state = row[column_map["state"]].strip()
                zipcode = row[column_map["zipcode"]].strip()
                
                combined_address1 = ""
                if address1:
                    combined_address1 = address1
                    if city:
                        combined_address1 += f", {city}"
                    if state:
                        combined_address1 += f", {state}"
                    if zipcode:
                        combined_address1 += f" {zipcode}"
                
                row["CombinedAddress1"] = combined_address1
                
                # Create combined address 2 if address2 column exists and isn't empty
                if "address2" in column_map:
                    address2 = row[column_map["address2"]].strip()
                    combined_address2 = ""
                    
                    if address2:
                        combined_address2 = address2
                        if city:
                            combined_address2 += f", {city}"
                        if state:
                            combined_address2 += f", {state}"
                        if zipcode:
                            combined_address2 += f" {zipcode}"
                    
                    row["CombinedAddress2"] = combined_address2
            else:
                # If required columns don't exist, add empty values
                row["CombinedAddress1"] = ""
                if "CombinedAddress2" in new_headers:
                    row["CombinedAddress2"] = ""
            
            # Combine street number and street name if both columns exist
            if "street_number" in column_map and "street_name" in column_map:
                street_number = row[column_map["street_number"]].strip()
                street_name = row[column_map["street_name"]].strip()
                
                combined_street = ""
                if street_number:
                    combined_street = street_number
                    if street_name:
                        combined_street += f" {street_name}"
                elif street_name:
                    combined_street = street_name
                
                row["CombinedStreetAddress"] = combined_street
            
            rows.append(row)
    
    # Write to the output CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=new_headers)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"Successfully processed {len(rows)} records.")
    print(f"Added combined address fields to output file: {output_file}")

def clean_property_address(input_file, output_file):
    """
    Process the input CSV file to clean PropertyAddress field only.
    The function will:
    1. Remove leading zeros from the PropertyAddress field
    2. Remove leading single quotes from the zip code in PropertyAddress
    
    Args:
        input_file (str): Path to the input CSV file
        output_file (str): Path to the output CSV file to create
    """
    print(f"Processing {input_file} to clean PropertyAddress field...")
    
    # Read the input CSV
    rows = []
    with open(input_file, 'r', newline='', encoding='utf-8-sig') as infile:
        reader = csv.DictReader(infile)
        headers = reader.fieldnames.copy()
        
        print(f"Found columns: {headers}")
        
        if "PropertyAddress" not in headers:
            print("Warning: PropertyAddress column not found in the input file.")
            print("Will continue processing but no changes to addresses will be made.")
        
        # Process each row
        total_modified = 0
        for row in reader:
            # Process PropertyAddress field if it exists
            if "PropertyAddress" in row and row["PropertyAddress"]:
                original_address = row["PropertyAddress"]
                property_address = original_address
                
                # Remove leading zeros from property address
                if property_address.startswith('0'):
                    # Count leading zeros and remove them
                    i = 0
                    while i < len(property_address) and property_address[i] == '0':
                        i += 1
                    property_address = property_address[i:]
                
                # Find the zip code at the end of the address and remove any leading quote
                parts = property_address.split()
                if parts and parts[-1].startswith("'"):
                    parts[-1] = parts[-1][1:]
                
                # Reassemble the address
                row["PropertyAddress"] = ' '.join(parts)
                
                # Count modified records
                if original_address != row["PropertyAddress"]:
                    total_modified += 1
                    if total_modified <= 5:  # Show a few examples of changes
                        print(f"Changed: '{original_address}' -> '{row['PropertyAddress']}'")
            
            rows.append(row)
    
    # Write to the output CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"Successfully processed {len(rows)} records.")
    print(f"Modified {total_modified} PropertyAddress entries.")
    print(f"Output saved to {output_file}")

if __name__ == "__main__":
    # Set up command line argument parser
    parser = argparse.ArgumentParser(description='Process tax records.')
    parser.add_argument('--input', '-i', dest='input_file', required=False, default="Davidson1.csv",
                        help='Input CSV file path (default: Davidson1.csv)')
    parser.add_argument('--output', '-o', dest='output_file', required=False, default="combined_records.csv",
                        help='Output CSV file path (default: combined_records.csv)')
    parser.add_argument('--combine-addresses', '-ca', dest='combine_addresses', action='store_true',
                        help='Combine address fields instead of combining records')
    parser.add_argument('--clean-only', '-co', dest='clean_only', action='store_true',
                        help='Only clean PropertyAddress field without combining records')
    parser.add_argument('--quote-fields', '-qf', dest='quote_fields', nargs='+', default=[],
                        help='Fields to add a preceding single quote to (space-separated)')
    
    args = parser.parse_args()
    
    try:
        if args.clean_only:
            clean_property_address(args.input_file, args.output_file)
        elif args.combine_addresses:
            combine_address_fields(args.input_file, args.output_file)
        else:
            combine_records(args.input_file, args.output_file, args.quote_fields)
    except Exception as e:
        print(f"Error processing file: {e}")
        # If you want to see the full traceback for debugging
        import traceback
        traceback.print_exc() 