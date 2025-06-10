#!/usr/bin/env python
import json
import pandas as pd
import os
import re
from pathlib import Path
import time
import csv
import numpy as np

def clean_field(field_value):
    """
    Clean up field values that have redundant prefixes.
    Example: "Account: 0381400000075" -> "0381400000075"
    """
    if not isinstance(field_value, str):
        return field_value
    
    # Pattern to match "Field: value" format
    match = re.match(r'([^:]+):\s*(.*)', field_value)
    if match:
        return match.group(2).strip()
    return field_value

def process_checkpoint(checkpoint_path, output_dir, timestamp_prefix):
    """Process a single checkpoint file and convert to CSV"""
    print(f"Processing checkpoint: {checkpoint_path}")
    
    try:
        # Load checkpoint data
        with open(checkpoint_path, "r") as f:
            checkpoint_data = json.load(f)
        
        records = []
        
        # Handle Rowan checkpoint format - the actual record data is in processed_details
        if "processed_details" in checkpoint_data:
            processed_details = checkpoint_data["processed_details"]
            print(f"Found processed_details with type: {type(processed_details)}")
            
            if isinstance(processed_details, list):
                print(f"Processing {len(processed_details)} detail records")
                for detail in processed_details:
                    if isinstance(detail, dict) and "final_result" in detail:
                        # Extract the final_result which contains all the property data
                        final_result = detail["final_result"]
                        if isinstance(final_result, dict):
                            records.append(final_result)
                        elif isinstance(final_result, str):
                            try:
                                parsed_result = json.loads(final_result)
                                if isinstance(parsed_result, dict):
                                    records.append(parsed_result)
                            except:
                                print(f"Could not parse final_result as JSON")
                    elif isinstance(detail, dict):
                        # If there's no final_result, use the detail itself
                        records.append(detail)
            
            elif isinstance(processed_details, dict):
                # If processed_details is a dict, check if it contains the record data
                for key, value in processed_details.items():
                    if isinstance(value, dict) and "final_result" in value:
                        final_result = value["final_result"]
                        if isinstance(final_result, dict):
                            records.append(final_result)
                    elif isinstance(value, list):
                        for item in value:
                            if isinstance(item, dict) and "final_result" in item:
                                final_result = item["final_result"]
                                if isinstance(final_result, dict):
                                    records.append(final_result)
        
        # If we didn't find records in processed_details, look in other fields
        if not records:
            print("No records found in processed_details, checking other fields...")
            for key, value in checkpoint_data.items():
                if isinstance(value, list) and len(value) > 0:
                    print(f"Checking key '{key}' with {len(value)} items")
                    sample_item = value[0]
                    if isinstance(sample_item, dict) and "final_result" in sample_item:
                        print(f"Found records with final_result in key '{key}'")
                        for item in value:
                            if isinstance(item, dict) and "final_result" in item:
                                final_result = item["final_result"]
                                if isinstance(final_result, dict):
                                    records.append(final_result)
                        break
                elif isinstance(value, dict):
                    # Check if this dict contains arrays of records
                    for sub_key, sub_value in value.items():
                        if isinstance(sub_value, list) and len(sub_value) > 0:
                            sample_sub_item = sub_value[0]
                            if isinstance(sample_sub_item, dict) and "final_result" in sample_sub_item:
                                print(f"Found records with final_result in {key}.{sub_key}")
                                for item in sub_value:
                                    if isinstance(item, dict) and "final_result" in item:
                                        final_result = item["final_result"]
                                        if isinstance(final_result, dict):
                                            records.append(final_result)
                                break
            
        if not records:
            print("No records found in checkpoint")
            print("Available keys:", list(checkpoint_data.keys()))
            return None
        
        print(f"Found {len(records)} records to process")
        
        # Clean up the data
        cleaned_records = []
        for record in records:
            cleaned_record = {}
            for key, value in record.items():
                # Skip layout or layout_type column
                if key.lower() in ["layout", "layout_type"]:
                    continue
                cleaned_record[key] = clean_field(value)
            cleaned_records.append(cleaned_record)
        
        # Create DataFrame
        df = pd.DataFrame(cleaned_records)
        
        # Ensure account numbers are properly handled as strings
        if 'Account_Number' in df.columns:
            # Convert all account numbers to strings to preserve leading zeros
            df['Account_Number'] = df['Account_Number'].astype(str)
            
            # Add a preceding apostrophe to all account numbers to ensure Excel treats them as text
            df['Account_Number'] = "'" + df['Account_Number']
            print("Added apostrophe prefix to account numbers to prevent Excel conversion")
        
        if 'PropertyID' in df.columns:
            # Convert PropertyID to strings and handle apostrophe prefix if needed
            df['PropertyID'] = df['PropertyID'].astype(str)
            # Remove existing apostrophe prefix if present and add it back
            df['PropertyID'] = df['PropertyID'].str.replace("^'", "", regex=True)
            df['PropertyID'] = "'" + df['PropertyID']
            print("Fixed PropertyID formatting")
        
        # Ensure proper string formatting for all numeric-looking columns
        for col in df.columns:
            # Convert any numeric columns to strings to prevent scientific notation
            if df[col].dtype == np.float64 or df[col].dtype == np.int64:
                df[col] = df[col].astype(str)
        
        # Get checkpoint file name for output file
        checkpoint_name = Path(checkpoint_path).stem
        
        # Save as CSV with proper handling for account numbers
        csv_path = output_dir / f"{timestamp_prefix}_{checkpoint_name}.csv"
        
        # Convert DataFrame to CSV with all text fields quoted
        df.to_csv(
            csv_path,
            index=False,
            encoding="utf-8-sig",  # Use UTF-8 with BOM for Excel compatibility
            quoting=csv.QUOTE_ALL,  # Quote all fields to preserve formatting
            escapechar="\\",  # Use backslash as escape character
            na_rep="",  # Empty string for NA values
        )
        
        print(f"CSV file saved to: {csv_path}")
        
        # Additional info
        print(f"Total records saved: {len(df)}")
        print("Column summary:")
        for column in df.columns:
            print(f"  - {column}: {df[column].notna().sum()} non-empty values")
        
        return df
        
    except Exception as e:
        print(f"Error processing {checkpoint_path}: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def main():
    try:
        # Process the most recent Rowan checkpoint file
        checkpoint_files = [
            "checkpoints/rowan_checkpoint_9197_606G025_20250609_022527_completed.json"
        ]
        
        # Create output directory
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        # Generate timestamp for filename
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        
        # Process each checkpoint file
        dfs = []
        for checkpoint_file in checkpoint_files:
            df = process_checkpoint(Path(checkpoint_file), output_dir, timestamp)
            if df is not None:
                dfs.append(df)
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main() 