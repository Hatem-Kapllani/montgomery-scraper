"""
Utility script to process tax data files
"""

import os
import sys
from pathlib import Path
import datetime
import subprocess

def run_data_processor():
    """Run the data processing script"""
    print("\n🚀 Starting data processing...\n")
    
    try:
        # Set up the logger from our custom logging module
        from src.lib.logger import get_logger
        logger = get_logger("data_processor")
        logger.info("Starting data processing")
    except ImportError:
        print("ℹ️ Custom logger not found, continuing with standard output")
    
    try:
        # Run the process_tax_data.py script
        input_file = input("Enter the input CSV file name (default: input.csv): ") or "input.csv"
        output_file = input("Enter the output CSV file name (default: output.csv): ") or "output.csv"
        
        subprocess.run([
            sys.executable, 
            "process_tax_data.py", 
            "--input", input_file,
            "--output", output_file
        ], check=True)
        
        print(f"\n✅ Data processing complete. Output saved to {output_file}")
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Error processing data: {e}")
        return False
    except KeyboardInterrupt:
        print("\n⚠️ Processing interrupted by user")
        return False
    
    return True

def run_combine_records():
    """Run the combine_records.py script to combine duplicate records"""
    print("\n🚀 Starting record combination process...\n")
    
    try:
        # Run the combine_records.py script
        input_file = input("Enter the input CSV file name (default: Davidson1.csv): ") or "Davidson1.csv"
        output_file = input("Enter the output CSV file name (default: combined_records.csv): ") or "combined_records.csv"
        
        subprocess.run([
            sys.executable, 
            "combine_records.py", 
            "--input", input_file,
            "--output", output_file
        ], check=True)
        
        print(f"\n✅ Record combination complete. Output saved to {output_file}")
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Error combining records: {e}")
        return False
    except KeyboardInterrupt:
        print("\n⚠️ Processing interrupted by user")
        return False
    
    return True

if __name__ == "__main__":
    print("=" * 60)
    print("🧹 DATA PROCESSING UTILITY")
    print("=" * 60)
    print("\nThis utility provides options for processing tax data files")
    
    while True:
        print("\nPlease select an option:")
        print("1. Process tax data (add risk metrics)")
        print("2. Combine duplicate records (by parcel ID and account number)")
        print("3. Exit")
        
        choice = input("\nEnter your choice (1-3): ")
        
        if choice == "1":
            run_data_processor()
        elif choice == "2":
            run_combine_records()
        elif choice == "3":
            print("\nExiting program...")
            break
        else:
            print("\n❌ Invalid choice. Please try again.")
    
    print("\n" + "=" * 60)
    print("✅ PROCESS COMPLETE")
    print("=" * 60) 