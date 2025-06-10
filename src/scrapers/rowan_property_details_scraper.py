#!/usr/bin/env python
import os
import csv
import json
import time
import logging
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from datetime import datetime
from pathlib import Path
import traceback
import local_proxy
import importlib
import subprocess
import sys
import re
import argparse

# Configure logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create handlers
c_handler = logging.StreamHandler()
f_handler = logging.FileHandler(log_dir / "rowan_data.log", mode='a')
e_handler = logging.FileHandler(log_dir / "rowan_data_error.log", mode='a')

# Set levels
c_handler.setLevel(logging.INFO)
f_handler.setLevel(logging.INFO)
e_handler.setLevel(logging.ERROR)

# Create formatters
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
c_handler.setFormatter(formatter)
f_handler.setFormatter(formatter)
e_handler.setFormatter(formatter)

# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)
logger.addHandler(e_handler)

class RowanPropertyDetailsScraper:
    """
    Scraper for Rowan County Tax website to get additional property details
    based on SiteFlowDetails.md
    """
    
    def __init__(self, checkpoint_dir="checkpoints", output_file=None):
        """
        Initialize the scraper with setup directories
        """
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(exist_ok=True)
        
        # Generate output filename if not provided
        if not output_file:
            output_file = f"output/rowan_additional_details_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        self.output_file = output_file
        Path(os.path.dirname(output_file)).mkdir(exist_ok=True)
        
        self.browser = None
        self.proxy_runner = None
        self.scraped_records = []
        
        # For tracking processed records
        self.processed_records = set()
        self.processed_details = []  # Store all processed record details
        self.checkpoint_counter = 0
        
        # Load the checkpoint if it exists
        self.load_checkpoint()

    def ensure_compatible_chromedriver(self):
        """
        Make sure we have a compatible ChromeDriver by running the update script
        """
        logger.info("Ensuring compatible ChromeDriver is available")
        try:
            # Method 1: Import and run the module directly
            try:
                import update_chromedriver
                update_chromedriver.main()
                logger.info("ChromeDriver updated successfully using direct import")
                return True
            except Exception as e:
                logger.warning(f"Failed to update ChromeDriver using direct import: {str(e)}")
                
            # Method 2: Run the script as subprocess
            logger.info("Trying to update ChromeDriver using subprocess")
            subprocess.run([sys.executable, "update_chromedriver.py"], check=True)
            logger.info("ChromeDriver updated successfully using subprocess")
            return True
            
        except Exception as e:
            logger.error(f"Error updating ChromeDriver: {str(e)}")
            return False

    def setup_browser(self):
        """
        Set up browser with proxy configuration
        """
        logger.info("Setting up browser with proxy")
        
        try:
            # Initialize proxy
            self.proxy_runner = local_proxy.LocalProxyRunner()
            self.proxy_runner.start()
            proxy_url = self.proxy_runner.get_proxy_url()
            
            # Set up Chrome options
            chrome_options = Options()
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--start-maximized")
            chrome_options.add_argument(f"--proxy-server={proxy_url}")
            
            # Use webdriver-manager for automatic ChromeDriver management
            try:
                from webdriver_manager.chrome import ChromeDriverManager
                from selenium.webdriver.chrome.service import Service as ChromeService
                
                service = ChromeService(ChromeDriverManager().install())
                self.browser = webdriver.Chrome(service=service, options=chrome_options)
                logger.info("Successfully initialized Chrome with webdriver-manager")
                
            except Exception as e:
                logger.error(f"Failed to initialize Chrome: {str(e)}")
                raise
                
            self.browser.implicitly_wait(10)
            logger.info("Browser setup completed")
            
        except Exception as e:
            logger.error(f"Browser setup failed: {str(e)}")
            if self.proxy_runner:
                self.proxy_runner.stop()
            raise

    def load_checkpoint(self):
        """
        Load the most recent checkpoint if it exists
        """
        # Look for incremental checkpoints first (updated pattern)
        checkpoint_files = list(self.checkpoint_dir.glob("rowan_checkpoint_*.json"))
        
        if checkpoint_files:
            logger.info(f"Found {len(checkpoint_files)} incremental checkpoint files")
            
            # Find the checkpoint with the highest checkpoint counter (not just most recent file)
            latest_checkpoint = None
            highest_counter = -1
            
            for checkpoint_file in checkpoint_files:
                try:
                    with open(checkpoint_file, 'r') as f:
                        checkpoint_data = json.load(f)
                    
                    counter = checkpoint_data.get('checkpoint_counter', 0)
                    if counter > highest_counter:
                        highest_counter = counter
                        latest_checkpoint = checkpoint_file
                        
                except Exception as e:
                    logger.warning(f"Error reading checkpoint file {checkpoint_file.name} for counter check: {str(e)}")
                    continue
            
            if latest_checkpoint:
                logger.info(f"Loading checkpoint with highest counter #{highest_counter}: {latest_checkpoint.name}")
                
                try:
                    with open(latest_checkpoint, 'r') as f:
                        checkpoint_data = json.load(f)
                    
                    self.processed_records = set(checkpoint_data.get('processed_records', []))
                    self.processed_details = checkpoint_data.get('processed_details', [])
                    self.checkpoint_counter = checkpoint_data.get('checkpoint_counter', 0)
                    
                    # Get status information from the latest checkpoint
                    record_status = checkpoint_data.get('record_status', 'unknown')
                    search_attempted = checkpoint_data.get('search_attempted', False)
                    
                    logger.info(f"Loaded checkpoint #{self.checkpoint_counter} with {len(self.processed_records)} processed records")
                    logger.info(f"Last processed record: {checkpoint_data.get('last_processed_record', {}).get('PropertyID', 'N/A')} (Status: {record_status})")
                    
                    # If the last checkpoint was "search_started", we need to handle this properly
                    if record_status == "search_started":
                        last_parcel_id = str(checkpoint_data.get('last_processed_record', {}).get('PropertyID', 'N/A')).strip("'")
                        logger.warning(f"Last checkpoint was 'search_started' for Parcel ID {last_parcel_id} - removing from processed records to retry")
                        # Remove this parcel ID from processed records so it gets reprocessed
                        if last_parcel_id != 'N/A' and last_parcel_id in self.processed_records:
                            self.processed_records.remove(last_parcel_id)
                            logger.info(f"Removed Parcel ID {last_parcel_id} from processed records - will be reprocessed")
                
                    # Count different types of checkpoints for better reporting
                    completed_count = 0
                    failed_count = 0
                    other_count = 0
                    
                    for checkpoint_file in checkpoint_files:
                        filename = checkpoint_file.name
                        if '_completed.json' in filename:
                            completed_count += 1
                        elif any(status in filename for status in ['_failed_', '_skipped']):
                            failed_count += 1
                        else:
                            other_count += 1
                    
                    logger.info(f"Checkpoint files summary: {completed_count} completed, {failed_count} failed/skipped, {other_count} other")
                    
                    return
                    
                except Exception as e:
                    logger.error(f"Error loading latest checkpoint: {str(e)}")
            else:
                logger.warning("No valid checkpoint files found with counter information")
        
        # Fallback to main checkpoint file (legacy support)
        checkpoint_file = self.checkpoint_dir / "rowan_details_checkpoint.json"
        if checkpoint_file.exists():
            try:
                with open(checkpoint_file, 'r') as f:
                    checkpoint_data = json.load(f)
                
                self.processed_records = set(checkpoint_data.get('processed_records', []))
                self.processed_details = checkpoint_data.get('processed_details', [])
                self.checkpoint_counter = checkpoint_data.get('checkpoint_counter', 0)
                
                logger.info(f"Loaded main checkpoint with {len(self.processed_records)} processed records")
                
            except Exception as e:
                logger.error(f"Error loading main checkpoint: {str(e)}")
        else:
            logger.info("No checkpoint files found. Starting from beginning.")

    def convert_for_json(self, obj):
        """
        Convert pandas NA values and other non-JSON-serializable objects to JSON-serializable format
        """
        # Check for pandas NA values properly
        try:
            if obj is pd.NA or (hasattr(pd, 'isna') and pd.isna(obj)):
                return None
        except (TypeError, ValueError):
            # If pd.isna fails, continue with other checks
            pass
        
        # Check for None explicitly
        if obj is None:
            return None
            
        if isinstance(obj, dict):
            # Remove _extraction_log from checkpoint data
            filtered_dict = {key: value for key, value in obj.items() if key != '_extraction_log'}
            return {key: self.convert_for_json(value) for key, value in filtered_dict.items()}
        elif isinstance(obj, list):
            return [self.convert_for_json(item) for item in obj]
        elif hasattr(obj, 'to_dict'):
            # Handle pandas Series
            return self.convert_for_json(obj.to_dict())
        elif hasattr(obj, '__dict__'):
            # Handle other objects with dict representation
            return self.convert_for_json(obj.__dict__)
        else:
            return obj

    def save_checkpoint(self, last_processed_record=None, extraction_details=None, record_status="completed", search_attempted=False):
        """
        Save current progress to checkpoint with detailed information
        Creates both main checkpoint and incremental checkpoint for each record
        
        Args:
            last_processed_record: The record that was processed
            extraction_details: Details of what was extracted
            record_status: Status of the record processing ("completed", "failed_search", "failed_extraction", "skipped")
            search_attempted: Whether a search was attempted for this record
        """
        timestamp = datetime.now()
        
        # Increment checkpoint counter for any record attempt (not just successful ones)
        if last_processed_record and search_attempted:
            self.checkpoint_counter += 1
        
        # Convert all data to JSON-serializable format
        json_safe_processed_details = self.convert_for_json(self.processed_details)
        json_safe_last_processed_record = self.convert_for_json(last_processed_record)
        json_safe_extraction_details = self.convert_for_json(extraction_details)
        
        # Prepare checkpoint data
        checkpoint_data = {
            'processed_records': list(self.processed_records),
            'processed_details': json_safe_processed_details,
            'timestamp': timestamp.isoformat(),
            'total_processed': len(self.processed_records),
            'last_processed_record': json_safe_last_processed_record,
            'extraction_details': json_safe_extraction_details,
            'checkpoint_counter': self.checkpoint_counter,
            'record_status': record_status,
            'search_attempted': search_attempted
        }
        
        try:
            # Save main checkpoint file (always updated)
            main_checkpoint_file = self.checkpoint_dir / "rowan_details_checkpoint.json"
            with open(main_checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
            
            # Save incremental checkpoint for each record attempt (successful or failed)
            if last_processed_record and search_attempted:
                parcel_id = str(last_processed_record.get('PropertyID', 'unknown')).strip("'")
                incremental_checkpoint_file = self.checkpoint_dir / f"rowan_checkpoint_{self.checkpoint_counter:04d}_{parcel_id}_{timestamp.strftime('%Y%m%d_%H%M%S')}_{record_status}.json"
                with open(incremental_checkpoint_file, 'w') as f:
                    json.dump(checkpoint_data, f, indent=2)
                
                logger.info(f"Incremental checkpoint #{self.checkpoint_counter} saved for Parcel ID {parcel_id} with status '{record_status}': {incremental_checkpoint_file.name}")
            
            logger.info(f"Main checkpoint saved successfully - {len(self.processed_records)} records processed")
            
            # Also save a detailed log entry
            if last_processed_record and extraction_details:
                logger.info(f"Last processed: PropertyID {last_processed_record.get('PropertyID')} with status: {record_status}")
                
        except Exception as e:
            logger.error(f"Error saving checkpoint: {str(e)}")
            logger.error(traceback.format_exc())
    
    def cleanup_old_checkpoints(self, keep_last_n=100):
        """
        Clean up old incremental checkpoint files, keeping only the last N
        Increased default from 50 to 100 since we're now creating more checkpoint files
        """
        try:
            checkpoint_files = list(self.checkpoint_dir.glob("rowan_checkpoint_*.json"))
            
            if len(checkpoint_files) > keep_last_n:
                # Sort by modification time
                checkpoint_files.sort(key=lambda x: x.stat().st_mtime)
                
                # Remove oldest files
                files_to_remove = checkpoint_files[:-keep_last_n]
                for file_to_remove in files_to_remove:
                    file_to_remove.unlink()
                    logger.info(f"Cleaned up old checkpoint: {file_to_remove.name}")
                
                logger.info(f"Cleaned up {len(files_to_remove)} old checkpoint files")
                
        except Exception as e:
            logger.error(f"Error cleaning up checkpoints: {str(e)}")

    def navigate_to_search_page(self):
        """
        Navigate to the tax search page
        """
        try:
            logger.info("Navigating to Rowan County tax search page.")
            self.browser.get("https://tax.rowancountync.gov/search/CommonSearch.aspx?mode=REALPROP")
            WebDriverWait(self.browser, 20).until(
                EC.presence_of_element_located((By.ID, "inpParid")) # Wait for Parcel ID input
            )
            logger.info("Successfully navigated to Rowan County search page")
            return True
        except Exception as e:
            logger.error(f"Error navigating to Rowan County search page: {str(e)}")
            return False

    def search_by_parcel_id(self, parcel_id):
        """
        Search for a property by Parcel ID on Rowan County site
        Returns:
            True: Search successful and results found
            False: Search failed due to technical issues (should retry)
            "no_results": Search successful but no results found (don't retry)
        """
        try:
            logger.info(f"Searching for Parcel ID: {parcel_id}")
            # Clear and enter parcel ID
            parcel_id_input = self.browser.find_element(By.ID, "inpParid")
            parcel_id_input.clear()
            parcel_id_input.send_keys(parcel_id)
            
            # Click search button
            search_button = self.browser.find_element(By.ID, "btSearch")
            search_button.click()
            
            # Wait for search results or no results message
            try:
                # Wait for either search results table or a "no results" indicator
                WebDriverWait(self.browser, 30).until(
                    lambda driver: (
                        driver.find_elements(By.ID, "searchResults") or
                        driver.find_elements(By.XPATH, "//*[contains(text(), 'No records found')]") or
                        driver.find_elements(By.XPATH, "//*[contains(text(), 'no results')]") or
                        driver.find_elements(By.XPATH, "//*[contains(text(), 'No results')]") or
                        driver.find_elements(By.XPATH, "//*[contains(text(), 'not found')]") or
                        driver.find_elements(By.CLASS_NAME, "no-results") or
                        # Check if we're still on the same search page but with no results
                        (driver.find_element(By.ID, "inpParid") and 
                         len(driver.find_elements(By.CSS_SELECTOR, "#searchResults tbody tr")) == 0)
                    )
                )
                
                # Check if we have actual search results
                search_results_table = self.browser.find_elements(By.ID, "searchResults")
                if search_results_table:
                    # Check if the table has actual data rows (not just headers)
                    data_rows = self.browser.find_elements(By.CSS_SELECTOR, "#searchResults tbody tr")
                    if data_rows and len(data_rows) > 0:
                        # Check if the first row contains actual data (not a "no results" message)
                        first_row_text = data_rows[0].text.strip().lower()
                        if ("no records" in first_row_text or 
                            "no results" in first_row_text or 
                            "not found" in first_row_text or
                            first_row_text == ""):
                            logger.info(f"Search completed but no records found for Parcel ID: {parcel_id}")
                            return "no_results"
                        else:
                            logger.info(f"Successfully searched for Parcel ID: {parcel_id} - Results found")
                            return True
                    else:
                        logger.info(f"Search completed but no data rows found for Parcel ID: {parcel_id}")
                        return "no_results"
                
                # Check for explicit "no results" messages
                no_results_indicators = [
                    "//*[contains(text(), 'No records found')]",
                    "//*[contains(text(), 'no results')]", 
                    "//*[contains(text(), 'No results')]",
                    "//*[contains(text(), 'not found')]"
                ]
                
                for indicator in no_results_indicators:
                    if self.browser.find_elements(By.XPATH, indicator):
                        logger.info(f"Search completed but explicitly shows 'no results' for Parcel ID: {parcel_id}")
                        return "no_results"
                
                # If we reach here, assume no results found
                logger.info(f"Search completed but no results detected for Parcel ID: {parcel_id}")
                return "no_results"
                
            except TimeoutException:
                logger.warning(f"Timeout waiting for search results for Parcel ID: {parcel_id}")
                return False
            
        except TimeoutException:
            logger.warning(f"Timeout during search operation for Parcel ID: {parcel_id}")
            return False
        except NoSuchElementException as e:
            logger.error(f"Could not find search elements for Parcel ID {parcel_id}: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error searching Parcel ID {parcel_id}: {str(e)}")
            return False

    def search_by_owner_name(self, owner_name):
        """
        Search for a property by Owner Name on Rowan County site (fallback search)
        Returns:
            True: Search successful and results found
            False: Search failed due to technical issues (should retry)
            "no_results": Search successful but no results found (don't retry)
        """
        try:
            logger.info(f"Searching by Owner Name: {owner_name}")
            
            # Navigate to the search page first
            self.browser.get("https://tax.rowancountync.gov/search/CommonSearch.aspx?mode=REALPROP")
            time.sleep(5)
            
            # Click on the Owner Name field
            logger.info("Clicking on Owner Name field")
            owner_name_field = WebDriverWait(self.browser, 30).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#inpOwner1"))
            )
            owner_name_field.click()
            
            # Clear and enter owner name
            logger.info(f"Entering owner name: {owner_name}")
            owner_name_field.clear()
            owner_name_field.send_keys(str(owner_name))
            
            # Set results per page to 100
            logger.info("Setting results per page to 100")
            results_dropdown = WebDriverWait(self.browser, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#selPageSize"))
            )
            results_dropdown.click()
            
            option_100 = WebDriverWait(self.browser, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#selPageSize > option:nth-child(4)"))
            )
            option_100.click()
            
            # Click search button
            logger.info("Clicking search button")
            search_button = WebDriverWait(self.browser, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#btSearch"))
            )
            search_button.click()
            
            # Wait for results to load
            logger.info("Waiting for search results to load...")
            time.sleep(20)
            
            # Check if we have results
            try:
                # Wait for either search results table or a "no results" indicator
                WebDriverWait(self.browser, 30).until(
                    lambda driver: (
                        driver.find_elements(By.ID, "searchResults") or
                        driver.find_elements(By.XPATH, "//*[contains(text(), 'No records found')]") or
                        driver.find_elements(By.XPATH, "//*[contains(text(), 'no results')]") or
                        driver.find_elements(By.XPATH, "//*[contains(text(), 'No results')]") or
                        driver.find_elements(By.XPATH, "//*[contains(text(), 'not found')]") or
                        driver.find_elements(By.CLASS_NAME, "no-results") or
                        # Check if we're still on the same search page but with no results
                        (driver.find_element(By.CSS_SELECTOR, "#inpOwner1") and 
                         len(driver.find_elements(By.CSS_SELECTOR, "#searchResults tbody tr")) == 0)
                    )
                )
                
                # Check if we have actual search results
                search_results_table = self.browser.find_elements(By.ID, "searchResults")
                if search_results_table:
                    # Check if the table has actual data rows (not just headers)
                    data_rows = self.browser.find_elements(By.CSS_SELECTOR, "#searchResults tbody tr")
                    if data_rows and len(data_rows) > 0:
                        # Check if the first row contains actual data (not a "no results" message)
                        first_row_text = data_rows[0].text.strip().lower()
                        if ("no records" in first_row_text or 
                            "no results" in first_row_text or 
                            "not found" in first_row_text or
                            first_row_text == ""):
                            logger.info(f"Owner name search completed but no records found for: {owner_name}")
                            return "no_results"
                        else:
                            logger.info(f"Successfully searched by Owner Name: {owner_name} - Results found")
                            return True
                    else:
                        logger.info(f"Owner name search completed but no data rows found for: {owner_name}")
                        return "no_results"
                
                # Check for explicit "no results" messages
                no_results_indicators = [
                    "//*[contains(text(), 'No records found')]",
                    "//*[contains(text(), 'no results')]", 
                    "//*[contains(text(), 'No results')]",
                    "//*[contains(text(), 'not found')]"
                ]
                
                for indicator in no_results_indicators:
                    if self.browser.find_elements(By.XPATH, indicator):
                        logger.info(f"Owner name search completed but explicitly shows 'no results' for: {owner_name}")
                        return "no_results"
                
                # If we reach here, assume no results found
                logger.info(f"Owner name search completed but no results detected for: {owner_name}")
                return "no_results"
                
            except TimeoutException:
                logger.warning(f"Timeout waiting for owner name search results for: {owner_name}")
                return False
            
            return True
            
        except TimeoutException:
            logger.warning(f"Timeout during owner name search operation for: {owner_name}")
            return False
        except NoSuchElementException as e:
            logger.error(f"Could not find owner name search elements for {owner_name}: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error searching by Owner Name {owner_name}: {str(e)}")
            return False

    def find_and_select_parcel_in_owner_search_results(self, target_parcel_id):
        """
        When owner name search returns results, click on the first owner name link
        Returns:
            True: Successfully clicked on the first result
            False: No results found or error clicking
        """
        try:
            logger.info(f"Owner name search returned results - clicking on first result for Parcel ID {target_parcel_id}")
            
            # Wait for the search results table to be present
            table = WebDriverWait(self.browser, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#searchResults > tbody"))
            )
            
            # Get all table rows (excluding header)
            rows = table.find_elements(By.CSS_SELECTOR, "tr")
            if len(rows) <= 1:
                logger.info("No data rows found in owner name search results")
                return False
                
            data_rows = rows[1:]  # Skip header
            logger.info(f"Found {len(data_rows)} rows in owner name search results")
            
            # Just click on the first result's owner name link
            try:
                first_row = data_rows[0]
                
                # Extract some info for logging
                try:
                    parcel_id_element = first_row.find_element(By.CSS_SELECTOR, f"td:nth-child(2) > div")
                    parcel_id_raw = parcel_id_element.text.strip()
                    
                    owner_name_element = first_row.find_element(By.CSS_SELECTOR, f"td:nth-child(3) > div")
                    owner_name = owner_name_element.text.strip()
                    
                    logger.info(f"First result: Parcel ID '{parcel_id_raw}', Owner Name '{owner_name}'")
                except:
                    logger.info("Could not extract details from first result for logging")
                
                # Click on the owner name in the first row
                owner_name_link = first_row.find_element(By.CSS_SELECTOR, f"td:nth-child(3) > div")
                logger.info(f"Clicking on owner name link in first result")
                owner_name_link.click()
                
                # Wait for tax bill details page to load
                logger.info(f"Waiting for tax bill details page to load...")
                WebDriverWait(self.browser, 30).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#datalet_div_0 > table:nth-child(3)"))
                )
                
                logger.info(f"Successfully clicked on first result and navigated to tax bill details")
                return True
                
            except NoSuchElementException as click_e:
                logger.error(f"Could not find owner name link to click in first result: {str(click_e)}")
                return False
            except TimeoutException as timeout_e:
                logger.error(f"Timeout waiting for tax bill details page after clicking first result: {str(timeout_e)}")
                return False
            except Exception as click_e:
                logger.error(f"Error clicking on owner name link in first result: {str(click_e)}")
                return False
            
        except (TimeoutException, NoSuchElementException) as e:
            logger.error(f"Error accessing owner search results: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"General error processing owner search results: {str(e)}")
            return False

    def recover_from_timeout(self, parcel_id, recovery_stage="search"):
        """
        Recover from timeout by refreshing and navigating back to the appropriate stage
        
        Args:
            parcel_id: The parcel ID being processed
            recovery_stage: The stage to recover to ("search", "tax_bill", "summary", "value_history")
        """
        try:
            logger.info(f"Attempting timeout recovery for Parcel ID {parcel_id} at stage: {recovery_stage}")
            
            # Refresh the browser
            self.browser.refresh()
            time.sleep(2)
            
            # Navigate back to search page
            if not self.navigate_to_search_page():
                logger.error(f"Failed to navigate to search page during timeout recovery for {parcel_id}")
                return False
            
            # Re-search for the parcel ID
            if not self.search_by_parcel_id(parcel_id):
                logger.error(f"Failed to re-search for Parcel ID {parcel_id} during timeout recovery")
                return False
            
            # Navigate to the appropriate stage based on recovery_stage
            if recovery_stage in ["tax_bill"]:
                # Already at search results, need to click owner name to get to tax bill
                try:
                    owner_name_link = WebDriverWait(self.browser, 20).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "#searchResults > tbody > tr > td:nth-child(3) > div"))
                    )
                    owner_name_link.click()
                    WebDriverWait(self.browser, 30).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "#datalet_div_0 > table:nth-child(3)"))
                    )
                    logger.info(f"Successfully recovered to tax bill page for {parcel_id}")
                    return True
                except Exception as e:
                    logger.error(f"Failed to recover to tax bill page for {parcel_id}: {str(e)}")
                    return False
                    
            elif recovery_stage == "summary":
                # Navigate through tax bill to summary
                try:
                    # Click owner name first
                    owner_name_link = WebDriverWait(self.browser, 20).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "#searchResults > tbody > tr > td:nth-child(3) > div"))
                    )
                    owner_name_link.click()
                    WebDriverWait(self.browser, 30).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "#datalet_div_0 > table:nth-child(3)"))
                    )
                    
                    # Click summary button
                    summary_button = WebDriverWait(self.browser, 20).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "#sidemenu > ul > li:nth-child(2) > a"))
                    )
                    summary_button.click()
                    WebDriverWait(self.browser, 30).until(
                        EC.presence_of_element_located((By.ID, "Owner Mailing"))
                    )
                    logger.info(f"Successfully recovered to summary page for {parcel_id}")
                    return True
                except Exception as e:
                    logger.error(f"Failed to recover to summary page for {parcel_id}: {str(e)}")
                    return False
                    
            elif recovery_stage == "value_history":
                # Navigate through tax bill to value history
                try:
                    # Click owner name first
                    owner_name_link = WebDriverWait(self.browser, 20).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "#searchResults > tbody > tr > td:nth-child(3) > div"))
                    )
                    owner_name_link.click()
                    WebDriverWait(self.browser, 30).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "#datalet_div_0 > table:nth-child(3)"))
                    )
                    
                    # Click value history button
                    value_history_button = WebDriverWait(self.browser, 20).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "#sidemenu > ul > li:nth-child(8) > a"))
                    )
                    value_history_button.click()
                    WebDriverWait(self.browser, 30).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "#datalet_div_0 > table:nth-child(3)"))
                    )
                    logger.info(f"Successfully recovered to value history page for {parcel_id}")
                    return True
                except Exception as e:
                    logger.error(f"Failed to recover to value history page for {parcel_id}: {str(e)}")
                    return False
            
            # For "search" stage, we're already at the right place
            logger.info(f"Successfully recovered to search stage for {parcel_id}")
            return True
            
        except Exception as e:
            logger.error(f"General error during timeout recovery for {parcel_id}: {str(e)}")
            return False

    def extract_rowan_details(self, parcel_id):
        """
        Extract all required property details from the Rowan County site based on SiteFlowDetails.md
        This method will orchestrate the data extraction according to the defined flow.
        """
        details = {
            'Parcel_ID_Processed': parcel_id,
            'Bill_Paid_Status': 'Unknown',
            'Amount_Due': pd.NA,
            'Tax_Bill_Page_URL': pd.NA,
            'Delinquent_Years': pd.NA,
            'Owner_Name2': pd.NA,
            'Mailing_Address_Combined': pd.NA,
            'Appr_Land': pd.NA,
            'Deferred_Land': pd.NA,
            'Appr_Bldg': pd.NA,
            'Assessed_Total': pd.NA,
            'Exempt_Amount': pd.NA,
            'SR_Exclusion': pd.NA,
            'Vet_Exclusion': pd.NA,
            'Taxable_Total': pd.NA,
            '_extraction_log': []
        }
        extraction_log = details['_extraction_log']
        max_retries = 2  # Maximum number of retry attempts for timeouts

        try:
            logger.info(f"Starting extraction for Parcel ID: {parcel_id}")
            extraction_log.append(f"Starting extraction for Parcel ID: {parcel_id}")

            # Check if we're already on the tax bill details page (e.g., from owner name search)
            already_on_tax_bill_page = False
            try:
                # Try to find the tax bill table - if it exists, we're already on the right page
                self.browser.find_element(By.CSS_SELECTOR, "#datalet_div_0 > table:nth-child(3)")
                already_on_tax_bill_page = True
                logger.info(f"Already on tax bill details page for Parcel ID: {parcel_id} - skipping navigation step")
                extraction_log.append("Already on tax bill details page - skipping navigation step.")
            except NoSuchElementException:
                logger.info(f"Not on tax bill details page for Parcel ID: {parcel_id} - need to navigate from search results")
                extraction_log.append("Not on tax bill details page - need to navigate from search results.")
            
            # Step 4: Click on the Owner Name field in search results (with retry logic) - only if not already on tax bill page
            if not already_on_tax_bill_page:
                for attempt in range(max_retries + 1):
                    try:
                        owner_name_link = WebDriverWait(self.browser, 20).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, "#searchResults > tbody > tr > td:nth-child(3) > div"))
                        )
                        owner_name_link.click()
                        logger.info(f"Clicked on Owner Name link for Parcel ID: {parcel_id}")
                        extraction_log.append("Clicked Owner Name link.")
                        # Wait for new page (tax bill details) to load
                        WebDriverWait(self.browser, 30).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "#datalet_div_0 > table:nth-child(3)"))
                        )
                        logger.info(f"Navigated to tax bill details page for Parcel ID: {parcel_id}")
                        extraction_log.append("Navigated to tax bill details page.")
                        break  # Success, exit retry loop
                    except TimeoutException:
                        if attempt < max_retries:
                            logger.warning(f"Timeout clicking owner name for {parcel_id}, attempt {attempt + 1}/{max_retries + 1}. Attempting recovery...")
                            extraction_log.append(f"Timeout clicking owner name, attempt {attempt + 1}. Attempting recovery...")
                            if not self.recover_from_timeout(parcel_id, "search"):
                                logger.error(f"Failed to recover from timeout for {parcel_id}")
                                extraction_log.append("Failed to recover from timeout.")
                                details['_extraction_error'] = "Failed to recover from timeout clicking owner name."
                                return details
                        else:
                            logger.error(f"Final timeout waiting for owner name link or tax bill details page for Parcel ID: {parcel_id}")
                            extraction_log.append("Final timeout clicking owner name or loading tax bill details page.")
                            details['_extraction_error'] = "Final timeout on owner name click/tax bill details page load."
                            return details
                    except NoSuchElementException:
                        logger.error(f"Owner Name link not found on search results for Parcel ID: {parcel_id}.")
                        extraction_log.append("Owner Name link not found on search results.")
                        details['_extraction_error'] = "Owner Name link not found on search results."
                        return details
                    except Exception as e:
                        logger.error(f"Error clicking Owner Name link for Parcel ID {parcel_id}: {str(e)}")
                        extraction_log.append(f"Error clicking Owner Name: {str(e)}")
                        details['_extraction_error'] = f"Error clicking Owner Name: {str(e)}"
                        return details

            # Step 5 & 6: Check if bill is paid and save amount/URL if not
            bill_is_paid = False
            try:
                # First, try to find the "Total" row specifically, since that's where the amount due is located
                amount_due_text = None
                working_selector = None
                amount_element = None
                
                logger.info(f"Looking for 'Total' row to extract amount due for Parcel ID: {parcel_id}")
                extraction_log.append("Looking for 'Total' row to extract amount due")
                
                try:
                    # Find the table first
                    table = WebDriverWait(self.browser, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "#datalet_div_0 > table:nth-child(3)"))
                    )
                    
                    # Scroll table into view
                    self.browser.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", table)
                    time.sleep(1)
                    
                    # Get all rows in the table
                    rows = table.find_elements(By.CSS_SELECTOR, "tbody > tr")
                    logger.info(f"Table has {len(rows)} rows for Parcel ID: {parcel_id}")
                    extraction_log.append(f"Table has {len(rows)} rows")
                    
                    # Look for the "Total" row
                    total_row_found = False
                    for i, row in enumerate(rows):
                        try:
                            # Get all cells in this row
                            cells = row.find_elements(By.TAG_NAME, "td")
                            if not cells:
                                continue
                                
                            # Check if the first cell contains "Total"
                            first_cell_text = cells[0].text.strip().upper()
                            
                            if "TOTAL" in first_cell_text:
                                logger.info(f"Found 'Total' row at index {i+1} for Parcel ID: {parcel_id}")
                                extraction_log.append(f"Found 'Total' row at index {i+1}")
                                
                                # Try to get the amount from different possible columns in this row
                                possible_amount_columns = [6, 7, len(cells)-1]  # Column 7 (index 6), column 8 (index 7), or last column
                                
                                for col_idx in possible_amount_columns:
                                    try:
                                        if col_idx < len(cells):
                                            amount_cell = cells[col_idx]
                                            
                                            # Scroll cell into view
                                            self.browser.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", amount_cell)
                                            time.sleep(0.5)
                                            
                                            # Try multiple methods to get cell text
                                            cell_text = amount_cell.text.strip()
                                            if not cell_text:
                                                cell_text = amount_cell.get_attribute('innerHTML').strip()
                                                if cell_text:
                                                    import re
                                                    cell_text = re.sub(r'<[^>]+>', '', cell_text).strip()
                                            if not cell_text:
                                                cell_text = self.browser.execute_script("return arguments[0].textContent;", amount_cell).strip()
                                            
                                            logger.info(f"Total row column {col_idx+1} text: '{cell_text}' for Parcel ID: {parcel_id}")
                                            extraction_log.append(f"Total row column {col_idx+1} text: '{cell_text}'")
                                            
                                            # Check if this looks like a monetary amount
                                            if cell_text and (
                                                '$' in cell_text or 
                                                (cell_text.replace(',', '').replace('.', '').replace('-', '').isdigit() and 
                                                 len(cell_text.replace(',', '').replace('.', '').replace('-', '')) > 0)
                                            ):
                                                amount_due_text = cell_text
                                                working_selector = f"Total row column {col_idx+1}"
                                                amount_element = amount_cell
                                                total_row_found = True
                                                logger.info(f"Found amount in Total row column {col_idx+1}: '{cell_text}' for Parcel ID: {parcel_id}")
                                                extraction_log.append(f"Found amount in Total row column {col_idx+1}: '{cell_text}'")
                                                break
                                    except Exception as col_e:
                                        logger.debug(f"Error checking column {col_idx+1} in Total row for {parcel_id}: {str(col_e)}")
                                        continue
                                
                                if total_row_found:
                                    break  # Found the amount, stop looking
                                    
                        except Exception as row_e:
                            logger.debug(f"Error checking row {i+1} for Total for {parcel_id}: {str(row_e)}")
                            continue
                    
                    if not total_row_found:
                        logger.warning(f"Could not find 'Total' row with amount for Parcel ID: {parcel_id}")
                        extraction_log.append("Could not find 'Total' row with amount")
                        
                except Exception as table_e:
                    logger.error(f"Error finding Total row in table for Parcel ID {parcel_id}: {str(table_e)}")
                    extraction_log.append(f"Error finding Total row: {str(table_e)}")
                
                # If we didn't find the Total row, fall back to the old method (try last row selectors)
                if not amount_due_text:
                    logger.info(f"Falling back to last-row selectors for Parcel ID: {parcel_id}")
                    extraction_log.append("Falling back to last-row selectors")
                    
                    # Try multiple possible selectors for the amount due (last row, different columns)
                    possible_selectors = [
                        "#datalet_div_0 > table:nth-child(3) > tbody > tr:last-child > td:nth-child(7)",
                        "#datalet_div_0 > table:nth-child(3) > tbody > tr:last-child > td:last-child", 
                        "#datalet_div_0 > table:nth-child(3) tbody tr:last-child td:nth-child(7)",
                        "#datalet_div_0 table:nth-child(3) tbody tr:last-child td:nth-child(7)"
                    ]
                    
                    for selector in possible_selectors:
                        try:
                            amount_element = WebDriverWait(self.browser, 5).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                            )
                            
                            # Scroll element into view to ensure it's visible
                            self.browser.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", amount_element)
                            time.sleep(1)  # Give time for scroll to complete
                            
                            # Try multiple methods to get the text
                            amount_due_text = amount_element.text.strip()
                            if not amount_due_text:
                                # Try getting innerHTML if text is empty
                                amount_due_text = amount_element.get_attribute('innerHTML').strip()
                                if amount_due_text:
                                    # Remove HTML tags if present
                                    import re
                                    amount_due_text = re.sub(r'<[^>]+>', '', amount_due_text).strip()
                            
                            if not amount_due_text:
                                # Try getting textContent
                                amount_due_text = self.browser.execute_script("return arguments[0].textContent;", amount_element).strip()
                            
                            working_selector = selector
                            logger.info(f"Found amount element with fallback selector: {selector} for Parcel ID: {parcel_id}")
                            logger.info(f"Amount element text: '{amount_due_text}' for Parcel ID: {parcel_id}")
                            extraction_log.append(f"Found amount element with fallback selector: {selector}")
                            extraction_log.append(f"Amount element text: '{amount_due_text}'")
                            
                            # If we found the element but text is still empty, try next selector
                            if not amount_due_text:
                                logger.debug(f"Fallback selector {selector} returned empty text for {parcel_id}")
                                continue
                            else:
                                break  # We found text, use this selector
                                
                        except (TimeoutException, NoSuchElementException):
                            logger.debug(f"Fallback selector failed: {selector} for Parcel ID: {parcel_id}")
                            continue

                if amount_due_text is not None and amount_due_text != "":
                    extraction_log.append(f"Final Amount Due Text: '{amount_due_text}' (using {working_selector})")
                    
                    # Sanitize amount_due_text: remove $, commas, and handle potential non-numeric values before converting to float
                    cleaned_amount_text = amount_due_text.replace('$', '').replace(',', '').replace(' ', '').strip()
                    current_amount_due = 0.0
                    
                    logger.info(f"Cleaned amount text: '{cleaned_amount_text}' for Parcel ID: {parcel_id}")
                    extraction_log.append(f"Cleaned amount text: '{cleaned_amount_text}'")
                    
                    if cleaned_amount_text:
                        try:
                            current_amount_due = float(cleaned_amount_text)
                            logger.info(f"Successfully converted amount: {current_amount_due} for Parcel ID: {parcel_id}")
                            extraction_log.append(f"Successfully converted amount: {current_amount_due}")
                        except ValueError:
                            logger.warning(f"Could not convert amount due '{cleaned_amount_text}' to float for Parcel ID {parcel_id}. Assuming 0.0.")
                            extraction_log.append(f"Could not convert amount due '{cleaned_amount_text}' to float. Assumed 0.0.")
                            current_amount_due = 0.0

                    # Always save the amount regardless of whether it's 0 or not
                    details['Amount_Due'] = current_amount_due
                    
                    if current_amount_due == 0.0:
                        details['Bill_Paid_Status'] = "Paid"
                        bill_is_paid = True
                        logger.info(f"Bill is PAID for Parcel ID: {parcel_id}. Amount: {amount_due_text}")
                        extraction_log.append(f"Bill is PAID. Amount: {amount_due_text}")
                        # As per Step 5, if paid, end process and go back. We'll handle navigation later.
                        # For now, we just return the details collected so far.
                        # Step 20: Go back to initial page (3 times)
                        try:
                            logger.info(f"Navigating back 3 times for Parcel ID: {parcel_id} as bill is paid.")
                            for _ in range(3):
                                self.browser.back()
                                time.sleep(1) # Wait for page to potentially reload
                            logger.info(f"Successfully navigated back 3 times for Parcel ID: {parcel_id}")
                            extraction_log.append("Navigated back 3 times (bill paid).")
                        except Exception as e_nav:
                            logger.error(f"Error navigating back for Parcel ID {parcel_id} (bill paid): {str(e_nav)}")
                            extraction_log.append(f"Error navigating back (bill paid): {str(e_nav)}")
                        return details # End processing for this parcel
                    else:
                        details['Bill_Paid_Status'] = "Not Paid"
                        details['Tax_Bill_Page_URL'] = self.browser.current_url
                        logger.info(f"Bill is NOT PAID for Parcel ID: {parcel_id}. Amount: {amount_due_text} ({current_amount_due}), URL: {details['Tax_Bill_Page_URL']}")
                        extraction_log.append(f"Bill is NOT PAID. Amount: {amount_due_text} ({current_amount_due}), URL: {details['Tax_Bill_Page_URL']}")
                else:
                    # No amount found at all
                    logger.error(f"Could not find amount due anywhere in table for Parcel ID: {parcel_id}")
                    extraction_log.append("Could not find amount due anywhere in table.")
                    details['Bill_Paid_Status'] = "Error - Amount not found"
                    details['Amount_Due'] = pd.NA

            except TimeoutException:
                logger.warning(f"Timeout waiting for table structure for Parcel ID: {parcel_id}. Assuming bill status unknown.")
                extraction_log.append("Timeout waiting for table structure.")
                details['Bill_Paid_Status'] = "Error - Timeout getting amount"
                details['Amount_Due'] = pd.NA
            except NoSuchElementException:
                logger.warning(f"Table structure not found for Parcel ID: {parcel_id}. Assuming bill status unknown.")
                extraction_log.append("Table structure not found.")
                details['Bill_Paid_Status'] = "Error - Table not found"
                details['Amount_Due'] = pd.NA
            except Exception as e:
                logger.error(f"Error checking bill status for Parcel ID {parcel_id}: {str(e)}")
                extraction_log.append(f"Error checking bill status: {str(e)}")
                details['Bill_Paid_Status'] = f"Error - {str(e)}"
                details['Amount_Due'] = pd.NA

            # If bill is paid, we would have returned already. So, proceed if not paid.
            if bill_is_paid:
                 # This block should ideally not be reached if logic above is correct.
                logger.warning(f"Reached unexpected state: bill_is_paid is true, but process continued. Parcel: {parcel_id}")
                return details

            # Step 7: Extract delinquent years if bill is not paid
            delinquent_years_list = []
            try:
                logger.info(f"Extracting delinquent years for Parcel ID: {parcel_id}")
                # Rows 2 to 12 for delinquent years as per SiteFlowDetails.md
                for i in range(2, 13): # tr:nth-child(2) to tr:nth-child(12)
                    year_selector = f"#datalet_div_0 > table:nth-child(3) > tbody > tr:nth-child({i}) > td:nth-child(1)"
                    amount_selector = f"#datalet_div_0 > table:nth-child(3) > tbody > tr:nth-child({i}) > td:nth-child(7)"
                    try:
                        year_text = self.browser.find_element(By.CSS_SELECTOR, year_selector).text.strip()
                        amount_text = self.browser.find_element(By.CSS_SELECTOR, amount_selector).text.strip()
                        extraction_log.append(f"Checking year row {i-1}: Year '{year_text}', Amount '{amount_text}'")
                        
                        # Sanitize amount_text before comparison
                        cleaned_row_amount = amount_text.replace('$', '').replace(',', '')
                        current_row_amount_val = 0.0
                        if cleaned_row_amount:
                            try:
                                current_row_amount_val = float(cleaned_row_amount)
                            except ValueError:
                                # If conversion fails, log it but treat as non-delinquent for this row to be safe
                                logger.warning(f"Could not convert amount '{cleaned_row_amount}' in year row for Parcel ID {parcel_id}. Skipping for delinquency check.")
                                extraction_log.append(f"Could not convert amount '{cleaned_row_amount}' to float in year row.")

                        if amount_text and current_row_amount_val != 0.0: # Check if not "0.00" or empty
                            # Clean up the year text - remove "total" and other unwanted text
                            year_cleaned = year_text.strip().upper()
                            # Skip if it contains "total" or is not a valid year format
                            if 'TOTAL' not in year_cleaned and year_cleaned.isdigit() and len(year_cleaned) == 4:
                                delinquent_years_list.append(year_text)
                                logger.info(f"Delinquent year found: {year_text} with amount {amount_text} for Parcel ID: {parcel_id}")
                                extraction_log.append(f"Delinquent: Year {year_text}, Amount {amount_text}")
                            else:
                                logger.info(f"Skipping non-year entry: {year_text} for Parcel ID: {parcel_id}")
                                extraction_log.append(f"Skipped non-year entry: {year_text}")
                    except NoSuchElementException:
                        logger.warning(f"Could not find year/amount element for row {i} for Parcel ID: {parcel_id}. Might be less than 11 years of data.")
                        extraction_log.append(f"Year/Amount element not found for row {i}.")
                        break # Stop if a row is missing, means no more year data
                    except Exception as e_year:
                        logger.error(f"Error extracting specific delinquent year (row {i}) for Parcel ID {parcel_id}: {str(e_year)}")
                        extraction_log.append(f"Error extracting delinquent year row {i}: {str(e_year)}")
                
                if delinquent_years_list:
                    details['Delinquent_Years'] = ", ".join(delinquent_years_list)
                    logger.info(f"Collected delinquent years for {parcel_id}: {details['Delinquent_Years']}")
                else:
                    details['Delinquent_Years'] = "None" # Or pd.NA if preferred for no delinquency
                    logger.info(f"No delinquent years found for Parcel ID: {parcel_id}")
                    extraction_log.append("No delinquent years found (all checked amounts were 0.00 or not found).")
            except Exception as e_delinq:
                logger.error(f"General error extracting delinquent years for Parcel ID {parcel_id}: {str(e_delinq)}")
                extraction_log.append(f"General error extracting delinquent years: {str(e_delinq)}")
                details['Delinquent_Years'] = "Error extracting"

            # Step 8: Click on the Summary button (with retry logic)
            if not details.get('_extraction_error'):
                for attempt in range(max_retries + 1):
                    try:
                        summary_button = WebDriverWait(self.browser, 20).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, "#sidemenu > ul > li:nth-child(2) > a"))
                        )
                        summary_button.click()
                        logger.info(f"Clicked Summary button for Parcel ID: {parcel_id}")
                        extraction_log.append("Clicked Summary button.")
                        # Wait for summary page to load
                        WebDriverWait(self.browser, 30).until(
                            EC.presence_of_element_located((By.ID, "Owner Mailing"))
                        )
                        logger.info(f"Navigated to Summary page for Parcel ID: {parcel_id}")
                        extraction_log.append("Navigated to Summary page.")
                        break  # Success, exit retry loop
                    except TimeoutException:
                        if attempt < max_retries:
                            logger.warning(f"Timeout on Summary navigation for {parcel_id}, attempt {attempt + 1}/{max_retries + 1}. Attempting recovery...")
                            extraction_log.append(f"Timeout on Summary navigation, attempt {attempt + 1}. Attempting recovery...")
                            if not self.recover_from_timeout(parcel_id, "tax_bill"):
                                logger.error(f"Failed to recover from Summary timeout for {parcel_id}")
                                extraction_log.append("Failed to recover from Summary timeout.")
                                details['_summary_extraction_error'] = "Failed to recover from Summary timeout."
                                break
                        else:
                            logger.error(f"Final timeout waiting for Summary button or Summary page for Parcel ID: {parcel_id}")
                            extraction_log.append("Final timeout clicking Summary button or loading Summary page.")
                            details['_summary_extraction_error'] = "Final timeout on Summary button/page."
                            break
                    except NoSuchElementException:
                        logger.error(f"Summary button not found for Parcel ID: {parcel_id}")
                        extraction_log.append("Summary button not found.")
                        details['_summary_extraction_error'] = "Summary button not found."
                        break
                    except Exception as e_summary_nav:
                        logger.error(f"Error navigating to Summary page for Parcel ID {parcel_id}: {str(e_summary_nav)}")
                        extraction_log.append(f"Error navigating to Summary: {str(e_summary_nav)}")
                        details['_summary_extraction_error'] = f"Error navigating to Summary: {str(e_summary_nav)}"
                        break

            if not details.get('_summary_extraction_error'):
                # Step 9: Owner_Name2
                owner_name2_parts = []
                try:
                    owner_name2_part1 = self.browser.find_element(By.CSS_SELECTOR, "#Owner\ Mailing > tbody > tr:nth-child(4) > td.DataletData").text.strip()
                    if owner_name2_part1: owner_name2_parts.append(owner_name2_part1)
                    extraction_log.append(f"Owner_Name2 part 1: '{owner_name2_part1}'")
                except NoSuchElementException:
                    logger.warning(f"Owner_Name2 part 1 not found for Parcel ID {parcel_id}.")
                    extraction_log.append("Owner_Name2 part 1 not found.")
                except Exception as e_own2_p1:
                    logger.error(f"Error extracting Owner_Name2 part 1 for {parcel_id}: {str(e_own2_p1)}")
                    extraction_log.append(f"Error Owner_Name2 part 1: {str(e_own2_p1)}")
                try:
                    owner_name2_part2 = self.browser.find_element(By.CSS_SELECTOR, "#Owner\ Mailing > tbody > tr:nth-child(5) > td.DataletData").text.strip()
                    if owner_name2_part2: owner_name2_parts.append(owner_name2_part2)
                    extraction_log.append(f"Owner_Name2 part 2: '{owner_name2_part2}'")
                except NoSuchElementException:
                    logger.warning(f"Owner_Name2 part 2 not found for Parcel ID {parcel_id}.")
                    extraction_log.append("Owner_Name2 part 2 not found.")
                except Exception as e_own2_p2:
                    logger.error(f"Error extracting Owner_Name2 part 2 for {parcel_id}: {str(e_own2_p2)}")
                    extraction_log.append(f"Error Owner_Name2 part 2: {str(e_own2_p2)}")
                
                if owner_name2_parts:
                    details['Owner_Name2'] = ", ".join(filter(None, owner_name2_parts))
                    logger.info(f"Extracted Owner_Name2 for {parcel_id}: {details['Owner_Name2']}")
                else:
                    details['Owner_Name2'] = pd.NA
                    logger.warning(f"Owner_Name2 not found or empty for Parcel ID {parcel_id}.")
                    extraction_log.append("Owner_Name2 not found or empty.")

                # Step 10: Mailing_Address_Combined
                mailing_address_parts = []
                try:
                    for i in range(6, 11):
                        try:
                            part_selector = f"#Owner\ Mailing > tbody > tr:nth-child({i}) > td.DataletData"
                            part_text = self.browser.find_element(By.CSS_SELECTOR, part_selector).text.strip()
                            if part_text: mailing_address_parts.append(part_text)
                            extraction_log.append(f"Mailing Address part (row {i}): '{part_text}'")
                        except NoSuchElementException:
                            logger.warning(f"Mailing Address part (row {i}) not found for Parcel ID {parcel_id}.")
                            extraction_log.append(f"Mailing Address part (row {i}) not found.")
                        except Exception as e_mail_part:
                            logger.error(f"Error extracting Mailing Address part (row {i}) for {parcel_id}: {str(e_mail_part)}")
                            extraction_log.append(f"Error Mailing Address part (row {i}): {str(e_mail_part)}")

                    if mailing_address_parts:
                        details['Mailing_Address_Combined'] = " ".join(filter(None, mailing_address_parts))
                        logger.info(f"Extracted Mailing_Address_Combined for {parcel_id}: {details['Mailing_Address_Combined']}")
                    else:
                        details['Mailing_Address_Combined'] = pd.NA
                        logger.warning(f"Mailing_Address_Combined not found or empty for Parcel ID {parcel_id}.")
                        extraction_log.append("Mailing_Address_Combined not found or empty.")
                except Exception as e_mail_addr:
                    logger.error(f"General error extracting Mailing Address for Parcel ID {parcel_id}: {str(e_mail_addr)}")
                    extraction_log.append(f"General error Mailing Address: {str(e_mail_addr)}")
                    details['Mailing_Address_Combined'] = "Error extracting"
            else:
                extraction_log.append("Skipped Owner_Name2 and Mailing_Address extraction due to Summary page navigation error.")
                logger.warning(f"Skipped Summary details extraction for {parcel_id} due to navigation error.")

            # Step 11: Click on the Value History button (with retry logic)
            for attempt in range(max_retries + 1):
                try:
                    value_history_button = WebDriverWait(self.browser, 20).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "#sidemenu > ul > li:nth-child(8) > a"))
                    )
                    value_history_button.click()
                    logger.info(f"Clicked Value History button for Parcel ID: {parcel_id}")
                    extraction_log.append("Clicked Value History button.")
                    # Wait for value history page to load
                    WebDriverWait(self.browser, 30).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "#datalet_div_0 > table:nth-child(3)"))
                    )
                    logger.info(f"Navigated to Value History page for Parcel ID: {parcel_id}")
                    extraction_log.append("Navigated to Value History page.")
                    break  # Success, exit retry loop
                except TimeoutException:
                    if attempt < max_retries:
                        logger.warning(f"Timeout on Value History navigation for {parcel_id}, attempt {attempt + 1}/{max_retries + 1}. Attempting recovery...")
                        extraction_log.append(f"Timeout on Value History navigation, attempt {attempt + 1}. Attempting recovery...")
                        if not self.recover_from_timeout(parcel_id, "summary"):
                            logger.error(f"Failed to recover from Value History timeout for {parcel_id}")
                            extraction_log.append("Failed to recover from Value History timeout.")
                            details['_value_history_extraction_error'] = "Failed to recover from Value History timeout."
                            break
                    else:
                        logger.error(f"Final timeout waiting for Value History button or page for Parcel ID: {parcel_id}")
                        extraction_log.append("Final timeout clicking Value History button or loading page.")
                        details['_value_history_extraction_error'] = "Final timeout on Value History button/page."
                        break
                except NoSuchElementException:
                    logger.error(f"Value History button not found for Parcel ID: {parcel_id}")
                    extraction_log.append("Value History button not found.")
                    details['_value_history_extraction_error'] = "Value History button not found."
                    break
                except Exception as e_val_hist_nav:
                    logger.error(f"Error navigating to Value History page for Parcel ID {parcel_id}: {str(e_val_hist_nav)}")
                    extraction_log.append(f"Error navigating to Value History: {str(e_val_hist_nav)}")
                    details['_value_history_extraction_error'] = f"Error navigating to Value History: {str(e_val_hist_nav)}"
                    break

            # Steps 12-19: Extract data from Value History page
            if not details.get('_value_history_extraction_error'):
                base_selector_path = "#datalet_div_0 > table:nth-child(3) > tbody > tr:nth-child(2) > " # Second row for current values
                value_fields_selectors = {
                    'Appr_Land': base_selector_path + "td:nth-child(2)",
                    'Deferred_Land': base_selector_path + "td:nth-child(3)",
                    'Appr_Bldg': base_selector_path + "td:nth-child(4)",
                    'Assessed_Total': base_selector_path + "td:nth-child(5)",
                    'Exempt_Amount': base_selector_path + "td:nth-child(6)", # Renamed from Exempt in flow
                    'SR_Exclusion': base_selector_path + "td:nth-child(7)",
                    'Vet_Exclusion': base_selector_path + "td:nth-child(8)",
                    'Taxable_Total': base_selector_path + "td:nth-child(9)",
                }

                for field_name, selector in value_fields_selectors.items():
                    try:
                        value = self.browser.find_element(By.CSS_SELECTOR, selector).text.strip()
                        details[field_name] = value
                        extraction_log.append(f"Extracted {field_name}: '{value}'")
                        logger.info(f"Extracted {field_name} for {parcel_id}: '{value}'")
                    except NoSuchElementException:
                        logger.warning(f"{field_name} element with selector '{selector}' not found for Parcel ID {parcel_id}.")
                        extraction_log.append(f"{field_name} not found.")
                        details[field_name] = pd.NA # Or "Not Found"
                    except Exception as e_field:
                        logger.error(f"Error extracting {field_name} for Parcel ID {parcel_id}: {str(e_field)}")
                        extraction_log.append(f"Error extracting {field_name}: {str(e_field)}")
                        details[field_name] = "Error extracting"
            else:
                extraction_log.append("Skipped Value History data extraction due to navigation error.")
                logger.warning(f"Skipped Value History data extraction for {parcel_id} due to navigation error.")

            # Step 20: Go back 3 times to initial search page (if bill was not paid and processing is complete for this record)
            try:
                logger.info(f"Navigating back 3 times for Parcel ID: {parcel_id} after data extraction.")
                for i in range(3):
                    self.browser.back()
                    logger.debug(f"Navigation back step {i+1} for {parcel_id}")
                    time.sleep(1) # Add a small delay to allow page to settle
                logger.info(f"Successfully navigated back 3 times to search results page for Parcel ID: {parcel_id}")
                extraction_log.append("Navigated back 3 times (post-extraction).")
                # It is crucial to wait for an element on the search results page to confirm we are back
                WebDriverWait(self.browser, 20).until(
                    EC.presence_of_element_located((By.ID, "inpParid"))
                )
                logger.info(f"Confirmed back on search page for Parcel ID: {parcel_id}")
                extraction_log.append("Confirmed back on search page.")

            except Exception as e_nav_back:
                logger.error(f"Error navigating back 3 times for Parcel ID {parcel_id}: {str(e_nav_back)}")
                extraction_log.append(f"Error navigating back 3 times: {str(e_nav_back)}")
                details['_navigation_back_error'] = str(e_nav_back)
                # This could be critical as it might affect the next record if not handled well.

            logger.info(f"Completed all extraction steps for Parcel ID: {parcel_id}")
            return details

        except Exception as e_general:
            logger.error(f"General error in extract_rowan_details for Parcel ID {parcel_id}: {str(e_general)}")
            logger.error(traceback.format_exc())
            details['_extraction_error'] = str(e_general)
            extraction_log.append(f"General error during extraction: {str(e_general)}")
            # Attempt to navigate back if a general error occurs mid-extraction to reset for the next record
            try:
                logger.warning(f"Attempting to navigate back to search page due to general error for {parcel_id}")
                # Determine how many .back() calls are needed based on current URL or state if possible
                # For now, a fixed number or a loop until a known element on search page appears
                # This is a simplified recovery attempt
                current_url = self.browser.current_url
                if "Search.aspx" not in current_url: # Check if not already on a search page variant
                    self.browser.get("https://tax.rowancountync.gov/search/CommonSearch.aspx?mode=REALPROP")
                    WebDriverWait(self.browser, 10).until(EC.presence_of_element_located((By.ID, "inpParid")))
                    logger.info(f"Navigated back to search page via GET after general error for {parcel_id}")
                    extraction_log.append("Navigated back to search page via GET after general error.")
            except Exception as e_nav_recovery:
                logger.error(f"Failed to navigate back to search page during error recovery for {parcel_id}: {e_nav_recovery}")
                extraction_log.append(f"Failed error recovery navigation: {e_nav_recovery}")
            return details

    def process_record(self, record, bypass_checkpoint=False):
        """
        Process a single record from the input CSV with robust retry logic
        
        Args:
            record: The record to process
            bypass_checkpoint: If True, skip the checkpoint check (useful for missing records)
        """
        max_record_retries = 3  # Maximum number of full record processing attempts
        
        for record_attempt in range(max_record_retries):
            try:
                # Use PropertyID from Rowan2.csv as the unique identifier and search key
                parcel_id = str(record['PropertyID']).strip("'") # Clean PropertyID, remove potential apostrophes
                
                # Skip if already processed (unless bypassing checkpoint)
                if not bypass_checkpoint and parcel_id in self.processed_records:
                    logger.info(f"Skipping already processed Parcel ID: {parcel_id}")
                    # Save checkpoint for skipped record
                    self.save_checkpoint(
                        last_processed_record=record,
                        extraction_details=None,
                        record_status="skipped",
                        search_attempted=False
                    )
                    return None
                
                # Log which attempt this is
                if record_attempt > 0:
                    logger.info(f"Retry attempt {record_attempt + 1}/{max_record_retries} for Parcel ID: {parcel_id}")
                
                # Create initial checkpoint for search attempt
                logger.info(f"Starting search attempt for Parcel ID: {parcel_id}")
                self.save_checkpoint(
                    last_processed_record=record,
                    extraction_details=None,
                    record_status="search_started",
                    search_attempted=True
                )
                
                # Navigate and search with retry logic
                navigation_success = False
                for nav_attempt in range(2):  # Try navigation twice before giving up on this attempt
                    try:
                        if not self.navigate_to_search_page():
                            logger.warning(f"Failed to navigate to search page for Parcel ID: {parcel_id}, nav attempt {nav_attempt + 1}")
                            if nav_attempt == 0:  # First failure, try to recover
                                logger.info(f"Attempting browser recovery for navigation failure, Parcel ID: {parcel_id}")
                                try:
                                    # Try to refresh and recover
                                    self.browser.refresh()
                                    time.sleep(3)
                                    continue  # Try navigation again
                                except Exception as recovery_e:
                                    logger.error(f"Browser recovery failed for {parcel_id}: {str(recovery_e)}")
                            else:
                                # Second failure, this navigation attempt failed completely
                                logger.error(f"Navigation failed after recovery attempt for Parcel ID: {parcel_id}")
                                break
                        else:
                            navigation_success = True
                            break  # Navigation succeeded
                    except Exception as nav_e:
                        logger.error(f"Exception during navigation for {parcel_id}, attempt {nav_attempt + 1}: {str(nav_e)}")
                        if nav_attempt == 0:
                            try:
                                self.browser.refresh()
                                time.sleep(3)
                                continue
                            except:
                                logger.error(f"Browser refresh failed during navigation recovery for {parcel_id}")
                        break
                
                if not navigation_success:
                    logger.error(f"All navigation attempts failed for Parcel ID: {parcel_id}")
                    self.save_checkpoint(
                        last_processed_record=record,
                        extraction_details=None,
                        record_status="failed_navigation",
                        search_attempted=True
                    )
                    
                    # Don't return None here - continue to next record attempt
                    if record_attempt < max_record_retries - 1:
                        logger.info(f"Will retry entire record processing for {parcel_id} (attempt {record_attempt + 1}/{max_record_retries})")
                        time.sleep(5)  # Wait before retrying
                        continue  # Try the entire record again
                    else:
                        logger.error(f"Final failure: All record processing attempts exhausted for Parcel ID: {parcel_id}")
                        return None  # Only return None after all attempts failed
                
                # Try the search
                search_success = False
                search_result = None
                for search_attempt in range(2):  # Try search twice
                    try:
                        search_result = self.search_by_parcel_id(parcel_id)
                        if search_result == "no_results":
                            # Parcel ID search failed, try owner name search as fallback
                            logger.info(f"Parcel ID {parcel_id} not found by Parcel ID search - trying Owner Name fallback")
                            
                            owner_name = record.get('Owner_Name', '')
                            if owner_name and owner_name.strip():
                                logger.info(f"Attempting owner name search for: {owner_name}")
                                try:
                                    owner_search_result = self.search_by_owner_name(owner_name)
                                    if owner_search_result == True:
                                        logger.info(f"Owner name search succeeded for {owner_name} (Parcel ID: {parcel_id})")
                                        # Owner name search found results, click on first result and proceed
                                        if self.find_and_select_parcel_in_owner_search_results(parcel_id):
                                            logger.info(f"Successfully clicked on first result from owner search for {parcel_id}")
                                            search_success = True
                                            break  # Owner name search succeeded, proceed with extraction
                                        else:
                                            logger.info(f"Failed to click on owner search results for {owner_name} (Parcel ID: {parcel_id})")
                                    elif owner_search_result == "no_results":
                                        logger.info(f"Owner name search also returned no results for {owner_name} (Parcel ID: {parcel_id})")
                                    else:  # owner_search_result == False (technical failure)
                                        logger.warning(f"Owner name search failed due to technical issues for {owner_name} (Parcel ID: {parcel_id})")
                                except Exception as owner_search_e:
                                    logger.error(f"Exception during owner name search for {owner_name} (Parcel ID: {parcel_id}): {str(owner_search_e)}")
                            else:
                                logger.warning(f"No valid owner name found for Parcel ID {parcel_id} - cannot try owner name fallback")
                            
                            # If we reach here, both parcel ID and owner name searches failed or returned no results
                            logger.info(f"Both Parcel ID and Owner Name searches failed for {parcel_id} - marking as 'not found'")
                            self.save_checkpoint(
                                last_processed_record=record,
                                extraction_details={"not_found_reason": "No results from both Parcel ID and Owner Name searches"},
                                record_status="not_found",
                                search_attempted=True
                            )
                            
                            # Create a result record indicating the property was not found
                            if hasattr(record, 'to_dict'):
                                result = record.to_dict()
                            else:
                                result = dict(record)
                            
                            # Add not found indicators
                            result['Bill_Paid_Status'] = "Not Found"
                            result['Amount_Due'] = pd.NA
                            result['Delinquent_Years'] = "Not Found"
                            result['Owner_Name2'] = "Not Found"
                            result['Mailing_Address_Combined'] = "Not Found"
                            result['Appr_Land'] = "Not Found"
                            result['Deferred_Land'] = "Not Found"
                            result['Appr_Bldg'] = "Not Found"
                            result['Assessed_Total'] = "Not Found"
                            result['Exempt_Amount'] = "Not Found"
                            result['SR_Exclusion'] = "Not Found"
                            result['Vet_Exclusion'] = "Not Found"
                            result['Taxable_Total'] = "Not Found"
                            result['Tax_Bill_Page_URL'] = "Not Found"
                            
                            # Mark as processed so we don't try again
                            self.processed_records.add(parcel_id)
                            
                            logger.info(f"Successfully marked Parcel ID {parcel_id} as 'not found' after trying both search methods")
                            return result  # Return result indicating not found
                            
                        elif search_result == True:
                            search_success = True
                            break  # Search succeeded
                        else:  # search_result == False (technical failure)
                            logger.warning(f"Search failed due to technical issues for Parcel ID: {parcel_id}, search attempt {search_attempt + 1}")
                            if search_attempt == 0:
                                # Try to recover and search again
                                if self.navigate_to_search_page():
                                    continue
                            break
                    except Exception as search_e:
                        logger.error(f"Exception during search for {parcel_id}, attempt {search_attempt + 1}: {str(search_e)}")
                        if search_attempt == 0:
                            try:
                                if self.navigate_to_search_page():
                                    continue
                            except:
                                logger.error(f"Recovery navigation failed during search for {parcel_id}")
                        break
                
                if not search_success:
                    logger.error(f"Search failed due to technical issues for Parcel ID: {parcel_id}")
                    self.save_checkpoint(
                        last_processed_record=record,
                        extraction_details=None,
                        record_status="failed_search",
                        search_attempted=True
                    )
                    
                    if record_attempt < max_record_retries - 1:
                        logger.info(f"Will retry entire record processing for {parcel_id} due to search failure")
                        time.sleep(5)
                        continue  # Try the entire record again
                    else:
                        logger.error(f"Final failure: Search failed after all attempts for Parcel ID: {parcel_id}")
                        return None
                    
                # Extract details - this will be the new Rowan specific extraction method
                details = self.extract_rowan_details(parcel_id) # Pass parcel_id for logging

                if details and not details.get('_extraction_error'): # Check for extraction errors
                    # Convert pandas Series to dict if needed
                    if hasattr(record, 'to_dict'):
                        result = record.to_dict()
                    else:
                        result = dict(record)
                    
                    # Update with extracted Rowan data
                    result['Parcel_ID_Processed'] = parcel_id
                    result.update(details) # Add all extracted details
                    
                    self.processed_records.add(parcel_id)
                    
                    # Store processed details for checkpoint
                    processed_detail = {
                        'account_number': parcel_id,
                        'bill_number': '',
                        'timestamp': datetime.now().isoformat(),
                        'original_record': record,
                        'extracted_details': details,
                        'final_result': result
                    }
                    self.processed_details.append(processed_detail)
                    
                    # Save checkpoint with detailed information for successful extraction
                    self.save_checkpoint(
                        last_processed_record=record, # Save the original input record
                        extraction_details=details,
                        record_status="completed",
                        search_attempted=True
                    )
                    
                    logger.info(f"Successfully processed Parcel ID: {parcel_id}")
                    return result  # Success! Return the result
                else:
                    logger.error(f"Failed to extract details for Parcel ID: {parcel_id}. Error: {details.get('_extraction_error', 'Unknown error')}")
                    # Save checkpoint for failed extraction
                    self.save_checkpoint(
                        last_processed_record=record,
                        extraction_details=details,
                        record_status="failed_extraction",
                        search_attempted=True
                    )
                    
                    if record_attempt < max_record_retries - 1:
                        logger.info(f"Will retry entire record processing for {parcel_id} due to extraction failure")
                        time.sleep(5)
                        continue  # Try the entire record again
                    else:
                        logger.error(f"Final failure: Extraction failed after all attempts for Parcel ID: {parcel_id}")
                        return None
                        
            except KeyError as e:
                logger.error(f"Missing expected key in record: {str(e)}. Record data: {record}")
                # Save checkpoint for missing key error
                self.save_checkpoint(
                    last_processed_record=record,
                    extraction_details=None,
                    record_status="failed_missing_key",
                    search_attempted=False
                )
                return None  # KeyError shouldn't retry
            except Exception as e:
                # Catch any other exceptions during record processing
                logger.error(f"Error processing record with Parcel ID (from PropertyID {record.get('PropertyID', 'N/A')}): {str(e)}")
                logger.error(traceback.format_exc()) # Log full traceback for debugging
                
                # Save checkpoint for general processing error
                self.save_checkpoint(
                    last_processed_record=record,
                    extraction_details=None,
                    record_status="failed_general_error",
                    search_attempted=True
                )
                
                if record_attempt < max_record_retries - 1:
                    logger.info(f"Will retry entire record processing for {record.get('PropertyID', 'N/A')} due to general error")
                    time.sleep(5)
                    continue  # Try the entire record again
                else:
                    logger.error(f"Final failure: General error after all attempts for {record.get('PropertyID', 'N/A')}")
                    return None
        
        # If we get here, all retry attempts for this record have been exhausted
        logger.error(f"All retry attempts exhausted for record {record.get('PropertyID', 'N/A')}")
        return None

    def run(self, input_csv="Rowan2.csv", process_remaining_only=False):
        """
        Main execution method
        """
        try:
            # Ensure compatible ChromeDriver
            if not self.ensure_compatible_chromedriver():
                logger.error("Failed to ensure compatible ChromeDriver")
                return
                
            # Setup browser
            self.setup_browser()
            
            # Read input CSV
            try:
                df = pd.read_csv(input_csv)
            except FileNotFoundError:
                logger.error(f"Input CSV file not found: {input_csv}")
                return
            except Exception as e:
                logger.error(f"Error reading input CSV {input_csv}: {e}")
                return

            total_records_in_csv = len(df)
            initial_processed_count = len(self.processed_records) 
            
            # Find the starting position based on last processed record
            start_index = 0
            if self.processed_records and not process_remaining_only:
                # Find the last processed record in the CSV to determine where to start
                logger.info("Determining starting position based on last processed record...")
                
                # Create a mapping of PropertyID to index for quick lookup
                property_id_to_index = {}
                for index, row in df.iterrows():
                    try:
                        parcel_id = str(row['PropertyID']).strip("'")
                        if parcel_id:
                            property_id_to_index[parcel_id] = index
                    except KeyError:
                        continue
                
                # Find the highest index of any processed record
                max_processed_index = -1
                last_processed_parcel_id = None
                
                for processed_parcel_id in self.processed_records:
                    if processed_parcel_id in property_id_to_index:
                        index = property_id_to_index[processed_parcel_id]
                        if index > max_processed_index:
                            max_processed_index = index
                            last_processed_parcel_id = processed_parcel_id
                
                if max_processed_index >= 0:
                    start_index = max_processed_index + 1  # Start from the next record
                    logger.info(f"Last processed record found: Parcel ID {last_processed_parcel_id} at CSV index {max_processed_index}")
                    logger.info(f"Starting processing from CSV index {start_index} (record {start_index + 1}/{total_records_in_csv})")
                else:
                    logger.info("No processed records found in current CSV order, starting from beginning")
            elif process_remaining_only:
                logger.info("Running in --remaining mode: will process all unprocessed records in CSV order")
            else:
                logger.info("No checkpoint data found, starting from beginning")
            
            # Calculate remaining records to process
            remaining_records = total_records_in_csv - start_index
            
            # For tracking progress within this specific run
            newly_processed_count_this_run = 0
            skipped_in_remaining_mode_count = 0

            logger.info(f"Starting processing of '{input_csv}'. Total records in CSV: {total_records_in_csv}.")
            logger.info(f"Records already processed according to checkpoints: {initial_processed_count}.")
            logger.info(f"Starting from CSV index {start_index}, {remaining_records} records remaining to process.")
            if process_remaining_only:
                logger.info("Running in --remaining mode: will only process records not found in checkpoints.")

            results_for_current_run = [] 
            
            # Process each record starting from the determined start_index
            for index in range(start_index, total_records_in_csv):
                row = df.iloc[index]
                record = row.to_dict()
                
                try:
                    # PropertyID from Rowan2.csv is used as parcel_id
                    parcel_id = str(record['PropertyID']).strip("'")
                    if not parcel_id: # Handle cases where PropertyID might be empty or missing in a row
                        logger.warning(f"Skipping record {index + 1}/{total_records_in_csv} due to missing or empty PropertyID.")
                        continue
                except KeyError:
                    logger.warning(f"Skipping record {index + 1}/{total_records_in_csv} due to missing 'PropertyID' key in input CSV.")
                    continue
                
                # Handle --remaining logic: skip if already processed
                if process_remaining_only and parcel_id in self.processed_records:
                    logger.debug(f"Skipping Parcel ID {parcel_id} (CSV record {index + 1}) as it's already processed (--remaining mode).")
                    skipped_in_remaining_mode_count +=1
                    continue

                current_record_log_label = f"CSV record {index + 1}/{total_records_in_csv}, Parcel ID {parcel_id}"
                logger.info(f"Processing: {current_record_log_label}")
                
                try:
                    # If not in --remaining mode, process_record internally checks self.processed_records 
                    # (because bypass_checkpoint will be False).
                    # If in --remaining mode, we've already confirmed it's a new record, 
                    # so bypass_checkpoint=True tells process_record not to re-check.
                    result = self.process_record(record, bypass_checkpoint=process_remaining_only)
                    
                    if result:
                        results_for_current_run.append(result)
                        newly_processed_count_this_run += 1
                        logger.info(f"Successfully processed: {current_record_log_label}")
                        
                        # Save progress periodically for new results from this run
                        if len(results_for_current_run) % 10 == 0:
                            self.save_results(results_for_current_run) # Pass only new results
                            logger.info(f"Saved batch of {len(results_for_current_run)} new results. "
                                        f"{newly_processed_count_this_run} new records processed in this run so far.")
                            results_for_current_run = [] # Clear the list after saving
                    else:
                        # process_record returns None if skipped due to checkpoint (non --remaining mode),
                        # or if extraction fails. Log is handled within process_record.
                        logger.info(f"No new result for: {current_record_log_label} (may have been skipped or failed extraction).")
                        
                except Exception as e_proc: # Catch errors from process_record call itself
                    logger.error(f"Unhandled error during process_record for {current_record_log_label}: {str(e_proc)}")
                    logger.error(traceback.format_exc())
                    # Optionally, attempt to save any partial data or mark as failed
                    continue # Continue with the next record
                    
            # Save any remaining results from this run
            if results_for_current_run:
                self.save_results(results_for_current_run)
                logger.info(f"Saved final batch of {len(results_for_current_run)} new results.")
            
            logger.info(f"Processing of '{input_csv}' completed.")
            logger.info(f"Total new records processed in this run: {newly_processed_count_this_run}.")
            if process_remaining_only:
                logger.info(f"Records skipped in --remaining mode (already in checkpoint): {skipped_in_remaining_mode_count}.")
            overall_processed_count = len(self.processed_records) # Reflects total after this run
            logger.info(f"Total unique records processed across all runs (in checkpoints): {overall_processed_count}.")
            
            # Clean up old checkpoints
            self.cleanup_old_checkpoints()
            
        except Exception as e:
            logger.error(f"Critical error in run method: {str(e)}")
            logger.error(traceback.format_exc())
            
        finally:
            # Cleanup
            if self.browser:
                self.browser.quit()
            if self.proxy_runner:
                self.proxy_runner.stop()

    def save_results(self, results_to_save):
        """
        Append new results to the output CSV file.
        Ensures consistent column structure and avoids duplicates.
        """
        try:
            if not results_to_save:
                logger.info("No new results to save in this batch.")
                return
            
            # Ensure the output directory exists
            output_dir = Path(os.path.dirname(self.output_file))
            output_dir.mkdir(parents=True, exist_ok=True)
                
            # Convert new results for this batch to DataFrame
            new_df = pd.DataFrame(results_to_save)
            
            # Define expected columns based on SiteFlowDetails.md and Rowan2.csv input
            # self.run_input_csv should be set by main() before run() is called
            try:
                input_df_columns = list(pd.read_csv(self.run_input_csv, nrows=0).columns)
            except FileNotFoundError:
                 logger.error(f"Input CSV {self.run_input_csv} not found for determining output columns. Using a default set.")
                 input_df_columns = ['PropertyID', 'Year', 'Account_Number', 'Owner_Name', 'Property_Address', 'is_business', 'has_heir'] # Fallback
            except Exception as e:
                 logger.error(f"Error reading input CSV {self.run_input_csv} columns: {e}. Using a default set.")
                 input_df_columns = ['PropertyID', 'Year', 'Account_Number', 'Owner_Name', 'Property_Address', 'is_business', 'has_heir'] # Fallback


            additional_output_columns = [
                'Amount_Due', 'Delinquent_Years', 'Owner_Name2', 
                'Mailing_Address_Combined', 'Appr_Land', 'Deferred_Land', 
                'Appr_Bldg', 'Assessed_Total', 'Exempt_Amount', 
                'SR_Exclusion', 'Vet_Exclusion', 'Taxable_Total', 
                'Bill_Paid_Status', 'Tax_Bill_Page_URL'
            ]
            # Combine and ensure no duplicate column names if any overlap by chance
            seen_cols = set()
            unique_input_cols = [col for col in input_df_columns if col not in seen_cols and not seen_cols.add(col)]
            
            final_output_columns = unique_input_cols + [col for col in additional_output_columns if col not in seen_cols and not seen_cols.add(col)]

            # Ensure all columns exist in new_df, add if missing with pd.NA
            for col in final_output_columns:
                if col not in new_df.columns:
                    new_df[col] = pd.NA 
            
            # Reorder new_df to have the consistent column order
            new_df = new_df.reindex(columns=final_output_columns)

            file_exists = os.path.exists(self.output_file)
            
            if file_exists:
                logger.info(f"Output file {self.output_file} exists. Attempting to append {len(new_df)} new records.")
                try:
                    existing_df = pd.read_csv(self.output_file)
                    # Ensure existing_df also conforms to the final_output_columns structure
                    for col in final_output_columns:
                        if col not in existing_df.columns:
                            existing_df[col] = pd.NA
                    existing_df = existing_df.reindex(columns=final_output_columns)
                    
                    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                    # Remove duplicates based on the unique identifier, keeping the last entry (most recent)
                    combined_df.drop_duplicates(subset=['Parcel_ID_Processed'], keep='last', inplace=True)
                    
                except pd.errors.EmptyDataError:
                    logger.warning(f"Output file {self.output_file} is empty. Writing new data.")
                    combined_df = new_df # new_df is already structured
                except Exception as e_read:
                    logger.error(f"Error reading or processing existing output file {self.output_file}: {e_read}. "
                                 f"Saving current batch to a new timestamped fallback file as a precaution.")
                    # Fallback to writing only the new data to the main file if reading existing fails badly,
                    # or implement a more robust recovery/backup of existing file.
                    # For now, we'll save the current batch to the intended file if reading fails.
                    combined_df = new_df 
            else:
                logger.info(f"Output file {self.output_file} does not exist. Creating new file.")
                combined_df = new_df # new_df is already structured

            combined_df.to_csv(self.output_file, index=False, quoting=csv.QUOTE_ALL)
            logger.info(f"Successfully saved/appended {len(new_df)} new records to {self.output_file}. "
                        f"Total records in CSV: {len(combined_df)}.")
                
        except Exception as e:
            logger.error(f"General error in save_results: {str(e)}")
            logger.error(traceback.format_exc())
            # Fallback: save to a separate timestamped file if main save fails
            fallback_file = output_dir / f"rowan_additional_details_fallback_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            try:
                logger.info(f"Attempting to save current batch of {len(results_to_save)} results to fallback file: {fallback_file}")
                if 'new_df' not in locals(): # If error occurred before new_df was created
                    new_df_fallback = pd.DataFrame(results_to_save)
                     # Ensure all columns exist in new_df_fallback, add if missing with pd.NA
                    for col in final_output_columns if 'final_output_columns' in locals() else additional_output_columns: # Use available column list
                        if col not in new_df_fallback.columns:
                            new_df_fallback[col] = pd.NA
                    new_df_fallback = new_df_fallback.reindex(columns=final_output_columns if 'final_output_columns' in locals() else None)

                else: # new_df exists and is structured
                    new_df_fallback = new_df

                new_df_fallback.to_csv(fallback_file, index=False, quoting=csv.QUOTE_ALL)
                logger.info(f"Saved new records to fallback file: {fallback_file}")
            except Exception as fb_e:
                logger.error(f"FATAL: Error saving to fallback file as well: {fb_e}")

    def get_checkpoint_status(self):
        """
        Get detailed status of all checkpoint files
        Returns a dictionary with comprehensive checkpoint information
        """
        try:
            checkpoint_files = list(self.checkpoint_dir.glob("rowan_checkpoint_*.json"))
            
            status_summary = {
                'total_checkpoint_files': len(checkpoint_files),
                'completed': 0,
                'failed_search': 0,
                'failed_extraction': 0,
                'failed_navigation': 0,
                'failed_general_error': 0,
                'failed_missing_key': 0,
                'search_started': 0,
                'skipped': 0,
                'other': 0,
                'unique_parcel_ids_attempted': set(),
                'latest_checkpoint': None,
                'earliest_checkpoint': None
            }
            
            if not checkpoint_files:
                return status_summary
            
            # Sort files by modification time
            checkpoint_files.sort(key=lambda x: x.stat().st_mtime)
            status_summary['earliest_checkpoint'] = checkpoint_files[0].name
            status_summary['latest_checkpoint'] = checkpoint_files[-1].name
            
            for checkpoint_file in checkpoint_files:
                filename = checkpoint_file.name
                
                # Extract parcel ID from filename (format: rowan_checkpoint_XXXX_PARCELID_...)
                try:
                    parts = filename.split('_')
                    if len(parts) >= 4:
                        parcel_id = parts[3]  # Third underscore-separated part
                        status_summary['unique_parcel_ids_attempted'].add(parcel_id)
                except:
                    pass  # Skip if filename doesn't match expected pattern
                
                # Count by status
                if '_completed.json' in filename:
                    status_summary['completed'] += 1
                elif '_failed_search.json' in filename:
                    status_summary['failed_search'] += 1
                elif '_failed_extraction.json' in filename:
                    status_summary['failed_extraction'] += 1
                elif '_failed_navigation.json' in filename:
                    status_summary['failed_navigation'] += 1
                elif '_failed_general_error.json' in filename:
                    status_summary['failed_general_error'] += 1
                elif '_failed_missing_key.json' in filename:
                    status_summary['failed_missing_key'] += 1
                elif '_search_started.json' in filename:
                    status_summary['search_started'] += 1
                elif '_skipped.json' in filename:
                    status_summary['skipped'] += 1
                else:
                    status_summary['other'] += 1
            
            # Convert set to count for JSON serialization
            status_summary['unique_parcel_ids_count'] = len(status_summary['unique_parcel_ids_attempted'])
            status_summary['unique_parcel_ids_attempted'] = list(status_summary['unique_parcel_ids_attempted'])
            
            return status_summary
            
        except Exception as e:
            logger.error(f"Error getting checkpoint status: {str(e)}")
            return {'error': str(e)}

    def print_checkpoint_summary(self):
        """
        Print a human-readable summary of checkpoint status
        """
        status = self.get_checkpoint_status()
        
        if 'error' in status:
            print(f"Error getting checkpoint status: {status['error']}")
            return
        
        print("\n" + "="*60)
        print("CHECKPOINT SUMMARY")
        print("="*60)
        print(f"Total checkpoint files: {status['total_checkpoint_files']}")
        print(f"Unique parcel IDs attempted: {status['unique_parcel_ids_count']}")
        print()
        print("Status breakdown:")
        print(f"  ✓ Completed successfully: {status['completed']}")
        print(f"  ✗ Failed - Search: {status['failed_search']}")
        print(f"  ✗ Failed - Extraction: {status['failed_extraction']}")
        print(f"  ✗ Failed - Navigation: {status['failed_navigation']}")
        print(f"  ✗ Failed - General Error: {status['failed_general_error']}")
        print(f"  ✗ Failed - Missing Key: {status['failed_missing_key']}")
        print(f"  ⏳ Search Started (incomplete): {status['search_started']}")
        print(f"  ⏩ Skipped (already processed): {status['skipped']}")
        print(f"  ? Other/Unknown: {status['other']}")
        print()
        if status['earliest_checkpoint']:
            print(f"Earliest checkpoint: {status['earliest_checkpoint']}")
        if status['latest_checkpoint']:
            print(f"Latest checkpoint: {status['latest_checkpoint']}")
        print("="*60)

    def get_failed_parcel_ids(self):
        """
        Get list of parcel IDs that failed processing (for retry purposes)
        Returns dict with failure reasons
        """
        try:
            checkpoint_files = list(self.checkpoint_dir.glob("rowan_checkpoint_*_failed_*.json"))
            failed_records = {}
            
            for checkpoint_file in checkpoint_files:
                try:
                    with open(checkpoint_file, 'r') as f:
                        checkpoint_data = json.load(f)
                    
                    record = checkpoint_data.get('last_processed_record', {})
                    parcel_id = str(record.get('PropertyID', 'unknown')).strip("'")
                    status = checkpoint_data.get('record_status', 'unknown')
                    
                    if parcel_id not in failed_records:
                        failed_records[parcel_id] = []
                    
                    failed_records[parcel_id].append({
                        'status': status,
                        'timestamp': checkpoint_data.get('timestamp', ''),
                        'file': checkpoint_file.name
                    })
                    
                except Exception as e:
                    logger.warning(f"Error reading failed checkpoint {checkpoint_file.name}: {e}")
            
            return failed_records
            
        except Exception as e:
            logger.error(f"Error getting failed parcel IDs: {str(e)}")
            return {}

def main(process_remaining_only=False, input_csv_path="Rowan2.csv", output_file_path=None, show_checkpoint_status=False):
    """
    Main function to scrape additional property details from Rowan County tax website
    
    Args:
        process_remaining_only (bool): If True, only process records not in checkpoint
        input_csv_path (str): Path to the input CSV file
        output_file_path (str): Path to the output CSV file (optional)
        show_checkpoint_status (bool): If True, show checkpoint status and exit
    """
    try:
        scraper = RowanPropertyDetailsScraper(output_file=output_file_path) 
        
        # If user just wants to see checkpoint status
        if show_checkpoint_status:
            scraper.print_checkpoint_summary()
            failed_records = scraper.get_failed_parcel_ids()
            if failed_records:
                print(f"\nFailed Records Summary:")
                print(f"Total parcel IDs with failures: {len(failed_records)}")
                print("Recent failures:")
                for parcel_id, failures in list(failed_records.items())[:10]:  # Show first 10
                    latest_failure = max(failures, key=lambda x: x['timestamp'])
                    print(f"  {parcel_id}: {latest_failure['status']} ({latest_failure['timestamp'][:16]})")
                if len(failed_records) > 10:
                    print(f"  ... and {len(failed_records) - 10} more")
            return
        
        scraper.run_input_csv = input_csv_path # Store for save_results to access columns
        scraper.run(input_csv=input_csv_path, process_remaining_only=process_remaining_only)
        
        # Show final checkpoint summary
        print("\n" + "="*60)
        print("FINAL RUN SUMMARY")
        print("="*60)
        scraper.print_checkpoint_summary()
        
    except Exception as e:
        logger.error(f"Main execution error: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Rowan County Additional Property Details Scraper")
    parser.add_argument(
        "--input_csv",
        default="Rowan2.csv",
        dest="input_csv_path", # Map to main function argument
        help="Path to the input CSV file (default: Rowan2.csv)"
    )
    parser.add_argument(
        "--output_file",
        default=None, # Will be generated by constructor if None
        dest="output_file_path", # Map to main function argument
        help="Path to the output CSV file (default: output/rowan_additional_details_YYYYMMDD_HHMMSS.csv)"
    )
    parser.add_argument(
        "--remaining",
        action="store_true",
        help="Process only records not found in the latest checkpoint."
    )
    parser.add_argument(
        "--status",
        action="store_true",
        dest="show_checkpoint_status", # Map to main function argument
        help="Show detailed checkpoint status and exit (no processing)."
    )
    
    args = parser.parse_args()

    main(
        process_remaining_only=args.remaining,
        input_csv_path=args.input_csv_path,
        output_file_path=args.output_file_path,
        show_checkpoint_status=args.show_checkpoint_status
    ) 