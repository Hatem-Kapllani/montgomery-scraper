import threading
import queue
import time
import logging
from pathlib import Path
import os
import traceback
import sys
from datetime import datetime
import argparse
import pandas as pd
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from concurrent.futures import ThreadPoolExecutor

# Load environment variables from .env file first
try:
    from dotenv import load_dotenv
    load_dotenv()  # Load .env file if it exists
    print("Environment variables loaded from .env file")
except ImportError:
    print("Warning: python-dotenv not available. Environment variables must be set manually.")

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

# Import our modules
from src.scrapers.multithreader.browser_manager import BrowserManager
from src.scrapers.multithreader.worker_health import WorkerHealthManager
from src.scrapers.multithreader.data_manager import DataManager
from src.utils.email_notifier import send_error_notification, send_completion_notification

# Get logger from root configuration
logger = logging.getLogger(__name__)

# Ensure we also log to a file directly from this module
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
log_file = log_dir / "montgomery_additional_run.log"

# Add a file handler to this module's logger
file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s - pid:%(process)d - %(threadName)s - %(name)s - %(levelname)s - %(message)s'
))
logger.addHandler(file_handler)

logger.info("Montgomery additional scraper module initialized")

class MontgomeryAdditionalScraper:
    def __init__(self, input_file="output/Montgomery2.csv", num_threads=3):
        self.input_file = input_file
        self.num_threads = num_threads
        self.records_df = pd.DataFrame()
        self.driver = None
        self.search_results = []
        self.lock = threading.Lock()
        
        # Initialize data manager for robust checkpointing
        self.data_manager = DataManager()
        
        # Load existing checkpoints first
        self._load_existing_checkpoint()
        
        # Load existing data
        self._load_records()
        self.completed_records, self.failed_records, self.skipped_records = self._load_completed_records()
        
        # Queue for remaining records
        self.record_queue = queue.Queue()
        self.results_queue = queue.Queue()
        self._queue_remaining_records()
    
    def _load_existing_checkpoint(self):
        """Load existing checkpoint data into the data manager"""
        try:
            from pathlib import Path
            import json
            
            checkpoint_dir = Path("checkpoints")
            if not checkpoint_dir.exists():
                logger.info("No checkpoint directory found")
                return
            
            # Find all Montgomery checkpoint files
            checkpoint_files = list(checkpoint_dir.glob("montgomery_checkpoint_*.json"))
            
            if not checkpoint_files:
                logger.info("No existing Montgomery checkpoint files found")
                return
            
            # Get the most recent checkpoint
            latest_checkpoint = max(checkpoint_files, key=os.path.getctime)
            logger.info(f"Loading checkpoint from: {latest_checkpoint}")
            
            with open(latest_checkpoint, 'r', encoding='utf-8') as f:
                checkpoint_data = json.load(f)
            
            # Load the checkpoint data into the data manager
            if "records" in checkpoint_data:
                self.data_manager.shared_data_store["records"] = checkpoint_data["records"]
                logger.info(f"Loaded {len(self.data_manager.shared_data_store['records'])} existing records from checkpoint")
            
            # Load additional scraper specific data
            if "completed_additional_records" in checkpoint_data:
                self.data_manager.shared_data_store["completed_additional_records"] = checkpoint_data["completed_additional_records"]
            
            if "failed_additional_records" in checkpoint_data:
                self.data_manager.shared_data_store["failed_additional_records"] = checkpoint_data["failed_additional_records"]
            
            if "skipped_additional_records" in checkpoint_data:
                self.data_manager.shared_data_store["skipped_additional_records"] = checkpoint_data["skipped_additional_records"]
            
        except Exception as e:
            logger.error(f"Error loading existing checkpoint: {str(e)}")
    
    def _load_records(self):
        """Load records from the CSV file"""
        try:
            if not os.path.exists(self.input_file):
                logger.error(f"Input file {self.input_file} not found")
                return False
                
            self.records_df = pd.read_csv(self.input_file)
            logger.info(f"Loaded {len(self.records_df)} records from {self.input_file}")
            return True
        except Exception as e:
            logger.error(f"Error loading records: {str(e)}")
            return False
    
    def _load_completed_records(self):
        """Load completed, failed, and skipped records from checkpoint"""
        try:
            completed_records = set(self.data_manager.shared_data_store.get("completed_additional_records", []))
            failed_records = set(self.data_manager.shared_data_store.get("failed_additional_records", []))
            skipped_records = set(self.data_manager.shared_data_store.get("skipped_additional_records", []))
            
            logger.info(f"Loaded {len(completed_records)} completed records, {len(failed_records)} failed records, and {len(skipped_records)} skipped records from checkpoint")
            
            return completed_records, failed_records, skipped_records
        except Exception as e:
            logger.error(f"Error loading completed records: {str(e)}")
            # Initialize empty sets on error
            return set(), set(), set()
    
    def _queue_remaining_records(self):
        """Queue up records that haven't been processed yet"""
        if self.records_df is None:
            return
            
        processed_records = self.completed_records.union(self.failed_records).union(self.skipped_records)
        
        for index, record in self.records_df.iterrows():
            record_key = f"{record['Account_Number']}_{index}"
            if record_key not in processed_records:
                self.record_queue.put((index, record.to_dict()))
                
        logger.info(f"Queued {self.record_queue.qsize()} remaining records for processing")
    
    def _save_checkpoint(self):
        """Save current progress to prevent data loss"""
        try:
            # Update data manager's shared data store with current records
            self.data_manager.shared_data_store["completed_additional_records"] = list(self.completed_records)
            self.data_manager.shared_data_store["failed_additional_records"] = list(self.failed_records)
            self.data_manager.shared_data_store["skipped_additional_records"] = list(self.skipped_records)
            
            # Save checkpoint
            self.data_manager.save_checkpoint()
        except Exception as e:
            logger.error(f"Error saving checkpoint: {str(e)}")
    
    def _verify_proxy_ports_available(self):
        """Verify that proxy ports are available before starting workers"""
        available_ports = WorkerHealthManager.verify_proxy_ports_available(self.num_threads)
        if len(available_ports) < self.num_threads:
            self.num_threads = len(available_ports)
        return available_ports
    
    def setup_driver(self):
        """Setup Chrome driver with basic configuration"""
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.service import Service
            from selenium.webdriver.chrome.options import Options
            
            # Chrome options (without proxy for simplicity)
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--disable-web-security")
            chrome_options.add_argument("--disable-features=VizDisplayCompositor")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
            
            # Disable images and CSS for faster loading
            prefs = {
                "profile.managed_default_content_settings.images": 2,
                "profile.managed_default_content_settings.stylesheets": 2,
                "profile.managed_default_content_settings.plugins": 2,
                "profile.managed_default_content_settings.popups": 2,
                "profile.managed_default_content_settings.geolocation": 2,
                "profile.managed_default_content_settings.notifications": 2,
                "profile.managed_default_content_settings.media_stream": 2,
            }
            chrome_options.add_experimental_option("prefs", prefs)
            
            # Set up Chrome service
            service = Service()
            
            # Create driver
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.driver.set_page_load_timeout(60)
            self.driver.implicitly_wait(10)
            
            logger.info("Chrome driver created successfully for additional scraper")
            return True
            
        except Exception as e:
            logger.error(f"Failed to setup Chrome driver: {str(e)}")
            return False
            
    def load_records(self):
        """Load records from the CSV file"""
        try:
            if not os.path.exists(self.input_file):
                logger.error(f"Input file {self.input_file} not found")
                return False
                
            self.records_df = pd.read_csv(self.input_file)
            logger.info(f"Loaded {len(self.records_df)} records from {self.input_file}")
            return True
        except Exception as e:
            logger.error(f"Error loading records: {str(e)}")
            return False
            
    def navigate_to_search_page(self):
        """Navigate to the Montgomery County search page"""
        return BrowserManager.navigate_to_search_page(self.driver, "Additional")
        
    def search_by_account_number(self, account_number):
        """Search for a record by account number"""
        try:
            # Clean the account number - remove leading quote
            clean_account = str(account_number).strip().lstrip("'")
            logger.debug(f"Searching for account number: '{clean_account}'")
            
            # First click the button to enable search by account number
            account_search_button = WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#content > table > tbody > tr:nth-child(1) > td > div:nth-child(3) > table > tbody > tr > td > center > form > div > label:nth-child(6)"))
            )
            account_search_button.click()
            time.sleep(1)
            
            # Enter account number in search field
            search_field = WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#criteria"))
            )
            search_field.clear()
            search_field.send_keys(clean_account)
            time.sleep(1)
            
            # Click search button - try multiple selectors for robustness
            search_button = None
            button_selectors = [
                "#content > table > tbody > tr:nth-child(1) > td > div:nth-child(3) > table > tbody > tr > td > center > form > table > tbody > tr:nth-child(3) > td:nth-child(2) > h3 > input[type=submit]",
                "input[type=submit]",
                "input[value*='Search']",
                "button[type=submit]"
            ]
            
            for selector in button_selectors:
                try:
                    search_button = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                    break
                except:
                    continue
                    
            if not search_button:
                logger.error("Could not find search button with any selector")
                return False
                
            search_button.click()
            time.sleep(3)  # Wait for results
            
            logger.info(f"Successfully searched for account number: {clean_account}")
            return True
            
        except Exception as e:
            logger.error(f"Error searching by account number {account_number}: {str(e)}")
            return False
            
    def click_account_number(self, account_number):
        """Click on the account number link"""
        try:
            # Clean the account number - remove leading quote and whitespace
            clean_account = str(account_number).strip().lstrip("'")
            logger.debug(f"Original account number: '{account_number}' -> Cleaned: '{clean_account}'")
            
            # Try multiple selector strategies
            selectors = [
                f'//a[contains(text(), "{clean_account}")]',
                f'//a[text()="{clean_account}"]',
                f'//a[contains(@href, "{clean_account}")]'
            ]
            
            account_link = None
            for i, xpath_expr in enumerate(selectors):
                try:
                    logger.debug(f"Trying selector {i+1}: {xpath_expr}")
                    account_link = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, xpath_expr))
                    )
                    logger.debug(f"Found account link using selector {i+1}")
                    break
                except Exception as selector_error:
                    logger.debug(f"Selector {i+1} failed: {str(selector_error)}")
                    continue
            
            if not account_link:
                logger.error(f"Could not find account number link for: {clean_account}")
                return False
                
            account_link.click()
            time.sleep(3)  # Wait for details to load
            return True
            
        except Exception as e:
            logger.error(f"Error clicking account number {account_number}: {str(e)}")
            return False
            
    def extract_property_details(self):
        """Extract additional property details from the page"""
        try:
            # Get current page URL
            current_url = self.driver.current_url
            
            # Extract property address
            property_address = self.driver.find_element(
                By.CSS_SELECTOR, 
                "#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(1) > h3:nth-child(3)"
            ).text.strip()
            
            # Extract total amount due
            amount_due = self.driver.find_element(
                By.CSS_SELECTOR,
                "#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(1) > h3:nth-child(8)"
            ).text.strip()
            
            # Extract total amount due
            amount_due_text = self.driver.find_element(
                By.CSS_SELECTOR,
                "#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(1) > h3:nth-child(8)"
            ).text.strip()
            
            # Clean the amount due text to get just the value
            amount_due = self._clean_currency_value(amount_due_text)
            
            # Skip if amount due is $0.00 to save time
            if amount_due == "$0.00":
                logger.info("Skipping record with $0.00 amount due")
                return {"skipped": True, "reason": "zero_amount_due", "Page_URL": current_url}
                
            # Extract other values and clean them
            gross_value_text = self._extract_value("#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(2) > h3:nth-child(2)")
            land_value_text = self._extract_value("#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(2) > h3:nth-child(3)")
            improvement_value_text = self._extract_value("#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(2) > h3:nth-child(4)")
            capped_value_text = self._extract_value("#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(2) > h3:nth-child(5)")
            agricultural_value_text = self._extract_value("#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(2) > h3:nth-child(6)")
            exemptions_text = self._extract_value("#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(2) > h3:nth-child(7)")
            
            # Clean all the values
            gross_value = self._clean_currency_value(gross_value_text)
            land_value = self._clean_currency_value(land_value_text)
            improvement_value = self._clean_currency_value(improvement_value_text)
            capped_value = self._clean_currency_value(capped_value_text)
            agricultural_value = self._clean_currency_value(agricultural_value_text)
            exemptions = self._clean_exemption_value(exemptions_text)
            
            # Calculate total taxable value
            total_taxable = self._calculate_total_taxable(gross_value, capped_value, exemptions)
            
            return {
                "Property_Address": property_address,
                "Total_Amount_Due": amount_due,
                "Gross_Value": gross_value,
                "Land_Value": land_value,
                "Improvement_Value": improvement_value,  
                "Capped_Value": capped_value,
                "Agricultural_Value": agricultural_value,
                "Exemptions": exemptions,
                "Total_Taxable": total_taxable,
                "Page_URL": current_url
            }
            
            logger.info("Chrome driver created successfully for additional scraper")
            return True
            
        except Exception as e:
            logger.error(f"Failed to setup Chrome driver: {str(e)}")
            return False
            
    def load_records(self):
        """Load records from the CSV file"""
        try:
            if not os.path.exists(self.input_file):
                logger.error(f"Input file {self.input_file} not found")
                return False
                
            self.records_df = pd.read_csv(self.input_file)
            logger.info(f"Loaded {len(self.records_df)} records from {self.input_file}")
            return True
        except Exception as e:
            logger.error(f"Error loading records: {str(e)}")
            return False
            
    def navigate_to_search_page(self):
        """Navigate to the Montgomery County search page"""
        return BrowserManager.navigate_to_search_page(self.driver, "Additional")
        
    def search_by_account_number(self, account_number):
        """Search for a record by account number"""
        try:
            # Clean the account number - remove leading quote
            clean_account = str(account_number).strip().lstrip("'")
            logger.debug(f"Searching for account number: '{clean_account}'")
            
            # First click the button to enable search by account number
            account_search_button = WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#content > table > tbody > tr:nth-child(1) > td > div:nth-child(3) > table > tbody > tr > td > center > form > div > label:nth-child(6)"))
            )
            account_search_button.click()
            time.sleep(1)
            
            # Enter account number in search field
            search_field = WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#criteria"))
            )
            search_field.clear()
            search_field.send_keys(clean_account)
            time.sleep(1)
            
            # Click search button - try multiple selectors for robustness
            search_button = None
            button_selectors = [
                "#content > table > tbody > tr:nth-child(1) > td > div:nth-child(3) > table > tbody > tr > td > center > form > table > tbody > tr:nth-child(3) > td:nth-child(2) > h3 > input[type=submit]",
                "input[type=submit]",
                "input[value*='Search']",
                "button[type=submit]"
            ]
            
            for selector in button_selectors:
                try:
                    search_button = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                    break
                except:
                    continue
                    
            if not search_button:
                logger.error("Could not find search button with any selector")
                return False
                
            search_button.click()
            time.sleep(3)  # Wait for results
            
            logger.info(f"Successfully searched for account number: {clean_account}")
            return True
            
        except Exception as e:
            logger.error(f"Error searching by account number {account_number}: {str(e)}")
            return False
            
    def click_account_number(self, account_number):
        """Click on the account number link"""
        try:
            # Clean the account number - remove leading quote and whitespace
            clean_account = str(account_number).strip().lstrip("'")
            logger.debug(f"Original account number: '{account_number}' -> Cleaned: '{clean_account}'")
            
            # Try multiple selector strategies
            selectors = [
                f'//a[contains(text(), "{clean_account}")]',
                f'//a[text()="{clean_account}"]',
                f'//a[contains(@href, "{clean_account}")]'
            ]
            
            account_link = None
            for i, xpath_expr in enumerate(selectors):
                try:
                    logger.debug(f"Trying selector {i+1}: {xpath_expr}")
                    account_link = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, xpath_expr))
                    )
                    logger.debug(f"Found account link using selector {i+1}")
                    break
                except Exception as selector_error:
                    logger.debug(f"Selector {i+1} failed: {str(selector_error)}")
                    continue
            
            if not account_link:
                logger.error(f"Could not find account number link for: {clean_account}")
                return False
                
            account_link.click()
            time.sleep(3)  # Wait for details to load
            return True
            
        except Exception as e:
            logger.error(f"Error clicking account number {account_number}: {str(e)}")
            return False
            
    def extract_property_details(self):
        """Extract additional property details from the page"""
        try:
            # Get current page URL
            current_url = self.driver.current_url
            
            # Extract property address
            property_address = self.driver.find_element(
                By.CSS_SELECTOR, 
                "#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(1) > h3:nth-child(3)"
            ).text.strip()
            
            # Extract total amount due
            amount_due = self.driver.find_element(
                By.CSS_SELECTOR,
                "#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(1) > h3:nth-child(8)"
            ).text.strip()
            
            # If amount due is $0.00, mark all values as "Paid"
            if amount_due == "$0.00":
                return {
                    "Property_Address": property_address,
                    "Total_Amount_Due": amount_due,
                    "Gross_Value": "Paid",
                    "Land_Value": "Paid",
                    "Improvement_Value": "Paid",
                    "Capped_Value": "Paid",
                    "Agricultural_Value": "Paid",
                    "Exemptions": "Paid",
                    "Total_Taxable": "Paid",
                    "Page_URL": current_url
                }
                
            # Extract other values
            gross_value = self._extract_value("#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(2) > h3:nth-child(2)")
            land_value = self._extract_value("#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(2) > h3:nth-child(3)")
            improvement_value = self._extract_value("#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(2) > h3:nth-child(4)")
            capped_value = self._extract_value("#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(2) > h3:nth-child(5)")
            agricultural_value = self._extract_value("#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(2) > h3:nth-child(6)")
            exemptions = self._extract_value("#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(2) > h3:nth-child(7)")
            
            # Convert "None" exemptions to "$0"
            if exemptions == "None":
                exemptions = "$0"
                
            # Calculate total taxable value
            total_taxable = self._calculate_total_taxable(gross_value, capped_value, exemptions)
            
            return {
                "Property_Address": property_address,
                "Total_Amount_Due": amount_due,
                "Gross_Value": gross_value,
                "Land_Value": land_value,
                "Improvement_Value": improvement_value,
                "Capped_Value": capped_value,
                "Agricultural_Value": agricultural_value,
                "Exemptions": exemptions,
                "Total_Taxable": total_taxable,
                "Page_URL": current_url
            }
            
        except Exception as e:
            logger.error(f"Error extracting property details: {str(e)}")
            return None
            
    def _extract_value(self, selector):
        """Helper method to extract value using selector"""
        try:
            return self.driver.find_element(By.CSS_SELECTOR, selector).text.strip()
        except NoSuchElementException:
            return "$0"
            
    def _calculate_total_taxable(self, gross_value, capped_value, exemptions):
        """Calculate total taxable value"""
        try:
            # Convert string values to float for gross and capped values
            gross_val = float(gross_value.replace("$", "").replace(",", ""))
            capped_val = float(capped_value.replace("$", "").replace(",", ""))
            
            # Handle exemptions - could be a dollar amount or an exemption code
            exempt_val = 0.0
            if exemptions and exemptions != "$0":
                # Check if it's a dollar amount (starts with $ and contains digits)
                exemption_clean = exemptions.replace("$", "").replace(",", "").strip()
                if exemption_clean.replace(".", "").isdigit():
                    exempt_val = float(exemption_clean)
                else:
                    # It's an exemption code like NCAP, CAP, etc.
                    # For tax calculation purposes, exemption codes typically mean 
                    # the property has some special treatment that reduces taxable value
                    if exemptions.upper() in ["NCAP", "CAP", "NONE"]:
                        # These codes typically indicate a capping or special valuation
                        # that already affects the taxable calculation
                        exempt_val = 0.0
                    else:
                        # For other exemption codes, assume no direct dollar exemption
                        exempt_val = 0.0
            
            # Determine base value: if one value is 0, use the other
            if gross_val == 0:
                base_value = capped_val
            elif capped_val == 0:
                base_value = gross_val
            else:
                # Use the lower of gross or capped value
                base_value = min(gross_val, capped_val)
                
            # Calculate total taxable
            total = max(0, base_value - exempt_val)  # Ensure non-negative
            
            # Format as currency string
            return f"${total:,.2f}"
            
        except (ValueError, AttributeError) as e:
            logger.error(f"Error calculating total taxable: {str(e)}")
            return "$0.00"
            
    def update_record(self, index, details):
        """Update record in the dataframe with new details"""
        try:
            # Update property address if new one is more detailed
            current_address = str(self.records_df.at[index, "Property_Address"])
            new_address = details["Property_Address"]
            if len(new_address) > len(current_address):
                self.records_df.at[index, "Property_Address"] = new_address
                
            # Add new columns if they don't exist
            new_columns = [
                "Total_Amount_Due", "Gross_Value", "Land_Value", 
                "Improvement_Value", "Capped_Value", "Agricultural_Value",
                "Exemptions", "Total_Taxable", "Page_URL"
            ]
            
            for col in new_columns:
                if col not in self.records_df.columns:
                    self.records_df[col] = None
                    
            # Update values
            for col in new_columns:
                self.records_df.at[index, col] = details[col]
                
        except Exception as e:
            logger.error(f"Error updating record: {str(e)}")
            
    def process_records_batch(self, records_batch, thread_id):
        """Process a batch of records in a separate thread"""
        local_driver = None
        try:
            # Setup driver for this thread
            local_driver = self.setup_driver_for_thread()
            if not local_driver:
                return
                
            logger.info(f"Thread {thread_id}: Processing {len(records_batch)} records")
            
            for index, record in records_batch:
                try:
                    logger.info(f"Thread {thread_id}: Processing record {index + 1}")
                    
                    # Navigate to search page
                    if not self.navigate_to_search_page_thread(local_driver):
                        continue
                        
                    # Search by account number
                    if not self.search_by_account_number_thread(local_driver, record["Account_Number"]):
                        continue
                        
                    # Click on account number
                    if not self.click_account_number_thread(local_driver, record["Account_Number"]):
                        continue
                        
                    # Extract and queue details
                    details = self.extract_property_details_thread(local_driver)
                    if details:
                        # Check if record was skipped
                        if details.get("skipped"):
                            # Mark as skipped - don't save additional details for zero amount records
                            with self.lock:
                                self.skipped_records.add(f"{record['Account_Number']}_{index}")
                                self._save_checkpoint()
                            
                            logger.info(f"Thread {thread_id}: Skipped record {record['Account_Number']} - {details.get('reason', 'unknown')}")
                        else:
                            # Store results with account number and record index for identification
                            result_data = {
                                'index': index,
                                'account_number': record["Account_Number"],
                                'details': details
                            }
                            self.results_queue.put(result_data)
                            
                            # Also store in data manager for checkpointing
                            with self.lock:
                                record_key = f"{record['Account_Number']}_{index}_additional"
                                self.data_manager.shared_data_store["records"][record_key] = {
                                    **record,  # Original record data
                                    **details  # Additional scraped details
                                }
                                self.completed_records.add(f"{record['Account_Number']}_{index}")
                                self._save_checkpoint()
                            
                            logger.info(f"Thread {thread_id}: Successfully extracted details for {record['Account_Number']}")
                    else:
                        # Mark as failed only for actual failures (not skipped records)
                        with self.lock:
                            self.failed_records.add(f"{record['Account_Number']}_{index}")
                            self._save_checkpoint()
                        
                        logger.error(f"Thread {thread_id}: Failed to extract details for {record['Account_Number']}")
                    
                    time.sleep(2)  # Brief pause between records
                    
                except Exception as e:
                    logger.error(f"Thread {thread_id}: Error processing record {index}: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"Thread {thread_id}: Error in batch processing: {str(e)}")
        finally:
            if local_driver:
                local_driver.quit()
                
    def setup_driver_for_thread(self):
        """Setup Chrome driver for a specific thread"""
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.service import Service
            from selenium.webdriver.chrome.options import Options
            
            # Chrome options (without proxy for simplicity)
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--disable-web-security")
            chrome_options.add_argument("--disable-features=VizDisplayCompositor")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
            
            # Disable images and CSS for faster loading
            prefs = {
                "profile.managed_default_content_settings.images": 2,
                "profile.managed_default_content_settings.stylesheets": 2,
                "profile.managed_default_content_settings.plugins": 2,
                "profile.managed_default_content_settings.popups": 2,
                "profile.managed_default_content_settings.geolocation": 2,
                "profile.managed_default_content_settings.notifications": 2,
                "profile.managed_default_content_settings.media_stream": 2,
            }
            chrome_options.add_experimental_option("prefs", prefs)
            
            # Set up Chrome service
            service = Service()
            
            # Create driver
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.set_page_load_timeout(60)
            driver.implicitly_wait(10)
            
            return driver
            
        except Exception as e:
            logger.error(f"Failed to setup Chrome driver for thread: {str(e)}")
            return None

    def navigate_to_search_page_thread(self, driver):
        """Navigate to the Montgomery County search page (thread version)"""
        return BrowserManager.navigate_to_search_page(driver, "Additional")
        
    def search_by_account_number_thread(self, driver, account_number):
        """Search for a record by account number (thread version)"""
        try:
            # Clean the account number - remove leading quote
            clean_account = str(account_number).strip().lstrip("'")
            logger.debug(f"Searching for account number: '{clean_account}'")
            
            # First click the button to enable search by account number
            account_search_button = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#content > table > tbody > tr:nth-child(1) > td > div:nth-child(3) > table > tbody > tr > td > center > form > div > label:nth-child(6)"))
            )
            account_search_button.click()
            time.sleep(1)
            
            # Enter account number in search field
            search_field = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#criteria"))
            )
            search_field.clear()
            search_field.send_keys(clean_account)
            time.sleep(1)
            
            # Click search button
            search_button = None
            button_selectors = [
                "#content > table > tbody > tr:nth-child(1) > td > div:nth-child(3) > table > tbody > tr > td > center > form > table > tbody > tr:nth-child(3) > td:nth-child(2) > h3 > input[type=submit]",
                "input[type=submit]",
                "input[value*='Search']",
                "button[type=submit]"
            ]
            
            for selector in button_selectors:
                try:
                    search_button = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                    break
                except:
                    continue
                    
            if not search_button:
                logger.error("Could not find search button with any selector")
                return False
                
            search_button.click()
            time.sleep(3)  # Wait for results
            
            return True
            
        except Exception as e:
            logger.error(f"Error searching by account number {account_number}: {str(e)}")
            return False
            
    def click_account_number_thread(self, driver, account_number):
        """Click on the account number link (thread version)"""
        try:
            # Clean the account number - remove leading quote and whitespace
            clean_account = str(account_number).strip().lstrip("'")
            logger.debug(f"Original account number: '{account_number}' -> Cleaned: '{clean_account}'")
            
            # Try multiple selector strategies
            selectors = [
                f'//a[contains(text(), "{clean_account}")]',
                f'//a[text()="{clean_account}"]',
                f'//a[contains(@href, "{clean_account}")]'
            ]
            
            account_link = None
            for i, xpath_expr in enumerate(selectors):
                try:
                    logger.debug(f"Trying selector {i+1}: {xpath_expr}")
                    account_link = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, xpath_expr))
                    )
                    logger.debug(f"Found account link using selector {i+1}")
                    break
                except Exception as selector_error:
                    logger.debug(f"Selector {i+1} failed: {str(selector_error)}")
                    continue
            
            if not account_link:
                logger.error(f"Could not find account number link for: {clean_account}")
                return False
                
            account_link.click()
            time.sleep(3)  # Wait for details to load
            return True
            
        except Exception as e:
            logger.error(f"Error clicking account number {account_number}: {str(e)}")
            return False
            
    def extract_property_details_thread(self, driver):
        """Extract additional property details from the page (thread version)"""
        try:
            # Get current page URL
            current_url = driver.current_url
            
            # Extract property address
            property_address = driver.find_element(
                By.CSS_SELECTOR, 
                "#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(1) > h3:nth-child(3)"
            ).text.strip()
            
            # Extract total amount due
            amount_due_text = driver.find_element(
                By.CSS_SELECTOR,
                "#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(1) > h3:nth-child(8)"
            ).text.strip()
            
            # Clean the amount due text to get just the value
            amount_due = self._clean_currency_value(amount_due_text)
            
            # Skip if amount due is $0.00 to save time
            if amount_due == "$0.00":
                logger.info("Skipping record with $0.00 amount due")
                return {"skipped": True, "reason": "zero_amount_due"}
                
            # Extract other values and clean them
            gross_value_text = self._extract_value_thread(driver, "#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(2) > h3:nth-child(2)")
            land_value_text = self._extract_value_thread(driver, "#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(2) > h3:nth-child(3)")
            improvement_value_text = self._extract_value_thread(driver, "#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(2) > h3:nth-child(4)")
            capped_value_text = self._extract_value_thread(driver, "#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(2) > h3:nth-child(5)")
            agricultural_value_text = self._extract_value_thread(driver, "#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(2) > h3:nth-child(6)")
            exemptions_text = self._extract_value_thread(driver, "#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(2) > h3:nth-child(7)")
            
            # Clean all the values
            gross_value = self._clean_currency_value(gross_value_text)
            land_value = self._clean_currency_value(land_value_text)
            improvement_value = self._clean_currency_value(improvement_value_text)
            capped_value = self._clean_currency_value(capped_value_text)
            agricultural_value = self._clean_currency_value(agricultural_value_text)
            exemptions = self._clean_exemption_value(exemptions_text)
            
            # Calculate total taxable value
            total_taxable = self._calculate_total_taxable(gross_value, capped_value, exemptions)
            
            return {
                "Property_Address": property_address,
                "Total_Amount_Due": amount_due,
                "Gross_Value": gross_value,
                "Land_Value": land_value,
                "Improvement_Value": improvement_value,
                "Capped_Value": capped_value,
                "Agricultural_Value": agricultural_value,
                "Exemptions": exemptions,
                "Total_Taxable": total_taxable,
                "Page_URL": current_url
            }
            
        except Exception as e:
            logger.error(f"Error extracting property details: {str(e)}")
            return None
            
    def _extract_value_thread(self, driver, selector):
        """Helper method to extract value using selector (thread version)"""
        try:
            return driver.find_element(By.CSS_SELECTOR, selector).text.strip()
        except NoSuchElementException:
            return "$0"
    
    def _clean_currency_value(self, text):
        """Clean currency values by removing labels and keeping only the amount"""
        if not text:
            return "$0"
        
        # Remove common prefixes and get the amount
        text = text.replace("Total Amount Due: ", "")
        text = text.replace("Gross Value:", "").replace("Gross Value: ", "")
        text = text.replace("Land Value:", "").replace("Land Value: ", "")
        text = text.replace("Improvement Value:", "").replace("Improvement Value: ", "")
        text = text.replace("Capped Value:", "").replace("Capped Value: ", "")
        text = text.replace("Agricultural Value:", "").replace("Agricultural Value: ", "")
        
        # Clean up extra spaces
        text = text.strip()
        
        # If it doesn't start with $, add it for consistency
        if text and not text.startswith("$"):
            if text.replace(",", "").replace(".", "").isdigit():
                text = "$" + text
        
        return text if text else "$0"
    
    def _clean_exemption_value(self, text):
        """Clean exemption values"""
        if not text:
            return "$0"
        
        # Remove the label
        text = text.replace("Exemptions:", "").replace("Exemptions: ", "")
        text = text.strip()
        
        # Convert "None" to "$0"
        if text.lower() == "none":
            return "$0"
        
        # If it doesn't start with $, add it for consistency
        if text and not text.startswith("$"):
            if text.replace(",", "").replace(".", "").isdigit():
                text = "$" + text
        
        return text if text else "$0"

    def save_results(self):
        """Save updated records back to CSV"""
        try:
            output_path = self.input_file.replace(".csv", "_updated.csv")
            self.records_df.to_csv(output_path, index=False)
            logger.info(f"Saved updated records to {output_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving results: {str(e)}")
            return False
            
    def run(self, use_threading=True):
        """Main method to run the scraper"""
        try:
            # Load records
            if not self.load_records():
                return False
                
            if use_threading:
                return self.run_with_threading()
            else:
                return self.run_single_threaded()
                
        except Exception as e:
            logger.error(f"Error running scraper: {str(e)}")
            return False
            
    def run_with_threading(self):
        """Run scraper with multithreading"""
        try:
            # Get remaining records from the queue (which already filters out processed records)
            remaining_records = []
            temp_queue = queue.Queue()
            
            # Extract all remaining records from the queue
            while not self.record_queue.empty():
                try:
                    record_item = self.record_queue.get_nowait()
                    remaining_records.append(record_item)
                    temp_queue.put(record_item)  # Keep a copy for the queue
                except queue.Empty:
                    break
            
            # Restore the queue
            self.record_queue = temp_queue
            
            if not remaining_records:
                logger.info("No remaining records to process - all records already completed/skipped")
                return True
            
            # Split remaining records into batches for threading
            batch_size = max(1, len(remaining_records) // self.num_threads)
            batches = [remaining_records[i:i + batch_size] for i in range(0, len(remaining_records), batch_size)]
            
            # Remove empty batches
            batches = [batch for batch in batches if batch]
            
            logger.info(f"Processing {len(remaining_records)} remaining records using {len(batches)} threads")
            
            # Adjust thread count if we have fewer batches than requested threads
            actual_threads = min(self.num_threads, len(batches))
            
            # Process batches in parallel
            with ThreadPoolExecutor(max_workers=actual_threads) as executor:
                futures = []
                for i, batch in enumerate(batches):
                    future = executor.submit(self.process_records_batch, batch, i + 1)
                    futures.append(future)
                
                # Wait for all threads to complete
                for future in futures:
                    future.result()
                    
            # Process results from queue
            logger.info("Processing results from all threads")
            processed_results = 0
            while not self.results_queue.empty():
                try:
                    result_data = self.results_queue.get()
                    processed_results += 1
                    logger.debug(f"Processed result {processed_results} for account {result_data['account_number']}")
                except queue.Empty:
                    break
                    
            logger.info(f"Processed {processed_results} results from queue")
            
            # Export results using data manager
            logger.info("Exporting results to CSV...")
            self.data_manager.export_to_csv()
            
            # Also save final checkpoint
            self._save_checkpoint()
            
            # Final summary
            logger.info(f"Processing completed - Completed: {len(self.completed_records)}, Failed: {len(self.failed_records)}, Skipped: {len(self.skipped_records)}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error running scraper with threading: {str(e)}")
            return False
            
    def run_single_threaded(self):
        """Run scraper in single-threaded mode (original behavior)"""
        try:
            # Setup driver
            if not self.setup_driver():
                return False
                
            # Process each record
            for index, record in self.records_df.iterrows():
                try:
                    logger.info(f"Processing record {index + 1}/{len(self.records_df)}")
                    
                    # Navigate to search page
                    if not self.navigate_to_search_page():
                        continue
                        
                    # Search by account number
                    if not self.search_by_account_number(record["Account_Number"]):
                        continue
                        
                    # Click on account number
                    if not self.click_account_number(record["Account_Number"]):
                        continue
                        
                    # Extract and update details
                    details = self.extract_property_details()
                    if details:
                        self.update_record(index, details)
                        
                    time.sleep(2)  # Brief pause between records
                    
                except Exception as e:
                    logger.error(f"Error processing record {index}: {str(e)}")
                    continue
                    
            # Save results
            return self.save_results()
            
        except Exception as e:
            logger.error(f"Error running single-threaded scraper: {str(e)}")
            return False
            
        finally:
            # Clean up
            if self.driver:
                self.driver.quit()
                
if __name__ == "__main__":
    import argparse
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Montgomery County Additional Property Scraper')
    parser.add_argument('--input-file', default='output/Montgomery2.csv', 
                        help='Input CSV file path (default: output/Montgomery2.csv)')
    parser.add_argument('--threads', type=int, default=3, 
                        help='Number of threads to use (default: 3)')
    parser.add_argument('--no-threading', action='store_true', 
                        help='Disable multithreading and run in single-threaded mode')
    
    args = parser.parse_args()
    
    # Run scraper
    scraper = MontgomeryAdditionalScraper(input_file=args.input_file, num_threads=args.threads)
    use_threading = not args.no_threading
    
    logger.info(f"Starting scraper with {'multithreading' if use_threading else 'single-threading'}")
    if use_threading:
        logger.info(f"Using {args.threads} threads")
        
    success = scraper.run(use_threading=use_threading)
    
    if success:
        logger.info("Scraper completed successfully!")
    else:
        logger.error("Scraper failed!")
        sys.exit(1)
