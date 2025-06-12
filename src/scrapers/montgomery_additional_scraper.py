import logging
import pandas as pd
import time
from pathlib import Path
import os
import sys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

# Import our modules
from src.scrapers.multithreader.browser_manager import BrowserManager
from src.utils.email_notifier import send_error_notification

# Get logger
logger = logging.getLogger(__name__)

class MontgomeryAdditionalScraper:
    def __init__(self, input_file="output/Montgomery2.csv"):
        self.input_file = input_file
        self.records_df = None
        self.driver = None
        self.base_url = "https://actweb.acttax.com/act_webdev/montgomery/index.jsp"
        
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
        
    def search_by_owner(self, owner_name):
        """Search for a record by owner name"""
        try:
            # Enter owner name in search field
            search_field = WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#criteria"))
            )
            search_field.clear()
            search_field.send_keys(owner_name)
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
            
            logger.info(f"Successfully searched for owner: {owner_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error searching by owner {owner_name}: {str(e)}")
            return False
            
    def click_account_number(self, account_number):
        """Click on the account number link"""
        try:
            # Wait for and find the account number link
            account_link = WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable((By.XPATH, f"//a[contains(text(), '{account_number}')]"))
            )
            account_link.click()
            time.sleep(3)  # Wait for details to load
            return True
        except Exception as e:
            logger.error(f"Error clicking account number {account_number}: {str(e)}")
            return False
            
    def extract_property_details(self):
        """Extract additional property details from the page"""
        try:
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
                    "Total_Taxable": "Paid"
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
                "Total_Taxable": total_taxable
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
            # Convert string values to float
            gross_val = float(gross_value.replace("$", "").replace(",", ""))
            capped_val = float(capped_value.replace("$", "").replace(",", ""))
            exempt_val = float(exemptions.replace("$", "").replace(",", ""))
            
            # If one value is 0, use the other
            if gross_val == 0:
                base_value = capped_val
            elif capped_val == 0:
                base_value = gross_val
            else:
                base_value = min(gross_val, capped_val)
                
            # Calculate total taxable
            total = base_value - exempt_val
            
            # Format as currency string
            return f"${total:,.2f}"
            
        except (ValueError, AttributeError):
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
                "Exemptions", "Total_Taxable"
            ]
            
            for col in new_columns:
                if col not in self.records_df.columns:
                    self.records_df[col] = None
                    
            # Update values
            for col in new_columns:
                self.records_df.at[index, col] = details[col]
                
        except Exception as e:
            logger.error(f"Error updating record: {str(e)}")
            
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
            
    def run(self):
        """Main method to run the scraper"""
        try:
            # Load records
            if not self.load_records():
                return False
                
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
                        
                    # Search by owner name
                    if not self.search_by_owner(record["Owner_Name"]):
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
            logger.error(f"Error running scraper: {str(e)}")
            return False
            
        finally:
            # Clean up
            if self.driver:
                self.driver.quit()
                
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run scraper
    scraper = MontgomeryAdditionalScraper()
    scraper.run()
