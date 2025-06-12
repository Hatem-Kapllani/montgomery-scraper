import logging
import re
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

logger = logging.getLogger(__name__)

class WebScraperOperations:
    """Handles web scraping operations and data extraction"""
    
    @staticmethod
    def extract_search_results(driver, worker_id):
        """Extract data from Montgomery search results with robust element detection"""
        records = []
        
        try:
            # Try multiple strategies to find the results table
            results_table = None
            
            # Strategy 1: Try the specific selector first based on Montgomery site structure
            try:
                results_table = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#content > table > tbody > tr:nth-child(1) > td > table:nth-child(9) > tbody > tr > td > table > tbody"))
                )
                logger.info(f"Worker {worker_id}: Found results table using specific selector")
            except:
                logger.warning(f"Worker {worker_id}: Specific selector failed, trying alternative methods...")
            
            # Strategy 2: Look for any table with multiple rows (likely results)
            if not results_table:
                try:
                    tables = driver.find_elements(By.CSS_SELECTOR, "table tbody")
                    for table in tables:
                        rows = table.find_elements(By.CSS_SELECTOR, "tr")
                        if len(rows) > 1:  # More than just header
                            # Check if this looks like a results table
                            first_row_cells = rows[0].find_elements(By.TAG_NAME, "td")
                            if len(first_row_cells) >= 4:  # Should have at least 4 columns
                                results_table = table
                                logger.info(f"Worker {worker_id}: Found results table using alternative selector (table with {len(rows)} rows)")
                                break
                except Exception as e:
                    logger.warning(f"Worker {worker_id}: Alternative table search failed: {str(e)}")
            
            # Strategy 3: Check for "no results" message
            if not results_table:
                try:
                    page_text = driver.page_source.lower()
                    if "no records found" in page_text or "no results" in page_text or "no matches" in page_text:
                        logger.info(f"Worker {worker_id}: No results found for this search pattern")
                        return []
                    else:
                        logger.warning(f"Worker {worker_id}: Could not find results table or 'no results' message")
                        return []
                except Exception as e:
                    logger.error(f"Worker {worker_id}: Error checking for no results message: {str(e)}")
                    return []
            
            if not results_table:
                logger.error(f"Worker {worker_id}: Could not locate results table with any strategy")
                return []
            
            # Get all result rows
            rows = results_table.find_elements(By.CSS_SELECTOR, "tr")
            
            for i, row in enumerate(rows):
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) < 4:
                        logger.debug(f"Worker {worker_id}: Skipping row {i} - insufficient columns ({len(cells)})")
                        continue
                    
                    # Check if this is a header row by looking for header indicators
                    first_cell_text = cells[0].text.strip().lower()
                    if first_cell_text in ['account', 'account number', 'account #', 'owner', 'property']:
                        logger.debug(f"Worker {worker_id}: Skipping row {i} - appears to be header row ('{first_cell_text}')")
                        continue
                    
                    # Try to extract account number
                    try:
                        account_number_element = cells[0].find_element(By.CSS_SELECTOR, "h3 > a")
                        account_number = account_number_element.text.strip()
                        
                        if not account_number:
                            logger.debug(f"Worker {worker_id}: Skipping row {i} - empty account number")
                            continue
                            
                    except Exception:
                        # If we can't find the account number link, try plain text
                        try:
                            account_number_element = cells[0].find_element(By.CSS_SELECTOR, "h3")
                            account_number = account_number_element.text.strip()
                            
                            if not account_number or account_number.lower() in ['account', 'account number', 'account #']:
                                logger.debug(f"Worker {worker_id}: Skipping row {i} - appears to be header or empty")
                                continue
                        except Exception:
                            logger.debug(f"Worker {worker_id}: Skipping row {i} - cannot extract account number")
                            continue
                    
                    # Extract Owner Name and Mailing Address (2nd column, combined)
                    try:
                        owner_mailing_element = cells[1].find_element(By.CSS_SELECTOR, "h3")
                        owner_mailing_text = owner_mailing_element.text.strip()
                    except Exception:
                        logger.debug(f"Worker {worker_id}: Row {i} - cannot extract owner info, skipping")
                        continue
                    
                    # Separate Owner Name and Mailing Address
                    owner_name, mailing_address = WebScraperOperations.separate_owner_and_address(owner_mailing_text)
                    
                    # Extract Property Address (3rd column)
                    try:
                        property_address_element = cells[2].find_element(By.CSS_SELECTOR, "h3")
                        property_address = property_address_element.text.strip()
                        if not property_address:
                            property_address = "UNKNOWN"
                    except Exception:
                        property_address = "UNKNOWN"
                    
                    # Extract Legal Description (4th column)
                    try:
                        legal_description_element = cells[3].find_element(By.CSS_SELECTOR, "h3")
                        legal_description = legal_description_element.text.strip()
                    except Exception:
                        legal_description = "UNKNOWN"
                    
                    # Create record
                    record = {
                        "Account_Number": account_number,
                        "Owner_Name": owner_name,
                        "Mailing_Address": mailing_address,
                        "Property_Address": property_address,
                        "Legal_Description": legal_description
                    }
                    
                    records.append(record)
                    logger.info(f"Worker {worker_id}: Extracted record for account {account_number}")
                    
                except Exception as e:
                    logger.warning(f"Worker {worker_id}: Error extracting row {i}: {str(e)}")
                    continue
            
            logger.info(f"Worker {worker_id}: Extracted {len(records)} records from search results")
            return records
            
        except Exception as e:
            logger.error(f"Worker {worker_id}: Error extracting search results: {str(e)}")
            return []
    
    @staticmethod
    def separate_owner_and_address(combined_text):
        """Separate owner name and mailing address from combined text"""
        if not combined_text:
            return "UNKNOWN", "UNKNOWN"
        
        # Clean up the text
        text = combined_text.strip()
        
        # Look for the first occurrence of a number (start of address)
        # Use regex to find the first digit
        match = re.search(r'\d', text)
        
        if match:
            # Split at the first digit
            split_index = match.start()
            
            # Find the start of the word containing the first digit
            # Go backwards to find the start of the word
            word_start = split_index
            while word_start > 0 and text[word_start - 1] not in [' ', '\n', '\t']:
                word_start -= 1
            
            owner_name = text[:word_start].strip()
            mailing_address = text[word_start:].strip()
            
            # Clean up any extra whitespace
            owner_name = ' '.join(owner_name.split())
            mailing_address = ' '.join(mailing_address.split())
            
            # If owner name is empty, use "UNKNOWN"
            if not owner_name:
                owner_name = "UNKNOWN"
            
            # If mailing address is empty, use "UNKNOWN"
            if not mailing_address:
                mailing_address = "UNKNOWN"
                
            return owner_name, mailing_address
        else:
            # No number found, treat entire text as owner name
            return text, "UNKNOWN" 