import threading
import queue
import time
import logging
from pathlib import Path
import json
import os
import traceback
import sys
from datetime import datetime
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from local_proxy import LocalProxyRunner
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementNotInteractableException
from src.utils import checkpoint_utils
from src.utils.email_notifier import send_error_notification, send_completion_notification
import pandas as pd
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GalvestonTaxScraper:
    def __init__(self, num_threads=3):
        self.num_threads = num_threads
        self.search_patterns_queue = queue.Queue()
        self.completed_patterns = set()
        self.lock = threading.Lock()
        self.proxy_runners = []
        self.scrapers = []
        self.checkpoint_file = None
        self.shared_data_store = {
            "headers": ["Account Number", "Owner Name", "Mailing Address", "Property Address", "Legal Description"],
            "records": {},
            "processed_pages": set(),
            "last_saved_timestamp": None,
            "total_pages": 0,
            "total_records": 0,
            "search_patterns_completed": set(),
            "current_search_pattern": None,
        }
        
        # Create checkpoint directory if it doesn't exist
        checkpoint_dir = Path("checkpoints")
        checkpoint_dir.mkdir(exist_ok=True)
        
        # Load existing checkpoints to get completed patterns
        self._load_completed_patterns()
        
        # Generate all search patterns
        self._generate_search_patterns()
    
    def _load_completed_patterns(self):
        """Load completed patterns from existing checkpoints"""
        try:
            # Use the checkpoint_utils to load the latest checkpoint
            loaded_data_store, checkpoint_path = checkpoint_utils.load_latest_checkpoint()
            
            if checkpoint_path:
                self.checkpoint_file = checkpoint_path
                
                # Update our shared data store with the loaded data
                self.shared_data_store.update(loaded_data_store)
                
                # Update completed patterns from the checkpoint
                if "search_patterns_completed" in loaded_data_store:
                    self.completed_patterns = loaded_data_store["search_patterns_completed"]
                    logger.info(f"Loaded {len(self.completed_patterns)} completed patterns from checkpoint")
                    
                logger.info(f"Loaded {len(self.shared_data_store['records'])} records from checkpoint")
        except Exception as e:
            logger.error(f"Error loading checkpoint: {str(e)}")
    
    def _save_checkpoint(self) -> None:
        """Save current progress to prevent data loss using a shared checkpoint file"""
        try:
            # Use the checkpoint_utils to save the checkpoint
            self.checkpoint_file = checkpoint_utils.save_checkpoint(self.shared_data_store)
        except Exception as e:
            logger.error(f"Error saving checkpoint: {str(e)}")
    
    def _generate_search_patterns(self):
        """Generate all search patterns (aaa%, aab%, etc.) - 3 letter patterns for Galveston"""
        all_patterns = []
        for first_letter in "abcdefghijklmnopqrstuvwxyz":
            for second_letter in "abcdefghijklmnopqrstuvwxyz":
                for third_letter in "abcdefghijklmnopqrstuvwxyz":
                    pattern = f"{first_letter}{second_letter}{third_letter}%"
                    all_patterns.append(pattern)
        
        # Sort all patterns to ensure consistent ordering
        all_patterns = sorted(all_patterns)
        
        # Filter out already completed patterns
        remaining_patterns = [p for p in all_patterns if p not in self.completed_patterns]
        
        # Add remaining patterns to queue
        for pattern in remaining_patterns:
            self.search_patterns_queue.put(pattern)
            
        logger.info(f"Generated {len(all_patterns)} total patterns")
        logger.info(f"{len(self.completed_patterns)} patterns already completed")
        logger.info(f"{self.search_patterns_queue.qsize()} patterns remaining")
    
    def _verify_proxy_ports_available(self):
        """Verify that proxy ports are available before starting workers"""
        import socket
        
        available_ports = []
        for i in range(self.num_threads):
            port = 8081 + i
            try:
                # Test if port is available
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1)
                result = sock.connect_ex(('127.0.0.1', port))
                sock.close()
                
                if result != 0:  # Port is available
                    available_ports.append(port)
                    logger.info(f"Port {port} is available for worker {i}")
                else:
                    logger.warning(f"Port {port} is already in use")
            except Exception as e:
                logger.error(f"Error checking port {port}: {str(e)}")
        
        if len(available_ports) < self.num_threads:
            logger.warning(f"Only {len(available_ports)} ports available, reducing threads from {self.num_threads}")
            self.num_threads = len(available_ports)
        
        return available_ports[:self.num_threads]
    
    def _verify_proxy_running(self, proxy_runner, worker_id, proxy_port, max_retries=3):
        """Verify that a proxy runner is actually working by testing connectivity"""
        import socket
        
        for attempt in range(max_retries):
            try:
                # Test if we can connect to the proxy port
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2)
                result = sock.connect_ex(('127.0.0.1', proxy_port))
                sock.close()
                
                if result == 0:  # Connection successful
                    logger.info(f"Worker {worker_id}: Proxy on port {proxy_port} verified running (attempt {attempt + 1})")
                    return True
                else:
                    logger.warning(f"Worker {worker_id}: Cannot connect to proxy on port {proxy_port} (attempt {attempt + 1})")
                    time.sleep(2)
            except Exception as e:
                logger.warning(f"Worker {worker_id}: Error verifying proxy on port {proxy_port} (attempt {attempt + 1}): {str(e)}")
                time.sleep(2)
        
        logger.error(f"Worker {worker_id}: Failed to verify proxy on port {proxy_port} after {max_retries} attempts")
        return False
    
    def _worker(self, worker_id):
        """Worker thread function that processes search patterns"""
        logger.info(f"Worker {worker_id} started")
        
        # Set unique proxy port for this worker (each worker gets its own port)
        proxy_port = 8081 + worker_id
        
        # Create unique proxy runner for this worker
        proxy_runner = None
        driver = None
        
        try:
            # Create and start unique proxy runner for this worker
            logger.info(f"Worker {worker_id}: Initializing unique proxy on port {proxy_port}")
            proxy_runner = LocalProxyRunner(local_port=proxy_port)
            proxy_runner.start()
            
            # Give proxy more time to start up
            logger.info(f"Worker {worker_id}: Waiting for proxy to initialize on port {proxy_port}")
            time.sleep(5)  # Give proxy time to start properly
            
            # Verify proxy is running
            if not self._verify_proxy_running(proxy_runner, worker_id, proxy_port):
                logger.warning(f"Worker {worker_id}: Proxy verification failed on port {proxy_port}, but continuing anyway")
                # Don't fail here - the proxy might be working even if verification fails
            else:
                logger.info(f"Worker {worker_id}: Successfully started and verified unique local proxy on port {proxy_port}")
            
            # Set environment variable for this worker's proxy
            worker_env = os.environ.copy()
            worker_env["PROXY_PORT"] = str(proxy_port)
            worker_env["HTTP_PROXY"] = f"http://127.0.0.1:{proxy_port}"
            worker_env["HTTPS_PROXY"] = f"http://127.0.0.1:{proxy_port}"
            
            # Initialize Chrome driver once for this worker with unique proxy
            try:
                driver = self._setup_driver(proxy_port, worker_id)
                logger.info(f"Worker {worker_id}: Successfully initialized Chrome driver with unique proxy on port {proxy_port}")
                
                # Navigate to search page once at the beginning
                self._navigate_to_search_page(driver, worker_id)
                
            except Exception as e:
                logger.error(f"Worker {worker_id}: Failed to initialize Chrome driver: {str(e)}")
                logger.error(f"Worker {worker_id}: {traceback.format_exc()}")
                # Unable to continue without a driver
                return
            
        except Exception as e:
            error_msg = f"Worker {worker_id}: Failed to start local proxy: {str(e)}"
            logger.error(error_msg)
            
            # Send error notification
            send_error_notification(
                error_message="Worker Proxy Initialization Failed",
                error_details=traceback.format_exc(),
                context={
                    "worker_id": worker_id,
                    "proxy_port": proxy_port,
                    "error": str(e)
                }
            )
            return
            
        # Process patterns - now driver is initialized once outside the loop
        while True:
            try:
                # Get next search pattern from queue
                try:
                    pattern = self.search_patterns_queue.get_nowait()
                except queue.Empty:
                    logger.info(f"Worker {worker_id}: No more patterns to process")
                    break
                
                logger.info(f"Worker {worker_id}: Processing pattern: {pattern}")
                
                # Check if pattern is already completed
                with self.lock:
                    if pattern in self.shared_data_store["search_patterns_completed"]:
                        logger.info(f"Worker {worker_id}: Pattern {pattern} already completed by another worker, skipping")
                        # Put the task back as done
                        self.search_patterns_queue.task_done()
                        continue
                    
                    # Mark as being processed
                    self.shared_data_store["current_search_pattern"] = pattern
                
                # Store current pattern for this worker
                current_pattern = pattern
                
                try:
                    # Scroll to top of page before starting a new search
                    driver.execute_script("window.scrollTo(0, 0);")
                    logger.info(f"Worker {worker_id}: Scrolled to top of page for new search")
                    time.sleep(1)
                    
                    # Clear and enter the new search pattern
                    self._enter_search_pattern(driver, pattern, worker_id)
                    
                    # Perform the search
                    self._click_search_button(driver, worker_id)
                    
                    # Wait for results with timeout handling
                    try:
                        # Wait for search results table to load
                        WebDriverWait(driver, 60).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "body > table:nth-child(2) > tbody > tr > td > table:nth-child(4) > tbody > tr > td > table:nth-child(7) > tbody > tr > td > table > tbody > tr:nth-child(1)"))
                        )
                        logger.info(f"Worker {worker_id}: Search results loaded")
                        
                        # Extract data from the search results
                        results = self._extract_search_results(driver, worker_id)
                        
                        # Mark pattern as completed only after successful extraction
                        with self.lock:
                            self.completed_patterns.add(pattern)
                            self.shared_data_store["search_patterns_completed"].add(pattern)
                            
                            # Add results to shared data store
                            if results:
                                for record in results:
                                    # Use a unique key for each record
                                    key = f"{record.get('Account_Number', 'unknown')}_{pattern}_{len(self.shared_data_store['records'])}"
                                    self.shared_data_store["records"][key] = record
                            
                            # Save checkpoint
                            self._save_checkpoint()
                            
                        logger.info(f"Worker {worker_id}: Pattern {pattern} completed with {len(results) if results else 0} records")
                        
                    except Exception as e:
                        logger.error(f"Worker {worker_id}: Error waiting for search results: {str(e)}")
                        # Still mark pattern as completed to avoid retrying failures
                        with self.lock:
                            self.completed_patterns.add(pattern)
                            self.shared_data_store["search_patterns_completed"].add(pattern)
                            self._save_checkpoint()
                
                except Exception as e:
                    logger.error(f"Worker {worker_id}: Error during search for pattern {pattern}: {str(e)}")
                    logger.error(f"Worker {worker_id}: {traceback.format_exc()}")
                    # Still mark as completed to avoid getting stuck
                    with self.lock:
                        self.completed_patterns.add(pattern)
                        self.shared_data_store["search_patterns_completed"].add(pattern)
                        self._save_checkpoint()
                
                finally:
                    # Mark task as done
                    self.search_patterns_queue.task_done()
                    
                    # Do NOT clean up browser here - we'll reuse it for the next pattern
                    # Just log completion of the current pattern
                    logger.info(f"Worker {worker_id}: Completed processing pattern {pattern}")
                    
            except Exception as e:
                logger.error(f"Worker {worker_id}: Unexpected error: {str(e)}")
                logger.error(f"Worker {worker_id}: {traceback.format_exc()}")
                
                # Make sure we mark the task as done even on error
                try:
                    self.search_patterns_queue.task_done()
                except:
                    pass
                
            # Add a delay between patterns to reduce load
            time.sleep(5)
        
        # Only clean up browser when all patterns are done
        if driver:
            try:
                logger.info(f"Worker {worker_id}: All patterns completed, closing Chrome driver")
                driver.quit()
            except Exception as e:
                logger.error(f"Worker {worker_id}: Error closing Chrome driver: {str(e)}")
        
        # Clean up worker resources when done
        if proxy_runner:
            try:
                logger.info(f"Worker {worker_id}: Stopping proxy runner")
                proxy_runner.stop()
            except Exception as e:
                logger.error(f"Worker {worker_id}: Error stopping proxy runner: {str(e)}")
                
        logger.info(f"Worker {worker_id}: Finished processing all patterns")
    
    def _setup_driver(self, proxy_port, worker_id):
        """Set up Chrome driver with unique proxy configuration for server environment"""
        try:
            logger.info(f"Worker {worker_id}: Setting up Chrome driver with proxy on port {proxy_port}")
            
            # Set up Chrome options with unique proxy - SERVER COMPATIBLE
            chrome_options = Options()
            
            # Server-specific options (headless mode)
            chrome_options.add_argument("--headless")  # Run in headless mode for server
            chrome_options.add_argument("--no-sandbox")  # Required for Docker/server environments
            chrome_options.add_argument("--disable-dev-shm-usage")  # Required for Docker
            chrome_options.add_argument("--disable-gpu")  # Disable GPU for server
            chrome_options.add_argument("--remote-debugging-port=0")  # Avoid port conflicts
            
            # Window size for headless mode
            chrome_options.add_argument("--window-size=1920,1080")
            
            # Configure unique proxy for this worker
            proxy_url = f"http://127.0.0.1:{proxy_port}"
            chrome_options.add_argument(f"--proxy-server={proxy_url}")
            logger.info(f"Worker {worker_id}: Configured Chrome to use proxy: {proxy_url}")
            
            # Performance and stability options
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-plugins")
            chrome_options.add_argument("--disable-images")  # Faster loading
            # Note: Keeping JavaScript and CSS enabled as they may be needed for scraping
            
            # Memory and process optimization for server
            chrome_options.add_argument("--memory-pressure-off")
            chrome_options.add_argument("--disable-background-timer-throttling")
            chrome_options.add_argument("--disable-renderer-backgrounding")
            chrome_options.add_argument("--disable-backgrounding-occluded-windows")
            
            # Add user agent to identify different workers
            chrome_options.add_argument(f"--user-agent=GalvestonScraper-Server-Worker-{worker_id}")
            
            # Server environment compatibility
            chrome_options.add_argument("--disable-ipc-flooding-protection")
            
            # Initialize browser with unique configuration
            try:
                from webdriver_manager.chrome import ChromeDriverManager
                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=chrome_options)
                logger.info(f"Worker {worker_id}: Chrome driver initialized with webdriver-manager (headless)")
            except ImportError:
                # Fallback to default Chrome setup
                service = Service()
                driver = webdriver.Chrome(service=service, options=chrome_options)
                logger.info(f"Worker {worker_id}: Chrome driver initialized with default setup (headless)")
            
            # Set timeouts
            driver.implicitly_wait(10)
            driver.set_page_load_timeout(60)
            
            logger.info(f"Worker {worker_id}: Chrome driver ready with proxy configuration (headless mode)")
            
            return driver
            
        except Exception as e:
            error_msg = f"Worker {worker_id}: Failed to setup Chrome driver with proxy {proxy_port}: {str(e)}"
            logger.error(error_msg)
            
            # Send error notification
            send_error_notification(
                error_message="Chrome Driver Setup Failed",
                error_details=traceback.format_exc(),
                context={
                    "worker_id": worker_id,
                    "proxy_port": proxy_port,
                    "error": str(e)
                }
            )
            raise
    
    def _extract_search_results(self, driver, worker_id):
        """Extract data from Galveston search results"""
        records = []
        
        try:
            # Wait for results table to be present
            results_table = WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "body > table:nth-child(2) > tbody > tr > td > table:nth-child(4) > tbody > tr > td > table:nth-child(7) > tbody > tr > td > table > tbody"))
            )
            
            # Get all result rows (skip header if any)
            rows = results_table.find_elements(By.CSS_SELECTOR, "tr")
            
            for i, row in enumerate(rows):
                try:
                    # Skip header row (usually first row)
                    if i == 0:
                        continue
                    
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) < 4:
                        continue
                    
                    # Extract Account Number (1st column)
                    account_number_element = cells[0].find_element(By.CSS_SELECTOR, "h3 > a")
                    account_number = account_number_element.text.strip()
                    
                    # Extract Owner Name and Mailing Address (2nd column, combined)
                    owner_mailing_element = cells[1].find_element(By.CSS_SELECTOR, "h3")
                    owner_mailing_text = owner_mailing_element.text.strip()
                    
                    # Separate Owner Name and Mailing Address
                    # Address starts with a number
                    owner_name, mailing_address = self._separate_owner_and_address(owner_mailing_text)
                    
                    # Extract Property Address (3rd column)
                    property_address_element = cells[2].find_element(By.CSS_SELECTOR, "h3")
                    property_address = property_address_element.text.strip()
                    if not property_address:
                        property_address = "UNKNOWN"
                    
                    # Extract Legal Description (4th column)
                    legal_description_element = cells[3].find_element(By.CSS_SELECTOR, "h3")
                    legal_description = legal_description_element.text.strip()
                    
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
    
    def _separate_owner_and_address(self, combined_text):
        """Separate owner name and mailing address based on first number"""
        try:
            # Find the first occurrence of a number (start of address)
            match = re.search(r'\d', combined_text)
            if match:
                split_index = match.start()
                owner_name = combined_text[:split_index].strip()
                mailing_address = combined_text[split_index:].strip()
                return owner_name, mailing_address
            else:
                # No number found, treat whole text as owner name
                return combined_text.strip(), ""
        except Exception as e:
            logger.warning(f"Error separating owner and address: {str(e)}")
            return combined_text.strip(), ""
    
    def _navigate_to_search_page(self, driver, worker_id):
        """Navigate to the Galveston County search page with retries"""
        max_retries = 5
        for retry in range(max_retries):
            try:
                # Use a longer timeout for initial navigation
                driver.set_page_load_timeout(120)
                driver.get("https://actweb.acttax.com/act_webdev/galveston/index.jsp")
                # Wait for page to be fully loaded
                WebDriverWait(driver, 60).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#criteria"))
                )
                logger.info(f"Worker {worker_id}: Navigated to Galveston search page")
                return True
            except Exception as e:
                if retry < max_retries - 1:
                    logger.warning(f"Worker {worker_id}: Navigation retry {retry+1}/{max_retries}: {str(e)}")
                    time.sleep(5)  # Longer wait before retry
                else:
                    raise
        return False
                    
    def _enter_search_pattern(self, driver, pattern, worker_id):
        """Enter the search pattern in the Galveston search box"""
        for input_retry in range(3):
            try:
                # Wait for the search box with the Galveston selector
                search_box = WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#criteria"))
                )
                
                # Ensure the element is visible and interactable
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", search_box)
                time.sleep(1)
                
                # Use JavaScript to clear the input field
                driver.execute_script("arguments[0].value = '';", search_box)
                
                # Clear any existing value using multiple techniques
                search_box.clear()
                for i in range(10):  # Send multiple backspaces to ensure clearing
                    search_box.send_keys(Keys.BACKSPACE)
                
                # Use JavaScript to set the value directly
                driver.execute_script(f"arguments[0].value = '{pattern}';", search_box)
                
                # Trigger input events to ensure the value is recognized
                driver.execute_script("""
                    var input = arguments[0];
                    var event = new Event('input', { bubbles: true });
                    input.dispatchEvent(event);
                    var change = new Event('change', { bubbles: true });
                    input.dispatchEvent(change);
                """, search_box)
                
                # Verify the value was set
                actual_value = driver.execute_script("return arguments[0].value;", search_box)
                if actual_value != pattern:
                    logger.warning(f"Worker {worker_id}: Input value mismatch. Expected: {pattern}, Got: {actual_value}")
                    continue
                    
                logger.info(f"Worker {worker_id}: Entered search pattern: {pattern}")
                return True
            except Exception as e:
                logger.warning(f"Worker {worker_id}: Search input retry {input_retry+1}/3 failed: {str(e)}")
                time.sleep(2)
                if input_retry == 2:  # Last retry
                    raise
        return False
                    
    def _click_search_button(self, driver, worker_id):
        """Click the Galveston search button"""
        # Wait a moment for the page to recognize the input
        time.sleep(2)
        
        # Find and click the search button using the Galveston selector
        try:
            search_button = WebDriverWait(driver, 30).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "body > table:nth-child(2) > tbody > tr > td > table:nth-child(4) > tbody > tr > td > p:nth-child(5) > table:nth-child(6) > tbody > tr > td > center > form > table > tbody > tr:nth-child(5) > td:nth-child(2) > h3:nth-child(2) > input[type=submit]"))
            )
            
            # Scroll the button into view
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", search_button)
            time.sleep(1)
            
            # Try multiple approaches to click the button
            logger.info(f"Worker {worker_id}: Attempting to click Galveston search button...")
            
            # First try: JavaScript click
            driver.execute_script("arguments[0].click();", search_button)
            logger.info(f"Worker {worker_id}: Clicked search button via JavaScript")
            return True
        except Exception as e:
            logger.warning(f"Worker {worker_id}: Failed to click search button with JS: {str(e)}")
            
            # Second try: Enter key in the search box
            try:
                search_box = driver.find_element(By.CSS_SELECTOR, "#criteria")
                search_box.send_keys(Keys.ENTER)
                logger.info(f"Worker {worker_id}: Sent ENTER key to search box")
                return True
            except Exception as key_e:
                logger.error(f"Worker {worker_id}: All button click methods failed: {str(key_e)}")
                raise
        return False
    
    def export_to_csv(self):
        """Export the shared data store to CSV using the utility module"""
        try:
            csv_path = checkpoint_utils.export_to_csv(
                self.shared_data_store, 
                output_prefix="galveston_tax_results"
            )
            
            if csv_path:
                logger.info(f"Results exported to {csv_path}")
            else:
                logger.warning("Failed to export results to CSV")
                
        except Exception as e:
            logger.error(f"Error exporting to CSV: {str(e)}")
    
    def run(self):
        """Start the multithreaded scraping process with error notifications"""
        start_time = datetime.now()
        logger.info(f"Starting multithreaded Galveston scraping with {self.num_threads} workers")
        
        try:
            # Verify proxy ports are available
            available_ports = self._verify_proxy_ports_available()
            if not available_ports:
                error_msg = "No proxy ports available. Cannot start workers."
                logger.error(error_msg)
                
                # Send error notification
                send_error_notification(
                    error_message="Scraper Startup Failed",
                    error_details="No proxy ports available for workers",
                    context={
                        "num_threads": self.num_threads,
                        "available_ports": len(available_ports)
                    }
                )
                return
            
            logger.info(f"Verified {len(available_ports)} unique proxy ports available: {available_ports}")
            
            # Start worker threads
            threads = []
            for i in range(self.num_threads):
                thread = threading.Thread(
                    target=self._worker,
                    args=(i,),
                    name=f"ScraperWorker-{i}",
                    daemon=True
                )
                threads.append(thread)
                thread.start()
                
                # Add a small delay between starting threads
                time.sleep(3)
            
            logger.info(f"Started {len(threads)} worker threads")
            
            # Wait for all threads to complete
            for thread in threads:
                thread.join()
                
            logger.info("All workers completed")
            
            # Calculate execution time
            end_time = datetime.now()
            execution_time = str(end_time - start_time)
            
            # Export results to CSV
            self.export_to_csv()
            
            # Send completion notification
            total_records = len(self.shared_data_store.get("records", {}))
            patterns_completed = len(self.shared_data_store.get("search_patterns_completed", set()))
            
            logger.info(f"Scraping completed: {total_records} records, {patterns_completed} patterns, {execution_time}")
            
            # Send completion notification email
            send_completion_notification(
                total_records=total_records,
                patterns_completed=patterns_completed,
                execution_time=execution_time
            )
            
        except Exception as e:
            error_msg = f"Critical error in scraping process: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            
            # Send critical error notification
            send_error_notification(
                error_message="Critical Scraper Error",
                error_details=traceback.format_exc(),
                context={
                    "num_threads": self.num_threads,
                    "error": str(e),
                    "execution_time": str(datetime.now() - start_time)
                }
            )
            raise
            
        finally:
            # Note: Individual worker proxies are cleaned up by each worker thread
            # No central proxy cleanup needed since each worker manages its own proxy
            logger.info("Galveston scraping process finished. Individual worker proxies cleaned up by respective threads.")

if __name__ == "__main__":
    # Create and run the Galveston tax scraper with 3 workers
    scraper = GalvestonTaxScraper(num_threads=3)
    scraper.run()