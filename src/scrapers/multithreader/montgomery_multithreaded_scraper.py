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

# Load environment variables from .env file first
try:
    from dotenv import load_dotenv
    load_dotenv()  # Load .env file if it exists
    print("Environment variables loaded from .env file")
except ImportError:
    print("Warning: python-dotenv not available. Environment variables must be set manually.")

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
# Add current directory to path for local imports
sys.path.append(os.path.dirname(__file__))

# Import our new modules
from browser_manager import BrowserManager
from worker_health import WorkerHealthManager
from web_scraper_operations import WebScraperOperations
from data_manager import DataManager
from src.utils.email_notifier import send_error_notification, send_completion_notification

# Get logger from root configuration
logger = logging.getLogger(__name__)

# Ensure we also log to a file directly from this module
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
log_file = log_dir / "montgomery_run.log"

# Add a file handler to this module's logger
file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s - pid:%(process)d - %(threadName)s - %(name)s - %(levelname)s - %(message)s'
))
logger.addHandler(file_handler)

logger.info("Montgomery multithreaded scraper module initialized")

class MontgomeryTaxScraper:
    def __init__(self, num_threads=3):
        self.num_threads = num_threads
        self.search_patterns_queue = queue.Queue()
        self.completed_patterns = set()
        self.failed_patterns = set()
        self.no_results_patterns = set()
        self.lock = threading.Lock()
        
        # Initialize data manager
        self.data_manager = DataManager()
        
        # Load existing checkpoints to get completed patterns
        self._load_completed_patterns()
        
        # Generate all search patterns
        self._generate_search_patterns()
    
    def _load_completed_patterns(self):
        """Load completed and failed patterns from existing checkpoints"""
        self.completed_patterns, self.failed_patterns, self.no_results_patterns = self.data_manager.load_completed_patterns()
    
    def _save_checkpoint(self) -> None:
        """Save current progress to prevent data loss"""
        try:
            # Update data manager's shared data store with current patterns
            self.data_manager.shared_data_store["search_patterns_completed"] = self.completed_patterns
            self.data_manager.shared_data_store["search_patterns_failed"] = self.failed_patterns
            self.data_manager.shared_data_store["search_patterns_no_results"] = self.no_results_patterns
            
            # Save checkpoint
            self.data_manager.save_checkpoint()
        except Exception as e:
            logger.error(f"Error saving checkpoint: {str(e)}")
    
    def _generate_search_patterns(self):
        """Generate all search patterns and filter out completed ones"""
        # Generate all patterns
        all_patterns = self.data_manager.generate_search_patterns()
        
        # Filter out already processed patterns
        processed_patterns = self.completed_patterns.union(self.failed_patterns).union(self.no_results_patterns)
        remaining_patterns = [p for p in all_patterns if p not in processed_patterns]
        
        # Clear queue and add remaining patterns
        while not self.search_patterns_queue.empty():
            try:
                self.search_patterns_queue.get_nowait()
            except queue.Empty:
                break
        
        for pattern in remaining_patterns:
            self.search_patterns_queue.put(pattern)
            
        logger.info(f"Generated {len(all_patterns)} total patterns")
        logger.info(f"{len(self.completed_patterns)} patterns already completed")
        logger.info(f"{len(self.failed_patterns)} patterns already failed")
        logger.info(f"{len(self.no_results_patterns)} patterns with no results")
        logger.info(f"{self.search_patterns_queue.qsize()} patterns remaining")
    
    def _verify_proxy_ports_available(self):
        """Verify that proxy ports are available before starting workers"""
        available_ports = WorkerHealthManager.verify_proxy_ports_available(self.num_threads)
        if len(available_ports) < self.num_threads:
            self.num_threads = len(available_ports)
        return available_ports
    
    def _process_search_pattern(self, driver, pattern, worker_id):
        """Process a single search pattern"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Navigate to search page
                if not BrowserManager.navigate_to_search_page(driver, worker_id):
                    logger.error(f"Worker {worker_id}: Failed to navigate to search page")
                    retry_count += 1
                    continue
                
                # Enter search pattern
                if not BrowserManager.enter_search_pattern(driver, pattern, worker_id):
                    logger.error(f"Worker {worker_id}: Failed to enter search pattern")
                    retry_count += 1
                    continue
                
                # Click search button
                if not BrowserManager.click_search_button(driver, worker_id):
                    logger.error(f"Worker {worker_id}: Failed to click search button")
                    retry_count += 1
                    continue
                
                # Wait for results
                time.sleep(3)
                
                # Check for no results
                if BrowserManager.check_for_no_results(driver, worker_id):
                    logger.info(f"Worker {worker_id}: No results found for pattern {pattern}")
                    with self.lock:
                        self.completed_patterns.add(pattern)
                        self.no_results_patterns.add(pattern)
                        self._save_checkpoint()
                    return True
                
                # Extract results
                results = WebScraperOperations.extract_search_results(driver, worker_id)
                
                # Save results
                with self.lock:
                    self.completed_patterns.add(pattern)
                    if results:
                        for record in results:
                            key = f"{record.get('Account_Number', 'unknown')}_{pattern}_{len(self.data_manager.shared_data_store['records'])}"
                            self.data_manager.shared_data_store["records"][key] = record
                    self._save_checkpoint()
                
                logger.info(f"Worker {worker_id}: Successfully processed pattern {pattern} with {len(results)} records")
                return True
                
            except Exception as e:
                logger.error(f"Worker {worker_id}: Error processing pattern {pattern} (attempt {retry_count + 1}): {str(e)}")
                retry_count += 1
                if retry_count < max_retries:
                    time.sleep(5)
        
        # Mark as failed after all retries
        logger.error(f"Worker {worker_id}: Pattern {pattern} failed after {max_retries} attempts")
        with self.lock:
            self.failed_patterns.add(pattern)
            self._save_checkpoint()
        return False
    
    def _worker(self, worker_id):
        """Worker thread function that processes search patterns"""
        logger.info(f"Worker {worker_id} started")
        
        # Set unique proxy port for this worker
        proxy_port = 8081 + worker_id
        
        # Initialize proxy and driver using WorkerHealthManager
        proxy_runner, driver = WorkerHealthManager.initialize_worker_proxy_and_driver(worker_id, proxy_port)
        
        if not proxy_runner or not driver:
            logger.error(f"Worker {worker_id}: Failed to initialize, exiting")
            return
        
        # Keep track of patterns processed by this worker
        patterns_processed_by_worker = []
        consecutive_failures = 0
        max_consecutive_failures = 5
        patterns_processed = 0
        comprehensive_check_interval = 50
        
        try:
            while True:
                try:
                    # Get next search pattern from queue
                    try:
                        pattern = self.search_patterns_queue.get_nowait()
                    except queue.Empty:
                        logger.info(f"Worker {worker_id}: No more patterns to process")
                        break
                    
                    logger.info(f"Worker {worker_id}: Processing pattern: {pattern}")
                    
                    # Periodic comprehensive health check
                    if patterns_processed > 0 and patterns_processed % comprehensive_check_interval == 0:
                        logger.info(f"Worker {worker_id}: Performing periodic comprehensive health check")
                        health_issues = WorkerHealthManager.check_worker_health(driver, proxy_runner, worker_id, include_ip_leak_test=True)
                        
                        if health_issues:
                            logger.warning(f"Worker {worker_id}: Health issues detected: {', '.join(health_issues)}")
                            
                            # Attempt recovery
                            recovery_success, new_proxy_runner, new_driver = WorkerHealthManager.attempt_worker_recovery(
                                proxy_runner, driver, worker_id, proxy_port
                            )
                            
                            if recovery_success:
                                proxy_runner = new_proxy_runner
                                driver = new_driver
                                consecutive_failures = 0
                                logger.info(f"Worker {worker_id}: Recovery successful")
                            else:
                                logger.error(f"Worker {worker_id}: Recovery failed, worker will exit")
                                self.search_patterns_queue.put(pattern)
                                break
                        else:
                            logger.info(f"Worker {worker_id}: Health check passed")
                    
                    # Regular health check for consecutive failures
                    if consecutive_failures >= max_consecutive_failures:
                        logger.warning(f"Worker {worker_id}: {consecutive_failures} consecutive failures, checking health")
                        health_issues = WorkerHealthManager.check_worker_health(driver, proxy_runner, worker_id)
                        
                        if health_issues:
                            recovery_success, new_proxy_runner, new_driver = WorkerHealthManager.attempt_worker_recovery(
                                proxy_runner, driver, worker_id, proxy_port
                            )
                            
                            if recovery_success:
                                proxy_runner = new_proxy_runner
                                driver = new_driver
                                consecutive_failures = 0
                            else:
                                logger.error(f"Worker {worker_id}: Recovery failed, worker will exit")
                                self.search_patterns_queue.put(pattern)
                                break
                        else:
                            consecutive_failures = 0
                    
                    # Check if pattern is already completed
                    with self.lock:
                        if pattern in self.completed_patterns:
                            logger.info(f"Worker {worker_id}: Pattern {pattern} already completed, skipping")
                            self.search_patterns_queue.task_done()
                            continue
                    
                    # Process the pattern
                    pattern_success = self._process_search_pattern(driver, pattern, worker_id)
                    
                    if pattern_success:
                        consecutive_failures = 0
                    else:
                        consecutive_failures += 1
                    
                    patterns_processed += 1
                    patterns_processed_by_worker.append(pattern)
                    self.search_patterns_queue.task_done()
                    
                    # Add delay between patterns
                    time.sleep(5)
                    
                except Exception as e:
                    logger.error(f"Worker {worker_id}: Unexpected error: {str(e)}")
                    consecutive_failures += 1
                    try:
                        self.search_patterns_queue.task_done()
                    except:
                        pass
        
        finally:
            # Clean up resources
            if driver:
                try:
                    driver.quit()
                    logger.info(f"Worker {worker_id}: Closed Chrome driver")
                except Exception as e:
                    logger.error(f"Worker {worker_id}: Error closing driver: {str(e)}")
            
            if proxy_runner:
                try:
                    proxy_runner.stop()
                    logger.info(f"Worker {worker_id}: Stopped proxy runner")
                except Exception as e:
                    logger.error(f"Worker {worker_id}: Error stopping proxy: {str(e)}")
            
            # Log summary
            if patterns_processed_by_worker:
                logger.info(f"Worker {worker_id}: Processed {len(patterns_processed_by_worker)} patterns")
            
            logger.info(f"Worker {worker_id}: Finished processing")
    
    def export_to_csv(self):
        """Export the shared data store to CSV using the data manager"""
        self.data_manager.export_to_csv()
    
    def run(self):
        """Start the multithreaded scraping process with error notifications"""
        start_time = datetime.now()
        logger.info(f"Starting multithreaded Montgomery scraping with {self.num_threads} workers")
        
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
            
            # Log pattern distribution information
            patterns_remaining = self.search_patterns_queue.qsize()
            if patterns_remaining > 0:
                patterns_per_thread = patterns_remaining // self.num_threads
                remainder = patterns_remaining % self.num_threads
                logger.info(f"Distributing approximately {patterns_per_thread} patterns per thread (plus {remainder} remainder)")
                
                # Get all patterns from the queue into a list to ensure proper distribution
                all_patterns = []
                temp_queue = queue.Queue()
                
                # Empty the queue into our temporary list
                while not self.search_patterns_queue.empty():
                    pattern = self.search_patterns_queue.get()
                    all_patterns.append(pattern)
                
                # Sort patterns alphabetically to ensure we always start from aaa%
                all_patterns.sort()
                
                # Preview the first few patterns
                preview_count = min(self.num_threads * 2, len(all_patterns))
                if all_patterns:
                    logger.info(f"First {preview_count} patterns to be processed: {', '.join(all_patterns[:preview_count])}")
                    for i in range(min(self.num_threads, len(all_patterns))):
                        logger.info(f"Worker {i} will start with pattern: {all_patterns[i]}")
                
                # Put patterns back into the queue
                for pattern in all_patterns:
                    self.search_patterns_queue.put(pattern)
            
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
            total_records = len(self.data_manager.shared_data_store.get("records", {}))
            patterns_completed = len(self.data_manager.shared_data_store.get("search_patterns_completed", set()))
            patterns_failed = len(self.data_manager.shared_data_store.get("search_patterns_failed", set()))
            patterns_no_results = len(self.data_manager.shared_data_store.get("search_patterns_no_results", set()))
            patterns_with_records = patterns_completed - patterns_no_results
            total_patterns_processed = patterns_completed + patterns_failed
            
            # Log detailed completion summary
            logger.info(f"Scraping completed: {total_records} records, {patterns_completed} patterns completed ({patterns_with_records} with records, {patterns_no_results} with no results), {patterns_failed} patterns failed, {execution_time}")
            
            if patterns_no_results > 0:
                no_results_patterns_list = sorted(list(self.data_manager.shared_data_store.get("search_patterns_no_results", set())))
                if len(no_results_patterns_list) <= 10:
                    logger.info(f"No-results patterns: {', '.join(no_results_patterns_list)}")
                else:
                    logger.info(f"No-results patterns (showing first 5): {', '.join(no_results_patterns_list[:5])}")
                    logger.info(f"Total no-results patterns: {len(no_results_patterns_list)}")
            
            if patterns_failed > 0:
                failed_patterns_list = sorted(list(self.data_manager.shared_data_store.get("search_patterns_failed", set())))
                if len(failed_patterns_list) <= 10:
                    logger.warning(f"Failed patterns: {', '.join(failed_patterns_list)}")
                else:
                    logger.warning(f"Failed patterns (showing first 5): {', '.join(failed_patterns_list[:5])}")
                    logger.warning(f"Total failed patterns: {len(failed_patterns_list)}")
            
            # Send completion notification email
            send_completion_notification(
                total_records=total_records,
                patterns_completed=patterns_completed,
                execution_time=execution_time,
                patterns_failed=patterns_failed,
                patterns_no_results=patterns_no_results
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
            logger.info("Montgomery scraping process finished. Individual worker proxies cleaned up by respective threads.")

if __name__ == "__main__":
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description='Montgomery County Tax Scraper')
    parser.add_argument('--threads', type=int, default=3, help='Number of worker threads to use (default: 3)')
    
    args = parser.parse_args()
    
    logger.info(f"Command line arguments: --threads {args.threads}")
    
    try:
        # Create and run the scraper with the specified number of threads
        scraper = MontgomeryTaxScraper(num_threads=args.threads)
        scraper.run()
        
        logger.info("Montgomery scraper completed successfully")
        
    except Exception as e:
        logger.error(f"Scraper execution failed: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Send error notification if email is configured
        try:
            send_error_notification(
                error_message="Scraper Execution Failed",
                error_details=traceback.format_exc(),
                context={
                    "threads": args.threads,
                    "error": str(e)
                }
            )
        except Exception as email_error:
            logger.warning(f"Failed to send error notification: {str(email_error)}")
        
        raise e