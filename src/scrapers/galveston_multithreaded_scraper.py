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
import argparse

# Get logger from root configuration
logger = logging.getLogger(__name__)

# Ensure we also log to a file directly from this module
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
log_file = log_dir / "galveston_run.log"

# Add a file handler to this module's logger
file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s - pid:%(process)d - %(threadName)s - %(name)s - %(levelname)s - %(message)s'
))
logger.addHandler(file_handler)

logger.info("Galveston multithreaded scraper module initialized")

class GalvestonTaxScraper:
    def __init__(self, num_threads=3):
        self.num_threads = num_threads
        self.search_patterns_queue = queue.Queue()
        self.completed_patterns = set()
        self.failed_patterns = set()  # Track patterns that failed after all retries
        self.no_results_patterns = set()  # Track patterns that successfully searched but found no results
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
            "search_patterns_failed": set(),  # Track failed patterns in checkpoint
            "search_patterns_no_results": set(),  # Track patterns with no results in checkpoint
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
        """Load completed and failed patterns from existing checkpoints"""
        try:
            # Initialize with empty sets to ensure we start fresh
            self.completed_patterns = set()
            self.failed_patterns = set()
            self.no_results_patterns = set()
            self.shared_data_store["search_patterns_completed"] = set()
            self.shared_data_store["search_patterns_failed"] = set()
            self.shared_data_store["search_patterns_no_results"] = set()
            
            # Use the checkpoint_utils to load the latest checkpoint
            loaded_data_store, checkpoint_path = checkpoint_utils.load_latest_checkpoint()
            
            if checkpoint_path:
                self.checkpoint_file = checkpoint_path
                
                # Update our shared data store with the loaded data
                self.shared_data_store.update(loaded_data_store)
                
                # Update completed patterns from the checkpoint
                if "search_patterns_completed" in loaded_data_store:
                    # Ensure we're working with a set
                    if not isinstance(loaded_data_store["search_patterns_completed"], set):
                        loaded_data_store["search_patterns_completed"] = set(loaded_data_store["search_patterns_completed"])
                    
                    # Update both our tracking sets
                    self.completed_patterns = loaded_data_store["search_patterns_completed"]
                    self.shared_data_store["search_patterns_completed"] = self.completed_patterns
                    
                    # Log the last completed pattern if any
                    if self.completed_patterns:
                        completed_patterns_list = sorted(list(self.completed_patterns))
                        last_completed = completed_patterns_list[-1] if completed_patterns_list else "None"
                        logger.info(f"Last completed pattern from checkpoint: {last_completed}")
                    
                    logger.info(f"Loaded {len(self.completed_patterns)} completed patterns from checkpoint")
                else:
                    # Initialize if not present
                    logger.info("No completed patterns found in checkpoint")
                
                # Update failed patterns from the checkpoint
                if "search_patterns_failed" in loaded_data_store:
                    # Ensure we're working with a set
                    if not isinstance(loaded_data_store["search_patterns_failed"], set):
                        loaded_data_store["search_patterns_failed"] = set(loaded_data_store["search_patterns_failed"])
                    
                    # Update both our tracking sets
                    self.failed_patterns = loaded_data_store["search_patterns_failed"]
                    self.shared_data_store["search_patterns_failed"] = self.failed_patterns
                    
                    if self.failed_patterns:
                        failed_patterns_list = sorted(list(self.failed_patterns))
                        logger.info(f"Loaded {len(self.failed_patterns)} failed patterns from checkpoint")
                        if len(failed_patterns_list) <= 10:
                            logger.info(f"Failed patterns: {', '.join(failed_patterns_list)}")
                        else:
                            logger.info(f"First failed pattern: {failed_patterns_list[0]}, Last failed pattern: {failed_patterns_list[-1]}")
                else:
                    # Initialize if not present
                    logger.info("No failed patterns found in checkpoint")
                
                # Update no results patterns from the checkpoint
                if "search_patterns_no_results" in loaded_data_store:
                    # Ensure we're working with a set
                    if not isinstance(loaded_data_store["search_patterns_no_results"], set):
                        loaded_data_store["search_patterns_no_results"] = set(loaded_data_store["search_patterns_no_results"])
                    
                    # Update both our tracking sets
                    self.no_results_patterns = loaded_data_store["search_patterns_no_results"]
                    self.shared_data_store["search_patterns_no_results"] = self.no_results_patterns
                    
                    if self.no_results_patterns:
                        no_results_patterns_list = sorted(list(self.no_results_patterns))
                        logger.info(f"Loaded {len(self.no_results_patterns)} no-results patterns from checkpoint")
                        if len(no_results_patterns_list) <= 10:
                            logger.info(f"No-results patterns: {', '.join(no_results_patterns_list)}")
                        else:
                            logger.info(f"First no-results pattern: {no_results_patterns_list[0]}, Last no-results pattern: {no_results_patterns_list[-1]}")
                else:
                    # Initialize if not present
                    logger.info("No no-results patterns found in checkpoint")
                    
                logger.info(f"Loaded {len(self.shared_data_store['records'])} records from checkpoint")
            else:
                logger.info("No checkpoint found. Starting from the beginning (aaa%)")
        except Exception as e:
            logger.error(f"Error loading checkpoint: {str(e)}")
            # Ensure we have empty sets if loading fails
            self.completed_patterns = set()
            self.failed_patterns = set()
            self.no_results_patterns = set()
            self.shared_data_store["search_patterns_completed"] = set()
            self.shared_data_store["search_patterns_failed"] = set()
            self.shared_data_store["search_patterns_no_results"] = set()
    
    def _save_checkpoint(self) -> None:
        """Save current progress to prevent data loss using a shared checkpoint file"""
        try:
            # Use the checkpoint_utils to save the checkpoint
            self.checkpoint_file = checkpoint_utils.save_checkpoint(self.shared_data_store)
        except Exception as e:
            logger.error(f"Error saving checkpoint: {str(e)}")
    
    def _generate_search_patterns(self):
        """Generate all search patterns (aaa%, aab%, etc.) - 3 letter patterns for Galveston"""
        # Generate all patterns first
        all_patterns = []
        for first_letter in "abcdefghijklmnopqrstuvwxyz":
            for second_letter in "abcdefghijklmnopqrstuvwxyz":
                for third_letter in "abcdefghijklmnopqrstuvwxyz":
                    pattern = f"{first_letter}{second_letter}{third_letter}%"
                    all_patterns.append(pattern)
        
        # Ensure patterns are sorted alphabetically
        all_patterns.sort()
        
        # Log the first few patterns to verify order
        if all_patterns:
            logger.info(f"First pattern: {all_patterns[0]}, Second: {all_patterns[1]}, Third: {all_patterns[2]}")
        
        # Clear queue first to ensure we're starting fresh
        while not self.search_patterns_queue.empty():
            try:
                self.search_patterns_queue.get_nowait()
            except queue.Empty:
                break
        
        # Filter out already completed, failed, and no-results patterns
        processed_patterns = self.completed_patterns.union(self.failed_patterns).union(self.no_results_patterns)
        remaining_patterns = [p for p in all_patterns if p not in processed_patterns]
        
        # If we have completed patterns, log the last completed one
        if self.completed_patterns:
            completed_patterns_list = sorted(list(self.completed_patterns))
            last_completed = completed_patterns_list[-1] if completed_patterns_list else "None"
            logger.info(f"Resuming from after last completed pattern: {last_completed}")
        
        # Add remaining patterns to queue in strictly sorted order
        for pattern in remaining_patterns:
            self.search_patterns_queue.put(pattern)
            
        logger.info(f"Generated {len(all_patterns)} total patterns")
        logger.info(f"{len(self.completed_patterns)} patterns already completed")
        logger.info(f"{len(self.failed_patterns)} patterns already failed")
        logger.info(f"{len(self.no_results_patterns)} patterns with no results")
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

    def _get_real_ip(self):
        """Get the real IP address without proxy for comparison"""
        import requests
        
        try:
            # Try multiple IP detection services for reliability
            ip_services = [
                'https://api.ipify.org?format=json',
                'https://httpbin.org/ip',
                'https://api.myip.com',
                'https://checkip.amazonaws.com'
            ]
            
            for service in ip_services:
                try:
                    response = requests.get(service, timeout=10)
                    if response.status_code == 200:
                        if 'ipify' in service:
                            return response.json().get('ip')
                        elif 'httpbin' in service:
                            return response.json().get('origin')
                        elif 'myip' in service:
                            return response.json().get('ip')
                        elif 'amazonaws' in service:
                            return response.text.strip()
                except Exception as e:
                    logger.debug(f"IP service {service} failed: {str(e)}")
                    continue
            
            logger.warning("All IP detection services failed, could not determine real IP")
            return None
            
        except Exception as e:
            logger.warning(f"Error getting real IP: {str(e)}")
            return None

    def _test_proxy_ip_leak(self, driver, worker_id, proxy_port, real_ip=None):
        """Test if the proxy is actually working and not leaking the real IP"""
        try:
            logger.info(f"Worker {worker_id}: Testing proxy IP leak protection...")
            
            # Get real IP if not provided
            if real_ip is None:
                real_ip = self._get_real_ip()
                if not real_ip:
                    logger.warning(f"Worker {worker_id}: Could not determine real IP, skipping leak test")
                    return False, "Could not determine real IP"
            
            logger.info(f"Worker {worker_id}: Real IP address: {real_ip}")
            
            # Test IP through browser with proxy using SAFE IP detection services (NOT target sites)
            ip_test_urls = [
                'https://api.ipify.org?format=json',
                'https://httpbin.org/ip',
                'https://checkip.amazonaws.com'
            ]
            
            logger.info(f"Worker {worker_id}: SECURITY TEST - Testing IP leak using safe IP detection services (NOT target site)")
            
            proxy_ip = None
            for test_url in ip_test_urls:
                try:
                    logger.debug(f"Worker {worker_id}: SAFE IP TEST - Accessing IP detection service: {test_url}")
                    driver.get(test_url)
                    
                    # Wait for page to load
                    time.sleep(3)
                    
                    page_text = driver.page_source.lower()
                    
                    # Extract IP from different response formats
                    if 'ipify' in test_url:
                        # JSON format: {"ip":"1.2.3.4"}
                        import re
                        ip_match = re.search(r'"ip"\s*:\s*"([^"]+)"', page_text)
                        if ip_match:
                            proxy_ip = ip_match.group(1)
                    elif 'httpbin' in test_url:
                        # JSON format: {"origin":"1.2.3.4"}
                        import re
                        ip_match = re.search(r'"origin"\s*:\s*"([^"]+)"', page_text)
                        if ip_match:
                            proxy_ip = ip_match.group(1)
                    elif 'amazonaws' in test_url:
                        # Plain text format: just the IP
                        import re
                        ip_match = re.search(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})', page_text)
                        if ip_match:
                            proxy_ip = ip_match.group(1)
                    
                    if proxy_ip:
                        logger.debug(f"Worker {worker_id}: Detected proxy IP via {test_url}: {proxy_ip}")
                        break
                        
                except Exception as e:
                    logger.debug(f"Worker {worker_id}: IP test via {test_url} failed: {str(e)}")
                    continue
            
            if not proxy_ip:
                logger.error(f"Worker {worker_id}: Could not determine proxy IP from any test service")
                return False, "Could not determine proxy IP"
            
            # Compare IPs
            if proxy_ip == real_ip:
                logger.error(f"Worker {worker_id}: IP LEAK DETECTED! Proxy IP ({proxy_ip}) matches real IP ({real_ip})")
                return False, f"IP leak detected: proxy IP {proxy_ip} matches real IP {real_ip}"
            else:
                logger.info(f"Worker {worker_id}: Proxy working correctly - Real IP: {real_ip}, Proxy IP: {proxy_ip}")
                return True, f"Proxy working: Real IP {real_ip} -> Proxy IP {proxy_ip}"
            
        except Exception as e:
            logger.error(f"Worker {worker_id}: Error during proxy leak test: {str(e)}")
            return False, f"Error during leak test: {str(e)}"

    def _comprehensive_proxy_verification(self, proxy_runner, driver, worker_id, proxy_port):
        """Comprehensive proxy verification including connectivity and IP leak testing"""
        try:
            logger.info(f"Worker {worker_id}: Starting comprehensive proxy verification...")
            
            # Step 1: Basic connectivity test
            if not self._verify_proxy_running(proxy_runner, worker_id, proxy_port):
                return False, "Basic connectivity test failed"
            
            # Step 2: Get real IP for comparison
            real_ip = self._get_real_ip()
            if not real_ip:
                logger.warning(f"Worker {worker_id}: Could not determine real IP, skipping IP leak test")
                return True, "Basic connectivity passed, IP leak test skipped"
            
            # Step 3: IP leak test
            leak_test_passed, leak_test_message = self._test_proxy_ip_leak(driver, worker_id, proxy_port, real_ip)
            
            if leak_test_passed:
                logger.info(f"Worker {worker_id}: Comprehensive proxy verification PASSED - {leak_test_message}")
                return True, leak_test_message
            else:
                logger.error(f"Worker {worker_id}: Comprehensive proxy verification FAILED - {leak_test_message}")
                return False, leak_test_message
            
        except Exception as e:
            error_msg = f"Error during comprehensive proxy verification: {str(e)}"
            logger.error(f"Worker {worker_id}: {error_msg}")
            return False, error_msg

    def _check_worker_health(self, driver, proxy_runner, worker_id, include_ip_leak_test=False):
        """Check if worker components (driver and proxy) are healthy"""
        health_issues = []
        
        # Check if browser driver is still responsive
        try:
            driver.current_url  # Simple check to see if driver responds
            driver.title  # Another simple check
        except Exception as e:
            health_issues.append(f"Browser driver unresponsive: {str(e)}")
        
        # Check if proxy is still running
        try:
            if proxy_runner and hasattr(proxy_runner, 'is_running') and not proxy_runner.is_running():
                health_issues.append("Proxy runner stopped")
        except Exception as e:
            health_issues.append(f"Proxy status check failed: {str(e)}")
        
        # Optional IP leak test during health checks (enabled for comprehensive checks)
        if include_ip_leak_test and not health_issues:  # Only test if basic health is good
            try:
                proxy_port = 8081 + worker_id
                leak_test_passed, leak_test_message = self._test_proxy_ip_leak(driver, worker_id, proxy_port)
                if not leak_test_passed:
                    health_issues.append(f"IP leak detected: {leak_test_message}")
                    logger.warning(f"Worker {worker_id}: IP leak detected during health check: {leak_test_message}")
                else:
                    logger.debug(f"Worker {worker_id}: IP leak test passed during health check")
            except Exception as e:
                logger.debug(f"Worker {worker_id}: IP leak test failed during health check: {str(e)}")
                # Don't add to health issues - IP leak test is not critical for basic health
        
        return health_issues

    def _recover_worker_connection(self, worker_id, proxy_port, proxy_runner, driver):
        """Attempt to recover worker connection by restarting components"""
        recovery_success = False
        new_proxy_runner = None
        new_driver = None
        
        logger.warning(f"Worker {worker_id}: Attempting connection recovery...")
        
        try:
            # Step 1: Clean up existing resources
            if driver:
                try:
                    driver.quit()
                    logger.info(f"Worker {worker_id}: Closed existing browser driver")
                except Exception as e:
                    logger.warning(f"Worker {worker_id}: Error closing driver: {str(e)}")
            
            if proxy_runner:
                try:
                    proxy_runner.stop()
                    logger.info(f"Worker {worker_id}: Stopped existing proxy runner")
                except Exception as e:
                    logger.warning(f"Worker {worker_id}: Error stopping proxy: {str(e)}")
            
            # Step 2: Wait for resources to fully release
            time.sleep(5)
            
            # Step 3: Restart proxy
            logger.info(f"Worker {worker_id}: Restarting proxy on port {proxy_port}")
            new_proxy_runner = LocalProxyRunner(local_port=proxy_port)
            new_proxy_runner.start()
            time.sleep(5)  # Give proxy time to start
            
            # Step 4: Restart browser driver
            logger.info(f"Worker {worker_id}: Restarting Chrome driver")
            new_driver = self._setup_driver(proxy_port, worker_id)
            
            # Step 5: CRITICAL SECURITY - Proxy verification BEFORE accessing target site
            logger.warning(f"Worker {worker_id}: RECOVERY SECURITY CHECK - Verifying proxy before target site access")
            proxy_verification_passed, verification_message = self._comprehensive_proxy_verification(
                new_proxy_runner, new_driver, worker_id, proxy_port
            )
            
            if not proxy_verification_passed:
                logger.error(f"Worker {worker_id}: RECOVERY SECURITY FAILURE - Proxy verification failed after restart")
                logger.error(f"Worker {worker_id}: RECOVERY PROTECTION - Preventing target site access with leaked IP")
                
                # Send critical security notification about recovery failure
                send_error_notification(
                    error_message="CRITICAL SECURITY: Recovery Proxy Verification Failed",
                    error_details=f"Worker {worker_id} proxy verification failed after recovery. Target site access blocked to prevent IP leak. Details: {verification_message}",
                    context={
                        "worker_id": worker_id,
                        "proxy_port": proxy_port,
                        "verification_message": verification_message,
                        "security_action": "Recovery failed - target site access blocked",
                        "recovery_stage": "post_restart_verification"
                    }
                )
                
                raise Exception(f"SECURITY: Proxy verification failed after restart - IP leak protection: {verification_message}")
            
            logger.info(f"Worker {worker_id}: RECOVERY SECURITY PASSED - Proxy verification successful: {verification_message}")
            logger.info(f"Worker {worker_id}: RECOVERY SAFE TO PROCEED - Target site access with verified secure proxy")
            
            # Step 6: ONLY navigate to target site AFTER recovery proxy verification passes
            logger.info(f"Worker {worker_id}: Accessing target site with recovered secure proxy")
            self._navigate_to_search_page(new_driver, worker_id)
            logger.info(f"Worker {worker_id}: Recovery complete - target site accessed securely")
            
            logger.info(f"Worker {worker_id}: Connection recovery successful")
            recovery_success = True
            
        except Exception as e:
            logger.error(f"Worker {worker_id}: Connection recovery failed: {str(e)}")
            
            # Clean up partial recovery attempts
            if new_driver:
                try:
                    new_driver.quit()
                except:
                    pass
            if new_proxy_runner:
                try:
                    new_proxy_runner.stop()
                except:
                    pass
            
            new_driver = None
            new_proxy_runner = None
        
        return recovery_success, new_proxy_runner, new_driver
    
    def _worker(self, worker_id):
        """Worker thread function that processes search patterns"""
        logger.info(f"Worker {worker_id} started")
        
        # Set unique proxy port for this worker (each worker gets its own port)
        proxy_port = 8081 + worker_id
        
        # Create unique proxy runner for this worker
        proxy_runner = None
        driver = None
        
        # Keep track of patterns processed by this worker
        patterns_processed_by_worker = []
        
        try:
            # Create and start unique proxy runner for this worker
            logger.info(f"Worker {worker_id}: Initializing unique proxy on port {proxy_port}")
            proxy_runner = LocalProxyRunner(local_port=proxy_port)
            proxy_runner.start()
            
            # Give proxy more time to start up
            logger.info(f"Worker {worker_id}: Waiting for proxy to initialize on port {proxy_port}")
            time.sleep(5)  # Give proxy time to start properly
            
            # Set environment variable for this worker's proxy
            worker_env = os.environ.copy()
            worker_env["PROXY_PORT"] = str(proxy_port)
            worker_env["HTTP_PROXY"] = f"http://127.0.0.1:{proxy_port}"
            worker_env["HTTPS_PROXY"] = f"http://127.0.0.1:{proxy_port}"
            
            # Initialize Chrome driver once for this worker with unique proxy
            try:
                driver = self._setup_driver(proxy_port, worker_id)
                logger.info(f"Worker {worker_id}: Successfully initialized Chrome driver with unique proxy on port {proxy_port}")
                
                # CRITICAL SECURITY: Comprehensive proxy verification BEFORE accessing any target sites
                logger.warning(f"Worker {worker_id}: SECURITY CHECK - Verifying proxy before accessing target site")
                proxy_verification_passed, verification_message = self._comprehensive_proxy_verification(
                    proxy_runner, driver, worker_id, proxy_port
                )
                
                if not proxy_verification_passed:
                    logger.error(f"Worker {worker_id}: SECURITY FAILURE - Proxy verification failed: {verification_message}")
                    logger.error(f"Worker {worker_id}: SECURITY PROTECTION - Worker stopped to prevent IP leak to target site")
                    
                    # Send critical security notification
                    send_error_notification(
                        error_message="CRITICAL SECURITY: Proxy Verification Failed - IP Leak Prevention",
                        error_details=f"Worker {worker_id} proxy verification failed. Worker stopped before accessing target site to prevent real IP exposure. Details: {verification_message}",
                        context={
                            "worker_id": worker_id,
                            "proxy_port": proxy_port,
                            "verification_message": verification_message,
                            "security_action": "Worker stopped to prevent target site access with leaked IP"
                        }
                    )
                    
                    # This is a critical security issue - don't continue
                    logger.error(f"Worker {worker_id}: Terminating worker - IP leak protection activated")
                    return
                else:
                    logger.info(f"Worker {worker_id}: SECURITY PASSED - Proxy verification successful: {verification_message}")
                    logger.info(f"Worker {worker_id}: SAFE TO PROCEED - Real IP protected, now accessing target site")
                
                # ONLY navigate to target site AFTER proxy verification passes
                logger.info(f"Worker {worker_id}: Navigating to target site with verified secure proxy")
                self._navigate_to_search_page(driver, worker_id)
                logger.info(f"Worker {worker_id}: Successfully accessed target site with secure proxy")
                
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
        consecutive_failures = 0
        max_consecutive_failures = 5  # After 5 consecutive failures, attempt recovery
        patterns_processed = 0
        comprehensive_check_interval = 50  # Perform comprehensive check every 50 patterns
        
        while True:
            try:
                # Get next search pattern from queue
                try:
                    pattern = self.search_patterns_queue.get_nowait()
                except queue.Empty:
                    logger.info(f"Worker {worker_id}: No more patterns to process")
                    break
                
                logger.info(f"Worker {worker_id}: Processing pattern: {pattern}")
                
                # Periodic comprehensive health check including IP leak testing
                if patterns_processed > 0 and patterns_processed % comprehensive_check_interval == 0:
                    logger.info(f"Worker {worker_id}: Performing periodic comprehensive health check (after {patterns_processed} patterns)")
                    health_issues = self._check_worker_health(driver, proxy_runner, worker_id, include_ip_leak_test=True)
                    
                    if health_issues:
                        logger.warning(f"Worker {worker_id}: Comprehensive health check failed: {', '.join(health_issues)}")
                        
                        # Check if any issues are IP leak related (critical)
                        ip_leak_issues = [issue for issue in health_issues if 'ip leak' in issue.lower()]
                        if ip_leak_issues:
                            logger.error(f"Worker {worker_id}: Critical IP leak detected during periodic check, forcing recovery")
                            
                            # Send notification about IP leak
                            send_error_notification(
                                error_message="IP Leak Detected During Operation",
                                error_details=f"Worker {worker_id} detected IP leak during periodic check: {', '.join(ip_leak_issues)}",
                                context={
                                    "worker_id": worker_id,
                                    "patterns_processed": patterns_processed,
                                    "health_issues": health_issues
                                }
                            )
                        
                        # Attempt recovery for any health issues
                        recovery_success, new_proxy_runner, new_driver = self._recover_worker_connection(
                            worker_id, proxy_port, proxy_runner, driver
                        )
                        
                        if recovery_success:
                            proxy_runner = new_proxy_runner
                            driver = new_driver
                            consecutive_failures = 0
                            logger.info(f"Worker {worker_id}: Recovery successful after comprehensive check")
                        else:
                            logger.error(f"Worker {worker_id}: Recovery failed after comprehensive check, worker will exit")
                            self.search_patterns_queue.put(pattern)
                            break
                    else:
                        logger.info(f"Worker {worker_id}: Comprehensive health check passed")
                
                # Regular health check for consecutive failures
                if consecutive_failures >= max_consecutive_failures:
                    logger.warning(f"Worker {worker_id}: {consecutive_failures} consecutive failures detected, performing health check")
                    health_issues = self._check_worker_health(driver, proxy_runner, worker_id)
                    
                    if health_issues:
                        logger.warning(f"Worker {worker_id}: Health issues detected: {', '.join(health_issues)}")
                        
                        # Attempt connection recovery
                        recovery_success, new_proxy_runner, new_driver = self._recover_worker_connection(
                            worker_id, proxy_port, proxy_runner, driver
                        )
                        
                        if recovery_success:
                            # Update references to new components
                            proxy_runner = new_proxy_runner
                            driver = new_driver
                            consecutive_failures = 0
                            logger.info(f"Worker {worker_id}: Recovery successful, continuing with processing")
                        else:
                            logger.error(f"Worker {worker_id}: Recovery failed, worker will exit")
                            # Put the pattern back in the queue for another worker
                            self.search_patterns_queue.put(pattern)
                            break
                    else:
                        logger.info(f"Worker {worker_id}: Health check passed, continuing")
                        consecutive_failures = 0
                
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
                patterns_processed_by_worker.append(pattern)
                
                # Retry logic for failed patterns
                max_retries = 3
                retry_count = 0
                pattern_completed = False
                
                while retry_count < max_retries and not pattern_completed:
                    try:
                        # Always navigate back to search page before processing each pattern
                        # (not just on retries - we need a clean starting point)
                        if retry_count > 0:
                            logger.info(f"Worker {worker_id}: Retrying pattern {pattern} (attempt {retry_count + 1}/{max_retries})")
                        else:
                            logger.info(f"Worker {worker_id}: Starting fresh attempt for pattern {pattern}")
                        
                        # Navigate back to search page for every attempt (including first)
                        self._navigate_to_search_page(driver, worker_id)
                        time.sleep(2)
                        
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
                            # Wait for the page to stabilize after search button click
                            logger.info(f"Worker {worker_id}: Waiting for page to stabilize after search...")
                            time.sleep(3)
                            
                            # Check if we're still on the same page or if there was a redirect
                            current_url = driver.current_url
                            logger.info(f"Worker {worker_id}: Current URL after search: {current_url}")
                            
                            # If the page has redirected or reloaded, wait for it to complete
                            if "loading" in current_url.lower() or "redirect" in current_url.lower():
                                logger.info(f"Worker {worker_id}: Page appears to be redirecting, waiting longer...")
                                time.sleep(5)
                            
                            # FIRST: Check if this is a "no results found" scenario
                            # This happens when the page reloads with the search pattern still in the field
                            no_results_detected = self._detect_no_results_scenario(driver, pattern, worker_id)
                            
                            if no_results_detected:
                                logger.info(f"Worker {worker_id}: No results found for pattern {pattern} - this is a successful search with 0 records")
                                results = []  # Empty results but successful search
                                
                                # Mark pattern as completed with no results
                                with self.lock:
                                    self.completed_patterns.add(pattern)
                                    self.no_results_patterns.add(pattern)
                                    self.shared_data_store["search_patterns_completed"].add(pattern)
                                    self.shared_data_store["search_patterns_no_results"].add(pattern)
                                    self._save_checkpoint()
                                
                                logger.info(f"Worker {worker_id}: Pattern {pattern} completed successfully with 0 records (no results found)")
                                pattern_completed = True  # Mark as successfully completed
                            else:
                                # Try multiple strategies to detect search results
                                search_results_found = False
                                
                                # Strategy 1: Look for the specific results table
                                try:
                                    WebDriverWait(driver, 30).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "body > table:nth-child(2) > tbody > tr > td > table:nth-child(4) > tbody > tr > td > table:nth-child(7) > tbody > tr > td > table > tbody > tr:nth-child(1)"))
                        )
                                    search_results_found = True
                                    logger.info(f"Worker {worker_id}: Found search results table (Strategy 1)")
                                except:
                                    logger.warning(f"Worker {worker_id}: Strategy 1 failed, trying alternative selectors...")
                                
                                # Strategy 2: Look for any table with search results
                                if not search_results_found:
                                    try:
                                        WebDriverWait(driver, 20).until(
                                            EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr td table tbody"))
                                        )
                                        search_results_found = True
                                        logger.info(f"Worker {worker_id}: Found search results table (Strategy 2)")
                                    except:
                                        logger.warning(f"Worker {worker_id}: Strategy 2 failed, trying text-based detection...")
                                
                                # Strategy 3: Look for text indicating results or no results
                                if not search_results_found:
                                    try:
                                        WebDriverWait(driver, 15).until(
                                            lambda d: ("account" in d.page_source.lower() or 
                                                     "no records found" in d.page_source.lower() or
                                                     "no results" in d.page_source.lower())
                                        )
                                        search_results_found = True
                                        logger.info(f"Worker {worker_id}: Found search results via text detection (Strategy 3)")
                                    except:
                                        logger.warning(f"Worker {worker_id}: All strategies failed, will proceed with extraction attempt...")
                                        search_results_found = True  # Proceed anyway
                                
                                if search_results_found:
                                    logger.info(f"Worker {worker_id}: Search results loaded for pattern {pattern}")
                        
                        # Extract data from the search results
                        results = self._extract_search_results(driver, worker_id)
                                else:
                                    logger.warning(f"Worker {worker_id}: Could not confirm search results loaded, but will attempt extraction...")
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
                            
                            logger.info(f"Worker {worker_id}: Pattern {pattern} completed successfully with {len(results) if results else 0} records")
                            pattern_completed = True  # Mark as successfully completed
                            consecutive_failures = 0  # Reset consecutive failure counter on success
                        
                    except Exception as e:
                            logger.error(f"Worker {worker_id}: Error waiting for search results for pattern {pattern}: {str(e)}")
                            consecutive_failures += 1
                            
                            # Check if this is a critical error that suggests connection issues
                            error_str = str(e).lower()
                            critical_errors = ['connection refused', 'timeout', 'unreachable', 'network', 'connection reset', 'session not created']
                            is_critical_error = any(error in error_str for error in critical_errors)
                            
                            if is_critical_error:
                                logger.warning(f"Worker {worker_id}: Critical connection error detected, attempting immediate recovery")
                                health_issues = self._check_worker_health(driver, proxy_runner, worker_id)
                                
                                if health_issues:
                                    logger.warning(f"Worker {worker_id}: Health issues confirmed: {', '.join(health_issues)}")
                                    
                                    # Attempt immediate recovery for critical errors
                                    recovery_success, new_proxy_runner, new_driver = self._recover_worker_connection(
                                        worker_id, proxy_port, proxy_runner, driver
                                    )
                                    
                                    if recovery_success:
                                        proxy_runner = new_proxy_runner
                                        driver = new_driver
                                        consecutive_failures = 0
                                        logger.info(f"Worker {worker_id}: Immediate recovery successful, retrying pattern")
                                        # Continue to retry the pattern
                                    else:
                                        logger.error(f"Worker {worker_id}: Immediate recovery failed")
                                        # Pattern will be marked as failed and worker may exit
                            
                            # Take a screenshot for debugging if possible
                            try:
                                screenshot_path = f"error_screenshots/worker_{worker_id}_pattern_{pattern}_attempt_{retry_count + 1}.png"
                                os.makedirs(os.path.dirname(screenshot_path), exist_ok=True)
                                driver.save_screenshot(screenshot_path)
                                logger.info(f"Worker {worker_id}: Screenshot saved to {screenshot_path}")
                            except Exception as screenshot_e:
                                logger.warning(f"Worker {worker_id}: Could not save screenshot: {str(screenshot_e)}")
                            
                            # Log current URL and page title for debugging
                            try:
                                current_url = driver.current_url
                                page_title = driver.title
                                logger.info(f"Worker {worker_id}: Current URL: {current_url}")
                                logger.info(f"Worker {worker_id}: Page title: {page_title}")
                            except Exception as debug_e:
                                logger.warning(f"Worker {worker_id}: Could not get debug info: {str(debug_e)}")
                            
                            retry_count += 1
                            if retry_count >= max_retries:
                                logger.error(f"Worker {worker_id}: Pattern {pattern} failed after {max_retries} attempts, marking as failed")
                                # Mark as failed instead of completed to distinguish from successful patterns
                        with self.lock:
                                    self.failed_patterns.add(pattern)
                                    self.shared_data_store["search_patterns_failed"].add(pattern)
                            self._save_checkpoint()
                                pattern_completed = True
                            else:
                                logger.warning(f"Worker {worker_id}: Will retry pattern {pattern} (attempt {retry_count + 1}/{max_retries})")
                                time.sleep(5)  # Wait before retry
                
                except Exception as e:
                    logger.error(f"Worker {worker_id}: Error during search for pattern {pattern}: {str(e)}")
                    logger.error(f"Worker {worker_id}: {traceback.format_exc()}")
                        consecutive_failures += 1
                        retry_count += 1
                        
                        # Check if this is a critical error that suggests connection issues
                        error_str = str(e).lower()
                        critical_errors = ['connection refused', 'timeout', 'unreachable', 'network', 'connection reset', 'session not created', 'chrome not reachable']
                        is_critical_error = any(error in error_str for error in critical_errors)
                        
                        if is_critical_error:
                            logger.warning(f"Worker {worker_id}: Critical connection error detected during pattern processing, attempting immediate recovery")
                            health_issues = self._check_worker_health(driver, proxy_runner, worker_id)
                            
                            if health_issues or is_critical_error:  # Attempt recovery if health issues or critical error
                                logger.warning(f"Worker {worker_id}: Attempting immediate recovery due to critical error")
                                
                                # Attempt immediate recovery for critical errors
                                recovery_success, new_proxy_runner, new_driver = self._recover_worker_connection(
                                    worker_id, proxy_port, proxy_runner, driver
                                )
                                
                                if recovery_success:
                                    proxy_runner = new_proxy_runner
                                    driver = new_driver
                                    consecutive_failures = 0
                                    logger.info(f"Worker {worker_id}: Immediate recovery successful, retrying pattern")
                                    # Continue to retry the pattern
                                else:
                                    logger.error(f"Worker {worker_id}: Immediate recovery failed, marking pattern as failed")
                                    # Mark pattern as failed and exit retry loop
                                    retry_count = max_retries
                        
                        if retry_count >= max_retries:
                            logger.error(f"Worker {worker_id}: Pattern {pattern} failed after {max_retries} attempts, marking as failed")
                            # Mark as failed instead of completed to distinguish from successful patterns
                    with self.lock:
                                self.failed_patterns.add(pattern)
                                self.shared_data_store["search_patterns_failed"].add(pattern)
                        self._save_checkpoint()
                            pattern_completed = True
                        else:
                            logger.warning(f"Worker {worker_id}: Will retry pattern {pattern} after error (attempt {retry_count + 1}/{max_retries})")
                            time.sleep(5)  # Wait before retry
                
                # Mark task as done only once per pattern
                    self.search_patterns_queue.task_done()
                    
                # Increment pattern counter for periodic checks
                patterns_processed += 1
                
                # Log completion of the current pattern
                logger.info(f"Worker {worker_id}: Completed processing pattern {pattern} (total processed: {patterns_processed})")
                    
            except Exception as e:
                logger.error(f"Worker {worker_id}: Unexpected error: {str(e)}")
                logger.error(f"Worker {worker_id}: {traceback.format_exc()}")
                consecutive_failures += 1
                
                # Check if this is a critical error that warrants immediate recovery
                error_str = str(e).lower()
                critical_errors = ['connection refused', 'timeout', 'unreachable', 'network', 'connection reset', 'session not created', 'chrome not reachable']
                is_critical_error = any(error in error_str for error in critical_errors)
                
                if is_critical_error and consecutive_failures >= 2:  # Lower threshold for unexpected errors
                    logger.warning(f"Worker {worker_id}: Critical unexpected error detected, attempting recovery")
                    health_issues = self._check_worker_health(driver, proxy_runner, worker_id)
                    
                    if health_issues or is_critical_error:
                        logger.warning(f"Worker {worker_id}: Attempting recovery due to unexpected critical error")
                        
                        # Attempt recovery
                        recovery_success, new_proxy_runner, new_driver = self._recover_worker_connection(
                            worker_id, proxy_port, proxy_runner, driver
                        )
                        
                        if recovery_success:
                            proxy_runner = new_proxy_runner
                            driver = new_driver
                            consecutive_failures = 0
                            logger.info(f"Worker {worker_id}: Recovery successful after unexpected error")
                        else:
                            logger.error(f"Worker {worker_id}: Recovery failed after unexpected error, worker may become unreliable")
                
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
                
        # Log summary of patterns processed by this worker
        if patterns_processed_by_worker:
            logger.info(f"Worker {worker_id}: Processed {len(patterns_processed_by_worker)} patterns")
            if len(patterns_processed_by_worker) <= 10:
                logger.info(f"Worker {worker_id}: Patterns processed: {', '.join(patterns_processed_by_worker)}")
            else:
                logger.info(f"Worker {worker_id}: First pattern: {patterns_processed_by_worker[0]}, Last pattern: {patterns_processed_by_worker[-1]}")
                
        logger.info(f"Worker {worker_id}: Finished processing all patterns")
    
    def _setup_driver(self, proxy_port, worker_id):
        """Set up Chrome driver with unique proxy configuration for local development"""
        try:
            logger.info(f"Worker {worker_id}: Setting up Chrome driver with proxy on port {proxy_port}")
            
            # Set up Chrome options with unique proxy - LOCAL DEVELOPMENT MODE
            chrome_options = Options()
            
            # Local development options (visible browser)
            # chrome_options.add_argument("--headless")  # REMOVED: Run with visible browser for development
            chrome_options.add_argument("--no-sandbox")  # Required for stability
            chrome_options.add_argument("--disable-dev-shm-usage")  # Required for stability
            # chrome_options.add_argument("--disable-gpu")  # REMOVED: Allow GPU for better performance
            chrome_options.add_argument("--remote-debugging-port=0")  # Avoid port conflicts
            
            # Window size for visible mode
            chrome_options.add_argument("--window-size=1200,800")
            
            # Configure unique proxy for this worker
            proxy_url = f"http://127.0.0.1:{proxy_port}"
            chrome_options.add_argument(f"--proxy-server={proxy_url}")
            logger.info(f"Worker {worker_id}: Configured Chrome to use proxy: {proxy_url}")
            
            # Performance and stability options (keeping minimal for development)
            chrome_options.add_argument("--disable-extensions")
            # chrome_options.add_argument("--disable-plugins")  # REMOVED: Allow plugins for debugging
            # chrome_options.add_argument("--disable-images")  # REMOVED: Show images for visual debugging
            
            # Memory and process optimization (reduced for development)
            chrome_options.add_argument("--disable-background-timer-throttling")
            chrome_options.add_argument("--disable-renderer-backgrounding")
            chrome_options.add_argument("--disable-backgrounding-occluded-windows")
            
            # Add user agent to identify different workers
            chrome_options.add_argument(f"--user-agent=GalvestonScraper-Local-Worker-{worker_id}")
            
            # Position windows for multiple workers (if running multiple threads)
            if worker_id > 0:
                x_offset = worker_id * 200
                y_offset = worker_id * 100
                chrome_options.add_argument(f"--window-position={x_offset},{y_offset}")
            
            # Initialize browser with unique configuration - Use direct path to chromedriver
            try:
                # Use the local chromedriver.exe in the project root directory
                chromedriver_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'chromedriver.exe')
                service = Service(executable_path=chromedriver_path)
                driver = webdriver.Chrome(service=service, options=chrome_options)
                logger.info(f"Worker {worker_id}: Chrome driver initialized with local chromedriver.exe (visible mode)")
            except Exception as e:
                logger.error(f"Worker {worker_id}: Failed to initialize with local chromedriver.exe: {str(e)}")
                # Fallback to default Chrome setup
                service = Service()
                driver = webdriver.Chrome(service=service, options=chrome_options)
                logger.info(f"Worker {worker_id}: Chrome driver initialized with default setup (visible mode)")
            
            # Set timeouts - increased for better handling of page reloads after search
            driver.implicitly_wait(15)
            driver.set_page_load_timeout(120)
            
            logger.info(f"Worker {worker_id}: Chrome driver ready with proxy configuration (visible mode)")
            
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
        """Extract data from Galveston search results with robust element detection"""
        records = []
        
        try:
            # Try multiple strategies to find the results table
            results_table = None
            
            # Strategy 1: Try the specific selector first
            try:
                results_table = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "body > table:nth-child(2) > tbody > tr > td > table:nth-child(4) > tbody > tr > td > table:nth-child(7) > tbody > tr > td > table > tbody"))
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
                    # Header rows typically contain text like "Account", "Owner", etc. instead of actual data
                    first_cell_text = cells[0].text.strip().lower()
                    if first_cell_text in ['account', 'account number', 'account #', 'owner', 'property']:
                        logger.debug(f"Worker {worker_id}: Skipping row {i} - appears to be header row ('{first_cell_text}')")
                        continue
                    
                    # Try to extract account number - if it fails, this might be a header or invalid row
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
                    owner_name, mailing_address = self._separate_owner_and_address(owner_mailing_text)
                    
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
    
    def _detect_no_results_scenario(self, driver, pattern, worker_id):
        """
        Detect if the search returned no results by checking for:
        1. Primary: Direct "no results" message using specific selector
        2. Fallback: Check if we're back on search page with pattern still in field
        """
        try:
            # Wait a moment for the page to fully load
            time.sleep(2)
            
            # PRIMARY METHOD: Look for the specific "no results" message using the direct selector
            try:
                no_results_element = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "body > table:nth-child(2) > tbody > tr > td > table:nth-child(4) > tbody > tr > td > p:nth-child(5) > table:nth-child(6) > tbody > tr > td > center > form > table > tbody > tr:nth-child(2) > td:nth-child(2) > h3 > font > h6 > div"))
                )
                
                no_results_text = no_results_element.text.strip().lower()
                logger.info(f"Worker {worker_id}: Found no results message element with text: '{no_results_text}'")
                
                # Check if the message contains "no records" or similar text
                if "no records" in no_results_text or "please try again" in no_results_text:
                    logger.info(f"Worker {worker_id}: Direct detection - No results found for pattern {pattern} (message: '{no_results_text}')")
                    return True
                    
            except Exception as selector_e:
                logger.debug(f"Worker {worker_id}: Direct selector method failed: {str(selector_e)} - trying fallback methods")
            
            # FALLBACK METHOD 1: Look for "no results" text anywhere on the page (faster than original method)
            try:
                page_source = driver.page_source.lower()
                no_results_indicators = [
                    "your search found no records",
                    "no records found",
                    "no results found", 
                    "please try again",
                    "no matches",
                    "0 records"
                ]
                
                for indicator in no_results_indicators:
                    if indicator in page_source:
                        logger.info(f"Worker {worker_id}: Text-based detection - No results found for pattern {pattern} (found: '{indicator}')")
                        return True
                        
            except Exception as text_e:
                logger.debug(f"Worker {worker_id}: Text-based detection failed: {str(text_e)}")
            
            # FALLBACK METHOD 2: Original logic - check if we're back on search page with pattern in field
            try:
                search_input = driver.find_element(By.CSS_SELECTOR, "#criteria")
                current_search_value = search_input.get_attribute("value")
                
                # Remove the '%' for comparison since the pattern includes it
                pattern_without_percent = pattern.rstrip('%')
                current_search_without_percent = current_search_value.rstrip('%')
                
                logger.debug(f"Worker {worker_id}: Search field value after search: '{current_search_value}', Expected pattern: '{pattern}'")
                
                # Check if the search field contains our pattern (case-insensitive)
                if pattern_without_percent.lower() in current_search_without_percent.lower():
                    # We're back on search page with our pattern still in the field
                    
                    # Check for absence of results table
                    results_table_present = False
                    try:
                        driver.find_element(By.CSS_SELECTOR, "body > table:nth-child(2) > tbody > tr > td > table:nth-child(4) > tbody > tr > td > table:nth-child(7) > tbody > tr > td > table > tbody > tr:nth-child(1)")
                        results_table_present = True
                    except:
                        pass
                    
                    # If we're on search page with our pattern AND no results table
                    if not results_table_present:
                        logger.info(f"Worker {worker_id}: Fallback detection - No results scenario detected for pattern {pattern} (search page + no results table)")
                        return True
                    else:
                        logger.debug(f"Worker {worker_id}: Search pattern found in field but results table is present - continuing with normal processing")
                        return False
                else:
                    logger.debug(f"Worker {worker_id}: Search field does not contain our pattern - continuing with normal processing")
                    return False
                    
            except Exception as fallback_e:
                logger.debug(f"Worker {worker_id}: Fallback detection failed: {str(fallback_e)} - continuing with normal processing")
                return False
            
            # If none of the methods detected "no results", assume there are results
            logger.debug(f"Worker {worker_id}: No 'no results' scenario detected for pattern {pattern} - continuing with normal processing")
            return False
                
        except Exception as e:
            logger.warning(f"Worker {worker_id}: Error in no results detection: {str(e)} - continuing with normal processing")
            return False
    
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
        """Navigate to the Galveston County search page with retries and proper page validation"""
        max_retries = 5
        for retry in range(max_retries):
            try:
                logger.info(f"Worker {worker_id}: Navigating to Galveston search page (attempt {retry + 1}/{max_retries})")
                
                # Use a longer timeout for initial navigation
                driver.set_page_load_timeout(120)
                driver.get("https://actweb.acttax.com/act_webdev/galveston/index.jsp")
                
                # Wait for the page to be fully loaded and ready
                WebDriverWait(driver, 60).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#criteria"))
                )
                
                # Additional check: ensure the search form is actually interactive
                search_box = driver.find_element(By.CSS_SELECTOR, "#criteria")
                if search_box.is_enabled() and search_box.is_displayed():
                    logger.info(f"Worker {worker_id}: Successfully navigated to Galveston search page and form is ready")
                    
                    # Clear any existing value in the search box
                    driver.execute_script("arguments[0].value = '';", search_box)
                    
                    # Scroll to ensure search area is visible
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", search_box)
                    time.sleep(1)
                    
                return True
                else:
                    logger.warning(f"Worker {worker_id}: Search form not ready (enabled: {search_box.is_enabled()}, displayed: {search_box.is_displayed()})")
                    if retry < max_retries - 1:
                        time.sleep(3)
                        continue
                    
            except Exception as e:
                if retry < max_retries - 1:
                    logger.warning(f"Worker {worker_id}: Navigation retry {retry+1}/{max_retries} failed: {str(e)}")
                    time.sleep(5)  # Longer wait before retry
                else:
                    logger.error(f"Worker {worker_id}: Failed to navigate to search page after {max_retries} attempts: {str(e)}")
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
        """Click the Galveston search button with enhanced reliability"""
        # Wait a moment for the page to recognize the input
        time.sleep(2)
        
        # Ensure we're ready to interact with the page
        try:
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
        except:
            logger.warning(f"Worker {worker_id}: Page may not be fully loaded, but proceeding...")
        
        # Find and click the search button using the Galveston selector
        try:
            search_button = WebDriverWait(driver, 30).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "body > table:nth-child(2) > tbody > tr > td > table:nth-child(4) > tbody > tr > td > p:nth-child(5) > table:nth-child(6) > tbody > tr > td > center > form > table > tbody > tr:nth-child(5) > td:nth-child(2) > h3:nth-child(2) > input[type=submit]"))
            )
            
            # Scroll the button into view
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", search_button)
            time.sleep(1)
            
            # Ensure the button is actually clickable
            if not search_button.is_enabled():
                logger.warning(f"Worker {worker_id}: Search button is not enabled, waiting...")
                time.sleep(2)
            
            # Try multiple approaches to click the button
            logger.info(f"Worker {worker_id}: Attempting to click Galveston search button...")
            
            # First try: JavaScript click (most reliable for this site)
            driver.execute_script("arguments[0].click();", search_button)
            logger.info(f"Worker {worker_id}: Clicked search button via JavaScript")
            
            # Wait a brief moment to see if the click triggered a page action
            time.sleep(1)
            
            return True
            
        except Exception as e:
            logger.warning(f"Worker {worker_id}: Failed to click search button with primary method: {str(e)}")
            
            # Second try: Enter key in the search box
            try:
                search_box = driver.find_element(By.CSS_SELECTOR, "#criteria")
                search_box.send_keys(Keys.ENTER)
                logger.info(f"Worker {worker_id}: Sent ENTER key to search box")
                time.sleep(1)
                return True
            except Exception as key_e:
                logger.warning(f"Worker {worker_id}: ENTER key method also failed: {str(key_e)}")
            
            # Third try: Direct form submission
            try:
                form = driver.find_element(By.CSS_SELECTOR, "form")
                driver.execute_script("arguments[0].submit();", form)
                logger.info(f"Worker {worker_id}: Submitted form directly")
                time.sleep(1)
                return True
            except Exception as form_e:
                logger.error(f"Worker {worker_id}: All button click methods failed. Form submit error: {str(form_e)}")
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
            patterns_failed = len(self.shared_data_store.get("search_patterns_failed", set()))
            patterns_no_results = len(self.shared_data_store.get("search_patterns_no_results", set()))
            patterns_with_records = patterns_completed - patterns_no_results
            total_patterns_processed = patterns_completed + patterns_failed
            
            # Log detailed completion summary
            logger.info(f"Scraping completed: {total_records} records, {patterns_completed} patterns completed ({patterns_with_records} with records, {patterns_no_results} with no results), {patterns_failed} patterns failed, {execution_time}")
            
            if patterns_no_results > 0:
                no_results_patterns_list = sorted(list(self.shared_data_store.get("search_patterns_no_results", set())))
                if len(no_results_patterns_list) <= 10:
                    logger.info(f"No-results patterns: {', '.join(no_results_patterns_list)}")
                else:
                    logger.info(f"No-results patterns (showing first 5): {', '.join(no_results_patterns_list[:5])}")
                    logger.info(f"Total no-results patterns: {len(no_results_patterns_list)}")
            
            if patterns_failed > 0:
                failed_patterns_list = sorted(list(self.shared_data_store.get("search_patterns_failed", set())))
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
            logger.info("Galveston scraping process finished. Individual worker proxies cleaned up by respective threads.")

if __name__ == "__main__":
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description='Galveston County Tax Scraper')
    parser.add_argument('--threads', type=int, default=3, help='Number of worker threads to use (default: 3)')
    
    args = parser.parse_args()
    
    logger.info(f"Command line arguments: --threads {args.threads}")
    
    try:
        # Create and run the scraper with the specified number of threads
        scraper = GalvestonTaxScraper(num_threads=args.threads)
    scraper.run()
        
        logger.info("Galveston scraper completed successfully")
        
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