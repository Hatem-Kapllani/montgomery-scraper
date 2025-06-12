import logging
import time
import traceback
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from src.utils.email_notifier import send_error_notification

logger = logging.getLogger(__name__)

class BrowserManager:
    """Handles Chrome driver setup, proxy verification, and browser management"""
    
    @staticmethod
    def setup_driver(proxy_port, worker_id):
        """Setup Chrome driver with proxy configuration"""
        logger.info(f"Worker {worker_id}: Setting up Chrome driver with proxy port {proxy_port}")
        
        try:
            # Chrome options
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--disable-web-security")
            chrome_options.add_argument("--disable-features=VizDisplayCompositor")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
            
            # Additional flags to reduce automatic network requests
            chrome_options.add_argument("--disable-background-networking")
            chrome_options.add_argument("--disable-background-timer-throttling")
            chrome_options.add_argument("--disable-client-side-phishing-detection")
            chrome_options.add_argument("--disable-default-apps")
            chrome_options.add_argument("--disable-hang-monitor")
            chrome_options.add_argument("--disable-prompt-on-repost")
            chrome_options.add_argument("--disable-sync")
            chrome_options.add_argument("--disable-domain-reliability")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-component-update")
            chrome_options.add_argument("--disable-background-downloads")
            chrome_options.add_argument("--no-default-browser-check")
            chrome_options.add_argument("--no-first-run")
            chrome_options.add_argument("--disable-logging")
            chrome_options.add_argument("--disable-logging-redirect")
            
            # Proxy configuration
            chrome_options.add_argument(f"--proxy-server=http://127.0.0.1:{proxy_port}")
            
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
            
            logger.info(f"Worker {worker_id}: Chrome driver created successfully with proxy {proxy_port}")
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
    
    @staticmethod
    def verify_proxy_connection(driver, worker_id):
        """Comprehensive IP leak test - verifies proxy is working and real IP is hidden"""
        logger.info(f"Worker {worker_id}: Performing comprehensive IP leak test...")
        
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                # Add delay between attempts to avoid hitting rate limits
                if attempt > 0:
                    logger.info(f"Worker {worker_id}: Retry attempt {attempt + 1} after {retry_delay} seconds")
                    time.sleep(retry_delay)
                
                # Step 1: Get the real IP (without proxy) for comparison
                # We'll use environment variables to get the expected real IP
                import os
                expected_real_ip = os.getenv('REAL_IP')  # You should set this in your .env file
                
                # Step 2: Test proxy IP through multiple services for reliability
                proxy_ips = []
                test_services = [
                    "https://httpbin.org/ip",
                    "https://api.ipify.org?format=json",
                    "https://ipinfo.io/json"
                ]
                
                for service_url in test_services:
                    try:
                        logger.info(f"Worker {worker_id}: Testing IP leak via {service_url}")
                        driver.get(service_url)
                        time.sleep(3)
                        
                        page_source = driver.page_source.lower()
                        
                        # Extract IP from different service formats
                        if "httpbin.org" in service_url:
                            # httpbin.org format: {"origin": "IP_ADDRESS"}
                            if '"origin"' in page_source:
                                import re
                                ip_match = re.search(r'"origin":\s*"([^"]+)"', page_source)
                                if ip_match:
                                    proxy_ip = ip_match.group(1).strip()
                                    proxy_ips.append(proxy_ip)
                                    logger.info(f"Worker {worker_id}: Detected proxy IP via httpbin: {proxy_ip}")
                        
                        elif "ipify.org" in service_url:
                            # ipify format: {"ip":"IP_ADDRESS"}
                            if '"ip"' in page_source:
                                import re
                                ip_match = re.search(r'"ip":\s*"([^"]+)"', page_source)
                                if ip_match:
                                    proxy_ip = ip_match.group(1).strip()
                                    proxy_ips.append(proxy_ip)
                                    logger.info(f"Worker {worker_id}: Detected proxy IP via ipify: {proxy_ip}")
                        
                        elif "ipinfo.io" in service_url:
                            # ipinfo format: {"ip":"IP_ADDRESS",...}
                            if '"ip"' in page_source:
                                import re
                                ip_match = re.search(r'"ip":\s*"([^"]+)"', page_source)
                                if ip_match:
                                    proxy_ip = ip_match.group(1).strip()
                                    proxy_ips.append(proxy_ip)
                                    logger.info(f"Worker {worker_id}: Detected proxy IP via ipinfo: {proxy_ip}")
                        
                    except Exception as service_error:
                        logger.warning(f"Worker {worker_id}: Failed to test {service_url}: {str(service_error)}")
                        continue
                
                # Step 3: Analyze results for IP leaks
                if not proxy_ips:
                    logger.error(f"Worker {worker_id}: 🚨 CRITICAL: No proxy IPs detected - proxy may be completely broken")
                    return False, "No proxy IPs detected from any service"
                
                # Check for IP leak - the ONLY thing that matters is real IP not being exposed
                unique_ips = list(set(proxy_ips))
                
                # Step 4: Check for IP leak (if real IP is known)
                if expected_real_ip:
                    # Check if ANY of the detected IPs match our real IP
                    real_ip_exposed = any(ip == expected_real_ip for ip in unique_ips)
                    
                    if real_ip_exposed:
                        logger.error(f"Worker {worker_id}: 🚨 IP LEAK DETECTED! Real IP exposed: {expected_real_ip}")
                        return False, f"IP LEAK: Real IP {expected_real_ip} is exposed through proxy"
                    else:
                        # Real IP is hidden - proxy rotation is normal and acceptable
                        if len(unique_ips) > 1:
                            logger.info(f"Worker {worker_id}: ✅ PROXY ROTATION DETECTED: Multiple proxy IPs {unique_ips} - this is normal")
                        else:
                            logger.info(f"Worker {worker_id}: ✅ CONSISTENT PROXY: Using proxy IP {unique_ips[0]}")
                        logger.info(f"Worker {worker_id}: ✅ IP LEAK TEST PASSED: Real IP {expected_real_ip} is properly hidden")
                else:
                    # No real IP configured - assume 45.88.222.69 based on previous detection
                    assumed_real_ip = "45.88.222.69"
                    real_ip_exposed = any(ip == assumed_real_ip for ip in unique_ips)
                    
                    if real_ip_exposed:
                        logger.error(f"Worker {worker_id}: 🚨 IP LEAK DETECTED! Assumed real IP exposed: {assumed_real_ip}")
                        return False, f"IP LEAK: Assumed real IP {assumed_real_ip} is exposed through proxy"
                    else:
                        logger.info(f"Worker {worker_id}: ✅ IP LEAK TEST PASSED: Assumed real IP {assumed_real_ip} is properly hidden")
                        if len(unique_ips) > 1:
                            logger.info(f"Worker {worker_id}: ✅ PROXY ROTATION DETECTED: Multiple proxy IPs {unique_ips} - this is normal")
                
                # Use the first detected IP for validation
                detected_ip = unique_ips[0]
                
                # Step 5: Verify proxy IP is valid and not a local/private IP
                if detected_ip.startswith(('127.', '192.168.', '10.', '172.')):
                    logger.error(f"Worker {worker_id}: 🚨 INVALID PROXY: Detected local/private IP: {detected_ip}")
                    return False, f"Invalid proxy IP (local/private): {detected_ip}"
                
                # Step 6: Success - proxy is working and no leaks detected
                success_message = f"Proxy working correctly - IP: {detected_ip}, Services tested: {len(proxy_ips)}"
                logger.info(f"Worker {worker_id}: ✅ SECURITY VERIFIED: {success_message}")
                return True, success_message
                    
            except Exception as e:
                logger.error(f"Worker {worker_id}: IP leak test error on attempt {attempt + 1}: {str(e)}")
                if attempt < max_retries - 1:
                    continue
                else:
                    return False, f"IP leak test failed after all retries: {str(e)}"
        
        return False, "IP leak test failed after all retries"
    
    @staticmethod
    def navigate_to_search_page(driver, worker_id):
        """Navigate to Montgomery County search page"""
        search_url = "https://actweb.acttax.com/act_webdev/montgomery/index.jsp"
        
        try:
            logger.info(f"Worker {worker_id}: Navigating to Montgomery search page")
            driver.get(search_url)
            
            # Wait for page to load
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#criteria"))
            )
            
            logger.info(f"Worker {worker_id}: Successfully loaded Montgomery search page")
            return True
            
        except TimeoutException:
            logger.error(f"Worker {worker_id}: Timeout waiting for search page to load")
            return False
        except Exception as e:
            logger.error(f"Worker {worker_id}: Error navigating to search page: {str(e)}")
            return False
    
    @staticmethod
    def enter_search_pattern(driver, pattern, worker_id):
        """Enter search pattern in the search field"""
        try:
            # Find and clear the search field
            search_field = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#criteria"))
            )
            
            # Clear any existing text
            search_field.clear()
            time.sleep(1)
            
            # Enter the search pattern
            search_field.send_keys(pattern)
            time.sleep(1)
            
            logger.info(f"Worker {worker_id}: Entered search pattern: {pattern}")
            return True
            
        except Exception as e:
            logger.error(f"Worker {worker_id}: Error entering search pattern {pattern}: {str(e)}")
            return False
    
    @staticmethod
    def click_search_button(driver, worker_id):
        """Click the search button"""
        try:
            # Find and click the search button
            search_button = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 
                    "#content > table > tbody > tr:nth-child(1) > td > div:nth-child(3) > table > tbody > tr > td > center > form > table > tbody > tr:nth-child(3) > td:nth-child(2) > h3 > input[type=submit]"))
            )
            
            search_button.click()
            time.sleep(3)
            
            logger.info(f"Worker {worker_id}: Clicked search button")
            return True
            
        except Exception as e:
            logger.error(f"Worker {worker_id}: Error clicking search button: {str(e)}")
            return False
    
    @staticmethod
    def check_for_no_results(driver, worker_id):
        """Check if search returned no results"""
        try:
            # Check for "no results" message using the specific selector
            no_results_element = driver.find_element(By.CSS_SELECTOR, 
                "#content > table > tbody > tr:nth-child(1) > td > div:nth-child(3) > table > tbody > tr > td > center > form > table > tbody > tr:nth-child(1) > td:nth-child(2) > h3 > font > h6 > div")
            
            if no_results_element and "no records" in no_results_element.text.lower():
                logger.info(f"Worker {worker_id}: Search returned no results")
                return True
                
        except Exception:
            # Element not found, check page source as fallback
            try:
                page_text = driver.page_source.lower()
                if "no records found" in page_text or "no results" in page_text or "your search found no records" in page_text:
                    logger.info(f"Worker {worker_id}: Search returned no results (detected in page source)")
                    return True
            except Exception as e:
                logger.warning(f"Worker {worker_id}: Error checking for no results: {str(e)}")
        
        return False 