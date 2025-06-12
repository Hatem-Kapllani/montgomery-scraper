import logging
import time
import traceback
import socket
import sys
import os

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
# Add current directory to path for local imports
sys.path.append(os.path.dirname(__file__))

# Fix: Use absolute import from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from local_proxy import LocalProxyRunner
from browser_manager import BrowserManager
from src.utils.email_notifier import send_error_notification

logger = logging.getLogger(__name__)

class WorkerHealthManager:
    """Handles worker health monitoring and recovery operations"""
    
    @staticmethod
    def verify_proxy_ports_available(num_threads):
        """Verify that proxy ports are available before starting workers"""
        available_ports = []
        for i in range(num_threads):
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
        
        if len(available_ports) < num_threads:
            logger.warning(f"Only {len(available_ports)} ports available, reducing threads from {num_threads}")
        
        return available_ports[:num_threads]
    
    @staticmethod
    def check_worker_health(driver, proxy_runner, worker_id, include_ip_leak_test=False):
        """Check worker health including proxy status and optional IP leak test"""
        health_issues = []
        
        try:
            # Check if driver is responsive
            try:
                current_url = driver.current_url
                logger.debug(f"Worker {worker_id}: Driver responsive, current URL: {current_url}")
            except Exception as e:
                health_issues.append(f"Driver unresponsive: {str(e)}")
                logger.warning(f"Worker {worker_id}: Driver health check failed: {str(e)}")
            
            # Check proxy runner status
            if proxy_runner and hasattr(proxy_runner, 'is_running'):
                if not proxy_runner.is_running():
                    health_issues.append("Proxy runner not running")
                    logger.warning(f"Worker {worker_id}: Proxy runner health check failed")
            
            # Optional IP leak test (more comprehensive but slower)
            if include_ip_leak_test:
                try:
                    logger.info(f"Worker {worker_id}: Performing IP leak test...")
                    is_secure, message = BrowserManager.verify_proxy_connection(driver, worker_id)
                    if not is_secure:
                        health_issues.append(f"IP leak detected: {message}")
                        logger.warning(f"Worker {worker_id}: IP leak test failed: {message}")
                    else:
                        logger.info(f"Worker {worker_id}: IP leak test passed: {message}")
                except Exception as e:
                    health_issues.append(f"IP leak test error: {str(e)}")
                    logger.warning(f"Worker {worker_id}: IP leak test error: {str(e)}")
            
            if health_issues:
                logger.warning(f"Worker {worker_id}: Health check found issues: {', '.join(health_issues)}")
            else:
                logger.debug(f"Worker {worker_id}: Health check passed")
                
            return health_issues
            
        except Exception as e:
            error_msg = f"Health check error: {str(e)}"
            health_issues.append(error_msg)
            logger.error(f"Worker {worker_id}: {error_msg}")
            return health_issues
    
    @staticmethod
    def attempt_worker_recovery(proxy_runner, driver, worker_id, proxy_port):
        """Attempt to recover a worker by restarting proxy and driver"""
        logger.info(f"Worker {worker_id}: Attempting recovery...")
        
        try:
            # Clean up existing resources
            if driver:
                try:
                    driver.quit()
                    logger.info(f"Worker {worker_id}: Closed existing driver")
                except Exception as e:
                    logger.warning(f"Worker {worker_id}: Error closing driver: {str(e)}")
            
            if proxy_runner:
                try:
                    proxy_runner.stop()
                    logger.info(f"Worker {worker_id}: Stopped existing proxy")
                except Exception as e:
                    logger.warning(f"Worker {worker_id}: Error stopping proxy: {str(e)}")
            
            # Wait for cleanup
            time.sleep(5)
            
            # Start new proxy
            logger.info(f"Worker {worker_id}: Starting new proxy on port {proxy_port}")
            new_proxy_runner = LocalProxyRunner(local_port=proxy_port)
            new_proxy_runner.start()
            time.sleep(5)
            
            # Create new driver
            logger.info(f"Worker {worker_id}: Creating new driver")
            new_driver = BrowserManager.setup_driver(proxy_port, worker_id)
            
            # Verify new setup
            is_secure, message = BrowserManager.verify_proxy_connection(new_driver, worker_id)
            if not is_secure:
                logger.error(f"Worker {worker_id}: Recovery failed - proxy verification failed: {message}")
                
                # Clean up failed recovery attempt
                if new_driver:
                    new_driver.quit()
                if new_proxy_runner:
                    new_proxy_runner.stop()
                    
                return False, None, None
            
            logger.info(f"Worker {worker_id}: Recovery successful")
            return True, new_proxy_runner, new_driver
            
        except Exception as e:
            logger.error(f"Worker {worker_id}: Recovery failed: {str(e)}")
            logger.error(f"Worker {worker_id}: {traceback.format_exc()}")
            
            # Send error notification for recovery failure
            send_error_notification(
                error_message="Worker Recovery Failed",
                error_details=traceback.format_exc(),
                context={
                    "worker_id": worker_id,
                    "proxy_port": proxy_port,
                    "error": str(e)
                }
            )
            
            return False, None, None
    
    @staticmethod
    def initialize_worker_proxy_and_driver(worker_id, proxy_port):
        """Initialize proxy and driver for a worker with mandatory IP leak test"""
        proxy_runner = None
        driver = None
        
        try:
            # Create and start unique proxy runner for this worker
            logger.info(f"Worker {worker_id}: Initializing unique proxy on port {proxy_port}")
            proxy_runner = LocalProxyRunner(local_port=proxy_port)
            proxy_runner.start()
            
            # Give proxy time to start up
            logger.info(f"Worker {worker_id}: Waiting for proxy to initialize on port {proxy_port}")
            time.sleep(5)
            
            # Create Chrome driver with proxy
            logger.info(f"Worker {worker_id}: Creating Chrome driver with proxy")
            driver = BrowserManager.setup_driver(proxy_port, worker_id)
            
            # 🚨 MANDATORY COMPREHENSIVE IP LEAK TEST 🚨
            logger.info(f"Worker {worker_id}: 🔒 PERFORMING MANDATORY IP LEAK TEST...")
            is_secure, verification_message = BrowserManager.verify_proxy_connection(driver, worker_id)
            
            if not is_secure:
                logger.error(f"Worker {worker_id}: 🚨 SECURITY BREACH DETECTED!")
                logger.error(f"Worker {worker_id}: 🚨 IP LEAK TEST FAILED: {verification_message}")
                logger.error(f"Worker {worker_id}: 🚨 ABORTING WORKER - CANNOT PROCEED WITH COMPROMISED SECURITY")
                
                # Clean up immediately
                if driver:
                    driver.quit()
                if proxy_runner:
                    proxy_runner.stop()
                
                # Send critical security alert
                send_error_notification(
                    error_message="🚨 CRITICAL SECURITY ALERT: IP LEAK DETECTED",
                    error_details=f"Worker {worker_id} failed comprehensive IP leak test: {verification_message}",
                    context={
                        "worker_id": worker_id,
                        "proxy_port": proxy_port,
                        "security_breach": True,
                        "leak_details": verification_message
                    }
                )
                
                # Return None to indicate complete failure
                return None, None
            else:
                logger.info(f"Worker {worker_id}: ✅ SECURITY VERIFIED - IP LEAK TEST PASSED")
                logger.info(f"Worker {worker_id}: ✅ {verification_message}")
                logger.info(f"Worker {worker_id}: 🔒 SAFE TO PROCEED - Real IP is protected")
            
            # Navigate to target site ONLY after comprehensive security verification
            logger.info(f"Worker {worker_id}: Navigating to target site with verified secure proxy")
            if not BrowserManager.navigate_to_search_page(driver, worker_id):
                logger.error(f"Worker {worker_id}: Failed to navigate to search page")
                if driver:
                    driver.quit()
                if proxy_runner:
                    proxy_runner.stop()
                return None, None
            
            logger.info(f"Worker {worker_id}: ✅ Successfully accessed target site with verified secure proxy")
            return proxy_runner, driver
            
        except Exception as e:
            logger.error(f"Worker {worker_id}: Failed to initialize: {str(e)}")
            logger.error(f"Worker {worker_id}: {traceback.format_exc()}")
            
            # Clean up on failure
            if driver:
                try:
                    driver.quit()
                except:
                    pass
            if proxy_runner:
                try:
                    proxy_runner.stop()
                except:
                    pass
            
            # Send error notification
            send_error_notification(
                error_message="Worker Initialization Failed",
                error_details=traceback.format_exc(),
                context={
                    "worker_id": worker_id,
                    "proxy_port": proxy_port,
                    "error": str(e)
                }
            )
            
            return None, None 