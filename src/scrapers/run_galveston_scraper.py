#!/usr/bin/env python
import os
import sys
import subprocess
import logging
import argparse
import traceback
from pathlib import Path
from datetime import datetime

# Create log directory if it doesn't exist
Path("logs").mkdir(exist_ok=True)

# Set up enhanced logging with timestamp in filename
current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = f"logs/galveston_run.log"

# Configure root logger to capture logs from all modules
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - pid:%(process)d - %(threadName)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file, mode='a', encoding='utf-8')
    ]
)

# Configure our specific logger
logger = logging.getLogger("galveston_runner")

# Make sure logs from selenium, requests, and other libraries are captured at WARNING level
for module_name in ['selenium', 'urllib3', 'requests', 'WDM']:
    logging.getLogger(module_name).setLevel(logging.WARNING)

# Log start of script execution
logger.info(f"=============== STARTING GALVESTON SCRAPER ===============")
logger.info(f"Logging to: {os.path.abspath(log_file)}")

def check_venv():
    """Check if running in a virtual environment"""
    return hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix)

def create_venv():
    """Create virtual environment if it doesn't exist"""
    if os.path.exists("scrape_delinquent_tax"):
        logger.info("Virtual environment already exists")
        return
    
    logger.info("Creating virtual environment...")
    try:
        subprocess.run([sys.executable, "-m", "venv", "scrape_delinquent_tax"], check=True)
        logger.info("Virtual environment created successfully")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to create virtual environment: {str(e)}")
        sys.exit(1)

def install_dependencies():
    """Install required dependencies"""
    logger.info("Installing dependencies...")
    
    # Determine the correct pip executable
    if os.name == 'nt':  # Windows
        pip_path = os.path.join("scrape_delinquent_tax", "Scripts", "pip.exe")
    else:  # Unix/macOS
        pip_path = os.path.join("scrape_delinquent_tax", "bin", "pip")
    
    # Ensure we have the latest version of pip
    try:
        logger.info("Upgrading pip...")
        subprocess.run([pip_path, "install", "--upgrade", "pip"], check=True)
    except subprocess.CalledProcessError as e:
        logger.warning(f"Failed to upgrade pip: {str(e)}")
    
    # Create requirements.txt if it doesn't exist
    if not os.path.exists("requirements.txt"):
        logger.info("Creating requirements.txt...")
        with open("requirements.txt", "w") as f:
            f.write("selenium==4.15.2\n")
            f.write("pandas==2.0.3\n")
            f.write("webdriver-manager==4.0.1\n")
            f.write("requests==2.31.0\n")
        logger.info("requirements.txt created")
    
    # Install dependencies
    try:
        logger.info("Installing required packages...")
        subprocess.run([pip_path, "install", "-r", "requirements.txt"], check=True)
        logger.info("Dependencies installed successfully")
        
        # Additional packages that might be needed
        try:
            logger.info("Installing additional helpful packages...")
            packages = [
                "msedge-selenium-tools",  # For Microsoft Edge support
                "webdriver-manager"        # For automatic webdriver management
            ]
            for package in packages:
                try:
                    subprocess.run([pip_path, "install", package], check=True)
                    logger.info(f"Installed {package}")
                except subprocess.CalledProcessError as e:
                    logger.warning(f"Failed to install {package}: {str(e)}")
        except Exception as e:
            logger.warning(f"Failed to install additional packages: {str(e)}")
            
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to install dependencies: {str(e)}")
        sys.exit(1)

def check_chromedriver():
    """Check if chromedriver exists and is valid"""
    # We'll directly use the update_chromedriver.py script to ensure compatibility
    logger.info("Updating ChromeDriver to ensure compatibility...")
    try:
        # Run the update_chromedriver.py script
        subprocess.run([sys.executable, "update_chromedriver.py"], check=True)
        logger.info("ChromeDriver updated successfully")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to update ChromeDriver: {str(e)}")
        logger.warning("The scraper will attempt to update ChromeDriver directly during execution.")
        return False

def create_directories():
    """Create required directories if they don't exist"""
    directories = ["checkpoints", "logs", "output"]
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
        logger.info(f"Created directory: {directory}")

def run_scraper(num_threads=3):
    """Run the Galveston multithreaded tax scraper"""
    logger.info(f"Starting Galveston tax scraper with {num_threads} threads...")
    
    # Get the python executable from the virtual environment
    if os.name == 'nt':  # Windows
        python_path = os.path.join("scrape_delinquent_tax", "Scripts", "python.exe")
    else:  # Unix/macOS
        python_path = os.path.join("scrape_delinquent_tax", "bin", "python")
    
    # Build command arguments for the Galveston scraper with thread count
    cmd = [python_path, "src/scrapers/galveston_multithreaded_scraper.py", "--threads", str(num_threads)]
    
    try:
        logger.info(f"Running command: {' '.join(cmd)}")
        
        # Open the log file in append mode for subprocess output
        with open(log_file, 'a', encoding='utf-8') as log_output:
            # Run the process and redirect stdout and stderr to the log file
            process = subprocess.Popen(
                cmd,
                stdout=log_output,
                stderr=log_output,
                universal_newlines=True,
                bufsize=1  # Line buffered
            )
            
            # Wait for the process to complete
            exit_code = process.wait()
            
            if exit_code != 0:
                logger.error(f"Galveston scraper failed with exit code: {exit_code}")
                sys.exit(1)
            else:
                logger.info("Galveston scraper completed successfully")
    except Exception as e:
        logger.error(f"Galveston scraper failed: {str(e)}")
        sys.exit(1)

def main():
    """Main function to run the setup and Galveston scraper"""
    parser = argparse.ArgumentParser(description='Galveston County Tax Scraper Runner')
    parser.add_argument('--threads', type=int, help='Number of worker threads to use')
    
    args = parser.parse_args()
    
    # Get threads from environment variable or command line argument
    threads = args.threads or int(os.getenv('SCRAPER_THREADS', 3))
    
    # Check if running in server mode
    server_mode = os.getenv('SERVER_MODE', 'local').lower() == 'production'
    
    logger.info(f"Starting Galveston tax scraper setup with {threads} threads")
    logger.info(f"Server mode: {server_mode}")
    
    # Create required directories
    create_directories()
    
    if not server_mode:
        # Local development setup
        # Check and create virtual environment if needed
        if not check_venv():
            create_venv()
            install_dependencies()
        
        # Check and download ChromeDriver if needed
        check_chromedriver()
        
        # Run the Galveston scraper
        run_scraper(num_threads=threads)
    else:
        # Server deployment - run scraper directly
        logger.info("Running in server mode - skipping venv and chromedriver setup")
        try:
            # Import and run the scraper directly for better error handling
            sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
            from src.scrapers.galveston_multithreaded_scraper import GalvestonTaxScraper
            from src.utils.email_notifier import send_error_notification
            
            # Create and run the scraper
            scraper = GalvestonTaxScraper(num_threads=threads)
            scraper.run()
            
            logger.info("Galveston scraper completed successfully")
            
        except Exception as scraper_error:
            logger.error(f"Scraper execution failed: {str(scraper_error)}")
            logger.error(traceback.format_exc())
            
            # Send error notification if email is configured
            try:
                send_error_notification(
                    error_message="Scraper Execution Failed",
                    error_details=traceback.format_exc(),
                    context={
                        "threads": threads,
                        "server_mode": server_mode,
                        "error": str(scraper_error)
                    }
                )
            except Exception as email_error:
                logger.warning(f"Failed to send error notification: {str(email_error)}")
            
            raise scraper_error
    
    logger.info("Galveston tax scraper process completed")
    logger.info(f"=============== SCRAPER EXECUTION COMPLETE ===============")

if __name__ == "__main__":
    main() 