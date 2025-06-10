#!/usr/bin/env python
"""
Script to download the correct ChromeDriver version for your installed Chrome browser.
"""

import os
import platform
import re
import subprocess
import sys
import zipfile
from urllib.request import urlopen, urlretrieve
from io import BytesIO

def get_chrome_version():
    """Get the current Chrome version on the system."""
    system = platform.system().lower()
    try:
        if system == "windows":
            # Windows registry lookup
            try:
                import winreg
                key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Software\Google\Chrome\BLBeacon")
                version, _ = winreg.QueryValueEx(key, "version")
                return version
            except:
                # Try running Chrome directly to get version
                output = subprocess.check_output(
                    r'reg query "HKLM\SOFTWARE\Wow6432Node\Microsoft\Windows\CurrentVersion\Uninstall\Google Chrome" /v version',
                    shell=True
                ).decode()
                version = re.search(r'version\s+REG_SZ\s+([\d\.]+)', output).group(1)
                return version
        elif system == "darwin":
            # macOS - get Chrome version
            subprocess_args = [
                '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
                '--version',
            ]
            version = subprocess.check_output(subprocess_args).decode('utf-8')
            version = re.search(r'Google Chrome\s+([\d\.]+)', version).group(1)
            return version
        elif system == "linux":
            # Linux - get Chrome version
            subprocess_args = ['google-chrome', '--version']
            version = subprocess.check_output(subprocess_args).decode('utf-8')
            version = re.search(r'Google Chrome\s+([\d\.]+)', version).group(1)
            return version
    except:
        # Fall back to manual input if automatic detection fails
        print("Could not automatically detect Chrome version.")
        return input("Please enter your Chrome version manually (e.g., 136.0.7103.48): ")

def download_chromedriver(version):
    """Download the ChromeDriver that matches the specified Chrome version."""
    # Extract major version
    major_version = version.split('.')[0]
    print(f"Chrome major version: {major_version}")
    
    # Determine the system architecture
    system = platform.system().lower()
    if system == "windows":
        platform_name = "win32"  # Windows is always win32 for ChromeDriver
    elif system == "darwin":
        if platform.machine() == "arm64":
            platform_name = "mac-arm64"  # M1/M2 Mac
        else:
            platform_name = "mac-x64"  # Intel Mac
    elif system == "linux":
        platform_name = "linux64"
    else:
        print(f"Unsupported platform: {system}")
        return None
    
    # Construct the download URL (for Chrome 115+)
    base_url = f"https://storage.googleapis.com/chrome-for-testing-public/{major_version}.0.{platform_name}"
    json_url = f"https://googlechromelabs.github.io/chrome-for-testing/known-good-versions-with-downloads.json"
    
    print(f"Fetching available versions from {json_url}...")
    try:
        import json
        response = urlopen(json_url)
        versions_data = json.loads(response.read())
        
        # Find the closest matching version
        available_versions = []
        for v in versions_data.get('versions', []):
            if v['version'].startswith(f"{major_version}."):
                available_versions.append(v)
        
        if not available_versions:
            print(f"No ChromeDriver versions found for Chrome {major_version}")
            return None
            
        # Sort by version and get the latest
        available_versions.sort(key=lambda x: [int(p) for p in x['version'].split('.')])
        latest_version = available_versions[-1]
        
        # Find the chromedriver download URL
        download_url = None
        for download in latest_version.get('downloads', {}).get('chromedriver', []):
            if download['platform'] == platform_name:
                download_url = download['url']
                break
                
        if not download_url:
            print(f"No ChromeDriver download found for {platform_name}")
            return None
            
        print(f"Downloading ChromeDriver {latest_version['version']} from {download_url}")
        # Download the zip file
        response = urlopen(download_url)
        zip_content = BytesIO(response.read())
        
        # Extract the zip file
        current_dir = os.path.abspath(os.path.dirname(__file__))
        with zipfile.ZipFile(zip_content) as zip_ref:
            # ChromeDriver is in a subdirectory in newer versions
            for file in zip_ref.namelist():
                if file.endswith('chromedriver.exe') or file.endswith('chromedriver'):
                    # Create the extract destination
                    extract_path = current_dir
                    
                    # Extract the driver executable
                    source = zip_ref.open(file)
                    target_path = os.path.join(extract_path, os.path.basename(file))
                    with open(target_path, 'wb') as target:
                        target.write(source.read())
                    
                    # Make it executable on Unix systems
                    if system != "windows":
                        os.chmod(target_path, 0o755)
                    
                    print(f"ChromeDriver extracted to: {target_path}")
                    return target_path
        
        print("Could not find chromedriver in the zip file")
        return None
        
    except Exception as e:
        print(f"Error downloading ChromeDriver: {str(e)}")
        return None

def main():
    print("Chrome WebDriver Updater")
    print("-----------------------")
    
    # Get the current Chrome version
    chrome_version = get_chrome_version()
    if not chrome_version:
        print("Failed to determine Chrome version.")
        return
    
    print(f"Detected Chrome version: {chrome_version}")
    
    # Download matching ChromeDriver
    driver_path = download_chromedriver(chrome_version)
    
    if driver_path:
        print(f"\nSuccess! ChromeDriver has been updated to match Chrome {chrome_version}")
        print(f"Driver location: {driver_path}")
        
        # Rename old chromedriver if exists
        current_dir = os.path.abspath(os.path.dirname(__file__))
        old_driver = os.path.join(current_dir, "chromedriver.exe")
        if os.path.exists(old_driver) and old_driver != driver_path:
            backup_name = os.path.join(current_dir, f"chromedriver_old.exe")
            try:
                os.rename(old_driver, backup_name)
                print(f"Old driver backed up to: {backup_name}")
            except:
                print(f"Could not rename old driver. Please manually replace it if needed.")
                
        # On Windows, make sure we have the correct filename
        if platform.system().lower() == "windows" and not driver_path.endswith("chromedriver.exe"):
            try:
                new_path = os.path.join(current_dir, "chromedriver.exe")
                os.rename(driver_path, new_path)
                print(f"Driver renamed to standard name: {new_path}")
            except:
                print(f"Could not rename driver to standard name. Please rename it manually if needed.")
    else:
        print("\nFailed to update ChromeDriver. Please download it manually from:")
        print("https://googlechromelabs.github.io/chrome-for-testing/")

if __name__ == "__main__":
    main() 