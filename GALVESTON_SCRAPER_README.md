# Galveston County Tax Scraper

This is a multithreaded web scraper for the Galveston County tax website that extracts property tax information based on the SiteFlow.md specifications.

## Features

- **Multithreaded Processing**: Runs 3 concurrent workers by default for faster data collection
- **Checkpoint System**: Automatically saves progress and can resume from interruptions
- **3-Letter Search Patterns**: Uses patterns like aaa%, aab%, aac%... through zzz%
- **Data Extraction**: Collects Account Number, Owner Name, Mailing Address, Property Address, and Legal Description
- **CSV Export**: Automatically exports results to timestamped CSV files

## File Structure

```
src/
├── scrapers/
│   ├── galveston_multithreaded_scraper.py  # Main scraper with threading
│   ├── run_galveston_scraper.py           # Runner script with setup
│   └── rowan_property_details_scraper.py  # (Legacy, not used)
├── utils/
│   ├── checkpoint_utils.py                # Checkpoint management
│   └── checkpoint_to_csv.py              # Checkpoint conversion utilities
```

## Usage

### Quick Start
```bash
python src/scrapers/run_galveston_scraper.py
```

### With Custom Thread Count
```bash
python src/scrapers/run_galveston_scraper.py --threads 5
```

### Direct Scraper Execution
```bash
python src/scrapers/galveston_multithreaded_scraper.py
```

## How It Works

1. **Website**: Scrapes `https://actweb.acttax.com/act_webdev/galveston/index.jsp`
2. **Search Process**: 
   - Enters 3-letter patterns (aaa%, aab%, etc.) in the criteria field
   - Clicks the search button using the specific selector
   - Waits for results table to load
3. **Data Extraction**:
   - Account Number from 1st column link
   - Owner Name & Mailing Address from 2nd column (separated by first number)
   - Property Address from 3rd column (marked as "UNKNOWN" if empty)
   - Legal Description from 4th column
4. **Threading**: Each worker handles different search patterns simultaneously
5. **Checkpointing**: Progress saved after each pattern completion

## Search Pattern Progression

The scraper follows the SiteFlow.md pattern:
- Starts with aaa%, aab%, aac%...
- Continues through the alphabet: aaz%, aba%, abb%...
- Ends with zzz%

## Output

Results are saved to:
- `output/galveston_tax_results_YYYYMMDD_HHMMSS.csv`
- Checkpoints: `checkpoints/galveston_checkpoint_YYYYMMDD_HHMMSS.json`

## Configuration

- **Threads**: Default 3, configurable via `--threads` argument
- **Unique Proxies**: Each worker uses its own dedicated local proxy on unique ports (8081, 8082, 8083, etc.)
- **Proxy Verification**: Automatic port availability checking and proxy verification before starting
- **Browser**: Chrome with stability options, unique user-agent per worker, falls back to default if webdriver-manager unavailable

## Dependencies

- selenium==4.15.2
- pandas==2.0.3
- webdriver-manager==4.0.1
- requests==2.31.0

## Checkpoint Recovery

If the scraper is interrupted, it will automatically resume from the last completed search pattern when restarted.

## Logging

Logs are written to:
- Console output for immediate feedback
- `logs/galveston_run.log` for runner script
- Individual worker logs in the main application

## Browser Configuration

- Visible browser windows (not headless) for monitoring
- **Unique proxy configuration per worker** - each thread gets its own proxy instance
- Unique user-agent identification per worker (GalvestonScraper-Worker-0, etc.)
- Stability and performance options to prevent crashes
- Automatic ChromeDriver management via webdriver-manager
- Port availability verification before worker startup

## Unique Proxy System

Each worker thread operates with complete isolation:

1. **Port Assignment**: Worker 0 uses port 8081, Worker 1 uses 8082, etc.
2. **Port Verification**: System checks port availability before starting
3. **Proxy Validation**: Each proxy is verified to be running before use
4. **Individual Cleanup**: Each worker manages its own proxy lifecycle
5. **Environment Isolation**: Separate HTTP_PROXY/HTTPS_PROXY settings per worker 