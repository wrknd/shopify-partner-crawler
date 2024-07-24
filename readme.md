# Shopify Partners Directory Scraper

This Python script scrapes data from the Shopify Partners Directory, focusing on Plus partners with English language services. It collects detailed information about each partner and saves it to a CSV file.

## Features

- Scrapes Shopify Plus partners' data from the directory
- Collects detailed information for each partner
- Saves data to a CSV file with a timestamp
- Provides a progress bar for visual feedback
- Handles interruptions gracefully
- Logs activities and errors for debugging

## Requirements

- Python 3.6+
- Required libraries: requests, csv, json, logging, datetime, time, tqdm, colorama

## Installation

1. Clone this repository or download the script.
2. Install the required libraries:

```
pip3 install requests tqdm colorama
```

## Usage

Run the script from the command line:

```
python3 main.py
```

The script will start scraping data and display a progress bar. You can interrupt the script at any time by pressing Ctrl+C, and it will gracefully stop and save the data collected up to that point.

## Output

- A CSV file named `shopify_partners_YYYYMMDD_HHMMSS.csv` containing the scraped data
- A log file named `shopify_scraper.log` with detailed logging information

## Data Collected

The script collects the following information for each partner:

- Business Name
- Location
- Languages
- Handle
- Description
- Services
- Industries
- Review Count
- Rating
- Website URL
- Avatar URL
- Contact Email
- Contact Phone Number
- City
- Country
- Secondary Countries
- Partner Since Date
- Social Media Links
- Specialized Services
- Other Services

## Notes

- The script includes a delay between requests to avoid hitting rate limits.
- Make sure to use this script responsibly and in accordance with Shopify's terms of service.
