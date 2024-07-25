import requests
import csv
import json
import logging
from datetime import datetime
import time
from tqdm import tqdm
from colorama import init, Fore, Back, Style
import signal
from requests.exceptions import RequestException
import random
import argparse

# Initialize colorama
init()

# ASCII art
ascii_art = """
\033[38;2;227;255;0m
░▒▓████████▓▒░▒▓███████▓▒░ ░▒▓██████▓▒░░▒▓███████▓▒░▒▓████████▓▒░      ░▒▓███████▓▒░ ░▒▓██████▓▒░░▒▓█▓▒░░▒▓█▓▒░░▒▓█▓▒░ 
░▒▓█▓▒░      ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░ ░▒▓█▓▒░          ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░░▒▓█▓▒░ 
░▒▓█▓▒░      ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░ ░▒▓█▓▒░          ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░░▒▓█▓▒░ 
░▒▓██████▓▒░ ░▒▓███████▓▒░░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░ ░▒▓█▓▒░          ░▒▓███████▓▒░░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░░▒▓█▓▒░ 
░▒▓█▓▒░      ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░ ░▒▓█▓▒░          ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░░▒▓█▓▒░ 
░▒▓█▓▒░      ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░ ░▒▓█▓▒░          ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░░▒▓█▓▒░ 
░▒▓█▓▒░      ░▒▓█▓▒░░▒▓█▓▒░░▒▓██████▓▒░░▒▓█▓▒░░▒▓█▓▒░ ░▒▓█▓▒░          ░▒▓█▓▒░░▒▓█▓▒░░▒▓██████▓▒░ ░▒▓█████████████▓░  
\033[0m
"""

print(ascii_art)

# Set up logging
logging.basicConfig(filename='shopify_scraper.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Generate a unique filename with timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f"shopify_partners_{timestamp}.csv"

base_url = "https://www.shopify.com/partners/directory/services?sort=AVERAGE_RATING&page={}&_data=pages%2Fshopify.com%2F%28%24locale%29%2Fpartners%2Fdirectory%2Fservices"
detail_url = "https://www.shopify.com/partners/directory/partner/{}?_data=pages%2Fshopify.com%2F%28%24locale%29%2Fpartners%2Fdirectory%2Fpartner%2F%24partner"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

total_profiles = 0

def format_service(service):
    return f"{service['service']['label']} (Featured: {service['featured']}, Pricing: {service['pricingType']}, Description: {service['description'][:100]}...)"

# Global flag for interruption
interrupted = False

def signal_handler(signum, frame):
    global interrupted
    interrupted = True
    print(f"\n\033[38;2;227;255;0mInterruption detected. Gracefully stopping...\033[0m")

# Set up the signal handler
signal.signal(signal.SIGINT, signal_handler)

def process_profile(profile):
    business_name = profile.get('businessName', '')
    location = profile.get('location', '')
    languages = ', '.join(profile.get('languages', []))
    handle = profile.get('handle', '')
    description = profile.get('description', '')
    services = ', '.join([service['service']['label'] for service in profile.get('serviceOfferings', [])])
    industries = ', '.join(profile.get('industries', []))
    review_count = profile.get('ratings', {}).get('total', '')
    rating = profile.get('ratings', {}).get('avg', '')
    website_url = profile.get('websiteUrl', '')
    avatar_url = profile.get('image', '')
    contact_email = profile.get('contactEmail', '')
    contact_phone_number = profile.get('contactPhoneNumber', '')
    city = location.split(',')[0].strip() if location else ''
    country = location.split(',')[-1].strip() if location else ''
    secondary_countries = ', '.join(profile.get('secondaryCountries', []))
    partner_since = profile.get('createdAt', '')
    social_media_links = ', '.join(profile.get('socialMediaLinks', []))
    specialized_services = ', '.join(profile.get('specializedServices', []))
    other_services = ', '.join(profile.get('otherServices', []))
    
    # Add Partner Status
    partner_status = 'PLUS' if 'PLUS' in profile.get('tags', []) else ''

    return [
        business_name, location, languages, handle, description, services, industries,
        review_count, rating, website_url, avatar_url, contact_email, contact_phone_number,
        city, country, secondary_countries, partner_since, social_media_links,
        specialized_services, other_services, partner_status
    ]

def make_request_with_retry(url, headers, max_retries=5, initial_delay=1, max_delay=60):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response
        except RequestException as e:
            if attempt == max_retries - 1:
                raise e
            
            delay = min(initial_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
            print(f"\n{Fore.YELLOW}Connection error. Retrying in {delay:.2f} seconds...{Style.RESET_ALL}")
            time.sleep(delay)

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Shopify Partners Scraper')
parser.add_argument('--start-page', type=int, default=1, help='Start scraping from this page')
args = parser.parse_args()

try:
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['businessName', 'location', 'languages', 'handle', 'description', 'services', 'industries', 
                      'reviewCount', 'rating', 'websiteUrl', 'avatarUrl', 'contactEmail', 'contactPhoneNumber', 
                      'city', 'country', 'secondaryCountries', 'partnerSince', 'socialMediaLinks',
                      'specializedServices', 'otherServices', 'partnerStatus']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        page = args.start_page  # Start from the specified page
        pbar = tqdm(desc="Scraping profiles", unit="profile", ncols=100)

        while not interrupted:
            try:
                response = make_request_with_retry(base_url.format(page), headers)
                data = json.loads(response.text)
                profiles = data.get('profiles', [])
            except json.JSONDecodeError:
                logging.error(f"Failed to parse JSON on page {page}. Response: {response.text[:500]}...")
                break

            if not profiles:
                break

            for profile in profiles:
                if interrupted:
                    break

                business_name = profile.get('businessName', 'Unknown')
                pbar.set_description(f"Processing: {business_name[:30]}...")  # Truncate long names

                row = {
                    'businessName': business_name,
                    'location': profile.get('location', ''),
                    'languages': ', '.join(profile.get('languages', [])),
                    'handle': profile.get('handle', ''),
                    'description': profile.get('description', ''),
                    'services': '',
                    'industries': '',
                    'reviewCount': '',
                    'rating': '',
                    'websiteUrl': '',
                    'avatarUrl': '',
                    'contactEmail': '',
                    'contactPhoneNumber': '',
                    'city': '',
                    'country': '',
                    'secondaryCountries': '',
                    'partnerSince': '',
                    'socialMediaLinks': '',
                    'specializedServices': '',
                    'otherServices': '',
                    'partnerStatus': 'PLUS' if 'PLUS' in profile.get('tags', []) else ''
                }

                try:
                    detail_response = make_request_with_retry(detail_url.format(row['handle']), headers)
                    detail_data = json.loads(detail_response.text)
                    partner_data = detail_data.get('profile', {})
                    
                    row.update({
                        'description': partner_data.get('bio', ''),
                        'services': ', '.join(service.get('service', {}).get('label', '') for service in partner_data.get('serviceOfferings', [])),
                        'industries': ', '.join(industry.get('label', '') for industry in partner_data.get('industries', [])),
                        'reviewCount': partner_data.get('numberOfRatings', ''),
                        'rating': partner_data.get('averageRating', ''),
                        'websiteUrl': partner_data.get('websiteUrl', ''),
                        'avatarUrl': partner_data.get('avatarUrl', ''),
                        'contactEmail': partner_data.get('contactEmail', ''),
                        'contactPhoneNumber': partner_data.get('contactPhoneNumber', ''),
                        'city': partner_data.get('city', ''),
                        'country': partner_data.get('country', {}).get('name', ''),
                        'secondaryCountries': ', '.join(country.get('name', '') for country in partner_data.get('secondaryCountries', [])),
                        'partnerSince': partner_data.get('partnerSince', ''),
                        'socialMediaLinks': ', '.join(f"{link['type']}: {link['url']}" for link in partner_data.get('socialMediaLinks', [])),
                        'specializedServices': ' | '.join(format_service(service) for service in partner_data.get('specializedServices', [])),
                        'otherServices': ' | '.join(format_service(service) for service in partner_data.get('otherServices', []))
                    })
                except RequestException as e:
                    logging.error(f"Failed to fetch details for {row['handle']} after multiple retries: {str(e)}")
                    continue

                writer.writerow(row)
                csvfile.flush()  # Ensure the data is written to the file
                
                total_profiles += 1
                pbar.update(1)
                
                time.sleep(1)  # Add a delay to avoid hitting rate limits
            
            if interrupted:
                break

            logging.info(f"Processed {len(profiles)} profiles from page {page}")
            page += 1
        
        pbar.close()
    
    logging.info(f"Data successfully saved to {filename}")
    logging.info(f"Total profiles scraped: {total_profiles}")

    print(f"\n{Fore.BLACK}{Back.YELLOW if not interrupted else Back.GREEN}{'Script execution completed.' if not interrupted else 'Script interrupted.'}{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}Data saved to: {Style.BRIGHT}{filename}{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}Total profiles scraped: {Style.BRIGHT}{total_profiles}{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}Check the {Style.BRIGHT}'shopify_scraper.log'{Style.NORMAL} file for details.{Style.RESET_ALL}")

except requests.RequestException as e:
    print(f"{Fore.RED}Request failed: {str(e)}{Style.RESET_ALL}")
    logging.error(f"Request failed: {str(e)}")
except json.JSONDecodeError:
    print(f"{Fore.RED}Failed to parse JSON response.{Style.RESET_ALL}")
    logging.error(f"Failed to parse JSON response. Response content: {response.text[:500]}...")
except IOError as e:
    print(f"{Fore.RED}IO error occurred: {str(e)}{Style.RESET_ALL}")
    logging.error(f"IO error occurred: {str(e)}")
except Exception as e:
    print(f"{Fore.RED}An unexpected error occurred: {str(e)}{Style.RESET_ALL}")
    logging.error(f"An unexpected error occurred: {str(e)}")

logging.info("Script execution completed")