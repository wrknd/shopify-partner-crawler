import requests
import csv
import json
import logging
from datetime import datetime
import time
from tqdm import tqdm
from colorama import init, Fore, Back, Style
import signal

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
░▒▓█▓▒░      ░▒▓█▓▒░░▒▓█▓▒░░▒▓██████▓▒░░▒▓█▓▒░░▒▓█▓▒░ ░▒▓█▓▒░          ░▒▓█▓▒░░▒▓█▓▒░░▒▓██████▓▒░ ░▒▓█████████████▓▒░  
\033[0m
"""

print(ascii_art)

# Set up logging
logging.basicConfig(filename='shopify_scraper.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Generate a unique filename with timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f"shopify_partners_{timestamp}.csv"

base_url = "https://www.shopify.com/partners/directory/services?partnerTypes=plus&languageCodes=lang-en&sort=AVERAGE_RATING&page={}&_data=pages%2Fshopify.com%2F%28%24locale%29%2Fpartners%2Fdirectory%2Fservices"
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

try:
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['businessName', 'location', 'languages', 'handle', 'description', 'services', 'industries', 
                      'reviewCount', 'rating', 'websiteUrl', 'avatarUrl', 'contactEmail', 'contactPhoneNumber', 
                      'city', 'country', 'secondaryCountries', 'partnerSince', 'socialMediaLinks',
                      'specializedServices', 'otherServices']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        page = 1
        pbar = tqdm(desc="Scraping profiles", unit="profile")

        while not interrupted:
            response = requests.get(base_url.format(page), headers=headers)
            if response.status_code != 200:
                logging.error(f"Failed to fetch page {page}. Status code: {response.status_code}")
                break

            try:
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

                row = {
                    'businessName': profile.get('businessName', ''),
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
                    'otherServices': ''
                }

                detail_response = requests.get(detail_url.format(row['handle']), headers=headers)
                if detail_response.status_code == 200:
                    try:
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
                    except json.JSONDecodeError:
                        logging.error(f"Failed to parse JSON for {row['handle']}. Response: {detail_response.text[:500]}...")
                else:
                    logging.error(f"Failed to fetch details for {row['handle']}. Status code: {detail_response.status_code}")

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