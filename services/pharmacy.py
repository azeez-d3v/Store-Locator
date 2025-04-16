import asyncio
import sys
import os
import csv
import json
from pathlib import Path
import re
from bs4 import BeautifulSoup

# append parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from services.session_manager import SessionManager
except ImportError:
    # Direct import if the related module is not found
    from session_manager import SessionManager

class PharmacyLocations:
    """
    Generic class to fetch pharmacy locations from different brands
    """
    BASE_URL = "https://app.medmate.com.au/connect/api/get_locations"
    DETAIL_URL = "https://app.medmate.com.au/connect/api/get_pharmacy"
    BLOOMS_URL = "https://api.storepoint.co/v2/15f056510a1d3a/locations"
    RAMSAY_URL = "https://ramsayportalapi-prod.azurewebsites.net/api/pharmacyclient/pharmacies"
    REVIVE_URL = "https://core.service.elfsight.com/p/boot/?page=https%3A%2F%2Frevivepharmacy.com.au%2Fstore-finder%2F&w=52ff3b25-4412-410c-bd3d-ea57b2814fac"
    OPTIMAL_URL = "https://core.service.elfsight.com/p/boot/?page=https%3A%2F%2Foptimalpharmacyplus.com.au%2F%23map&w=d70b40db-e8b3-43bc-a63b-b3cce68941bf"
    COMMUNITY_URL = "https://www.communitycarechemist.com.au/"
    FOOTES_URL = "https://footespharmacies.com/stores/"
    
    BRAND_CONFIGS = {
        "dds": {
            "businessid": "2",
            "session_id": "",
            "source": "DDSPharmacyWebsite"
        },
        "amcal": {
            "businessid": "4",
            "session_id": "",
            "source": "AmcalPharmacyWebsite"
        }
    }
    
    COMMON_HEADERS = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'no-cache',
        'Host': 'app.medmate.com.au',
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
        'X-Requested-With': 'XMLHttpRequest',
    }
    
    BLOOMS_HEADERS = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'origin': 'https://www.bloomsthechemist.com.au',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': 'https://www.bloomsthechemist.com.au/',
        'sec-ch-ua': '"Microsoft Edge";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0'
    }
    
    RAMSAY_HEADERS = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json; charset=UTF-8',
        'SessionId': 'cDIxVGhWUkQweWJkeTlJajlLcktDckUzVmxCUEg1MlFrYzUzbGEweG1MOEhSOW5qUlhsWnFSWGloUEY5OXZzT2dhRFJTSWZaeExHVU5nbXcvNzJjaWNleVR6WWlZTW5weUFLRGZpNHFCVXpJUk14UWh5ek1QakplMXRCdG1DeWhyVUw1bkVrR1cxZGVQSVFUY2YvQ2RvRkJpeTZrVkZHZ0ZmbW9hK3Q4OFljPQ==',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
    }
    
    REVIVE_HEADERS = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'origin': 'https://revivepharmacy.com.au',
        'referer': 'https://revivepharmacy.com.au/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
    }
    
    OPTIMAL_HEADERS = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'origin': 'https://optimalpharmacyplus.com.au',
        'referer': 'https://optimalpharmacyplus.com.au/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
    }
    
    COMMUNITY_HEADERS = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'sec-ch-ua': '"Microsoft Edge";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0'
    }
    
    FOOTES_HEADERS = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'sec-ch-ua': '"Microsoft Edge";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0'
    }
    
    RAMSAY_PAYLOAD = {
        "Services": None,
        "PharmacyName": "ramsay",
        "WeekDayId": None,
        "TodayId": 3,
        "TodayTime": "13:12:40",
        "IsOpenNow": False,
        "IsClickCollect": False,
        "Is24Hours": False,
        "IsOpenWeekend": False,
        "Region": None,
        "Distance": 0,
        "Latitude": 0,
        "Longitude": 0,
        "PageIndex": 1,
        "PageSize": 100,
        "OrderBy": ""
    }
    
    def __init__(self):
        self.session_manager = SessionManager()
        
    async def fetch_locations(self, brand):
        """
        Fetch locations for a specific pharmacy brand.
        
        Args:
            brand: String identifier for the brand (e.g., "dds", "amcal", "blooms", "ramsay", "revive", "optimal", "community")
            
        Returns:
            Processed location data
        """
        if brand == "blooms":
            return await self.fetch_blooms_locations()
        elif brand == "ramsay":
            return await self.fetch_ramsay_locations()
        elif brand == "revive":
            return await self.fetch_revive_locations()
        elif brand == "optimal":
            return await self.fetch_optimal_locations()
        elif brand == "community":
            return await self.fetch_community_locations()
        elif brand == "footes":
            return await self.fetch_footes_locations()
            
        if brand not in self.BRAND_CONFIGS:
            raise ValueError(f"Unknown pharmacy brand: {brand}")
            
        payload = self.BRAND_CONFIGS[brand]
        
        response = await self.session_manager.post(
            url=self.BASE_URL,
            json=payload,
            headers=self.COMMON_HEADERS
        )
        
        # Process the response
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            raise Exception(f"Failed to fetch {brand.upper()} locations: {response.status_code}")
    
    async def fetch_community_locations(self):
        """
        Fetch all Community Care Chemist locations.
        
        Returns:
            List of Community Care Chemist locations
        """
        response = await self.session_manager.get(
            url=self.COMMUNITY_URL,
            headers=self.COMMUNITY_HEADERS
        )
        
        if response.status_code == 200:
            html_content = response.text
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Updated selector to find all pharmacy location articles
            pharmacy_articles = soup.find_all('article', class_='stores')
            
            if not pharmacy_articles:
                # Try alternative selector if the first one doesn't work
                pharmacy_articles = soup.select('.dce-posts-wrapper article.dce-post')
            
            if pharmacy_articles:
                locations = []
                for article in pharmacy_articles:
                    try:
                        # Extract pharmacy details from the HTML structure
                        pharmacy_data = {}
                        
                        # Extract name
                        name_element = article.select_one('.elementor-heading-title')
                        if name_element:
                            pharmacy_data['name'] = name_element.text.strip()
                        
                        # Extract address
                        address_element = article.select_one('.elementor-element-5eec216 .dynamic-content-for-elementor-acf')
                        if address_element:
                            pharmacy_data['address'] = address_element.text.strip()
                        
                        # Extract phone
                        phone_element = article.select_one('.elementor-element-ba94b59 .dynamic-content-for-elementor-acf')
                        if phone_element:
                            phone_text = phone_element.text.strip()
                            # Remove "PH: " prefix if present
                            pharmacy_data['phone'] = phone_text.replace('PH:', '').strip()
                        
                        # Extract fax
                        fax_element = article.select_one('.elementor-element-0b379dd .dynamic-content-for-elementor-acf')
                        if fax_element:
                            fax_text = fax_element.text.strip()
                            # Remove "FAX:" prefix if present
                            pharmacy_data['fax'] = fax_text.replace('FAX:', '').strip()
                        
                        # Extract email from the href attribute
                        email_element = article.select_one('.dce-tokens a')
                        if email_element:
                            email_href = email_element.get('href', '')
                            if email_href.startswith('mailto:'):
                                pharmacy_data['email'] = email_href.replace('mailto:', '')
                        
                        # Extract trading hours
                        trading_hours = {}
                        
                        # Mon-Fri hours
                        mon_fri_element = article.select_one('.elementor-element-2a0c443 .dynamic-content-for-elementor-acf')
                        if mon_fri_element:
                            hours_text = mon_fri_element.text.strip()
                            hours_match = re.search(r'Mon - Fri:\s*(.*)', hours_text)
                            if hours_match:
                                hours_value = hours_match.group(1).strip()
                                # Split into open and close times
                                if '-' in hours_value:
                                    open_time, close_time = map(str.strip, hours_value.split('-'))
                                    for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']:
                                        trading_hours[day] = {'open': open_time, 'closed': close_time}
                        
                        # Saturday hours
                        sat_element = article.select_one('.elementor-element-81cd6c4 .dynamic-content-for-elementor-acf')
                        if sat_element:
                            hours_text = sat_element.text.strip()
                            hours_match = re.search(r'Sat:\s*(.*)', hours_text)
                            if hours_match:
                                hours_value = hours_match.group(1).strip()
                                if hours_value.lower() == 'closed':
                                    trading_hours['Saturday'] = {'open': 'Closed', 'closed': 'Closed'}
                                elif '-' in hours_value:
                                    open_time, close_time = map(str.strip, hours_value.split('-'))
                                    trading_hours['Saturday'] = {'open': open_time, 'closed': close_time}
                        
                        # Sunday hours
                        sun_element = article.select_one('.elementor-element-a58e969 .dynamic-content-for-elementor-acf')
                        if sun_element:
                            hours_text = sun_element.text.strip()
                            hours_match = re.search(r'Sun:\s*(.*)', hours_text)
                            if hours_match:
                                hours_value = hours_match.group(1).strip()
                                if hours_value.lower() == 'closed':
                                    trading_hours['Sunday'] = {'open': 'Closed', 'closed': 'Closed'}
                                elif '-' in hours_value:
                                    open_time, close_time = map(str.strip, hours_value.split('-'))
                                    trading_hours['Sunday'] = {'open': open_time, 'closed': close_time}
                        
                        pharmacy_data['trading_hours'] = trading_hours
                        
                        # Extract state and postcode from address if possible
                        if 'address' in pharmacy_data:
                            address = pharmacy_data['address']
                            # Australian state pattern
                            state_pattern = r'\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b'
                            state_match = re.search(state_pattern, address)
                            if state_match:
                                pharmacy_data['state'] = state_match.group(0)
                            
                            # Australian postcode pattern (4 digits)
                            postcode_pattern = r'\b(\d{4})\b'
                            postcode_match = re.search(postcode_pattern, address)
                            if postcode_match:
                                pharmacy_data['postcode'] = postcode_match.group(0)
                        
                        # Add a unique identifier for this pharmacy
                        if 'name' in pharmacy_data:
                            pharmacy_data['id'] = f"community_{pharmacy_data['name'].lower().replace(' ', '_')}"
                        
                        # Add debug information to better understand the extraction
                        if 'name' not in pharmacy_data:
                            print(f"Warning: Could not extract name for a pharmacy. HTML structure might have changed.")
                            
                        locations.append(pharmacy_data)
                    except Exception as e:
                        print(f"Error processing Community Care Chemist location: {e}")
                
                if locations:
                    return locations
                else:
                    print("No pharmacy data could be extracted from the found articles")
                    print(f"Found {len(pharmacy_articles)} article elements but couldn't extract data")
                    
                    # Print the HTML of the first article for debugging
                    if pharmacy_articles:
                        print(f"First article HTML sample: {str(pharmacy_articles[0])[:200]}...")
                    
                    raise Exception("No pharmacy data could be extracted from Community Care Chemist page")
            else:
                # Print some debugging information
                print("No matching pharmacy articles found with either selector")
                
                # Try to find any articles on the page to see what's available
                all_articles = soup.find_all('article')
                if all_articles:
                    print(f"Found {len(all_articles)} articles with generic 'article' selector")
                    print(f"First article classes: {all_articles[0].get('class', [])}")
                else:
                    print("No articles found with any selector")
                
                # Try to find the main content div
                main_content = soup.select('.dce-posts-wrapper')
                if main_content:
                    print(f"Found main content wrapper with {len(main_content[0].find_all())} child elements")
                
                raise Exception("No pharmacy locations found in Community Care Chemist page")
        else:
            raise Exception(f"Failed to fetch Community Care Chemist locations: {response.status_code}")
    
    async def fetch_footes_locations(self):
        """
        Fetch all Footes Pharmacy locations from the main store page.
        
        Returns:
            List of Footes Pharmacy locations with basic information
        """
        response = await self.session_manager.get(
            url=self.FOOTES_URL,
            headers=self.FOOTES_HEADERS
        )
        
        if response.status_code == 200:
            html_content = response.text
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find all pharmacy location articles in the loop container
            store_items = soup.select('.elementor-loop-container .e-loop-item')
            
            if store_items:
                locations = []
                for store in store_items:
                    try:
                        # Extract pharmacy details from the HTML structure
                        pharmacy_data = {}
                        
                        # Get the store URL for detailed info
                        store_url = None
                        store_link = store.select_one('a.elementor-element')
                        if store_link:
                            store_url = store_link.get('href')
                            
                        # Extract name
                        name_element = store.select_one('.elementor-heading-title')
                        if name_element:
                            pharmacy_data['name'] = name_element.text.strip()
                        
                        # Extract phone
                        phone_element = store.select_one('.elementor-element-c6b3bcc')
                        if phone_element:
                            phone = phone_element.text.strip()
                            pharmacy_data['phone'] = phone
                        
                        # Store URL for fetching additional details
                        if store_url:
                            pharmacy_data['detail_url'] = store_url
                        
                        # Generate a unique ID
                        if 'name' in pharmacy_data:
                            pharmacy_data['id'] = f"footes_{pharmacy_data['name'].lower().replace(' ', '_').replace('-', '_')}"
                        
                        locations.append(pharmacy_data)
                    except Exception as e:
                        print(f"Error processing Footes Pharmacy location: {e}")
                
                if locations:
                    return locations
                else:
                    print("No pharmacy data could be extracted from the found store items")
                    print(f"Found {len(store_items)} store items but couldn't extract data")
                    
                    # Print the HTML of the first item for debugging
                    if store_items:
                        print(f"First store HTML sample: {str(store_items[0])[:200]}...")
                    
                    raise Exception("No pharmacy data could be extracted from Footes Pharmacy page")
            else:
                # Try alternative selector if the first one doesn't work
                store_items = soup.select('div[data-elementor-type="loop-item"]')
                if store_items:
                    print(f"Found {len(store_items)} stores with alternative selector")
                    locations = []
                    for store in store_items:
                        try:
                            pharmacy_data = {}
                            
                            # Get the store URL for detailed info
                            store_url = None
                            store_link = store.select_one('a.elementor-element')
                            if store_link:
                                store_url = store_link.get('href')
                                
                            # Extract name
                            name_element = store.select_one('.elementor-heading-title')
                            if name_element:
                                pharmacy_data['name'] = name_element.text.strip()
                            
                            # Extract phone
                            phone_element = store.select_one('.elementor-element-c6b3bcc')
                            if phone_element:
                                phone = phone_element.text.strip()
                                pharmacy_data['phone'] = phone
                            
                            # Store URL for fetching additional details
                            if store_url:
                                pharmacy_data['detail_url'] = store_url
                            
                            # Generate a unique ID
                            if 'name' in pharmacy_data:
                                pharmacy_data['id'] = f"footes_{pharmacy_data['name'].lower().replace(' ', '_').replace('-', '_')}"
                            
                            locations.append(pharmacy_data)
                        except Exception as e:
                            print(f"Error processing Footes Pharmacy location with alternative selector: {e}")
                    
                    if locations:
                        return locations
                    else:
                        raise Exception("No pharmacy data could be extracted from Footes Pharmacy page with alternative selector")
                else:
                    print("No store items found with any selector")
                    
                    # Try to find the main content div
                    main_content = soup.select('.elementor-loop-container')
                    if main_content:
                        print(f"Found main content container with {len(main_content[0].find_all())} child elements")
                    
                    raise Exception("No pharmacy locations found in Footes Pharmacy page")
        else:
            raise Exception(f"Failed to fetch Footes Pharmacy locations: {response.status_code}")
    
    async def fetch_footes_store_details(self, url):
        """
        Fetch detailed information for a specific Footes Pharmacy store.
        
        Args:
            url: URL of the store detail page
            
        Returns:
            Dictionary containing detailed store information
        """
        response = await self.session_manager.get(
            url=url,
            headers=self.FOOTES_HEADERS
        )
        
        if response.status_code == 200:
            html_content = response.text
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Initialize store details dictionary
            store_details = {}
            
            # Extract contact details section
            contact_section = soup.select_one('.elementor-element-5e7ef26')
            if contact_section:
                # Extract full address
                address_element = contact_section.select_one('.elementor-element-d9bbb9b .elementor-heading-title')
                if address_element:
                    address = address_element.text.strip()
                    store_details['address'] = address
                    
                    # Extract state and postcode from address
                    state_pattern = r'\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b'
                    state_match = re.search(state_pattern, address)
                    if state_match:
                        store_details['state'] = state_match.group(0)
                    
                    postcode_pattern = r'\b(\d{4})\b'
                    postcode_match = re.search(postcode_pattern, address)
                    if postcode_match:
                        store_details['postcode'] = postcode_match.group(0)
                
                # Extract phone number
                phone_element = contact_section.select_one('.store-phone a')
                if phone_element:
                    store_details['phone'] = phone_element.text.strip()
                    
                # Extract email address
                email_element = contact_section.select_one('.store-email a')
                if email_element:
                    store_details['email'] = email_element.text.strip()
                
            # Extract trading hours
            trading_hours = {}
            
            # Find the trading hours section
            hours_section = soup.select_one('.elementor-element-683e6b3')
            if hours_section:
                # Find weekday labels
                day_elements = hours_section.select('.elementor-element-fb1522c .elementor-widget-text-editor')
                # Find corresponding hours
                hour_elements = hours_section.select('.elementor-element-b96bcb7 .elementor-widget-text-editor')
                
                # Map days to their hours
                if len(day_elements) == len(hour_elements):
                    for i in range(len(day_elements)):
                        day_text = day_elements[i].text.strip()
                        hour_text = hour_elements[i].text.strip()
                        
                        # Handle "Monday - Friday" format
                        if "–" in day_text or "-" in day_text:
                            # Replace both Unicode and ASCII dashes
                            day_text = day_text.replace("–", "-")
                            day_range = day_text.split("-")
                            if len(day_range) == 2:
                                start_day, end_day = day_range[0].strip(), day_range[1].strip()
                                # Map weekday names
                                weekdays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
                                start_index = next((i for i, day in enumerate(weekdays) if start_day.lower() in day.lower()), -1)
                                end_index = next((i for i, day in enumerate(weekdays) if end_day.lower() in day.lower()), -1)
                                
                                if start_index >= 0 and end_index >= 0:
                                    for day_index in range(start_index, end_index + 1):
                                        day_name = weekdays[day_index]
                                        if hour_text.lower() == 'closed':
                                            trading_hours[day_name] = {'open': 'Closed', 'closed': 'Closed'}
                                        else:
                                            # Parse open/closed times
                                            try:
                                                open_close = hour_text.split('–') if '–' in hour_text else hour_text.split('-')
                                                if len(open_close) == 2:
                                                    trading_hours[day_name] = {
                                                        'open': open_close[0].strip(), 
                                                        'closed': open_close[1].strip()
                                                    }
                                            except Exception as e:
                                                print(f"Error parsing hours for {day_name}: {e}")
                        else:
                            # Single day format (Saturday, Sunday)
                            day_name = day_text
                            if hour_text.lower() == 'closed':
                                trading_hours[day_name] = {'open': 'Closed', 'closed': 'Closed'}
                            else:
                                # Parse open/closed times
                                try:
                                    open_close = hour_text.split('–') if '–' in hour_text else hour_text.split('-')
                                    if len(open_close) == 2:
                                        trading_hours[day_name] = {
                                            'open': open_close[0].strip(), 
                                            'closed': open_close[1].strip()
                                        }
                                except Exception as e:
                                    print(f"Error parsing hours for {day_name}: {e}")
            
            # Add trading hours to store details
            if trading_hours:
                store_details['trading_hours'] = trading_hours
                
            return store_details
        else:
            raise Exception(f"Failed to fetch Footes store details: {response.status_code}")
    
    async def fetch_pharmacy_details(self, brand, location_id):
        """
        Fetch detailed information for a specific pharmacy.
        
        Args:
            brand: String identifier for the brand (e.g., "dds", "amcal", "blooms", "ramsay", "revive", "optimal", "community")
            location_id: The ID of the location to get details for
            
        Returns:
            Detailed pharmacy data
        """
        if brand in ["blooms", "ramsay", "revive", "optimal", "community", "footes"]:
            # For these brands, we already have all the data in the locations response
            # This is a placeholder for API consistency
            return {"location_details": location_id}
            
        if brand not in self.BRAND_CONFIGS:
            raise ValueError(f"Unknown pharmacy brand: {brand}")
            
        brand_config = self.BRAND_CONFIGS[brand]
        
        payload = {
            "session_id": brand_config["session_id"],
            "businessid": brand_config["businessid"],
            "locationid": location_id,
            "include_services": True,
            "source": brand_config["source"]
        }
        
        response = await self.session_manager.post(
            url=self.DETAIL_URL,
            json=payload,
            headers=self.COMMON_HEADERS
        )
        
        # Process the response
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            raise Exception(f"Failed to fetch pharmacy details: {response.status_code}")
            
    def extract_pharmacy_details(self, pharmacy_data, brand=""):
        """
        Extract specific fields from pharmacy location details response.
        
        Args:
            pharmacy_data: The raw pharmacy data from the API
            brand: The brand of the pharmacy (to handle different response formats)
            
        Returns:
            Dictionary containing only the requested fields in a standardized order
        """
        result = {}
        
        if brand == "blooms":
            # Handle Blooms' different data structure
            # Convert the trading hours to the same format as other pharmacies
            trading_hours = {}
            for day in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']:
                if day in pharmacy_data and pharmacy_data[day]:
                    hours = pharmacy_data[day].strip()
                    if hours.upper() != "CLOSED":
                        try:
                            open_close = hours.split("-")
                            if len(open_close) == 2:
                                trading_hours[day.capitalize()] = {
                                    "open": open_close[0].strip(),
                                    "closed": open_close[1].strip()
                                }
                        except Exception:
                            # Fall back to storing the raw string if parsing fails
                            trading_hours[day.capitalize()] = {"raw": hours}
            
            # Extract state and postcode from the address
            state = ""
            postcode = ""
            suburb = ""
            address = pharmacy_data.get('streetaddress', '')
            
            # Typical format: Street address, Suburb, STATE POSTCODE, Country
            address_parts = address.split(',')
            if len(address_parts) >= 3:
                # Try to extract state and postcode from the second last part (before country)
                state_part = address_parts[-2].strip() if len(address_parts) > 2 else ""
                state_postcode = state_part.split()
                if len(state_postcode) >= 2:
                    # Assume format: NSW 2000
                    state = state_postcode[0]
                    postcode = state_postcode[1]
                
                # Try to extract suburb from the part before state/postcode
                if len(address_parts) > 3:
                    suburb = address_parts[-3].strip()
            
            # Get coordinates from loc_lat and loc_long fields
            latitude = pharmacy_data.get('loc_lat')
            longitude = pharmacy_data.get('loc_long')
            
            # Using fixed column order
            result = {
                'name': pharmacy_data.get('name'),
                'address': address,
                'email': pharmacy_data.get('email'),
                'fax': None,  # Blooms doesn't provide fax numbers in the API
                'latitude': latitude,
                'longitude': longitude,
                'phone': pharmacy_data.get('phone'),
                'postcode': postcode,
                'state': state,
                'street_address': address,
                'suburb': suburb,
                'trading_hours': trading_hours,
                'website': pharmacy_data.get('website')
            }
            
        elif brand == "ramsay":
            # Handle Ramsay's different data structure
            # Extract address, state, and postcode
            address = pharmacy_data.get('Address', '')
            address = address.replace('<br>', ', ')  # Replace HTML line breaks with commas
            address_parts = address.split(',')
            
            # Try to extract state and postcode
            state = None
            postcode = None
            suburb = None
            
            # Look for state and postcode in address (typically at the end)
            for part in reversed(address_parts):
                part = part.strip()
                if ',' in part:
                    subparts = part.split(',')
                    for subpart in subparts:
                        subpart = subpart.strip()
                        # Common Australian state abbreviations
                        if any(s in subpart for s in ['NSW', 'VIC', 'QLD', 'SA', 'WA', 'TAS', 'NT', 'ACT']):
                            state_postcode = subpart.split()
                            if len(state_postcode) >= 2:
                                state = state_postcode[0]
                                postcode = state_postcode[1]
                            break
                elif 'NSW' in part or 'VIC' in part or 'QLD' in part or 'SA' in part or 'WA' in part or 'TAS' in part or 'NT' in part or 'ACT' in part:
                    state_postcode = part.split()
                    if len(state_postcode) >= 2:
                        state = state_postcode[0]
                        postcode = state_postcode[1]
                    break
            
            # Try to extract suburb (usually before state and postcode)
            for i, part in enumerate(address_parts):
                part = part.strip()
                if state and part.endswith(state):
                    if i > 0:
                        suburb = address_parts[i - 1].strip()
                    break
            
            # Parse operating hours
            trading_hours = {}
            if pharmacy_data.get('OpereatingHourDescription'):
                hours_desc = pharmacy_data.get('OpereatingHourDescription').replace('<br/>', '\n')
                for line in hours_desc.split('\n'):
                    if ':' in line:
                        day_hours = line.strip().split(':')
                        if len(day_hours) >= 2:
                            day = day_hours[0].strip()
                            hours = day_hours[1].strip()
                            
                            if 'Closed' in hours:
                                # Handle closed days
                                trading_hours[day] = {
                                    "open": "Closed",
                                    "closed": "Closed"
                                }
                            else:
                                # Try to parse open/close times
                                try:
                                    open_close = hours.split('-')
                                    if len(open_close) == 2:
                                        trading_hours[day] = {
                                            "open": open_close[0].strip(),
                                            "closed": open_close[1].strip()
                                        }
                                except Exception:
                                    # Fall back to storing the raw string
                                    trading_hours[day] = {"raw": hours}
            
            # Using fixed column order
            result = {
                'name': pharmacy_data.get('PharmacyName'),
                'address': address,
                'email': None,  # Ramsay doesn't provide email in API
                'fax': pharmacy_data.get('FaxNumber'),
                'latitude': pharmacy_data.get('Latitude'),
                'longitude': pharmacy_data.get('Longitude'),
                'phone': pharmacy_data.get('PhoneNumber'),
                'postcode': postcode,
                'state': state,
                'street_address': address,
                'suburb': suburb,
                'trading_hours': trading_hours,
                'website': None  # Ramsay doesn't provide website in API
            }
            
            # Keep additional fields outside the standard columns
            if 'reference_id' not in result:
                result['reference_id'] = pharmacy_data.get('ReferenceId')
            if 'pharmacy_id' not in result:
                result['pharmacy_id'] = pharmacy_data.get('PharmacyId')
            if 'where_to_find' not in result:
                result['where_to_find'] = pharmacy_data.get('WhereToFind')
                
        elif brand in ["revive", "optimal"]:
            # Handle Revive and Optimal's identical data structure
            # Extract coordinates from the place object
            latitude = None
            longitude = None
            if 'place' in pharmacy_data and 'coordinates' in pharmacy_data['place']:
                coordinates = pharmacy_data['place']['coordinates']
                latitude = coordinates.get('lat')
                longitude = coordinates.get('lng')
            
            # Extract address information
            address = pharmacy_data.get('address', '')
            # Typical format: "42 Herbert St Allora QLD 4362"
            address_parts = address.split()
            
            # Try to extract state and postcode
            state = None
            postcode = None
            suburb = None
            
            # Look for state abbreviations (usually second-to-last element)
            state_abbreviations = ['NSW', 'VIC', 'QLD', 'SA', 'WA', 'TAS', 'NT', 'ACT']
            for i, part in enumerate(address_parts):
                if part in state_abbreviations and i < len(address_parts) - 1:
                    state = part
                    postcode = address_parts[i + 1]
                    # Suburb is usually right before the state
                    if i > 0:
                        # Collect all parts between street address and state as suburb
                        suburb_parts = []
                        # Find where the street number/name ends - typically after a St, Rd, Dr, etc.
                        street_indicators = ['St', 'Rd', 'Dr', 'Ave', 'Ln', 'Cres', 'Pl', 'Ct', 'Way', 'Blvd', 'Street', 'Road', 'Drive']
                        street_end_idx = -1
                        for j, addr_part in enumerate(address_parts[:i]):
                            if addr_part in street_indicators:
                                street_end_idx = j
                                break
                        
                        if street_end_idx >= 0:
                            suburb_parts = address_parts[street_end_idx + 1:i]
                            if suburb_parts:
                                suburb = ' '.join(suburb_parts)
                    break
            
            # Parse trading hours from the daily open/hours fields
            trading_hours = {}
            days = {
                'Monday': ('dayMondayOpen', 'dayMondayHours'),
                'Tuesday': ('dayTuesdayOpen', 'dayTuesdayHours'),
                'Wednesday': ('dayWednesdayOpen', 'dayWednesdayHours'),
                'Thursday': ('dayThursdayOpen', 'dayThursdayHours'),
                'Friday': ('dayFridayOpen', 'dayFridayHours'),
                'Saturday': ('daySaturdayOpen', 'daySaturdayHours'),
                'Sunday': ('daySundayOpen', 'daySundayHours')
            }
            
            for day, (open_key, hours_key) in days.items():
                # Check if pharmacy has hours for this day
                # Note: For Revive, False means open; for Optimal, True means open
                if brand == "revive":
                    is_open = not pharmacy_data.get(open_key, False)
                else:  # optimal
                    is_open = pharmacy_data.get(open_key, False)
                    
                if is_open and hours_key in pharmacy_data and pharmacy_data[hours_key]:
                    hours_data = pharmacy_data[hours_key]
                    if hours_data and isinstance(hours_data, list) and len(hours_data) > 0:
                        time_range = hours_data[0].get('timeRange', [])
                        if len(time_range) == 2:
                            trading_hours[day] = {
                                'open': time_range[0],
                                'closed': time_range[1]
                            }
                else:
                    trading_hours[day] = {'open': 'Closed', 'closed': 'Closed'}
            
            # Using fixed column order
            result = {
                'name': pharmacy_data.get('name'),
                'address': address,
                'email': pharmacy_data.get('email'),
                'fax': None,  # These brands don't provide fax numbers
                'latitude': latitude,
                'longitude': longitude,
                'phone': pharmacy_data.get('phone'),
                'postcode': postcode,
                'state': state,
                'street_address': address,
                'suburb': suburb,
                'trading_hours': trading_hours,
                'website': pharmacy_data.get('website')
            }
            
        elif brand == "community":
            # Handle Community Care Chemist's unique data structure
            address = pharmacy_data.get('address', '')
            
            # Try to extract state and postcode from address if not already parsed
            state = pharmacy_data.get('state')
            postcode = pharmacy_data.get('postcode')
            suburb = None
            
            # Try to find suburb in address if not already extracted
            if address and state:
                # Simplistic extraction of suburb - assumes format like "X Street, Suburb STATE POSTCODE"
                parts = address.split(',')
                if len(parts) > 1:
                    suburb_part = parts[-1].strip() if len(parts) == 2 else parts[-2].strip()
                    # Remove the state and postcode from the suburb part
                    suburb_part = re.sub(r'\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b', '', suburb_part)
                    suburb_part = re.sub(r'\b\d{4}\b', '', suburb_part)
                    suburb = suburb_part.strip()
            
            # Using fixed column order
            result = {
                'name': pharmacy_data.get('name'),
                'address': address,
                'email': pharmacy_data.get('email'),
                'fax': pharmacy_data.get('fax'),
                'latitude': None,  # Community Care Chemist doesn't provide coordinates
                'longitude': None, # Community Care Chemist doesn't provide coordinates
                'phone': pharmacy_data.get('phone'),
                'postcode': postcode,
                'state': state,
                'street_address': address,
                'suburb': suburb,
                'trading_hours': pharmacy_data.get('trading_hours', {}),
                'website': "https://www.communitycarechemist.com.au/"  # Default website
            }
            
        elif brand == "footes":
            # Handle Footes Pharmacy's data structure
            address = pharmacy_data.get('address', '')
            
            # State and postcode are already extracted in fetch_footes_store_details
            state = pharmacy_data.get('state')
            postcode = pharmacy_data.get('postcode')
            
            # Try to extract suburb from address if available
            suburb = None
            if address:
                # Extract suburb from address - typically format is "Street, Suburb STATE POSTCODE"
                address_parts = address.split(',')
                if len(address_parts) > 1:
                    # Last part likely contains STATE POSTCODE
                    # Part before that likely contains suburb
                    possible_suburb = address_parts[-2].strip() if len(address_parts) >= 2 else ""
                    # Clean up the suburb by removing state/postcode if present
                    if state:
                        possible_suburb = possible_suburb.replace(state, "")
                    if postcode:
                        possible_suburb = possible_suburb.replace(postcode, "")
                    suburb = possible_suburb.strip()
            
            # Phone might need cleaning (removing parentheses, etc.)
            phone = pharmacy_data.get('phone')
            if phone:
                # Clean up phone number format if needed
                phone = phone.replace('(', '').replace(')', ' ')
            
            # Using fixed column order
            result = {
                'name': pharmacy_data.get('name', ''),
                'address': address,
                'email': pharmacy_data.get('email'),
                'fax': None,  # Footes doesn't provide fax numbers
                'latitude': None,  # Not available in Footes data
                'longitude': None,  # Not available in Footes data
                'phone': phone,
                'postcode': postcode,
                'state': state,
                'street_address': address,
                'suburb': suburb,
                'trading_hours': pharmacy_data.get('trading_hours', {}),
                'website': "https://footespharmacies.com/"  # Default website
            }
            
        else:
            # Original extraction logic for DDS and Amcal
            location_details = pharmacy_data.get('location_details', {})
            
            # Extract trading hours directly from the top-level field
            trading_hours = pharmacy_data.get('trading_hours', {})
            
            # Using fixed column order
            result = {
                'name': location_details.get('locationname'),
                'address': location_details.get('address'),
                'email': location_details.get('email'),
                'fax': location_details.get('fax_number'),
                'latitude': location_details.get('latitude'),
                'longitude': location_details.get('longitude'),
                'phone': location_details.get('phone'),
                'postcode': location_details.get('postcode'),
                'state': location_details.get('state'),
                'street_address': location_details.get('streetaddress'),
                'suburb': location_details.get('suburb'),
                'trading_hours': trading_hours,
                'website': location_details.get('website')
            }
            
        # Remove any None values to keep the CSV clean
        cleaned_result = {}
        for key, value in result.items():
            if value is not None:
                cleaned_result[key] = value
                
        return cleaned_result
    
    async def fetch_dds_locations(self):
        """Fetch Discount Drug Stores locations"""
        return await self.fetch_locations("dds")
        
    async def fetch_amcal_locations(self):
        """Fetch Amcal Pharmacy locations"""
        return await self.fetch_locations("amcal")
        
    async def fetch_blooms_locations_list(self):
        """Fetch Blooms The Chemist locations"""
        return await self.fetch_locations("blooms")
        
    async def fetch_ramsay_locations_list(self):
        """Fetch Ramsay Pharmacy locations"""
        return await self.fetch_locations("ramsay")
        
    async def fetch_revive_locations_list(self):
        """Fetch Revive Pharmacy locations"""
        return await self.fetch_locations("revive")
    
    async def fetch_optimal_locations_list(self):
        """Fetch Optimal Pharmacy Plus locations"""
        return await self.fetch_locations("optimal")
    
    async def fetch_community_locations_list(self):
        """Fetch Community Care Chemist locations"""
        return await self.fetch_locations("community")
    
    async def fetch_footes_locations_list(self):
        """Fetch Footes Pharmacy locations"""
        return await self.fetch_locations("footes")
        
    async def fetch_dds_pharmacy_details(self, location_id):
        """Fetch details for a specific Discount Drug Store"""
        data = await self.fetch_pharmacy_details("dds", location_id)
        return self.extract_pharmacy_details(data)
        
    async def fetch_amcal_pharmacy_details(self, location_id):
        """Fetch details for a specific Amcal Pharmacy"""
        data = await self.fetch_pharmacy_details("amcal", location_id)
        return self.extract_pharmacy_details(data)
        
    async def fetch_all_locations_details(self, brand):
        """
        Fetch details for all locations of a specific brand and return as a list.
        Uses concurrent requests for better performance.
        
        Args:
            brand: String identifier for the brand (e.g., "dds", "amcal", "blooms", "ramsay", "revive", "optimal", "community")
            
        Returns:
            List of dictionaries containing pharmacy details
        """
        if brand == "blooms":
            # For Blooms, all details are included in the locations endpoint
            print(f"Fetching all {brand.upper()} locations...")
            locations = await self.fetch_blooms_locations()
            if not locations:
                print(f"No {brand.upper()} locations found.")
                return []
                
            print(f"Found {len(locations)} {brand.upper()} locations. Processing details...")
            all_details = []
            
            for location in locations:
                try:
                    extracted_details = self.extract_pharmacy_details(location, brand="blooms")
                    all_details.append(extracted_details)
                except Exception as e:
                    print(f"Error processing Blooms location {location.get('id')}: {e}")
                    
            print(f"Completed processing details for {len(all_details)} {brand.upper()} locations.")
            return all_details
        elif brand == "ramsay":
            # For Ramsay, all details are included in the locations endpoint
            print(f"Fetching all {brand.upper()} locations...")
            locations = await self.fetch_ramsay_locations()
            if not locations:
                print(f"No {brand.upper()} locations found.")
                return []
                
            print(f"Found {len(locations)} {brand.upper()} locations. Processing details...")
            all_details = []
            
            for location in locations:
                try:
                    extracted_details = self.extract_pharmacy_details(location, brand="ramsay")
                    all_details.append(extracted_details)
                except Exception as e:
                    print(f"Error processing Ramsay location {location.get('PharmacyId')}: {e}")
                    
            print(f"Completed processing details for {len(all_details)} {brand.upper()} locations.")
            return all_details
        elif brand == "revive":
            # For Revive, all details are included in the locations endpoint
            print(f"Fetching all {brand.upper()} locations...")
            locations = await self.fetch_revive_locations()
            if not locations:
                print(f"No {brand.upper()} locations found.")
                return []
                
            print(f"Found {len(locations)} {brand.upper()} locations. Processing details...")
            all_details = []
            
            for location in locations:
                try:
                    extracted_details = self.extract_pharmacy_details(location, brand="revive")
                    all_details.append(extracted_details)
                except Exception as e:
                    print(f"Error processing Revive location {location.get('id')}: {e}")
                    
            print(f"Completed processing details for {len(all_details)} {brand.upper()} locations.")
            return all_details
        elif brand == "optimal":
            # For Optimal, all details are included in the locations endpoint
            print(f"Fetching all {brand.upper()} locations...")
            locations = await self.fetch_optimal_locations()
            if not locations:
                print(f"No {brand.upper()} locations found.")
                return []
                
            print(f"Found {len(locations)} {brand.upper()} locations. Processing details...")
            all_details = []
            
            for location in locations:
                try:
                    extracted_details = self.extract_pharmacy_details(location, brand="optimal")
                    all_details.append(extracted_details)
                except Exception as e:
                    print(f"Error processing Optimal location {location.get('id')}: {e}")
                    
            print(f"Completed processing details for {len(all_details)} {brand.upper()} locations.")
            return all_details
        elif brand == "community":
            # For Community Care Chemist, all details are included in the locations endpoint
            print(f"Fetching all {brand.upper()} locations...")
            locations = await self.fetch_community_locations()
            if not locations:
                print(f"No {brand.upper()} locations found.")
                return []
                
            print(f"Found {len(locations)} {brand.upper()} locations. Processing details...")
            all_details = []
            
            for location in locations:
                try:
                    extracted_details = self.extract_pharmacy_details(location, brand="community")
                    all_details.append(extracted_details)
                except Exception as e:
                    print(f"Error processing Community Care Chemist location {location.get('id')}: {e}")
                    
            print(f"Completed processing details for {len(all_details)} {brand.upper()} locations.")
            return all_details
        elif brand == "footes":
            # For Footes Pharmacy, all details are included in the locations endpoint
            print(f"Fetching all {brand.upper()} locations...")
            locations = await self.fetch_footes_locations()
            if not locations:
                print(f"No {brand.upper()} locations found.")
                return []
                
            print(f"Found {len(locations)} {brand.upper()} locations. Processing details...")
            all_details = []
            
            for location in locations:
                try:
                    if 'detail_url' in location:
                        detailed_info = await self.fetch_footes_store_details(location['detail_url'])
                        detailed_info['name'] = location.get('name', '')  # Preserve name from main list
                        extracted_details = self.extract_pharmacy_details(detailed_info, brand="footes")
                        all_details.append(extracted_details)
                except Exception as e:
                    print(f"Error processing Footes Pharmacy location {location.get('id')}: {e}")
                    
            print(f"Completed processing details for {len(all_details)} {brand.upper()} locations.")
            return all_details
            
        if brand not in self.BRAND_CONFIGS:
            raise ValueError(f"Unknown pharmacy brand: {brand}")
            
        # First get all locations
        print(f"Fetching all {brand.upper()} locations...")
        locations = await self.fetch_locations(brand)
        if not locations:
            print(f"No {brand.upper()} locations found.")
            return []
            
        print(f"Found {len(locations)} {brand.upper()} locations. Fetching details concurrently...")
        
        # Create tasks for all locations
        tasks = []
        for location in locations:
            location_id = location.get('locationid')
            if location_id:
                task = self.fetch_pharmacy_details(brand, location_id)
                tasks.append((location_id, task))
        
        # Process each location to get details
        all_details = []
        batch_size = 10  # Process in batches to avoid overwhelming the server
        total_batches = (len(tasks) + batch_size - 1) // batch_size
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(tasks))
            batch_tasks = tasks[start_idx:end_idx]
            
            print(f"Processing batch {batch_idx + 1}/{total_batches} ({start_idx+1}-{end_idx} of {len(tasks)})...")
            
            # Run the batch concurrently
            results = await asyncio.gather(
                *[task for _, task in batch_tasks],
                return_exceptions=True
            )
            
            # Process results
            for i, result in enumerate(results):
                location_id, _ = batch_tasks[i]
                if isinstance(result, Exception):
                    print(f"Error fetching details for location {location_id}: {result}")
                else:
                    try:
                        extracted_details = self.extract_pharmacy_details(result)
                        all_details.append(extracted_details)
                    except Exception as e:
                        print(f"Error extracting details for location {location_id}: {e}")
                
        print(f"Completed fetching details for {len(all_details)} {brand.upper()} locations.")
        return all_details
        
    def save_to_csv(self, data, filename):
        """
        Save a list of dictionaries to a CSV file.
        
        Args:
            data: List of dictionaries with pharmacy details
            filename: Name of the CSV file to create
        """
        if not data:
            print(f"No data to save to {filename}")
            return
            
        # Ensure the output directory exists
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        filepath = output_dir / filename
        
        # Define the exact field order we want
        fixed_fieldnames = [
            'name', 'address', 'email', 'fax', 'latitude', 'longitude', 'phone', 
            'postcode', 'state', 'street_address', 'suburb', 'trading_hours', 'website'
        ]
        
        # Filter data to only include the specified fields
        filtered_data = []
        for item in data:
            filtered_item = {}
            for field in fixed_fieldnames:
                filtered_item[field] = item.get(field, None)
            filtered_data.append(filtered_item)
        
        print(f"Saving {len(filtered_data)} records to {filepath}...")
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fixed_fieldnames)
            writer.writeheader()
            writer.writerows(filtered_data)
            
        print(f"Data successfully saved to {filepath}")
        
    async def fetch_and_save_all(self):
        """
        Fetch all locations for all brands concurrently and save to CSV files.
        """
        # Create task for each brand, now including community and footes
        brands = list(self.BRAND_CONFIGS.keys()) + ["blooms", "ramsay", "revive", "optimal", "community", "footes"]
        tasks = {brand: self.fetch_all_locations_details(brand) for brand in brands}
        
        # Execute all brand tasks concurrently
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        
        # Process results and save to CSV
        for brand, result in zip(tasks.keys(), results):
            try:
                if isinstance(result, Exception):
                    print(f"Error processing {brand} pharmacies: {result}")
                elif result:
                    self.save_to_csv(result, f"{brand}_pharmacies.csv")
                else:
                    print(f"No data found for {brand} pharmacies")
            except Exception as e:
                print(f"Error saving {brand} pharmacies data: {e}")

# Example usage
async def main():
    pharmacy_api = PharmacyLocations()
    
    # Fetch DDS locations
    dds_locations = await pharmacy_api.fetch_dds_locations()
    print(f"Found {len(dds_locations)} Discount Drug Store locations")
    
    # Fetch Amcal locations
    amcal_locations = await pharmacy_api.fetch_amcal_locations()
    print(f"Found {len(amcal_locations)} Amcal locations")
    
    # Fetch Blooms locations
    blooms_locations = await pharmacy_api.fetch_blooms_locations_list()
    print(f"Found {len(blooms_locations)} Blooms The Chemist locations")
    
    # Fetch Ramsay locations
    ramsay_locations = await pharmacy_api.fetch_ramsay_locations_list()
    print(f"Found {len(ramsay_locations)} Ramsay Pharmacy locations")
    
    # Fetch Revive locations
    revive_locations = await pharmacy_api.fetch_revive_locations_list()
    print(f"Found {len(revive_locations)} Revive Pharmacy locations")
    
    # Fetch Optimal locations
    optimal_locations = await pharmacy_api.fetch_optimal_locations_list()
    print(f"Found {len(optimal_locations)} Optimal Pharmacy Plus locations")
    
    # Fetch Community Care Chemist locations
    community_locations = await pharmacy_api.fetch_community_locations_list()
    print(f"Found {len(community_locations)} Community Care Chemist locations")
    
    # Fetch Footes Pharmacy locations
    footes_locations = await pharmacy_api.fetch_footes_locations_list()
    print(f"Found {len(footes_locations)} Footes Pharmacy locations")
    
    # Fetch DDS pharmacy details
    if dds_locations:
        dds_details = await pharmacy_api.fetch_dds_pharmacy_details(dds_locations[0]['locationid'])
        print(f"DDS Pharmacy Details: {dds_details}")
    
    # Fetch Amcal pharmacy details
    if amcal_locations:
        amcal_details = await pharmacy_api.fetch_amcal_pharmacy_details(amcal_locations[0]['locationid'])
        print(f"Amcal Pharmacy Details: {amcal_details}")
    
    # Fetch and save all pharmacy details
    print("\nFetching and saving all pharmacy details...")
    await pharmacy_api.fetch_and_save_all()

if __name__ == "__main__":
    asyncio.run(main())