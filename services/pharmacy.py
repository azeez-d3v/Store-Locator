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
    OPTIMAL_URL = "https://core.service.elfsight.com/p/boot/?page=https%3A%2F%2Foptimalpharmacyplus.com.au%2Flocations%2F&w=d70b40db-e8b3-43bc-a63b-b3cce68941bf"
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
        'Origin': 'https://www.ramsaypharmacy.com.au',
        'Referer': 'https://www.ramsaypharmacy.com.au/',
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
        "TodayTime": "15:51:37",
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
        
    async def get_ramsay_session_id(self):
        """
        Fetch the dynamic session ID from Ramsay Pharmacy's store finder page.
        This is needed for the API to work correctly.
        
        Returns:
            String containing the session ID
        """
        url = "https://www.ramsaypharmacy.com.au/Store-Finder"
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
        }
        
        response = await self.session_manager.get(
            url=url,
            headers=headers
        )
        
        if response.status_code == 200:
            html_content = response.text
            
            # Look for the session ID in the script tag
            session_id_pattern = r"StoreLocator\.LoadInitialData\('([^']+)', ''\);"
            match = re.search(session_id_pattern, html_content)
            
            if match:
                session_id = match.group(1)
                print(f"Successfully extracted Ramsay session ID: {session_id[:10]}...")
                return session_id
            else:
                print("Could not find session ID in Ramsay Store Finder page")
                return None
        else:
            print(f"Failed to fetch Ramsay Store Finder page: {response.status_code}")
            return None

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
    
    async def fetch_blooms_locations(self):
        """
        Fetch Blooms The Chemist locations from their API.
        
        Returns:
            List of Blooms The Chemist locations
        """
        response = await self.session_manager.get(
            url=self.BLOOMS_URL,
            headers=self.BLOOMS_HEADERS
        )
        
        if response.status_code == 200:
            data = response.json()
            # Check for the new response format where locations are under 'results'
            if 'results' in data and 'locations' in data['results']:
                print(f"Found {len(data['results']['locations'])} Blooms locations in API response")
                return data['results']['locations']
            # Check for older API formats just in case
            elif 'collection' in data and 'locations' in data['collection']:
                print(f"Found {len(data['collection']['locations'])} Blooms locations in API response (collection format)")
                return data['collection']['locations']
            elif 'locations' in data:
                print(f"Found {len(data['locations'])} Blooms locations in API response (direct format)")
                return data['locations']
            else:
                print("No locations found in Blooms API response")
                print(f"API response keys: {list(data.keys())}")
                return []
        else:
            raise Exception(f"Failed to fetch Blooms The Chemist locations: {response.status_code}")
    
    async def fetch_ramsay_locations(self):
        """
        Fetch Ramsay Pharmacy locations from their API.
        
        Returns:
            List of Ramsay Pharmacy locations
        """
        # First get the dynamic session ID
        session_id = await self.get_ramsay_session_id()
        
        # If we couldn't get a session ID, use a default empty payload
        payload = self.RAMSAY_PAYLOAD.copy()
        
        # If we have a session ID, add it to a special header
        headers = self.RAMSAY_HEADERS.copy()
        if session_id:
            headers['SessionId'] = session_id
            print(f"Using session ID for Ramsay API request")
        else:
            print("Warning: No session ID found for Ramsay API request")
            
        response = await self.session_manager.post(
            url=self.RAMSAY_URL,
            json=payload,
            headers=headers
        )
        
        if response.status_code == 200:
            data = response.json()
            
            # Check if data is a direct array (based on sample response)
            if isinstance(data, list):
                print(f"Found {len(data)} Ramsay locations in API response (direct array)")
                return data
            # Check nested structure (older API format)
            elif 'Data' in data and 'Results' in data['Data']:
                print(f"Found {len(data['Data']['Results'])} Ramsay locations in API response (nested)")
                return data['Data']['Results']
            else:
                print("No locations found in Ramsay API response")
                print(f"API response keys: {list(data.keys()) if isinstance(data, dict) else 'array'}")
                return []
        else:
            raise Exception(f"Failed to fetch Ramsay Pharmacy locations: {response.status_code}")
    
    async def fetch_revive_locations(self):
        """
        Fetch Revive Pharmacy locations from the Elfsight widget API.
        
        Returns:
            List of Revive Pharmacy locations
        """
        response = await self.session_manager.get(
            url=self.REVIVE_URL,
            headers=self.REVIVE_HEADERS
        )
        
        if response.status_code == 200:
            data = response.json()
            # Extract the widgets data from the response
            if 'status' in data and data['status'] == 1 and 'data' in data:
                widget_data = data['data'].get('widgets', {})
                if widget_data:
                    # Get the first widget's data
                    widget_id = list(widget_data.keys())[0]
                    widget = widget_data[widget_id]
                    
                    # Check for the specific path to locations based on the sample response
                    if ('data' in widget and 'settings' in widget['data'] and 
                        'locations' in widget['data']['settings']):
                        locations = widget['data']['settings']['locations']
                        print(f"Found {len(locations)} Revive locations in widget data settings")
                        return locations
                    
                    # Check alternative paths if the specific path doesn't work
                    if 'settings' in widget:
                        if 'locations' in widget['settings']:
                            locations = widget['settings']['locations']
                            print(f"Found {len(locations)} Revive locations in widget settings")
                            return locations
                        
                    # If we get here, dump some debug info about the structure
                    print(f"Widget data keys: {list(widget.keys())}")
                    if 'data' in widget:
                        print(f"Widget data keys: {list(widget['data'].keys())}")
                        if 'settings' in widget['data']:
                            print(f"Widget data settings keys: {list(widget['data']['settings'].keys())}")
            
            print("No locations found in Revive Pharmacy widget data")
            print("API response structure:", list(data.keys()))
            return []
        else:
            raise Exception(f"Failed to fetch Revive Pharmacy locations: {response.status_code}")
    
    async def fetch_optimal_locations(self):
        """
        Fetch Optimal Pharmacy Plus locations from the Elfsight widget API.
        
        Returns:
            List of Optimal Pharmacy Plus locations
        """
        response = await self.session_manager.get(
            url=self.OPTIMAL_URL,
            headers=self.OPTIMAL_HEADERS
        )
        
        if response.status_code == 200:
            data = response.json()
            # Extract the widgets data from the response
            if 'status' in data and data['status'] == 1 and 'data' in data:
                widget_data = data['data'].get('widgets', {})
                if widget_data:
                    # Get the first widget's data
                    widget_id = list(widget_data.keys())[0]
                    widget = widget_data[widget_id]
                    
                    # Check for the specific path to locations based on the sample response
                    if ('data' in widget and 'settings' in widget['data'] and 
                        'locations' in widget['data']['settings']):
                        locations = widget['data']['settings']['locations']
                        print(f"Found {len(locations)} Optimal locations in widget data settings")
                        return locations
                    
                    # Check alternative paths if the specific path doesn't work
                    if 'settings' in widget:
                        if 'locations' in widget['settings']:
                            locations = widget['settings']['locations']
                            print(f"Found {len(locations)} Optimal locations in widget settings")
                            return locations
                        
                    # If we get here, dump some debug info about the structure
                    print(f"Widget data keys: {list(widget.keys())}")
                    if 'data' in widget:
                        print(f"Widget data keys: {list(widget['data'].keys())}")
                        if 'settings' in widget['data']:
                            print(f"Widget data settings keys: {list(widget['data']['settings'].keys())}")
            
            print("No locations found in Optimal Pharmacy Plus widget data")
            print("API response structure:", list(data.keys()))
            return []
        else:
            raise Exception(f"Failed to fetch Optimal Pharmacy Plus locations: {response.status_code}")
    
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
                    # Process these stores similarly...
                    # Implementation similar to above
                else:
                    print("No store items found with any selector")
                    
                    # Try to find the main content div
                    main_content = soup.select('.elementor-loop-container')
                    if main_content:
                        print(f"Found main content container with {len(main_content[0].find_all())} child elements")
                    
                    raise Exception("No pharmacy locations found in Footes Pharmacy page")
        else:
            raise Exception(f"Failed to fetch Footes Pharmacy locations: {response.status_code}")
    
    async def fetch_footes_location_details(self, detail_url):
        """
        Fetch detailed information for a specific Footes Pharmacy from its detail page.
        
        Args:
            detail_url: URL to the store's detail page
            
        Returns:
            Dictionary containing detailed pharmacy information
        """
        # Helper function to decode obfuscated email addresses
        def decodeEmail(e):
            de = ""
            k = int(e[:2], 16)

            for i in range(2, len(e)-1, 2):
                de += chr(int(e[i:i+2], 16)^k)

            return de
            
        if not detail_url:
            return {}
            
        response = await self.session_manager.get(
            url=detail_url,
            headers=self.FOOTES_HEADERS
        )
        
        if response.status_code != 200:
            print(f"Failed to fetch Footes pharmacy details from {detail_url}: {response.status_code}")
            return {}
            
        html_content = response.text
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Dictionary to store detailed information
        details = {}
        
        try:
            # Extract address from contact details section
            address_element = soup.select_one('.elementor-element-d9bbb9b .elementor-heading-title')
            if address_element:
                # Handle address with <br> tags
                address_text = address_element.get_text('\n').strip()
                details['address'] = address_text.replace('\n', ', ')
                
                # Extract state and postcode from the address
                # Australian state pattern
                state_pattern = r'\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b'
                state_match = re.search(state_pattern, address_text)
                if state_match:
                    details['state'] = state_match.group(0)
                
                # Australian postcode pattern (4 digits)
                postcode_pattern = r'\b(\d{4})\b'
                postcode_match = re.search(postcode_pattern, address_text)
                if postcode_match:
                    details['postcode'] = postcode_match.group(0)
                    
                # Extract suburb (typically before state and postcode)
                # In address like "82 High Street, Boonah QLD 4310"
                # We want to extract "Boonah" as the suburb
                lines = address_text.split('\n')
                if len(lines) >= 2:
                    second_line = lines[1]
                    suburb_parts = second_line.split()
                    if len(suburb_parts) >= 3:  # Assuming format like "Suburb STATE POSTCODE"
                        details['suburb'] = suburb_parts[0]
                        
                # Store the street address (first line typically)
                if lines:
                    details['street_address'] = lines[0].strip()
            
            # Extract phone number
            phone_element = soup.select_one('.store-phone a')
            if phone_element:
                details['phone'] = phone_element.text.strip()
            
            # Extract fax number
            fax_element = soup.select_one('.elementor-element-2008741')
            if fax_element:
                fax_text = fax_element.text.strip()
                if fax_text.startswith('Fx:'):
                    details['fax'] = fax_text.replace('Fx:', '').strip()
            
            # Find email using the decoder function
            # First check for encoded emails in the HTML
            encoded_email_pattern = r'data-cfemail="([a-f0-9]+)"'
            email_matches = re.findall(encoded_email_pattern, html_content)
            
            if email_matches:
                # Found encoded email, decode it
                details['email'] = decodeEmail(email_matches[0])
            else:
                # If no encoded email, try the regular extraction methods
                email_div = soup.select_one('.elementor-element.store-email')
                if email_div:
                    # Find the anchor tag with href=mailto inside this div
                    email_link = email_div.select_one('a[href^="mailto:"]')
                    if email_link:
                        # Extract the text directly from the a tag
                        details['email'] = email_link.text.strip()
                        
                        # If the text extraction fails, try from the href
                        if not details['email']:
                            href = email_link.get('href', '')
                            if href and href.startswith('mailto:'):
                                details['email'] = href[7:]  # Skip 'mailto:' prefix
                
                # If we still don't have an email, try a more direct approach
                if 'email' not in details or not details['email']:
                    # Try to find the email container directly
                    email_container = soup.select_one('.elementor-element-ce44c5f')
                    if email_container:
                        email_link = email_container.select_one('a')
                        if email_link:
                            # Check if this is a CloudFlare protected email
                            cf_email = email_link.select_one('.__cf_email__')
                            if cf_email and cf_email.has_attr('data-cfemail'):
                                # Decode the email using the decodeEmail function
                                encoded_email = cf_email['data-cfemail']
                                details['email'] = decodeEmail(encoded_email)
                            else:
                                # Regular email extraction
                                details['email'] = email_link.text.strip()
                                
                                # Double-check by extracting from href if needed
                                if not details['email'] or '[email protected]' in details['email']:
                                    href = email_link.get('href', '')
                                    if href and href.startswith('mailto:'):
                                        # Extract raw email from the href
                                        raw_email = href[7:]  # Skip 'mailto:' prefix
                                        if '@' in raw_email:
                                            details['email'] = raw_email
                
                # Final fallback - extract from raw HTML using regex
                if 'email' not in details or not details['email'] or '[email protected]' in details['email']:
                    # Extract all mailto links from the HTML
                    email_matches = re.findall(r'href="mailto:([^"]+)"', html_content)
                    if email_matches:
                        details['email'] = email_matches[0]
            
            # Extract trading hours
            trading_hours = {}
            
            # Find the containers for day names and hours
            days_container = soup.select_one('.elementor-element-fb1522c')
            hours_container = soup.select_one('.elementor-element-b96bcb7')
            
            if days_container and hours_container:
                # Extract all day elements
                day_elements = days_container.select('.elementor-widget-text-editor')
                hour_elements = hours_container.select('.elementor-widget-text-editor')
                
                # Process each day and its hours
                for i, (day_element, hour_element) in enumerate(zip(day_elements, hour_elements)):
                    day_text = day_element.text.strip()
                    hours_text = hour_element.text.strip()
                    
                    # For "Monday – Friday" type entries
                    if '–' in day_text or '-' in day_text:
                        day_range = day_text.replace('–', '-').split('-')
                        if len(day_range) == 2:
                            start_day = day_range[0].strip()
                            end_day = day_range[1].strip()
                            
                            # Map day names to standardized format
                            day_mapping = {
                                'Monday': 'Monday',
                                'Tuesday': 'Tuesday', 
                                'Wednesday': 'Wednesday',
                                'Thursday': 'Thursday',
                                'Friday': 'Friday',
                                'Saturday': 'Saturday',
                                'Sunday': 'Sunday',
                                'Mon': 'Monday',
                                'Tue': 'Tuesday',
                                'Wed': 'Wednesday',
                                'Thu': 'Thursday',
                                'Fri': 'Friday',
                                'Sat': 'Saturday',
                                'Sun': 'Sunday'
                            }
                            
                            # Get the full day names
                            days_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                            start_idx = days_of_week.index(day_mapping.get(start_day, start_day))
                            end_idx = days_of_week.index(day_mapping.get(end_day, end_day))
                            
                            # Apply hours to all days in the range
                            for day_idx in range(start_idx, end_idx + 1):
                                day = days_of_week[day_idx]
                                if hours_text.lower() == 'closed':
                                    trading_hours[day] = {'open': 'Closed', 'closed': 'Closed'}
                                elif '–' in hours_text or '-' in hours_text:
                                    hours_parts = hours_text.replace('–', '-').split('-')
                                    if len(hours_parts) == 2:
                                        trading_hours[day] = {
                                            'open': hours_parts[0].strip(),
                                            'closed': hours_parts[1].strip()
                                        }
                    else:
                        # Single day entry
                        if day_text.lower() in ['saturday', 'sunday', 'sat', 'sun']:
                            day = 'Saturday' if day_text.lower() in ['saturday', 'sat'] else 'Sunday'
                            
                            if hours_text.lower() == 'closed':
                                trading_hours[day] = {'open': 'Closed', 'closed': 'Closed'}
                            elif '–' in hours_text or '-' in hours_text:
                                hours_parts = hours_text.replace('–', '-').split('-')
                                if len(hours_parts) == 2:
                                    trading_hours[day] = {
                                        'open': hours_parts[0].strip(),
                                        'closed': hours_parts[1].strip()
                                    }
            
            details['trading_hours'] = trading_hours
            
        except Exception as e:
            print(f"Error extracting Footes pharmacy details from {detail_url}: {e}")
            
        return details
    
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
            # For Footes Pharmacy, fetch details from individual store pages
            print(f"Fetching all {brand.upper()} locations...")
            locations = await self.fetch_footes_locations()
            if not locations:
                print(f"No {brand.upper()} locations found.")
                return []
                
            print(f"Found {len(locations)} {brand.upper()} locations. Fetching details from individual pages...")
            all_details = []
            
            for location in locations:
                try:
                    # Get basic information from the main listing
                    base_info = {
                        'name': location.get('name'),
                        'phone': location.get('phone'),
                        'website': location.get('detail_url')
                    }
                    
                    # Fetch detailed information from the store's individual page
                    detail_url = location.get('detail_url')
                    if detail_url:
                        print(f"Fetching details for {location.get('name')} from {detail_url}")
                        detailed_info = await self.fetch_footes_location_details(detail_url)
                        
                        # Merge basic info with detailed info, with detailed info taking precedence
                        merged_info = {**base_info, **detailed_info}
                        all_details.append(merged_info)
                    else:
                        # If we don't have a detail URL, just use the basic info
                        print(f"No detail URL for {location.get('name')}, using basic info only")
                        all_details.append(base_info)
                except Exception as e:
                    print(f"Error processing Footes Pharmacy location {location.get('name')}: {e}")
                    # Still add the basic info even if detailed fetch fails
                    all_details.append({
                        'name': location.get('name'),
                        'phone': location.get('phone'),
                        'website': location.get('detail_url')
                    })
                    
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
        
    async def fetch_and_save_all(self, selected_brands=None):
        """
        Fetch all locations for all brands concurrently and save to CSV files.
        
        Args:
            selected_brands: List of brands to fetch. If None, fetch all brands.
        """
        # Get list of brands to process
        if selected_brands is None:
            # If no brands specified, use all brands
            brands = list(self.BRAND_CONFIGS.keys()) + ["blooms", "ramsay", "revive", "optimal", "community", "footes"]
        else:
            # Only use the brands that were selected
            brands = selected_brands
            
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