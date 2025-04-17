import asyncio
import sys
import os
import csv
import json
import warnings
from pathlib import Path
import re
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

# Filter out the XML parsed as HTML warning
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

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
    FOOTES_SITEMAP_URL = "https://footespharmacies.com/stores-sitemap.xml"
    
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
        Fetch all Footes Pharmacy locations using the sitemap XML.
        
        Returns:
            List of Footes Pharmacy locations with basic information
        """
        # First fetch the sitemap XML to get all store URLs
        response = await self.session_manager.get(
            url=self.FOOTES_SITEMAP_URL,
            headers=self.FOOTES_HEADERS
        )
        
        if response.status_code == 200:
            xml_content = response.text
            
            # Parse as XML with correct features
            soup = BeautifulSoup(xml_content, 'xml' if 'xml' in 'lxml' else 'html.parser')
            
            # Find all store URLs in the XML sitemap format
            store_links = []
            url_elements = soup.find_all('url')
            
            if url_elements:
                for url_elem in url_elements:
                    loc_elem = url_elem.find('loc')
                    if loc_elem:
                        store_url = loc_elem.text
                        if store_url and '/stores/' in store_url and not store_url.endswith('/stores/'):
                            store_links.append(store_url)
                
                if not store_links:
                    print("No store links found in sitemap")
                    return []
                
                print(f"Found {len(store_links)} store links in sitemap")
                
                # Process each store URL to extract basic information
                locations = []
                for store_url in store_links:
                    try:
                        # Extract store name from URL
                        store_name = store_url.rstrip('/').split('/')[-1].replace('-', ' ').title()
                        
                        pharmacy_data = {
                            'name': f"Footes Pharmacy {store_name}",
                            'detail_url': store_url,
                            'id': f"footes_{store_name.lower().replace(' ', '_')}"
                        }
                        
                        locations.append(pharmacy_data)
                    except Exception as e:
                        print(f"Error processing Footes Pharmacy location URL {store_url}: {e}")
                
                if locations:
                    # Now fetch additional details for each location
                    return await self.enrich_footes_locations(locations)
                else:
                    print("No pharmacy data could be extracted from the sitemap links")
                    raise Exception("No pharmacy data could be extracted from Footes Pharmacy sitemap")
            else:
                # Try alternative approach by directly parsing the XML with regex if parsing fails
                print("No URL elements found in sitemap XML, trying regex approach")
                url_pattern = r'<loc>(https://footespharmacies\.com/stores/[^/]+/)</loc>'
                matches = re.findall(url_pattern, xml_content)
                
                if matches:
                    print(f"Found {len(matches)} store links with regex")
                    store_links = matches
                    
                    # Process each store URL just as above
                    locations = []
                    for store_url in store_links:
                        try:
                            store_name = store_url.rstrip('/').split('/')[-1].replace('-', ' ').title()
                            
                            pharmacy_data = {
                                'name': f"Footes Pharmacy {store_name}",
                                'detail_url': store_url,
                                'id': f"footes_{store_name.lower().replace(' ', '_')}"
                            }
                            
                            locations.append(pharmacy_data)
                        except Exception as e:
                            print(f"Error processing Footes Pharmacy location URL {store_url}: {e}")
                    
                    if locations:
                        return await self.enrich_footes_locations(locations)
                
                print("No store links found in sitemap with any method")
                print(f"Sitemap XML preview: {xml_content[:300]}...")
                raise Exception("No pharmacy locations found in Footes Pharmacy sitemap")
        else:
            raise Exception(f"Failed to fetch Footes Pharmacy sitemap: {response.status_code}")
    
    async def enrich_footes_locations(self, locations):
        """
        Fetch additional details for Footes Pharmacy locations.
        
        Args:
            locations: List of location dictionaries with store URLs
            
        Returns:
            List of locations with additional details
        """
        enriched_locations = []
        
        # Process 5 locations at a time to avoid overwhelming the server
        batch_size = 5
        for i in range(0, len(locations), batch_size):
            batch = locations[i:i+batch_size]
            print(f"Processing Footes locations batch {i//batch_size + 1}/{(len(locations) + batch_size - 1)//batch_size}")
            
            # Create tasks for fetching details
            tasks = []
            for location in batch:
                task = self.fetch_footes_store_details(location)
                tasks.append(task)
            
            # Run tasks concurrently
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for result in results:
                if isinstance(result, Exception):
                    print(f"Error fetching Footes store details: {result}")
                elif result:
                    enriched_locations.append(result)
        
        print(f"Successfully processed {len(enriched_locations)} out of {len(locations)} Footes locations")
        return enriched_locations
    
    async def fetch_footes_store_details(self, location):
        """
        Fetch details for a specific Footes Pharmacy location.
        
        Args:
            location: Dictionary containing basic location information including detail_url
            
        Returns:
            Dictionary with enriched location data
        """
        try:
            store_url = location.get('detail_url')
            if not store_url:
                return location
            
            response = await self.session_manager.get(
                url=store_url,
                headers=self.FOOTES_HEADERS
            )
            
            if response.status_code != 200:
                print(f"Failed to fetch details for {location.get('name')}: {response.status_code}")
                return location
            
            html_content = response.text
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract address from heading element
            address_element = soup.select_one('.elementor-element-d9bbb9b .elementor-heading-title, [data-id="d9bbb9b"] .elementor-heading-title')
            if address_element:
                address = address_element.text.strip()
                location['address'] = address
                
                # Try to extract state and postcode from address
                state_pattern = r'\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b'
                state_match = re.search(state_pattern, address)
                if state_match:
                    location['state'] = state_match.group(0)
                
                # Australian postcode pattern (4 digits)
                postcode_pattern = r'\b(\d{4})\b'
                postcode_match = re.search(postcode_pattern, address)
                if postcode_match:
                    location['postcode'] = postcode_match.group(0)
            
            # Extract phone number from store-phone element
            phone_element = soup.select_one('.store-phone a')
            if phone_element:
                phone = phone_element.text.strip()
                location['phone'] = phone
            
            # Extract fax number - using the class name or finding text that contains "Fx:"
            fax_element = soup.select_one('.elementor-element-2008741, [data-id="2008741"]')
            if fax_element:
                fax_text = fax_element.text.strip()
                if "Fx:" in fax_text:
                    location['fax'] = fax_text.replace("Fx:", "").strip()
                else:
                    location['fax'] = fax_text
            
            # Extract email from store-email element
            email_element = soup.select_one('.store-email a')
            if email_element:
                email = email_element.text.strip()
                location['email'] = email
            
            # Extract trading hours - new structure with days and hours in separate columns
            trading_hours = {}
            
            # Days are in one column, hours in another
            day_elements = soup.select('.elementor-element-fb1522c .elementor-widget-text-editor')
            hour_elements = soup.select('.elementor-element-b96bcb7 .elementor-widget-text-editor')
            
            # Map each day to its hours
            for i in range(min(len(day_elements), len(hour_elements))):
                day_text = day_elements[i].text.strip()
                hour_text = hour_elements[i].text.strip()
                
                # Handle "Monday – Friday" format
                if '–' in day_text or '-' in day_text:
                    day_range = re.split(r'–|-', day_text)
                    if len(day_range) == 2:
                        start_day = day_range[0].strip().lower()
                        end_day = day_range[1].strip().lower()
                        
                        # Map day names to standardized format
                        day_mapping = {
                            'monday': 'Monday',
                            'tuesday': 'Tuesday', 
                            'wednesday': 'Wednesday',
                            'thursday': 'Thursday',
                            'friday': 'Friday',
                            'saturday': 'Saturday',
                            'sunday': 'Sunday'
                        }
                        
                        # Determine which days are in the range
                        days_in_order = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
                        start_idx = next((i for i, d in enumerate(days_in_order) if start_day in d), -1)
                        end_idx = next((i for i, d in enumerate(days_in_order) if end_day in d), -1)
                        
                        if start_idx != -1 and end_idx != -1:
                            for day_idx in range(start_idx, end_idx + 1):
                                day_name = day_mapping[days_in_order[day_idx]]
                                
                                # Parse hours
                                if 'closed' in hour_text.lower():
                                    trading_hours[day_name] = {'open': 'Closed', 'closed': 'Closed'}
                                else:
                                    # Extract opening and closing times
                                    hour_parts = re.split(r'–|-', hour_text)
                                    if len(hour_parts) == 2:
                                        open_time = hour_parts[0].strip()
                                        close_time = hour_parts[1].strip()
                                        trading_hours[day_name] = {'open': open_time, 'closed': close_time}
                else:
                    # Single day
                    day_name = None
                    for key, value in {'monday': 'Monday', 'tuesday': 'Tuesday', 'wednesday': 'Wednesday', 
                                      'thursday': 'Thursday', 'friday': 'Friday', 'saturday': 'Saturday', 
                                      'sunday': 'Sunday'}.items():
                        if key in day_text.lower():
                            day_name = value
                            break
                    
                    if day_name:
                        # Parse hours
                        if 'closed' in hour_text.lower():
                            trading_hours[day_name] = {'open': 'Closed', 'closed': 'Closed'}
                        else:
                            # Extract opening and closing times
                            hour_parts = re.split(r'–|-', hour_text)
                            if len(hour_parts) == 2:
                                open_time = hour_parts[0].strip()
                                close_time = hour_parts[1].strip()
                                trading_hours[day_name] = {'open': open_time, 'closed': close_time}
            
            # If we found trading hours, add them to the location
            if trading_hours:
                location['trading_hours'] = trading_hours
                
            # Add website
            location['website'] = 'https://footespharmacies.com/'
            
            # If we still don't have basic fields, search more broadly
            if not location.get('phone') or not location.get('address') or not location.get('email') or not location.get('fax'):
                # Try more general selectors for missing information
                
                # Try to find phone by looking for tel: links if not found yet
                if not location.get('phone'):
                    phone_links = soup.select('a[href^="tel:"]')
                    if phone_links:
                        location['phone'] = phone_links[0].text.strip()
                
                # Try to find email by looking for mailto: links if not found yet
                if not location.get('email'):
                    email_links = soup.select('a[href^="mailto:"]')
                    if email_links:
                        location['email'] = email_links[0].text.strip()
                        
                # Try to find fax in any text containing "Fx:" if not found yet
                if not location.get('fax'):
                    for element in soup.select('.elementor-text-editor'):
                        text = element.text.strip()
                        if 'Fx:' in text:
                            location['fax'] = text.replace('Fx:', '').strip()
                            break
                
                # Try to find address in any heading if not found yet
                if not location.get('address'):
                    for element in soup.select('.elementor-heading-title'):
                        text = element.text.strip()
                        # Check for address pattern (look for postcode)
                        if re.search(r'\b\d{4}\b', text):
                            location['address'] = text
                            # Extract state and postcode
                            state_match = re.search(r'\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b', text)
                            if state_match:
                                location['state'] = state_match.group(0)
                            postcode_match = re.search(r'\b(\d{4})\b', text)
                            if postcode_match:
                                location['postcode'] = postcode_match.group(0)
                            break
            
            return location
        except Exception as e:
            print(f"Error fetching details for Footes store {location.get('name')}: {e}")
            return location
    
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
            # Handle Footes Pharmacy's structure
            # Extract address, state, and postcode
            address = pharmacy_data.get('address', '')
            state = pharmacy_data.get('state', '')
            postcode = pharmacy_data.get('postcode', '')
            
            # Try to extract suburb from address
            suburb = None
            if address:
                # Try to parse out the suburb from the address
                # First remove state and postcode if present
                address_without_state = re.sub(r'\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b', '', address)
                address_without_postcode = re.sub(r'\b\d{4}\b', '', address_without_state)
                
                # Check if there's a comma in the address that might separate street and suburb
                address_parts = address_without_postcode.split(',')
                if len(address_parts) > 1:
                    suburb = address_parts[-1].strip()
            
            # Parse trading hours
            trading_hours = pharmacy_data.get('trading_hours', {})
            
            # Using fixed column order
            result = {
                'name': pharmacy_data.get('name'),
                'address': address,
                'email': None,  # Footes typically doesn't list email on the website
                'fax': None,    # Footes typically doesn't list fax on the website
                'latitude': None,  # No coordinates in the data
                'longitude': None, # No coordinates in the data
                'phone': pharmacy_data.get('phone'),
                'postcode': postcode,
                'state': state,
                'street_address': address,
                'suburb': suburb,
                'trading_hours': trading_hours,
                'website': pharmacy_data.get('website', 'https://footespharmacies.com/')
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
                    extracted_details = self.extract_pharmacy_details(location, brand="footes")
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