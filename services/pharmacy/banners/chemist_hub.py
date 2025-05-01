import re
import logging
from rich import print
from datetime import datetime
from bs4 import BeautifulSoup
from ..base_handler import BasePharmacyHandler

class ChemistHubHandler(BasePharmacyHandler):
    """Handler for Chemist Hub pharmacies"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "chemist_hub"
        self.base_url = "https://www.chemisthub.au/store-locator"
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
        }
        self.logger = logging.getLogger(__name__)
        # Pre-defined store URLs based on the provided information
        self.store_urls = self.pharmacy_locations.CHEMIST_HUB_URLS
    
    async def fetch_locations(self):
        """
        Create location objects for each predefined store URL
        
        Returns:
            List of dictionaries containing basic pharmacy information with URLs
        """
        self.logger.info("Setting up Chemist Hub pharmacy locations")
        
        all_locations = []
        for i, url in enumerate(self.store_urls):
            try:
                # Extract store name from URL
                store_slug = url.split("/")[-1]
                store_name = store_slug.replace("-", " ").title()
                store_id = f"chemist-hub-{i+1}"
                
                # Create basic location info
                location = {
                    'id': store_id,
                    'name': store_name,
                    'url': url,
                    'brand': 'Chemist Hub'
                }
                
                all_locations.append(location)
            except Exception as e:
                self.logger.warning(f"Error creating Chemist Hub location for {url}: {str(e)}")
        
        self.logger.info(f"Created {len(all_locations)} Chemist Hub pharmacy locations")
        return all_locations
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract pharmacy details from the pharmacy data
        
        Args:
            pharmacy_data: HTML content of the pharmacy detail page
            
        Returns:
            Dictionary with pharmacy details
        """
        if isinstance(pharmacy_data, tuple) and len(pharmacy_data) == 2:
            location, html_content = pharmacy_data
            return self._parse_detail_page(location, html_content)
        elif isinstance(pharmacy_data, dict):
            # If we're already given processed data, just return it
            return pharmacy_data
        else:
            self.logger.warning(f"Unexpected pharmacy_data type: {type(pharmacy_data)}")
            return {}
    
    async def fetch_pharmacy_details(self, location):
        """
        Fetch detailed information for a specific Chemist Hub pharmacy
        
        Args:
            location: Location dictionary containing the store URL
            
        Returns:
            Dictionary with detailed pharmacy information
        """
        url = location.get('url')
        if not url:
            self.logger.error(f"No URL provided for location: {location.get('name', 'Unknown')}")
            return None
        
        try:
            # Make GET request to the store detail page
            response = await self.session_manager.get(
                url,
                headers=self.headers
            )
            
            if response.status_code == 200:
                html_content = response.text
                # Parse the detail page and return the pharmacy details
                return self._parse_detail_page(location, html_content)
            else:
                self.logger.error(f"Failed to fetch pharmacy details for {url}: {response.status_code}")
                return None
        except Exception as e:
            self.logger.error(f"Exception when fetching details for {url}: {str(e)}")
            return None
    
    async def fetch_all_locations_details(self):
        """
        Fetch details for all Chemist Hub pharmacy locations
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        self.logger.info("Fetching details for all Chemist Hub pharmacy locations")
        
        # First, get all locations with their URLs
        locations = await self.fetch_locations()
        
        if not locations:
            self.logger.warning("No Chemist Hub pharmacy locations found")
            return []
        
        # Now fetch details for each location
        all_details = []
        for location in locations:
            details = await self.fetch_pharmacy_details(location)
            if details:
                all_details.append(details)
        
        self.logger.info(f"Successfully fetched details for {len(all_details)} Chemist Hub pharmacy locations")
        return all_details
    
    def _parse_detail_page(self, location, html_content):
        """
        Parse the HTML content of a store detail page
        
        Args:
            location: Location dictionary
            html_content: HTML content of the pharmacy detail page
            
        Returns:
            Dictionary with pharmacy details
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Initialize variables
        store_id = location.get('id', '')
        name = location.get('name', '')
        address = ""
        street_address = ""
        suburb = ""
        state = ""
        postcode = ""
        phone = ""
        email = ""
        trading_hours = {}
        
        # Extract store name from page title
        page_title = soup.find('h1', id='pageTitleText')
        if page_title:
            name = page_title.text.strip()
        
        # Extract address details
        address_div = soup.find('div', class_='address-item postal')
        if address_div:
            # Extract street address (first div)
            street_div = address_div.find('div')
            if street_div:
                street_address = street_div.text.strip()
            
            # Extract suburb (second div)
            address_divs = address_div.find_all('div')
            if len(address_divs) >= 2:
                suburb = address_divs[1].text.strip()
            
            # Extract state and postcode (third div)
            if len(address_divs) >= 3:
                state_postcode = address_divs[2].text.strip()
                # Split state and postcode (format: "New South Wales 2213")
                state_postcode_match = re.match(r'(.+?)\s+(\d{4})', state_postcode)
                if state_postcode_match:
                    state = state_postcode_match.group(1).strip()
                    postcode = state_postcode_match.group(2).strip()
            
            # Construct full address
            address_parts = [part for part in [street_address, suburb, f"{state} {postcode}"] if part]
            address = ", ".join(address_parts)
        
        # Extract email
        email_link = soup.find('div', class_='address-item email').find('a') if soup.find('div', class_='address-item email') else None
        if email_link:
            email = email_link.text.strip()
        
        # Extract phone
        phone_link = soup.find('div', class_='address-item phone').find('a') if soup.find('div', class_='address-item phone') else None
        if phone_link:
            phone = phone_link.text.strip()
        
        # Extract trading hours
        hours_list = soup.find('div', class_='openingHoursList')
        if hours_list:
            for item in hours_list.find_all('div', class_='openingHoursListItem'):
                day_elem = item.find('div', class_='openingHoursLabel')
                hours_elem = item.find('div', class_='openingHoursValue')
                
                if day_elem and hours_elem:
                    day = day_elem.text.strip()
                    
                    # Check if it's closed
                    closed_elem = hours_elem.find('div', class_='closed')
                    if closed_elem:
                        trading_hours[day] = {'open': 'Closed', 'close': 'Closed'}
                    else:
                        # Extract open and close times
                        sessions = hours_elem.find('div', class_='sessions')
                        if sessions:
                            time_spans = sessions.find_all('span')
                            if len(time_spans) >= 2:
                                open_time = time_spans[0].text.strip()
                                close_time = time_spans[1].text.strip()
                                trading_hours[day] = {'open': open_time, 'close': close_time}
        
        # Create the final result
        result = {
            'brand': 'Chemist Hub',
            'name': name,
            'store_id': store_id,
            'address': address,
            'street_address': street_address,
            'suburb': suburb,
            'state': state,
            'postcode': postcode,
            'phone': self._format_phone(phone),
            'email': email,
            'website': location.get('url', ''),
            'trading_hours': trading_hours,
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Convert state to abbreviation
        result = self._standardize_state(result)
        
        # Clean up the result
        return {k: v for k, v in result.items() if v not in (None, '', {}, [])}
    
    def _format_phone(self, phone):
        """Format phone number consistently"""
        if not phone:
            return None
        
        # Remove any non-digit characters except opening/closing brackets
        formatted = re.sub(r'[^\d\(\)\+]', '', phone)
        return formatted
    
    def _standardize_state(self, result):
        """Convert full state names to standard abbreviations"""
        state_mapping = {
            'NEW SOUTH WALES': 'NSW',
            'VICTORIA': 'VIC',
            'QUEENSLAND': 'QLD',
            'SOUTH AUSTRALIA': 'SA',
            'WESTERN AUSTRALIA': 'WA',
            'TASMANIA': 'TAS',
            'NORTHERN TERRITORY': 'NT',
            'AUSTRALIAN CAPITAL TERRITORY': 'ACT',
        }
        
        if result.get('state'):
            state_upper = result['state'].upper()
            if state_upper in state_mapping:
                result['state'] = state_mapping[state_upper]
        
        return result