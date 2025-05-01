import re
import logging
from rich import print
from datetime import datetime
from bs4 import BeautifulSoup, Comment
from ..base_handler import BasePharmacyHandler

class WizardPharmacyHandler(BasePharmacyHandler):
    """Handler for Wizard Pharmacy stores"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "wizard"
        self.base_url = self.pharmacy_locations.WIZARD_URL
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
        }
        self.logger = logging.getLogger(__name__)
    
    async def fetch_locations(self):
        """
        Fetch all Wizard Pharmacy locations from the store finder page
        
        Returns:
            List of dictionaries containing basic pharmacy information with URLs
        """
        self.logger.info("Fetching Wizard Pharmacy locations")
        
        try:
            # Make GET request to the store finder page
            response = await self.session_manager.get(
                self.base_url,
                headers=self.headers
            )
            
            if response.status_code == 200:
                html_content = response.text
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Find all store tiles in the page
                store_tiles = soup.find_all('div', class_='store-tiles')
                
                all_locations = []
                for i, store_tile in enumerate(store_tiles):
                    try:
                        # Extract store name
                        title_div = store_tile.find('div', class_='store-title')
                        if not title_div:
                            continue
                            
                        store_name = title_div.text.strip()
                        store_id = f"wizard-{i+1}"
                        
                        # Extract the detail URL - this is crucial for the next step
                        detail_link = store_tile.select_one('a[href^="/store-location/"]')
                        if not detail_link:
                            self.logger.warning(f"Could not find detail link for {store_name}")
                            continue
                            
                        detail_url = "https://www.wizardpharmacy.com.au" + detail_link.get('href')
                        
                        # Create basic location object with the URL
                        location = {
                            'id': store_id,
                            'name': f"Wizard Pharmacy {store_name}",
                            'url': detail_url,
                            'brand': 'Wizard Pharmacy'
                        }
                        
                        all_locations.append(location)
                    except Exception as e:
                        self.logger.warning(f"Error extracting Wizard Pharmacy location item {i}: {str(e)}")
                
                self.logger.info(f"Found {len(all_locations)} Wizard Pharmacy locations")
                return all_locations
            else:
                self.logger.error(f"Failed to fetch Wizard Pharmacy locations: {response.status_code}")
                return []
        except Exception as e:
            self.logger.error(f"Exception when fetching Wizard Pharmacy locations: {str(e)}")
            return []
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract pharmacy details from the pharmacy data
        Implementation of the required abstract method
        
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
        Fetch detailed information for a specific Wizard Pharmacy
        
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
        Fetch details for all Wizard Pharmacy locations
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        self.logger.info("Fetching details for all Wizard Pharmacy locations")
        
        # First, get all locations with their URLs
        locations = await self.fetch_locations()
        
        if not locations:
            self.logger.warning("No Wizard Pharmacy locations found")
            return []
        
        # Now fetch details for each location
        all_details = []
        for location in locations:
            details = await self.fetch_pharmacy_details(location)
            if details:
                all_details.append(details)
        
        self.logger.info(f"Successfully fetched details for {len(all_details)} Wizard Pharmacy locations")
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
        fax = ""
        trading_hours = {}
        
        # Extract store name and location from the header
        store_name_div = soup.find('div', class_='store-name')
        store_location_div = soup.find('div', class_='store-location')
        
        if store_name_div and store_location_div:
            name = f"Wizard Pharmacy {store_location_div.text.strip()}"
        
        # Find the Contact section
        contact_section = None
        for section in soup.find_all('div', class_='section-container'):
            if section.find('h3', class_='section-title', string=lambda s: s and 'Contact' in s):
                contact_section = section
                break
        
        if contact_section:
            # Extract address and contact details
            address_elem = contact_section.find('address', class_='address')
            if address_elem:
                # First, remove all comment nodes to prevent parsing commented-out text
                for comment in address_elem.find_all(text=lambda text: isinstance(text, Comment)):
                    comment.extract()
                
                # Extract the street address (before the <br> tag)
                street_content = None
                for content in address_elem.contents:
                    if isinstance(content, str) and content.strip():
                        street_content = content.strip()
                        break
                
                if street_content:
                    street_address = street_content
                
                # Extract suburb, state, and postcode
                # These are typically after the <br> tag and separated by span separators
                br_tag = address_elem.find('br')
                if br_tag:
                    # Get all content after the <br> tag
                    address_parts = []
                    current = br_tag.next_sibling
                    
                    # Collect all text nodes and spans
                    while current:
                        if isinstance(current, str) and current.strip():
                            address_parts.append(current.strip())
                        elif current.name == 'span' and 'separator' in current.get('class', []):
                            # Skip the separator spans
                            pass
                        current = current.next_sibling
                    
                    # Now parse the address parts
                    if len(address_parts) >= 3:
                        suburb = address_parts[0].strip()
                        state = address_parts[1].strip()  # This should be the state code like WA, NSW, etc.
                        postcode = address_parts[2].strip()
                
                # Construct full address without any "Shop x, Belmont Village S/C | Belmont |" prefix
                address_parts = [part for part in [street_address, suburb, state, postcode] if part]
                address = ", ".join(address_parts)
                
                # Extract phone, email, fax
                for div in address_elem.find_all('div'):
                    div_text = div.get_text()
                    
                    # Phone
                    if 'T' in div_text:
                        phone_match = re.search(r'T\s*(?:&nbsp;)?(.+)', div_text)
                        if phone_match:
                            phone = phone_match.group(1).strip()
                    
                    # Email
                    if 'E' in div_text:
                        email_link = div.find('a')
                        if email_link:
                            email = email_link.text.strip()
                    
                    # Fax
                    if 'F' in div_text:
                        fax_match = re.search(r'F\s*(?:&nbsp;)?(.+)', div_text)
                        if fax_match:
                            fax = fax_match.group(1).strip()
        
        # Find the Trading Hours section
        hours_section = None
        for section in soup.find_all('div', class_='section-container'):
            if section.find('h3', class_='section-title', string=lambda s: s and 'Trading Hours' in s):
                hours_section = section
                break
        
        if hours_section:
            # Extract trading hours
            trading_rows = hours_section.find_all('div', class_='trading-row')
            for row in trading_rows:
                day_elem = row.find('dt')
                hours_elem = row.find('dd')
                
                if day_elem and hours_elem:
                    day = day_elem.text.strip()
                    hours_text = hours_elem.text.strip()
                    
                    if hours_text.lower() == 'closed':
                        trading_hours[day] = {'open': 'Closed', 'close': 'Closed'}
                    else:
                        # Parse hours like "09:00 AM to 05:00 PM"
                        hours_match = re.search(r'([\d:]+\s*(?:AM|PM))\s*to\s*([\d:]+\s*(?:AM|PM))', hours_text, re.IGNORECASE)
                        if hours_match:
                            open_time = hours_match.group(1).strip()
                            close_time = hours_match.group(2).strip()
                            trading_hours[day] = {'open': open_time, 'close': close_time}
        
        # Create the final result
        result = {
            'brand': 'Wizard Pharmacy',
            'name': name,
            'store_id': store_id,
            'address': address,
            'street_address': street_address,
            'suburb': suburb,
            'state': state,
            'postcode': postcode,
            'phone': self._format_phone(phone),
            'fax': self._format_phone(fax),
            'email': email,
            'website': location.get('url', ''),
            'trading_hours': trading_hours,
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Clean up the result
        return {k: v for k, v in result.items() if v not in (None, '', {}, [])}
    
    def _format_phone(self, phone):
        """Format phone number consistently"""
        if not phone:
            return None
        
        # Remove any non-digit characters except opening/closing brackets
        formatted = re.sub(r'[^\d\(\)\+]', '', phone)
        return formatted