from ..base_handler import BasePharmacyHandler
import logging
import re
import json
from datetime import datetime
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

class HealthyPharmacyHandler(BasePharmacyHandler):
    """Handler for Healthy Pharmacy stores"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "healthy_pharmacy"

        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
        }
        self.logger = logging.getLogger(__name__)
        
    async def fetch_locations(self):
        """
        Fetch all Healthy Pharmacy store locations from the sitemap XML
        
        Returns:
            List of Healthy Pharmacy locations with basic details
        """
        try:
            # Make request to the sitemap XML
            response = await self.session_manager.get(
                url=self.pharmacy_locations.HEALTHY_LIFE_SITEMAP_URL,
                headers=self.headers
            )
            
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch Healthy Pharmacy locations: HTTP {response.status_code}")
                return []
            
            # Parse the XML content
            try:
                # Use BeautifulSoup with 'lxml' parser for XML
                soup = BeautifulSoup(response.text, 'lxml')
                
                # Find all location URLs in the sitemap
                urls = []
                for url_tag in soup.find_all('loc'):
                    url = url_tag.text.strip()
                    self.logger.debug(f"Processing URL: {url}")
                    # Skip the main store listing page
                    if url != "https://www.healthylife.com.au/stores":
                        urls.append(url)
                
                # Initialize the list for storing basic pharmacy information
                all_locations = []
                
                for i, url in enumerate(urls):
                    try:
                        # Extract store name and ID from URL
                        store_name = url.split('/')[-1].replace('-', ' ').title()
                        store_id = str(i + 1)  # Use index as ID if no better ID available
                        
                        # Create basic location info
                        location = {
                            'id': store_id,
                            'name': store_name,
                            'url': url,
                            'brand': 'Healthy Pharmacy'
                        }
                        
                        all_locations.append(location)
                    except Exception as e:
                        self.logger.warning(f"Error extracting Healthy Pharmacy location item {i}: {str(e)}")
                
                self.logger.info(f"Found {len(all_locations)} Healthy Pharmacy locations")
                return all_locations
            except Exception as e:
                self.logger.error(f"XML parsing error for Healthy Pharmacy locations: {str(e)}")
                return []
        except Exception as e:
            self.logger.error(f"Exception when fetching Healthy Pharmacy locations: {str(e)}")
            return []
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract specific fields from pharmacy data
        
        Args:
            pharmacy_data: Dictionary containing raw pharmacy data
            
        Returns:
            Standardized pharmacy details dictionary
        """
        if not pharmacy_data:
            return {}
            
        # For Healthy Pharmacy, data is already in the right format from _extract_store_details
        # Just return it as is
        return pharmacy_data
    
    async def fetch_pharmacy_details(self, location):
        """
        Get details for a specific pharmacy location
        
        Args:
            location: Dict containing basic pharmacy location info
            
        Returns:
            Complete pharmacy details
        """
        try:
            # Get the store URL from the location data
            store_url = location.get('url', '')
            if not store_url:
                self.logger.error(f"No URL found for Healthy Pharmacy location {location.get('id', '')}")
                return {}
            
            # Make request to the store page
            response = await self.session_manager.get(
                url=store_url,
                headers=self.headers
            )

            print(f"Fetching details for {location.get('name', '')} from {store_url}")  # Debugging line
            
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch Healthy Pharmacy details: HTTP {response.status_code}")
                return {}
            
            # Parse the HTML content
            try:
                # Use BeautifulSoup with 'html.parser'
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extract detailed store information
                store_details = self._extract_store_details(soup, location)
                
                # Debug logging to see what we're getting
                self.logger.info(f"Extracted details for {location.get('name', '')}: {list(store_details.keys())}")
                
                return store_details
            except Exception as e:
                self.logger.error(f"HTML parsing error for Healthy Pharmacy details: {str(e)}")
                self.logger.error(f"Store URL: {store_url}")
                return {}
        except Exception as e:
            self.logger.error(f"Exception when fetching Healthy Pharmacy details: {str(e)}")
            return {}
    
    async def fetch_all_locations_details(self):
        """
        Fetch details for all Healthy Pharmacy locations
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        self.logger.info("Fetching all Healthy Pharmacy locations...")
        
        try:
            # First get all basic location data
            locations = await self.fetch_locations()
            if not locations:
                return []
            
            # Initialize the list for storing complete pharmacy details
            all_details = []
            
            # Fetch details for each location
            for i, location in enumerate(locations):
                try:
                    self.logger.info(f"Fetching details for Healthy Pharmacy location {i+1}/{len(locations)}: {location.get('name', '')}")
                    store_details = await self.fetch_pharmacy_details(location)
                    if store_details:
                        all_details.append(store_details)
                except Exception as e:
                    self.logger.warning(f"Error fetching Healthy Pharmacy location {i}: {str(e)}")
            
            self.logger.info(f"Successfully processed {len(all_details)} Healthy Pharmacy locations")
            return all_details
        except Exception as e:
            self.logger.error(f"Exception when fetching all Healthy Pharmacy locations: {str(e)}")
            return []
    
    def _extract_store_details(self, soup, location):
        """
        Extract all store details from a single pharmacy page
        
        Args:
            soup: BeautifulSoup object of the store page
            location: Basic location information
            
        Returns:
            Dictionary with complete pharmacy details
        """
        try:
            # Extract store information from HTML
            store_id = location.get('id', '')
            store_name = location.get('name', '')
            store_url = location.get('url', '')
            
            # Initialize variables for store details
            address = None
            phone = None
            email = None
            trading_hours = {}
            
            # Find all divs that might contain the "Where to find us" and "Opening Hours" sections
            content_divs = soup.find_all('div', {'class': 'border border-blue-greyscale-200 mb-3 rounded-xl lg:rounded-[18px] p-6'})
            
            for div in content_divs:
                # Look for "Where to find us" section
                where_to_find_header = div.find('div', {'class': 'rich-text_richText__0_Axt mb-2'})
                if where_to_find_header and where_to_find_header.find('h3') and "Where to find us" in where_to_find_header.text:
                    # Found the section with contact information
                    info_div = div.find('div', {'class': 'rich-text_richText__0_Axt text-small'})
                    if info_div:
                        # Get the list items containing address, phone, email
                        list_items = info_div.find('ul').find_all('li') if info_div.find('ul') else []
                        
                        if len(list_items) >= 1:
                            address = list_items[0].text.strip()
                        
                        if len(list_items) >= 2:
                            phone = list_items[1].text.strip()
                        
                        if len(list_items) >= 3:
                            email = list_items[2].text.strip()
                
                # Look for "Opening Hours" section
                hours_header = div.find('div', {'class': 'rich-text_richText__0_Axt mb-2'})
                if hours_header and hours_header.find('h3') and "Opening Hours" in hours_header.text:
                    # Found the section with opening hours
                    hours_div = div.find('div', {'class': 'rich-text_richText__0_Axt text-small'})
                    if hours_div:
                        # Get the list items containing hours for each day
                        hours_items = hours_div.find('ul').find_all('li') if hours_div.find('ul') else []
                        
                        for item in hours_items:
                            hours_text = item.text.strip()
                            # Parse day and hours (format: "Monday: 8am to 6pm")
                            day_hours_match = re.match(r'([^:]+):\s*(.*)', hours_text)
                            if day_hours_match:
                                day = day_hours_match.group(1).strip()
                                hours_value = day_hours_match.group(2).strip()
                                
                                # Handle closed days
                                if hours_value.lower() == 'closed':
                                    trading_hours[day] = {'open': 'Closed', 'close': 'Closed'}
                                else:
                                    # Parse time ranges like "8am to 6pm"
                                    time_parts = hours_value.split(' to ')
                                    if len(time_parts) == 2:
                                        trading_hours[day] = {
                                            'open': time_parts[0].strip(),
                                            'close': time_parts[1].strip()
                                        }
            
            # If we couldn't find the structured content, try a more general approach
            if not address and not phone and not email:
                # Look for specific text patterns within any div
                all_divs = soup.find_all('div', {'class': 'rich-text_richText__0_Axt'})
                for div in all_divs:
                    div_text = div.text
                    
                    # Check for a list that might contain address, phone, email
                    unordered_list = div.find('ul')
                    if unordered_list:
                        list_items = unordered_list.find_all('li')
                        
                        for item in list_items:
                            item_text = item.text.strip()
                            # Look for address pattern (contains state and postcode)
                            if re.search(r'[A-Z]{2,3},?\s+\d{4}', item_text) or re.search(r'\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b', item_text):
                                address = item_text
                            # Look for phone pattern
                            elif re.search(r'\(\d{2}\)\s*\d{4}\s*\d{4}', item_text) or re.search(r'\d{8,10}', item_text):
                                phone = item_text
                            # Look for email pattern
                            elif '@' in item_text and '.' in item_text.split('@')[1]:
                                email = item_text
            
            # Parse address into components
            address_components = self._parse_address(address)
            
            # Try to extract latitude and longitude from Google Maps iframe if present
            latitude = None
            longitude = None
            
            # Look for Google Maps iframe
            maps_iframe = soup.find('iframe', {'src': lambda x: x and 'google.com/maps' in x})
            if maps_iframe:
                src = maps_iframe.get('src', '')
                # Extract coordinates from iframe src
                lat_long_match = re.search(r'!3d(-?\d+\.\d+)!2d(-?\d+\.\d+)', src)
                if lat_long_match:
                    latitude = lat_long_match.group(1)
                    longitude = lat_long_match.group(2)
                else:
                    # Try another pattern commonly found in Google Maps embeds
                    lat_long_match = re.search(r'll=(-?\d+\.\d+),(-?\d+\.\d+)', src)
                    if lat_long_match:
                        latitude = lat_long_match.group(1)
                        longitude = lat_long_match.group(2)
                    else:
                        # One more pattern that might be used
                        lat_long_match = re.search(r'q=(-?\d+\.\d+),(-?\d+\.\d+)', src)
                        if lat_long_match:
                            latitude = lat_long_match.group(1)
                            longitude = lat_long_match.group(2)
            
            # Create the final pharmacy details object
            result = {
                'brand': 'Healthy Pharmacy',
                'name': store_name,
                'store_id': store_id,
                'address': address,
                'street_address': address_components.get('street', ''),
                'suburb': address_components.get('suburb', ''),
                'state': address_components.get('state', ''),
                'postcode': address_components.get('postcode', ''),
                'phone': self._format_phone(phone),
                'email': email,
                'website': store_url,
                'trading_hours': trading_hours,
                'latitude': latitude,
                'longitude': longitude,
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Remove any None values
            return {k: v for k, v in result.items() if v is not None}
        except Exception as e:
            self.logger.error(f"Error extracting store details for {location.get('name', '')}: {str(e)}")
            return {
                'brand': 'Healthy Pharmacy',
                'name': store_name,
                'store_id': store_id,
                'website': store_url,
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
    
    def _parse_address(self, address):
        """
        Parse address string into components
        
        Args:
            address: Full address string
            
        Returns:
            Dictionary with address components
        """
        result = {'street': '', 'suburb': '', 'state': '', 'postcode': ''}
        
        if not address:
            return result
        
        # Normalize address - replace multiple whitespace with single space
        normalized_address = re.sub(r'\s+', ' ', address)
        
        # Australian full state names and their abbreviations
        state_mapping = {
            'NEW SOUTH WALES': 'NSW',
            'VICTORIA': 'VIC',
            'QUEENSLAND': 'QLD',
            'SOUTH AUSTRALIA': 'SA',
            'WESTERN AUSTRALIA': 'WA',
            'TASMANIA': 'TAS',
            'NORTHERN TERRITORY': 'NT',
            'AUSTRALIAN CAPITAL TERRITORY': 'ACT',
            # Keep abbreviations for backward compatibility
            'NSW': 'NSW',
            'VIC': 'VIC',
            'QLD': 'QLD',
            'SA': 'SA',
            'WA': 'WA',
            'TAS': 'TAS',
            'NT': 'NT',
            'ACT': 'ACT'
        }
        
        # Suburb to state mapping for common suburbs that might be in addresses without explicit state
        suburb_to_state = {
            'MONA VALE': 'NSW',
            'SYDNEY': 'NSW',
            'MELBOURNE': 'VIC',
            'BRISBANE': 'QLD',
            'PERTH': 'WA',
            'ADELAIDE': 'SA',
            'HOBART': 'TAS',
            'DARWIN': 'NT',
            'CANBERRA': 'ACT',
            # Add more suburb mappings as needed
            'BONDI': 'NSW',
            'MANLY': 'NSW',
            'CRONULLA': 'NSW',
            'NEWTOWN': 'NSW',
            'PARRAMATTA': 'NSW',
            'CHATSWOOD': 'NSW',
            'RANDWICK': 'NSW',
            'HURSTVILLE': 'NSW',
            'PENRITH': 'NSW',
            'NORTH SYDNEY': 'NSW',
            'SURRY HILLS': 'NSW',
            'CROWS NEST': 'NSW',
            'ST KILDA': 'VIC',
            'CARLTON': 'VIC',
            'FITZROY': 'VIC',
            'FOOTSCRAY': 'VIC',
            'GEELONG': 'VIC',
            'SOUTH YARRA': 'VIC',
            'PRAHRAN': 'VIC',
            'SOUTH BRISBANE': 'QLD',
            'GOLD COAST': 'QLD',
            'FORTITUDE VALLEY': 'QLD',
            'SUNSHINE COAST': 'QLD',
            'CAIRNS': 'QLD',
            'TOWNSVILLE': 'QLD',
            'FREMANTLE': 'WA',
            'SUBIACO': 'WA',
            'JOONDALUP': 'WA',
            'GLENELG': 'SA',
            'NORWOOD': 'SA'
        }
        
        # Pattern to match addresses in format: street, suburb, state, postcode
        # Example: 187 Franklin Street, Adelaide, South Australia, 5000
        pattern = r'(.*?),\s*([^,]+?),\s*([^,]+?),\s*(\d{4})$'
        match = re.search(pattern, normalized_address)
        
        if match:
            street = match.group(1).strip()
            suburb = match.group(2).strip()
            state_name = match.group(3).strip()
            postcode = match.group(4).strip()
            
            # Standardize the state name to abbreviation
            state_upper = state_name.upper()
            state_abbr = state_mapping.get(state_upper, '')
            
            # Check if what we think is a state is actually a suburb with missing state
            if not state_abbr and state_upper in suburb_to_state:
                # This is a suburb, not a state
                suburb = state_name  # The value we thought was a state is actually a suburb
                state_abbr = suburb_to_state[state_upper]  # Set state based on suburb mapping
            
            result = {
                'street': street,
                'suburb': suburb,
                'state': state_abbr,  # Use the abbreviated state code
                'postcode': postcode
            }
        else:
            # Try to handle format like: street, suburb, postcode (missing state)
            # Example: Shop 1, 1785 Pittwater Road, Mona Vale, 2103
            missing_state_pattern = r'(.*?),\s*([^,]+?),\s*(\d{4})$'
            missing_state_match = re.search(missing_state_pattern, normalized_address)
            
            if missing_state_match:
                street = missing_state_match.group(1).strip()
                suburb = missing_state_match.group(2).strip()
                postcode = missing_state_match.group(3).strip()
                
                # Try to infer state from suburb
                suburb_upper = suburb.upper()
                state_abbr = suburb_to_state.get(suburb_upper, '')
                
                # If we don't know this suburb specifically, try to infer from postcode
                if not state_abbr:
                    # Infer state from postcode ranges
                    postcode_num = int(postcode)
                    if 1000 <= postcode_num <= 2999:
                        state_abbr = 'NSW'
                    elif 3000 <= postcode_num <= 3999:
                        state_abbr = 'VIC'
                    elif 4000 <= postcode_num <= 4999:
                        state_abbr = 'QLD'
                    elif 5000 <= postcode_num <= 5999:
                        state_abbr = 'SA'
                    elif 6000 <= postcode_num <= 6999:
                        state_abbr = 'WA'
                    elif 7000 <= postcode_num <= 7999:
                        state_abbr = 'TAS'
                    elif 800 <= postcode_num <= 999:
                        state_abbr = 'NT'
                    elif 2600 <= postcode_num <= 2618 or 2900 <= postcode_num <= 2920:
                        state_abbr = 'ACT'
                
                result = {
                    'street': street,
                    'suburb': suburb,
                    'state': state_abbr,
                    'postcode': postcode
                }
            else:
                # Try to handle format like: 187 Franklin Street, Adelaide SA, 5000
                alt_pattern = r'(.*?),\s*([^,]+?)\s+([A-Za-z]{2,3}),\s*(\d{4})$'
                alt_match = re.search(alt_pattern, normalized_address)
                
                if alt_match:
                    street = alt_match.group(1).strip()
                    suburb = alt_match.group(2).strip()
                    state_abbr = alt_match.group(3).strip().upper()
                    postcode = alt_match.group(4).strip()
                    
                    # Validate state abbreviation
                    if state_abbr in state_mapping.values():
                        result = {
                            'street': street,
                            'suburb': suburb,
                            'state': state_abbr,
                            'postcode': postcode
                        }
                    else:
                        # If not a valid state code, try to find it in the full address
                        for state_name, abbr in state_mapping.items():
                            if state_name in normalized_address.upper():
                                result['state'] = abbr
                                break
                        
                        result['street'] = street
                        result['suburb'] = suburb
                        result['postcode'] = postcode
                else:
                    # Final attempt - look for the state name directly in the address
                    for state_name, abbr in state_mapping.items():
                        if state_name in normalized_address.upper():
                            # Extract the state from the address
                            result['state'] = abbr
                            break
                        # Also try abbreviations
                        elif f" {abbr} " in f" {normalized_address.upper()} ":
                            result['state'] = abbr
                            break
                    
                    # Try to extract postcode (4 digits at the end of the string)
                    postcode_match = re.search(r'(\d{4})$', normalized_address)
                    if postcode_match:
                        result['postcode'] = postcode_match.group(1)
                        
                        # Remove postcode from address for further processing
                        remaining_address = normalized_address[:postcode_match.start()].strip()
                        if remaining_address.endswith(','):
                            remaining_address = remaining_address[:-1].strip()
                        
                        # Try to extract suburb (typically the last part before postcode)
                        if ',' in remaining_address:
                            parts = remaining_address.split(',')
                            result['street'] = ','.join(parts[:-1]).strip()
                            result['suburb'] = parts[-1].strip()
                            
                            # If we found a suburb but no state, try to infer from suburb
                            if result['suburb'] and not result['state']:
                                suburb_upper = result['suburb'].upper()
                                if suburb_upper in suburb_to_state:
                                    result['state'] = suburb_to_state[suburb_upper]
                                else:
                                    # Try to infer from postcode if we have one
                                    if result['postcode']:
                                        postcode_num = int(result['postcode'])
                                        if 1000 <= postcode_num <= 2999:
                                            result['state'] = 'NSW'
                                        elif 3000 <= postcode_num <= 3999:
                                            result['state'] = 'VIC'
                                        elif 4000 <= postcode_num <= 4999:
                                            result['state'] = 'QLD'
                                        elif 5000 <= postcode_num <= 5999:
                                            result['state'] = 'SA'
                                        elif 6000 <= postcode_num <= 6999:
                                            result['state'] = 'WA'
                                        elif 7000 <= postcode_num <= 7999:
                                            result['state'] = 'TAS'
                                        elif 800 <= postcode_num <= 999:
                                            result['state'] = 'NT'
                                        elif 2600 <= postcode_num <= 2618 or 2900 <= postcode_num <= 2920:
                                            result['state'] = 'ACT'
                        else:
                            result['street'] = remaining_address
                    else:
                        # No postcode found - just split the address by commas if possible
                        if ',' in normalized_address:
                            parts = normalized_address.split(',')
                            if len(parts) >= 2:
                                result['street'] = parts[0].strip()
                                result['suburb'] = parts[1].strip()
                                if len(parts) >= 3:
                                    # Try to determine if the 3rd part is state or postcode
                                    if parts[2].strip().isdigit() and len(parts[2].strip()) == 4:
                                        result['postcode'] = parts[2].strip()
                                    else:
                                        for state_name, abbr in state_mapping.items():
                                            if state_name in parts[2].upper():
                                                result['state'] = abbr
                                                break
                                
                                # If we have a suburb but no state, check suburb mapping
                                if result['suburb'] and not result['state']:
                                    suburb_upper = result['suburb'].upper()
                                    if suburb_upper in suburb_to_state:
                                        result['state'] = suburb_to_state[suburb_upper]
                        else:
                            result['street'] = normalized_address
        
        # Final check - if we have a postcode but no state, try to infer state from postcode
        if not result['state'] and result['postcode']:
            try:
                postcode_num = int(result['postcode'])
                if 1000 <= postcode_num <= 2999:
                    result['state'] = 'NSW'
                elif 3000 <= postcode_num <= 3999:
                    result['state'] = 'VIC'
                elif 4000 <= postcode_num <= 4999:
                    result['state'] = 'QLD'
                elif 5000 <= postcode_num <= 5999:
                    result['state'] = 'SA'
                elif 6000 <= postcode_num <= 6999:
                    result['state'] = 'WA'
                elif 7000 <= postcode_num <= 7999:
                    result['state'] = 'TAS'
                elif 800 <= postcode_num <= 999:
                    result['state'] = 'NT'
                elif 2600 <= postcode_num <= 2618 or 2900 <= postcode_num <= 2920:
                    result['state'] = 'ACT'
            except (ValueError, TypeError):
                pass
        
        return result
    
    def _format_phone(self, phone):
        """
        Format phone number consistently
        
        Args:
            phone: Raw phone number string
            
        Returns:
            Formatted phone number
        """
        if not phone:
            return None
            
        # Remove non-numeric characters except for the leading + if present
        if phone.startswith('+'):
            digits_only = '+' + re.sub(r'\D', '', phone[1:])
        else:
            digits_only = re.sub(r'\D', '', phone)
        
        # Handle Australian phone number formats
        if len(digits_only) == 10 and digits_only.startswith('0'):
            # Format as 0X XXXX XXXX
            return f"{digits_only[0:2]} {digits_only[2:6]} {digits_only[6:10]}"
        elif len(digits_only) == 8:
            # Local number, no area code - keep as is
            return phone.strip()
        else:
            # Return original if we can't standardize
            return phone.strip()