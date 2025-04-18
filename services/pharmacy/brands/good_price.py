from ..base_handler import BasePharmacyHandler
import logging
import re
import json
from datetime import datetime
from bs4 import BeautifulSoup

class GoodPriceHandler(BasePharmacyHandler):
    """Handler for Good Price Pharmacy stores"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "good_price"
        self.main_url = "https://www.goodpricepharmacy.com.au/amlocator/index/ajax/"
        self.headers = {
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest',
            'referer': 'https://www.goodpricepharmacy.com.au/find-a-store'
        }
        self.logger = logging.getLogger(__name__)
        
    async def fetch_locations(self):
        """
        Fetch all Good Price Pharmacy store locations from the main API endpoint
        
        Returns:
            List of Good Price Pharmacy locations with basic details
        """
        try:
            # Make request to the locations endpoint
            # The API seems to return all locations at once in the 'block' key of the response
            response = await self.session_manager.post(
                url=self.main_url,
                headers=self.headers,
                data={
                    'filter': '',
                    'p': '1',  # Page number
                    'limit': '100'  # Fetch more results to ensure we get all stores
                }
            )
            
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch Good Price locations: HTTP {response.status_code}")
                return []
            
            # Parse the JSON response
            try:
                json_data = response.json()
                if 'block' not in json_data:
                    self.logger.error("No 'block' key in Good Price Pharmacy API response")
                    return []
                
                # Parse the HTML content in the 'block' key
                html_content = json_data['block']
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Find all pharmacy store descriptions
                pharmacy_items = soup.find_all('div', {'class': 'amlocator-store-desc'})
                
                # Initialize the list for storing basic pharmacy information
                all_locations = []
                
                for i, item in enumerate(pharmacy_items):
                    try:
                        # Extract store ID from the 'id' attribute of the div
                        store_id = item.get('id', '').replace('am-loc-', '')
                        
                        # Extract store name
                        link_elem = item.find('a', {'class': 'amlocator-link'})
                        store_name = link_elem.get_text().strip() if link_elem else f"Good Price Pharmacy {i+1}"
                        
                        # Create basic location info
                        location = {
                            'id': store_id,
                            'name': store_name,
                            'brand': 'Good Price Pharmacy'
                        }
                        
                        all_locations.append(location)
                    except Exception as e:
                        self.logger.warning(f"Error extracting Good Price location item {i}: {str(e)}")
                
                self.logger.info(f"Found {len(all_locations)} Good Price Pharmacy locations")
                return all_locations
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON decode error for Good Price locations: {str(e)}")
                return []
        except Exception as e:
            self.logger.error(f"Exception when fetching Good Price locations: {str(e)}")
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
            
        # For Good Price Pharmacy, data is already in the right format from _extract_store_details
        # Just return it as is, or perform any additional standardization if needed
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
            # Make request to the locations endpoint to get all data
            response = await self.session_manager.post(
                url=self.main_url,
                headers=self.headers,
                data={
                    'filter': '',
                    'p': '1',
                    'limit': '100'
                }
            )
            
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch Good Price pharmacy details: HTTP {response.status_code}")
                return {}
            
            # Parse the JSON response
            try:
                json_data = response.json()
                if 'block' not in json_data:
                    self.logger.error("No 'block' key in Good Price Pharmacy API response")
                    return {}
                
                # Parse the HTML content in the 'block' key
                html_content = json_data['block']
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Find the specific pharmacy by ID
                store_id = location.get('id', '')
                pharmacy_item = soup.find('div', {'id': f'am-loc-{store_id}'})
                
                if not pharmacy_item:
                    self.logger.error(f"Store with ID {store_id} not found in Good Price data")
                    return {}
                
                # Extract detailed store information
                return self._extract_store_details(pharmacy_item)
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON decode error for Good Price details: {str(e)}")
                return {}
        except Exception as e:
            self.logger.error(f"Exception when fetching Good Price pharmacy details: {str(e)}")
            return {}
    
    async def fetch_all_locations_details(self):
        """
        Fetch details for all Good Price Pharmacy locations
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        self.logger.info("Fetching all Good Price Pharmacy locations...")
        
        try:
            # Make request to the locations endpoint
            response = await self.session_manager.post(
                url=self.main_url,
                headers=self.headers,
                data={
                    'filter': '',
                    'p': '1',
                    'limit': '100'
                }
            )
            
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch Good Price locations: HTTP {response.status_code}")
                return []
            
            # Parse the JSON response
            try:
                json_data = response.json()
                if 'block' not in json_data:
                    self.logger.error("No 'block' key in Good Price Pharmacy API response")
                    return []
                
                # Parse the HTML content in the 'block' key
                html_content = json_data['block']
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Find all pharmacy store descriptions
                pharmacy_items = soup.find_all('div', {'class': 'amlocator-store-desc'})
                
                # Initialize the list for storing complete pharmacy details
                all_details = []
                
                for i, item in enumerate(pharmacy_items):
                    try:
                        # Extract detailed store information
                        store_details = self._extract_store_details(item)
                        if store_details:
                            all_details.append(store_details)
                    except Exception as e:
                        self.logger.warning(f"Error extracting Good Price location item {i}: {str(e)}")
                
                self.logger.info(f"Successfully processed {len(all_details)} Good Price Pharmacy locations")
                return all_details
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON decode error for Good Price locations: {str(e)}")
                return []
        except Exception as e:
            self.logger.error(f"Exception when fetching all Good Price locations: {str(e)}")
            return []
    
    def _extract_store_details(self, item):
        """
        Extract all store details from a single pharmacy item on the page
        
        Args:
            item: BeautifulSoup element representing a pharmacy location
            
        Returns:
            Dictionary with complete pharmacy details
        """
        try:
            # Extract store ID from the 'id' attribute of the div
            store_id = item.get('id', '').replace('am-loc-', '')
            
            # Extract store name from title
            link_elem = item.find('a', {'class': 'amlocator-link'})
            store_name = link_elem.get_text().strip() if link_elem else f"Good Price Pharmacy {store_id}"
            
            # Extract store URL (detailed page link)
            store_url = link_elem.get('href', '') if link_elem else ''
            
            # Extract address from the text following the title
            store_info_div = item.find('div', {'class': 'amlocator-store-information'})
            address = ""
            email = None
            phone = None
            fax = None
            
            if store_info_div:
                # Get the raw address from text after title
                address_text = None
                title_div = store_info_div.find('div', {'class': 'amlocator-title'})
                if title_div and title_div.next_sibling:
                    address_text = str(title_div.next_sibling).strip()
                
                # Extract address - it's the text immediately following the title div
                if address_text:
                    address = address_text
                
                # Find phone number
                phone_elem = store_info_div.find('a', {'class': 'phone'})
                if phone_elem:
                    phone_content = phone_elem.find('span', {'class': 'phone-content'})
                    if phone_content:
                        phone = phone_content.get_text().strip()
                
                # Find fax number
                fax_elems = store_info_div.find_all('a', {'class': 'fax'})
                for fax_elem in fax_elems:
                    fax_label = fax_elem.find('span', {'class': 'phone-label'})
                    if fax_label and 'Fax:' in fax_label.get_text():
                        fax_content = fax_elem.find('span', {'class': 'phone-content'})
                        if fax_content:
                            fax = fax_content.get_text().strip()
                    elif fax_label and 'Email:' in fax_label.get_text():
                        # Extract email from mailto link
                        href = fax_elem.get('href', '')
                        if href.startswith('mailto:'):
                            email = href[7:]  # Remove 'mailto:' prefix
            
            # Extract trading hours
            hours_container = item.find('div', {'class': 'amlocator-schedule-container'})
            trading_hours = {}
            if hours_container:
                # Extract regular hours
                hour_rows = hours_container.find_all('div', {'class': 'amlocator-row'})
                for row in hour_rows:
                    day_elem = row.find('span', {'class': '-day'})
                    time_elem = row.find('span', {'class': '-time'})
                    
                    if day_elem and time_elem:
                        day = day_elem.get_text().strip()
                        time_str = time_elem.get_text().strip()
                        
                        # Check if the store is closed that day
                        if time_str == '-':
                            trading_hours[day] = {'open': 'Closed', 'close': 'Closed'}
                        else:
                            # Parse opening and closing hours
                            time_parts = time_str.split('-')
                            if len(time_parts) == 2:
                                opening = time_parts[0].strip()
                                closing = time_parts[1].strip()
                                trading_hours[day] = {'open': opening, 'close': closing}
                
                # Extract holiday hours
                extra_schedule = hours_container.find('div', {'class': 'extra_schedule'})
                if extra_schedule:
                    holiday_rows = extra_schedule.find_all('div', {'class': 'amlocator-row'})
                    for row in holiday_rows:
                        day_elem = row.find('span', {'class': '-day'})
                        time_elem = row.find('span', {'class': '-time'})
                        
                        if day_elem and time_elem:
                            day = day_elem.get_text().strip()
                            time_str = time_elem.get_text().strip()
                            
                            # Check if the store is closed that day
                            if time_str == 'Closed':
                                trading_hours[day] = {'open': 'Closed', 'close': 'Closed'}
                            else:
                                # Parse opening and closing hours
                                time_parts = time_str.split('-')
                                if len(time_parts) == 2:
                                    opening = time_parts[0].strip()
                                    closing = time_parts[1].strip()
                                    trading_hours[day] = {'open': opening, 'close': closing}
            
            # Parse address into components
            address_components = self._parse_address(address)
            
            # Create the final pharmacy details object
            result = {
                'brand': 'Good Price Pharmacy',
                'name': store_name,
                'store_id': store_id,
                'address': address,
                'street_address': address_components.get('street', ''),
                'suburb': address_components.get('suburb', ''),
                'state': address_components.get('state', ''),
                'postcode': address_components.get('postcode', ''),
                'phone': self._format_phone(phone),
                'fax': self._format_phone(fax),
                'email': email,
                'website': store_url,
                'trading_hours': trading_hours,
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Remove any None values
            return {k: v for k, v in result.items() if v}
        except Exception as e:
            self.logger.error(f"Error extracting store details: {str(e)}")
            return {}
    
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
        
        # Pattern to match addresses in format: address, suburb, postcode, state
        # This is the exact format used in the Good Price Pharmacy website
        pattern = r'(.*?),\s*([^,]+?),\s*(\d{4}),\s*([^,]+)$'
        match = re.search(pattern, normalized_address)
        
        if match:
            street_and_suburb = match.group(1).strip()
            postcode = match.group(3).strip()
            state_name = match.group(4).strip()
            
            # Extract suburb from the last part of street_and_suburb if there's a comma
            # Otherwise use what was captured in the second group
            if ',' in street_and_suburb:
                parts = street_and_suburb.split(',')
                street = ','.join(parts[:-1]).strip() 
                suburb = parts[-1].strip()
            else:
                # If no comma in street_and_suburb, use what was captured in group 2
                street = street_and_suburb
                suburb = match.group(2).strip()
            
            # Standardize the state name to abbreviation
            state_upper = state_name.upper()
            state_abbr = state_mapping.get(state_upper, state_name)
            
            result = {
                'street': street,
                'suburb': suburb,
                'state': state_abbr,  # Use the abbreviated state code
                'postcode': postcode
            }
        else:
            # This pattern looks for postcode followed by state at the end
            alt_pattern = r'(.*?),\s*([^,]+?),\s*(\d{4})[,\s]+([^,]+)$'
            alt_match = re.search(alt_pattern, normalized_address)
            
            if alt_match:
                address_part = alt_match.group(1).strip()
                suburb = alt_match.group(2).strip()
                postcode = alt_match.group(3).strip()
                state_name = alt_match.group(4).strip()
                
                # Standardize the state name to abbreviation
                state_upper = state_name.upper()
                state_abbr = state_mapping.get(state_upper, state_name)
                
                result = {
                    'street': address_part,
                    'suburb': suburb,
                    'state': state_abbr,
                    'postcode': postcode
                }
            else:
                # Final attempt - look for the state name directly in the address
                for state_name, abbr in state_mapping.items():
                    if state_name in normalized_address.upper():
                        # Extract the state from the end of the address
                        parts = normalized_address.upper().split(state_name)
                        if len(parts) > 1:
                            result['state'] = abbr
                            
                            # Try to extract postcode that typically comes before state
                            postcode_match = re.search(r'(\d{4})[,\s]+' + re.escape(state_name), 
                                                      normalized_address.upper())
                            if postcode_match:
                                result['postcode'] = postcode_match.group(1)
                                
                                # Remove postcode and state from address to process the rest
                                remaining_address = normalized_address[:postcode_match.start()].strip()
                                # Split remaining address for street and suburb
                                if ',' in remaining_address:
                                    parts = remaining_address.split(',')
                                    result['street'] = ','.join(parts[:-1]).strip()
                                    result['suburb'] = parts[-1].strip()
                                else:
                                    result['street'] = remaining_address
                            break
                
                # If we still don't have results, use the simplest pattern to at least get something
                if not result['state'] or not result['postcode']:
                    # Extract the last 4-digit number as postcode
                    postcode_match = re.search(r'(\d{4})', normalized_address)
                    if postcode_match:
                        result['postcode'] = postcode_match.group(1)
                        
                        # Assume everything before postcode is the address
                        address_before_postcode = normalized_address[:postcode_match.start()].strip()
                        if ',' in address_before_postcode:
                            parts = address_before_postcode.split(',')
                            result['street'] = ','.join(parts[:-1]).strip()
                            result['suburb'] = parts[-1].strip()
                        else:
                            result['street'] = address_before_postcode
                    else:
                        # No postcode found - just use the whole string as street address
                        result['street'] = normalized_address
        
        # Ensure we have values for all fields
        for key in result:
            if not result[key]:
                if key == 'suburb' and result['street']:
                    # If no suburb was found but we have a street, try to extract it from there
                    if ',' in result['street']:
                        parts = result['street'].split(',')
                        result['street'] = ','.join(parts[:-1]).strip()
                        result['suburb'] = parts[-1].strip()
        
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
        
        # If it already says "Click here", we got the wrong field
        if phone == "Click here":
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