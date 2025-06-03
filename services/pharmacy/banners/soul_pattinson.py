from ..base_handler import BasePharmacyHandler
import re
import html
from rich import print
from datetime import datetime
import xml.etree.ElementTree as ET

class SoulPattinsonHandler(BasePharmacyHandler):
    """Handler for Soul Pattinson Chemist stores"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "soul_pattinson"
        # Define brand-specific headers for API requests
        self.headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0'
        }
        
    async def fetch_locations(self):
        """
        Fetch all Soul Pattinson Chemist pharmacy locations.
        
        Returns:
            List of Soul Pattinson Chemist locations
        """
        # Make API call to get location data
        response = await self.session_manager.get(
            url=self.pharmacy_locations.SOUL_PATTINSON_URL,
            headers=self.headers
        )
        
        if response.status_code == 200:
            # Parse the XML-like response
            xml_content = response.text
            try:
                # Parse the content as XML
                locations = self._parse_xml_response(xml_content)
                print(f"Found {len(locations)} Soul Pattinson Chemist locations")
                return locations
            except Exception as e:
                print(f"Error parsing Soul Pattinson Chemist XML response: {e}")
                return []
        else:
            raise Exception(f"Failed to fetch Soul Pattinson Chemist locations: {response.status_code}")
    
    def _parse_xml_response(self, xml_content):
        """
        Parse the XML response from Soul Pattinson Chemist API
        
        Args:
            xml_content: The XML string from the API
            
        Returns:
            List of dictionaries containing location data
        """
        locations = []
        
        try:
            # Parse the XML content
            root = ET.fromstring(xml_content)
            
            # Find all item elements within the store element
            store_element = root.find('store')
            if store_element is not None:
                items = store_element.findall('item')
                
                for item in items:
                    location = {}
                    
                    # Extract all child elements as key-value pairs
                    for child in item:
                        location[child.tag] = child.text if child.text else ''
                    
                    locations.append(location)
            
            return locations
            
        except ET.ParseError as e:
            print(f"XML Parse Error: {e}")
            # If XML parsing fails, try to handle malformed XML
            try:
                # Clean up common XML issues
                cleaned_xml = xml_content.replace('&amp;amp;', '&amp;')
                root = ET.fromstring(cleaned_xml)
                
                store_element = root.find('store')
                if store_element is not None:
                    items = store_element.findall('item')
                    
                    for item in items:
                        location = {}
                        
                        for child in item:
                            location[child.tag] = child.text if child.text else ''
                        
                        locations.append(location)
                
                return locations
                
            except Exception as fallback_error:
                print(f"Fallback parsing also failed: {fallback_error}")
                return []
    
    async def fetch_pharmacy_details(self, location_id):
        """
        Fetch details for a specific pharmacy location.
        For Soul Pattinson Chemist, all details are included in the main API call.
        
        Args:
            location_id: The ID of the location to get details for
            
        Returns:
            Location details data
        """
        # All details are included in the locations response for Soul Pattinson Chemist
        return {"location_details": location_id}
        
    async def fetch_all_locations_details(self):
        """
        Fetch details for all locations and return as a list.
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        print("Fetching all Soul Pattinson Chemist locations...")
        locations = await self.fetch_locations()
        if not locations:
            print("No Soul Pattinson Chemist locations found.")
            return []
            
        print(f"Found {len(locations)} Soul Pattinson Chemist locations. Processing details...")
        all_details = []
        
        for location in locations:
            try:
                extracted_details = self.extract_pharmacy_details(location)
                all_details.append(extracted_details)
            except Exception as e:
                print(f"Error processing Soul Pattinson Chemist location {location.get('id')}: {e}")
                
        print(f"Completed processing details for {len(all_details)} Soul Pattinson Chemist locations.")
        return all_details
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract and standardize pharmacy details from raw API data.
        
        Args:
            pharmacy_data: Raw pharmacy data from the API
            
        Returns:
            Dictionary with standardized pharmacy details
        """
        # Parse trading hours from operatingHours field
        trading_hours = self._parse_trading_hours(pharmacy_data)
        
        # Parse address - Soul Pattinson Chemist uses a single address field
        # Format: "85 Welsby Parade  BONGAREE,  QLD 4507"
        full_address = pharmacy_data.get('address', '').strip()
        
        # Try to extract components from the address
        state = ''
        postcode = ''
        suburb = ''
        street_address = ''
        
        if full_address:
            # Try to parse the address format used by Soul Pattinson Chemist
            # Look for state and postcode pattern at the end
            import re
            match = re.search(r'(.+?)\s+([A-Z]{2,3})\s+(\d{4})$', full_address)
            if match:
                address_without_state_postcode = match.group(1).strip()
                state = match.group(2)
                postcode = match.group(3)
                
                # Split the remaining address to get suburb and street
                parts = address_without_state_postcode.split(',')
                if len(parts) >= 2:
                    street_address = parts[0].strip()
                    suburb = parts[-1].strip()
                else:
                    street_address = address_without_state_postcode
        
        # Extract name from location field
        name = pharmacy_data.get('location', '').strip()
        
        # Extract coordinates
        latitude = pharmacy_data.get('latitude', '').strip()
        longitude = pharmacy_data.get('longitude', '').strip()
        
        # Convert coordinates to float if they're valid
        try:
            latitude = float(latitude) if latitude else None
        except (ValueError, TypeError):
            latitude = None
            
        try:
            longitude = float(longitude) if longitude else None
        except (ValueError, TypeError):
            longitude = None
        
        # Extract contact details
        phone = pharmacy_data.get('telephone', '').strip()
        email = pharmacy_data.get('email', '').strip()
        fax = pharmacy_data.get('fax', '').strip()
        website = pharmacy_data.get('website', '').strip()
        
        # Format the data according to our standardized structure
        result = {
            'brand': 'Soul Pattinson Chemist',
            'name': name,
            'address': full_address,
            'email': email if email else None,
            'latitude': latitude,
            'longitude': longitude,
            'phone': phone if phone else None,
            'postcode': postcode if postcode else None,
            'state': state if state else None,
            'street_address': street_address if street_address else None,
            'suburb': suburb if suburb else None,
            'trading_hours': trading_hours,
            'fax': fax if fax else None,
            'store_id': pharmacy_data.get('storeId', '').strip(),
            'website': website if website else "https://soulpattinson.com.au/",
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Clean up the result by removing None values
        cleaned_result = {}
        for key, value in result.items():
            if value is not None and value != '':
                cleaned_result[key] = value
                
        return cleaned_result
    
    def _parse_trading_hours(self, pharmacy_data):
        """
        Parse trading hours from the operatingHours field.
        
        Args:
            pharmacy_data: Raw pharmacy data
            
        Returns:
            Dictionary with days as keys and hours as values
        """
        import html
        
        # Initialize trading hours
        trading_hours = {
            'Monday': {'open': 'Closed', 'closed': 'Closed'},
            'Tuesday': {'open': 'Closed', 'closed': 'Closed'},
            'Wednesday': {'open': 'Closed', 'closed': 'Closed'},
            'Thursday': {'open': 'Closed', 'closed': 'Closed'},
            'Friday': {'open': 'Closed', 'closed': 'Closed'},
            'Saturday': {'open': 'Closed', 'closed': 'Closed'},
            'Sunday': {'open': 'Closed', 'closed': 'Closed'}
        }
        
        # Get the operating hours HTML
        operating_hours_html = pharmacy_data.get('operatingHours', '')
        
        if not operating_hours_html:
            return trading_hours
        
        # Decode HTML entities
        operating_hours_text = html.unescape(operating_hours_html)
        
        # Try to extract hours from HTML table structure or plain text
        day_patterns = {
            'Monday': ['Monday', 'monday', 'Mon'],
            'Tuesday': ['Tuesday', 'tuesday', 'Tue'],
            'Wednesday': ['Wednesday', 'wednesday', 'Wed'],
            'Thursday': ['Thursday', 'thursday', 'Thu'],
            'Friday': ['Friday', 'friday', 'Fri'],
            'Saturday': ['Saturday', 'saturday', 'Sat'],
            'Sunday': ['Sunday', 'sunday', 'Sun']
        }
        
        # First, try to parse HTML table format
        if '<table' in operating_hours_text and '<tr' in operating_hours_text:
            # Extract table rows
            import re
            row_pattern = r'<tr[^>]*>(.*?)</tr>'
            rows = re.findall(row_pattern, operating_hours_text, re.DOTALL | re.IGNORECASE)
            
            for row in rows:
                # Extract cell contents
                cell_pattern = r'<td[^>]*>(.*?)</td>'
                cells = re.findall(cell_pattern, row, re.DOTALL | re.IGNORECASE)
                
                if len(cells) >= 2:
                    day_cell = re.sub(r'<[^>]+>', '', cells[0]).strip()
                    hours_cell = re.sub(r'<[^>]+>', '', cells[1]).strip()
                    
                    # Find matching day
                    for standard_day, day_variations in day_patterns.items():
                        if any(variation.lower() in day_cell.lower() for variation in day_variations):
                            # Parse hours
                            if 'closed' in hours_cell.lower():
                                trading_hours[standard_day] = {'open': 'Closed', 'closed': 'Closed'}
                            else:
                                # Look for time pattern like "8:30 AM - 6:00 PM"
                                time_match = re.search(r'(\d{1,2}:\d{2}\s*(?:AM|PM))\s*[-–]\s*(\d{1,2}:\d{2}\s*(?:AM|PM))', hours_cell, re.IGNORECASE)
                                if time_match:
                                    open_time = time_match.group(1).strip()
                                    close_time = time_match.group(2).strip()
                                    trading_hours[standard_day] = {'open': open_time, 'closed': close_time}
                            break
        
        # If table parsing didn't work, try plain text parsing
        else:
            # Remove HTML tags
            plain_text = re.sub(r'<[^>]+>', ' ', operating_hours_text)
            lines = plain_text.split('\n')
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                # Look for day patterns in each line
                for standard_day, day_variations in day_patterns.items():
                    if any(variation.lower() in line.lower() for variation in day_variations):
                        # Extract hours from this line
                        if 'closed' in line.lower():
                            trading_hours[standard_day] = {'open': 'Closed', 'closed': 'Closed'}
                        else:
                            # Look for time pattern
                            time_match = re.search(r'(\d{1,2}:\d{2}\s*(?:am|pm))\s*[-–]\s*(\d{1,2}:\d{2}\s*(?:am|pm))', line, re.IGNORECASE)
                            if time_match:
                                open_time = time_match.group(1).strip()
                                close_time = time_match.group(2).strip()
                                trading_hours[standard_day] = {'open': open_time, 'closed': close_time}
                        break
        
        return trading_hours
