from ..base_handler import BasePharmacyHandler
import re
from rich import print
from datetime import datetime
import xml.etree.ElementTree as ET
from io import StringIO

class MyChemistHandler(BasePharmacyHandler):
    """Handler for My Chemist stores"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "my_chemist"
        # Define brand-specific headers for API requests
        self.headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0'
        }
        
    async def fetch_locations(self):
        """
        Fetch all My Chemist pharmacy locations.
        
        Returns:
            List of My Chemist locations
        """
        # Make API call to get location data
        response = await self.session_manager.get(
            url=self.pharmacy_locations.MY_CHEMIST_URL,
            headers=self.headers
        )
        
        if response.status_code == 200:
            # Parse the XML-like response
            xml_content = response.text
            try:
                # Parse the content as XML
                locations = self._parse_xml_response(xml_content)
                print(f"Found {len(locations)} My Chemist locations")
                return locations
            except Exception as e:
                print(f"Error parsing My Chemist XML response: {e}")
                return []
        else:
            raise Exception(f"Failed to fetch My Chemist locations: {response.status_code}")
    
    def _parse_xml_response(self, xml_content):
        """
        Parse the XML-like response from My Chemist API
        
        Args:
            xml_content: The XML string from the API
            
        Returns:
            List of dictionaries containing location data
        """
        locations = []
        
        try:
            # Parse the XML content
            # Ensure we have proper XML with opening and closing tags
            if not xml_content.startswith("<markers>"):
                xml_content = f"<markers>{xml_content}</markers>"
            
            root = ET.fromstring(xml_content)
            
            # Extract each marker (store)
            for marker in root.findall('marker'):
                location = {}
                
                # Extract all attributes
                for attr_name, attr_value in marker.attrib.items():
                    location[attr_name] = attr_value
                
                locations.append(location)
                
            return locations
        except ET.ParseError as e:
            print(f"XML Parse Error: {e}")
            
            # Fallback: Try regex-based parsing for malformed XML
            pattern = r'<marker\s+([^>]*)\/>'
            matches = re.findall(pattern, xml_content)
            
            for match in matches:
                location = {}
                # Extract attributes
                attr_pattern = r'(\w+)="([^"]*)"'
                attrs = re.findall(attr_pattern, match)
                
                for attr_name, attr_value in attrs:
                    location[attr_name] = attr_value
                
                locations.append(location)
            
            return locations
    
    async def fetch_pharmacy_details(self, location_id):
        """
        Fetch details for a specific pharmacy location.
        For My Chemist, all details are included in the main API call.
        
        Args:
            location_id: The ID of the location to get details for
            
        Returns:
            Location details data
        """
        # All details are included in the locations response for My Chemist
        return {"location_details": location_id}
        
    async def fetch_all_locations_details(self):
        """
        Fetch details for all locations and return as a list.
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        print(f"Fetching all My Chemist locations...")
        locations = await self.fetch_locations()
        if not locations:
            print(f"No My Chemist locations found.")
            return []
            
        print(f"Found {len(locations)} My Chemist locations. Processing details...")
        all_details = []
        
        for location in locations:
            try:
                extracted_details = self.extract_pharmacy_details(location)
                all_details.append(extracted_details)
            except Exception as e:
                print(f"Error processing My Chemist location {location.get('id')}: {e}")
                
        print(f"Completed processing details for {len(all_details)} My Chemist locations.")
        return all_details
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract and standardize pharmacy details from raw API data.
        
        Args:
            pharmacy_data: Raw pharmacy data from the API
            
        Returns:
            Dictionary with standardized pharmacy details
        """
        # Parse trading hours
        trading_hours = self._parse_trading_hours(pharmacy_data)
        
        # Construct address
        full_address = f"{pharmacy_data.get('storeaddress', '')}, {pharmacy_data.get('storesuburb', '')}, {pharmacy_data.get('storestate', '')} {pharmacy_data.get('storepostcode', '')}"
        full_address = re.sub(r'\s+', ' ', full_address).strip().strip(',')
        
        # Format the data according to our standardized structure
        result = {
            'name': pharmacy_data.get('storename'),
            'address': full_address,
            'email': pharmacy_data.get('storeemail'),
            'latitude': pharmacy_data.get('lat'),
            'longitude': pharmacy_data.get('lng'),
            'phone': pharmacy_data.get('storephone'),
            'postcode': pharmacy_data.get('storepostcode'),
            'state': pharmacy_data.get('storestate'),
            'street_address': pharmacy_data.get('storeaddress'),
            'suburb': pharmacy_data.get('storesuburb'),
            'trading_hours': trading_hours,
            'fax': pharmacy_data.get('storefax'),
            'store_id': pharmacy_data.get('id'),
            'abn': pharmacy_data.get('abn'),
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
        Parse trading hours from the store data.
        
        Args:
            pharmacy_data: Raw pharmacy data
            
        Returns:
            Dictionary with days as keys and hours as values
        """
        # Initialize trading hours
        trading_hours = {
            'Monday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Tuesday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Wednesday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Thursday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Friday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Saturday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Sunday': {'open': '12:00 AM', 'closed': '12:00 AM'}
        }
        
        # Day mapping
        day_fields = {
            'storemon': 'Monday',
            'storetue': 'Tuesday',
            'storewed': 'Wednesday', 
            'storethu': 'Thursday',
            'storefri': 'Friday',
            'storesat': 'Saturday',
            'storesun': 'Sunday'
        }
        
        # Extract hours for each day
        for field, day in day_fields.items():
            hours_str = pharmacy_data.get(field, '')
            
            if hours_str and '-' in hours_str:
                # Split into open and close times
                parts = hours_str.split('-')
                if len(parts) == 2:
                    open_time, close_time = parts[0].strip(), parts[1].strip()
                    
                    # Check if store closed that day (typically empty)
                    if open_time and close_time and open_time != ' ' and close_time != ' ':
                        trading_hours[day] = {
                            'open': open_time,
                            'closed': close_time
                        }
        
        return trading_hours