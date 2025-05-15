from ..base_handler import BasePharmacyHandler
import re
from rich import print
from datetime import datetime

class TerryWhiteHandler(BasePharmacyHandler):
    """Handler for TerryWhite Pharmacy stores"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "terry_white"
        # Define brand-specific headers for API requests
        self.headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0'
        }
        
    async def fetch_locations(self):
        """
        Fetch all TerryWhite pharmacy locations.
        
        Returns:
            List of TerryWhite locations
        """
        # Make API call to get location data
        response = await self.session_manager.post(
            url=self.pharmacy_locations.TERRY_WHITE_URL,
            headers=self.headers,
            json={}  # Empty payload as per API requirements
        )
        
        if response.status_code == 200:
            data = response.json()
            if "data" in data and isinstance(data["data"], list):
                locations = data["data"]
                print(f"Found {len(locations)} TerryWhite Pharmacy locations")
                return locations
            else:
                print("Unexpected response format from TerryWhite API")
                return []
        else:
            raise Exception(f"Failed to fetch TerryWhite Pharmacy locations: {response.status_code}")
    
    async def fetch_pharmacy_details(self, location_id):
        """
        Fetch details for a specific pharmacy location.
        For TerryWhite Pharmacy, all details are included in the main API call.
        
        Args:
            location_id: The location object from fetch_locations
            
        Returns:
            Location details data
        """
        # All details are included in the locations response for TerryWhite
        return {"location_details": location_id}
        
    async def fetch_all_locations_details(self):
        """
        Fetch details for all locations and return as a list.
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        print("Fetching all TerryWhite Pharmacy locations...")
        locations = await self.fetch_locations()
        if not locations:
            print("No TerryWhite Pharmacy locations found.")
            return []
            
        print(f"Found {len(locations)} TerryWhite Pharmacy locations. Processing details...")
        all_details = []
        
        for location in locations:
            try:
                extracted_details = self.extract_pharmacy_details(location)
                all_details.append(extracted_details)
            except Exception as e:
                print(f"Error processing TerryWhite Pharmacy location {location.get('storeId')}: {e}")
                
        print(f"Completed processing details for {len(all_details)} TerryWhite Pharmacy locations.")
        return all_details
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract and standardize pharmacy details from raw API data.
        
        Args:
            pharmacy_data: Raw pharmacy data from the API
            
        Returns:
            Dictionary with standardized pharmacy details
        """
        # Parse the availability string to extract trading hours
        trading_hours = self._parse_availability(pharmacy_data.get('availability', ''))
        
        # Construct complete address
        address_parts = []
        if pharmacy_data.get('addressLine1'):
            address_parts.append(pharmacy_data.get('addressLine1'))
        if pharmacy_data.get('addressLine2'):
            address_parts.append(pharmacy_data.get('addressLine2'))
        if pharmacy_data.get('suburb'):
            address_parts.append(pharmacy_data.get('suburb'))
        if pharmacy_data.get('state'):
            address_parts.append(pharmacy_data.get('state'))
        if pharmacy_data.get('postcode'):
            address_parts.append(pharmacy_data.get('postcode'))
            
        full_address = ', '.join(filter(None, address_parts))
        
        # Format the data according to our standardized structure
        result = {
            'name': pharmacy_data.get('storeName'),
            'address': full_address,
            'email': pharmacy_data.get('email'),
            'latitude': pharmacy_data.get('lat'),
            'longitude': pharmacy_data.get('lng'),
            'phone': pharmacy_data.get('phone'),
            'postcode': pharmacy_data.get('postcode'),
            'state': pharmacy_data.get('state'),
            'street_address': pharmacy_data.get('addressLine1') + (', ' + pharmacy_data.get('addressLine2') if pharmacy_data.get('addressLine2') else ''),
            'suburb': pharmacy_data.get('suburb'),
            'trading_hours': trading_hours,
            'fax': pharmacy_data.get('fax'),
            'store_id': pharmacy_data.get('storeId'),
            'abn': pharmacy_data.get('abn'),
            'status': pharmacy_data.get('status'),
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Clean up the result by removing None values
        cleaned_result = {}
        for key, value in result.items():
            if value is not None and value != '':
                cleaned_result[key] = value
                
        return cleaned_result
    
    def _parse_availability(self, availability_str):
        """
        Parse availability string in format "MO09002100TU09002100WE09002100..."
        
        Args:
            availability_str: String containing hours data
            
        Returns:
            Dictionary with days as keys and hours as values
        """
        # Initialize all days with closed hours
        trading_hours = {
            'Monday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Tuesday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Wednesday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Thursday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Friday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Saturday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Sunday': {'open': '12:00 AM', 'closed': '12:00 AM'}
        }
        
        if not availability_str:
            return trading_hours
        
        # Day code mapping
        day_map = {
            'MO': 'Monday',
            'TU': 'Tuesday',
            'WE': 'Wednesday',
            'TH': 'Thursday',
            'FR': 'Friday',
            'SA': 'Saturday',
            'SU': 'Sunday'
        }
        
        # Regular expression to parse each day's hours
        # Each entry is 2 char day code + 4 digit opening time + 4 digit closing time
        pattern = r'([A-Z]{2})(\d{4})(\d{4})'
        
        matches = re.findall(pattern, availability_str)
        for day_code, open_time, close_time in matches:
            if day_code in day_map:
                day_name = day_map[day_code]
                
                # Convert military time to 12-hour format
                open_hour = int(open_time[:2])
                open_minute = int(open_time[2:])
                close_hour = int(close_time[:2])
                close_minute = int(close_time[2:])
                
                # Format opening time
                open_period = "AM" if open_hour < 12 else "PM"
                display_open_hour = open_hour if open_hour <= 12 else open_hour - 12
                display_open_hour = 12 if display_open_hour == 0 else display_open_hour
                
                # Format closing time
                close_period = "AM" if close_hour < 12 else "PM"
                display_close_hour = close_hour if close_hour <= 12 else close_hour - 12
                display_close_hour = 12 if display_close_hour == 0 else display_close_hour
                
                # Set the hours in the result dictionary
                trading_hours[day_name] = {
                    'open': f"{display_open_hour:02d}:{open_minute:02d} {open_period}",
                    'closed': f"{display_close_hour:02d}:{close_minute:02d} {close_period}"
                }
                
        return trading_hours