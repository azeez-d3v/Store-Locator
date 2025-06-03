from ..base_handler import BasePharmacyHandler
import re
import json
from rich import print

class Pharmacy4LessHandler(BasePharmacyHandler):
    """Handler for Pharmacy 4 Less pharmacies"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "pharmacy4less"
        self.headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0'
        }
        
    async def fetch_locations(self):
        """
        Fetch all Pharmacy 4 Less locations.
        
        Returns:
            List of Pharmacy 4 Less locations
        """
        # Make API call to get location data
        response = await self.session_manager.get(
            url=self.pharmacy_locations.PHARMACY4LESS_URL,
            headers=self.headers
        )
        
        if response.status_code == 200:
            # The API returns JSONP format with callback function
            # We need to extract the JSON from the JSONP response
            response_text = response.text
            
            # Remove the JSONP callback wrapper
            # Pattern: slw({...json...})
            json_match = re.search(r'slw\((.+)\)$', response_text, re.DOTALL)
            if not json_match:
                raise Exception("Failed to parse JSONP response")
            
            json_data = json.loads(json_match.group(1))
            
            # Extract stores from the response
            stores = json_data.get('stores', [])
            print(f"Found {len(stores)} Pharmacy 4 Less locations")
            return stores
        else:
            raise Exception(f"Failed to fetch Pharmacy 4 Less locations: {response.status_code}")
    
    async def fetch_pharmacy_details(self, location_id):
        """
        Fetch details for a specific pharmacy location.
        For Pharmacy 4 Less, all details are included in the main API call.
        
        Args:
            location_id: The ID of the location to get details for
            
        Returns:
            Location details data
        """
        # All details are included in the locations response for Pharmacy 4 Less
        return {"location_details": location_id}
        
    async def fetch_all_locations_details(self):
        """
        Fetch details for all locations and return as a list.
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        print("Fetching all Pharmacy 4 Less locations...")
        locations = await self.fetch_locations()
        if not locations:
            print("No Pharmacy 4 Less locations found.")
            return []
            
        print(f"Found {len(locations)} Pharmacy 4 Less locations. Processing details...")
        all_details = []
        
        for location in locations:
            try:
                extracted_details = self.extract_pharmacy_details(location)
                all_details.append(extracted_details)
            except Exception as e:
                print(f"Error processing Pharmacy 4 Less location {location.get('storeid')}: {e}")
                
        print(f"Completed processing details for {len(all_details)} Pharmacy 4 Less locations.")
        return all_details
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract and standardize pharmacy details from raw API data.
        
        Args:
            pharmacy_data: Raw pharmacy data from the API
            
        Returns:
            Dictionary with standardized pharmacy details
        """
        data = pharmacy_data.get('data', {})
        
        # Extract trading hours from individual day fields
        trading_hours = {}
        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        for day in days:
            hours_key = f'hours_{day}'
            if hours_key in data:
                trading_hours[day] = data[hours_key]
        
        # Convert trading hours to formatted string
        trading_hours_text = self._format_trading_hours(trading_hours)
        
        # Extract phone number - remove spaces and format consistently
        phone = data.get('phone', '').strip()
        if phone:
            # Clean up phone number formatting
            phone = re.sub(r'\s+', ' ', phone)
        
        # Extract address
        address = data.get('address', '').strip()
        
        # Extract coordinates
        latitude = data.get('map_lat')
        longitude = data.get('map_lng')
        
        # Extract website if available
        website = data.get('website', '').strip()
        
        # Extract public holidays info
        public_holidays_info = data.get('1e58a441f345021785c311e6e780c1f2', '')
        if public_holidays_info:
            # Parse the public holidays field (format: "info|||Public Holidays")
            parts = public_holidays_info.split('|||')
            if len(parts) > 0:
                public_holidays_info = parts[0].strip()
        
        return {
            'name': pharmacy_data.get('name', '').strip(),
            'address': address,
            'phone': phone,
            'fax': '',  # Not provided in API
            'email': '',  # Not provided in API
            'website': website,
            'trading_hours': trading_hours_text,
            'latitude': latitude,
            'longitude': longitude,
            'brand': 'Pharmacy 4 Less',
            'store_id': pharmacy_data.get('storeid', ''),
            'state': self._extract_state_from_address(address),
            'public_holidays': public_holidays_info,
            'filters': pharmacy_data.get('filters', [])
        }
    
    def _format_trading_hours(self, hours_dict):
        """
        Format trading hours dictionary into a readable string.
        
        Args:
            hours_dict: Dictionary with day names as keys and hours as values
            
        Returns:
            Formatted trading hours string
        """
        if not hours_dict:
            return ''
        
        formatted_hours = []
        for day, hours in hours_dict.items():
            if hours and hours.strip():
                formatted_hours.append(f"{day}: {hours}")
        
        return ' | '.join(formatted_hours)
    
    def _extract_state_from_address(self, address):
        """
        Extract state from address string.
        
        Args:
            address: Full address string
            
        Returns:
            State abbreviation or empty string
        """
        if not address:
            return ''
        
        # Look for state patterns in address
        state_patterns = {
            'NSW': r'\bNSW\b',
            'VIC': r'\bVIC\b', 
            'QLD': r'\bQLD\b',
            'WA': r'\bWA\b',
            'SA': r'\bSA\b',
            'TAS': r'\bTAS\b',
            'NT': r'\bNT\b',
            'ACT': r'\bACT\b'
        }
        
        for state, pattern in state_patterns.items():
            if re.search(pattern, address, re.IGNORECASE):
                return state
        
        return ''
