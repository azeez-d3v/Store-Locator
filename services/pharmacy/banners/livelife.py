from ..base_handler import BasePharmacyHandler
import re
from rich import print
from bs4 import BeautifulSoup

class LivelifeHandler(BasePharmacyHandler):
    """Handler for Livelife Pharmacies"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "livelife"
        # Define brand-specific headers for API requests
        self.headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://livelifepharmacy.com/stores/',
            'sec-ch-ua': '"Microsoft Edge";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
            'x-requested-with': 'XMLHttpRequest'
        }
        
    async def fetch_locations(self):
        """
        Fetch all Livelife pharmacy locations.
        
        Returns:
            List of Livelife locations
        """
        # Make API call to get location data
        response = await self.session_manager.get(
            url=self.pharmacy_locations.LIVELIFE_URL,
            headers=self.headers
        )
        
        if response.status_code == 200:
            # The API returns a list directly
            locations = response.json()
            print(f"Found {len(locations)} Livelife locations")
            return locations
        else:
            raise Exception(f"Failed to fetch Livelife locations: {response.status_code}")
    
    async def fetch_pharmacy_details(self, location_id):
        """
        Fetch detailed information for a specific Livelife pharmacy.
        Note: Details are typically included in the main location data,
        so this method returns the basic location data.
        
        Args:
            location_id: The ID of the pharmacy location
            
        Returns:
            Dictionary containing pharmacy details
        """
        # For Livelife, the main location data usually contains all necessary details
        # If more detailed information is needed, it would require additional API calls
        return {"message": "Details included in main location data"}
    
    def transform_location_data(self, location):
        """
        Transform Livelife location data to standardized format.
        
        Args:
            location: Raw location data from Livelife API
            
        Returns:
            Dictionary with standardized location data
        """
        try:
            # Extract location details - adjust field names based on actual API response
            name = location.get('store', location.get('name', ''))
            address = location.get('address', '')
            
            # Extract city, state, postcode from address if they're not separate fields
            city = location.get('city', '')
            state = location.get('state', '')
            postcode = location.get('zip', location.get('postcode', ''))
            
            # If city/state/postcode aren't separate, try to extract from address
            if not city and address:
                # Basic address parsing - this may need adjustment based on actual data format
                address_parts = address.split(', ')
                if len(address_parts) >= 2:
                    city = address_parts[-2] if len(address_parts) > 2 else ''
                    # Extract state and postcode from last part
                    last_part = address_parts[-1]
                    parts = last_part.split()
                    if len(parts) >= 2:
                        state = parts[0]
                        postcode = parts[-1]
            
            # Get coordinates
            lat = location.get('lat', location.get('latitude'))
            lng = location.get('lng', location.get('longitude'))
            
            # Get phone number
            phone = location.get('phone', location.get('tel', ''))
            
            # Get store hours
            hours = location.get('hours', location.get('opening_hours', ''))
            
            # Get store URL/website
            url = location.get('url', location.get('website', ''))
            
            return {
                'name': name,
                'address': address,
                'city': city,
                'state': state,
                'postcode': postcode,
                'phone': phone,
                'latitude': lat,
                'longitude': lng,
                'hours': hours,
                'url': url,
                'brand': 'Livelife Pharmacy'
            }
            
        except Exception as e:
            print(f"Error transforming Livelife location data: {e}")
            return None
    
    def format_hours(self, hours_data):
        """
        Format Livelife hours data into a readable string.
        
        Args:
            hours_data: Raw hours data from API
            
        Returns:
            Formatted hours string
        """
        if not hours_data:
            return "Hours not available"
        
        try:
            # If hours_data is already a string, return it
            if isinstance(hours_data, str):
                return hours_data
            
            # If it's a dictionary, format it appropriately
            if isinstance(hours_data, dict):
                formatted_hours = []
                day_order = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
                
                for day in day_order:
                    if day in hours_data:
                        day_hours = hours_data[day]
                        if day_hours and day_hours.lower() != 'closed':
                            formatted_hours.append(f"{day.capitalize()}: {day_hours}")
                        else:
                            formatted_hours.append(f"{day.capitalize()}: Closed")
                
                return "\n".join(formatted_hours)
            
            return str(hours_data)
            
        except Exception as e:
            print(f"Error formatting Livelife hours: {e}")
            return "Hours formatting error"
    
    def clean_phone_number(self, phone):
        """
        Clean and format Livelife phone number.
        
        Args:
            phone: Raw phone number string
            
        Returns:
            Cleaned phone number
        """
        if not phone:
            return ""
        
        # Remove common formatting characters
        cleaned = re.sub(r'[^\d\+\(\)\-\s]', '', str(phone))
        
        # Basic Australian phone number formatting
        if cleaned.startswith('0'):
            # Convert to international format
            cleaned = '+61 ' + cleaned[1:]
        
        return cleaned.strip()
    
    def extract_store_id(self, location):
        """
        Extract store ID from Livelife location data.
        
        Args:
            location: Location data dictionary
            
        Returns:
            Store ID string
        """
        # Try common ID field names
        return str(location.get('id', location.get('store_id', location.get('location_id', ''))))
    
    async def get_all_locations(self):
        """
        Get all Livelife pharmacy locations in standardized format.
        
        Returns:
            List of dictionaries containing standardized location data
        """
        try:
            raw_locations = await self.fetch_locations()
            
            if not raw_locations:
                print("No Livelife locations found")
                return []
            
            standardized_locations = []
            
            for location in raw_locations:
                transformed = self.transform_location_data(location)
                if transformed:
                    standardized_locations.append(transformed)
            
            print(f"Successfully processed {len(standardized_locations)} Livelife locations")
            return standardized_locations
            
        except Exception as e:
            print(f"Error getting Livelife locations: {e}")
            return []
    
    async def fetch_all_locations_details(self):
        """
        Fetch details for all locations and return as a list.
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        print("Fetching all Livelife locations...")
        locations = await self.fetch_locations()
        if not locations:
            print("No Livelife locations found.")
            return []
            
        print(f"Found {len(locations)} Livelife locations. Processing details...")
        all_details = []
        
        for location in locations:
            try:
                extracted_details = self.extract_pharmacy_details(location)
                all_details.append(extracted_details)
            except Exception as e:
                print(f"Error processing Livelife location {location.get('id')}: {e}")
                
        print(f"Completed processing details for {len(all_details)} Livelife locations.")
        return all_details
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract and standardize pharmacy details from raw API data.
        
        Args:
            pharmacy_data: Raw pharmacy data from the API
            
        Returns:
            Dictionary with standardized pharmacy details
        """
        # Parse opening hours from HTML
        trading_hours = self._parse_trading_hours(pharmacy_data.get('hours', ''))
        
        # Construct complete address
        address_parts = []
        if pharmacy_data.get('address'):
            address_parts.append(pharmacy_data.get('address'))
        if pharmacy_data.get('address2'):
            address_parts.append(pharmacy_data.get('address2'))
        if pharmacy_data.get('city'):
            address_parts.append(pharmacy_data.get('city'))
        if pharmacy_data.get('state'):
            address_parts.append(pharmacy_data.get('state'))
        if pharmacy_data.get('zip'):
            address_parts.append(pharmacy_data.get('zip'))
            
        full_address = ', '.join(filter(None, address_parts))
        
        # Format the data according to our standardized structure
        result = {
            'name': pharmacy_data.get('store'),
            'address': full_address,
            'email': pharmacy_data.get('email'),
            'latitude': pharmacy_data.get('lat'),
            'longitude': pharmacy_data.get('lng'),
            'phone': pharmacy_data.get('phone'),
            'postcode': pharmacy_data.get('zip'),
            'state': pharmacy_data.get('state'),
            'street_address': pharmacy_data.get('address'),
            'suburb': pharmacy_data.get('city'),
            'trading_hours': trading_hours,
            'fax': pharmacy_data.get('fax'),
            'website': pharmacy_data.get('url') or pharmacy_data.get('permalink')
        }
        
        # Clean up the result by removing None values
        cleaned_result = {}
        for key, value in result.items():
            if value is not None and value != '':
                cleaned_result[key] = value
                
        return cleaned_result
    
    def _parse_trading_hours(self, hours_html):
        """
        Parse trading hours from HTML table.
        
        Args:
            hours_html: HTML string containing trading hours table
            
        Returns:
            String with formatted trading hours
        """
        if not hours_html:
            return "Hours not available"
        
        try:
            # If it's already a plain string, return it
            if isinstance(hours_html, str) and '<' not in hours_html:
                return hours_html
                
            # Parse HTML using BeautifulSoup
            soup = BeautifulSoup(hours_html, 'html.parser')
            
            # Extract all text and clean it up
            text = soup.get_text(separator=' ', strip=True)
            
            # Basic formatting - replace multiple spaces with single space
            text = re.sub(r'\s+', ' ', text)
            
            return text if text else "Hours not available"
            
        except Exception as e:
            print(f"Error parsing Livelife trading hours: {e}")
            return "Hours not available"
