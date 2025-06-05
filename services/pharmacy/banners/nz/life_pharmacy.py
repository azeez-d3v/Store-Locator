import logging
from ...base_handler import BasePharmacyHandler

class LifePharmacyNZHandler(BasePharmacyHandler):
    """Handler for Life Pharmacy NZ stores using REST API"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "life_pharmacy_nz"
        self.api_url = self.pharmacy_locations.LIFE_PHARMACY_NZ_URL
        
        self.headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'origin': 'https://www.lifepharmacy.co.nz',
            'priority': 'u=1, i',
            'referer': 'https://www.lifepharmacy.co.nz/',
            'sec-ch-ua': '"Chromium";v="136", "Microsoft Edge";v="136", "Not.A/Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'cross-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0'
        }
        self.logger = logging.getLogger(__name__)
        
    async def fetch_locations(self):
        """
        Fetch all Life Pharmacy NZ locations from the API.
        
        Returns:
            List of Life Pharmacy NZ locations
        """
        try:
            response = await self.session_manager.get(
                url=self.api_url,
                headers=self.headers
            )
            
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch Life Pharmacy NZ locations: HTTP {response.status_code}")
                return []
                
            locations_data = response.json()
            
            if not isinstance(locations_data, list):
                self.logger.error("API response is not a list")
                return []
            
            locations = []
            for i, location_data in enumerate(locations_data):
                try:
                    processed_location = self._process_location_data(location_data, i)
                    if processed_location:
                        locations.append(processed_location)
                except Exception as e:
                    self.logger.warning(f"Error processing location {i}: {str(e)}")
                    continue
            
            self.logger.info(f"Found {len(locations)} Life Pharmacy NZ locations")
            return locations
            
        except Exception as e:
            self.logger.error(f"Exception when fetching Life Pharmacy NZ locations: {str(e)}")
            return []
    
    def _process_location_data(self, location_data, index):
        """
        Process a single location from the API response.
        
        Args:
            location_data: Dictionary containing location data from API
            index: Index of the location for fallback ID generation
            
        Returns:
            Dictionary containing processed location information
        """
        try:
            # Use API ID if available, otherwise fallback to index-based ID
            location_id = location_data.get('id', f"life_pharmacy_nz_{index}")
            
            store_data = {
                'id': f"life_pharmacy_nz_{location_id}",
                'brand': 'Life Pharmacy NZ',
                'name': location_data.get('name', ''),
                'latitude': self._safe_float(location_data.get('latitude')),
                'longitude': self._safe_float(location_data.get('longitude')),
                'phone': location_data.get('phone', ''),
                'email': location_data.get('email', ''),
                'city': location_data.get('city', ''),
                'state': location_data.get('state', ''),
                'postcode': location_data.get('postal_code', ''),
                'country': location_data.get('country', 'New Zealand')
            }
            
            # Build full address from address components
            address_parts = []
            
            address_line_1 = location_data.get('address_line_1', '')
            if address_line_1:
                address_parts.append(address_line_1)
            
            address_line_2 = location_data.get('address_line_2', '')
            if address_line_2:
                address_parts.append(address_line_2)
            
            city = location_data.get('city', '')
            if city:
                address_parts.append(city)
            
            # Add state and postal code
            state = location_data.get('state', '')
            postal_code = location_data.get('postal_code', '')
            
            if state and postal_code:
                address_parts.append(f"{state} {postal_code}")
            elif state:
                address_parts.append(state)
            elif postal_code:
                address_parts.append(postal_code)
            
            if address_parts:
                store_data['address'] = ', '.join(address_parts)
            
            # Extract website from custom fields
            website = self._extract_website_from_custom_fields(location_data.get('custom_fields', []))
            if website:
                store_data['website'] = website
            
            # Extract services/filters
            filters = location_data.get('filters', [])
            if filters:
                services = [f['name'] for f in filters if f.get('name')]
                if services:
                    store_data['services'] = ', '.join(services)
            
            # Clean up empty values
            store_data = {k: v for k, v in store_data.items() if v and v != ''}
            
            return store_data
            
        except Exception as e:
            self.logger.error(f"Error processing location data: {str(e)}")
            return None
    
    def _safe_float(self, value):
        """
        Safely convert a value to float, returning None if conversion fails.
        
        Args:
            value: Value to convert to float
            
        Returns:
            Float value or None if conversion fails
        """
        try:
            if value is None or value == '':
                return None
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _extract_website_from_custom_fields(self, custom_fields):
        """
        Extract website URL from custom fields.
        
        Args:
            custom_fields: List of custom field dictionaries
            
        Returns:
            Website URL string or None
        """
        try:
            for field in custom_fields:
                if field.get('name') == 'View Store':
                    website_url = field.get('value', '')
                    if website_url and website_url.startswith('http'):
                        return website_url
            return None
        except Exception as e:
            self.logger.error(f"Error extracting website from custom fields: {e}")
            return None
    
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
        
        # For Life Pharmacy NZ, the data is already in the correct format
        return pharmacy_data
    
    async def fetch_pharmacy_details(self, location_id):
        """
        Get details for a specific pharmacy location
        
        Args:
            location_id: The ID of the location to get details for
            
        Returns:
            Complete pharmacy details
        """
        # Since we extract all details during the initial fetch,
        # we need to implement this to comply with the interface
        # but it's not used in our current implementation
        self.logger.warning("fetch_pharmacy_details called but all details are fetched in fetch_locations")
        return {}
    
    async def fetch_all_locations_details(self):
        """
        Fetch details for all Life Pharmacy NZ locations
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        self.logger.info("Fetching all Life Pharmacy NZ locations...")
        
        try:
            # Get all locations with details
            locations = await self.fetch_locations()
            if not locations:
                return []
            
            # Process each location to ensure proper formatting
            all_details = []
            for location in locations:
                try:
                    # Extract standardized details
                    details = self.extract_pharmacy_details(location)
                    if details:
                        all_details.append(details)
                except Exception as e:
                    self.logger.warning(f"Error processing location details: {str(e)}")
                    continue
            
            self.logger.info(f"Successfully processed {len(all_details)} Life Pharmacy NZ locations")
            return all_details
            
        except Exception as e:
            self.logger.error(f"Exception when fetching all Life Pharmacy NZ location details: {str(e)}")
            return []
