from ..base_handler import BasePharmacyHandler
from rich import print

class ChemistWarehouseHandler(BasePharmacyHandler):
    """Handler for Chemist Warehouse Pharmacies"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "chemist_warehouse"
        # Define Australian cities coordinates
        self.australian_cities = [
            {"name": "Sydney", "lat": -33.8688, "lng": 151.2093},
            {"name": "Melbourne", "lat": -37.8136, "lng": 144.9631},
            {"name": "Brisbane", "lat": -27.4698, "lng": 153.0251},
            {"name": "Perth", "lat": -31.9505, "lng": 115.8605},
            {"name": "Adelaide", "lat": -34.9285, "lng": 138.6007},
            {"name": "Darwin", "lat": -12.4634, "lng": 130.8456},
            {"name": "Hobart", "lat": -42.8821, "lng": 147.3272},
            {"name": "Canberra", "lat": -35.2809, "lng": 149.1300}
        ]
        # Define brand-specific headers for API requests
        self.headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Pragma': 'no-cache',
            'Referer': 'https://www.chemistwarehouse.com.au/aboutus/store-locator',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
            'X-Requested-With': 'XMLHttpRequest',
        }

    async def fetch_locations(self):
        """
        Fetch all Chemist Warehouse locations using the new API with pagination.
        Makes requests to each major Australian city with pagination to get comprehensive coverage.
        
        Returns:
            List of Chemist Warehouse locations
        """
        all_locations = []
        unique_locations = set()  # Track unique stores by key to avoid duplicates
        
        for city in self.australian_cities:
            print(f"Fetching Chemist Warehouse locations near {city['name']}...")
            
            # Fetch all pages for this city with pagination
            city_locations = await self._fetch_city_locations_with_pagination(city, unique_locations)
            all_locations.extend(city_locations)
            
            print(f"Found {len(city_locations)} unique locations near {city['name']} ({len(all_locations)} total unique)")
        
        print(f"Found {len(all_locations)} total unique Chemist Warehouse locations")
        return all_locations
    
    async def _fetch_city_locations_with_pagination(self, city, unique_locations):
        """
        Fetch all locations for a city using pagination.
        
        Args:
            city: Dictionary with city name, lat, lng
            unique_locations: Set to track unique store keys
            
        Returns:
            List of unique locations for this city
        """
        city_locations = []
        offset = 0
        page_size = 20
        max_pages = 50  # Safety limit to prevent infinite loops
        page_count = 0
        
        while page_count < max_pages:
            try:
                # Build URL with pagination and larger radius
                base_url = self.pharmacy_locations.CHEMIST_WAREHOUSE_URL.format(
                    latitude=city['lat'],
                    longitude=city['lng']
                )
                
                # Add pagination parameters and larger radius
                url = f"{base_url}&offset={offset}&radius=100000"  # 100km radius
                
                print(f"  Fetching page {page_count + 1} for {city['name']} (offset: {offset})...")
                
                # Make API call to get location data
                response = await self.session_manager.get(
                    url=url,
                    headers=self.headers
                )
                
                # Add small delay between requests to be respectful to the API
                import asyncio
                await asyncio.sleep(0.5)  # 500ms delay between requests
                
                # Handle API response with detailed error information
                success, data, error_msg = self._handle_api_response(response, city['name'], page_count + 1)
                if not success:
                    print(f"  Error fetching page {page_count + 1} for {city['name']}: {error_msg}")
                    break
                
                channels = data.get('channels', [])
                
                if not channels:
                    print(f"  No more results for {city['name']} at offset {offset}")
                    break
                
                new_locations_count = 0
                for channel_data in channels:
                    channel = channel_data.get('channel', {})
                    store_key = channel.get('key')
                    
                    # Check for duplicates using both key and location-based criteria
                    is_duplicate = False
                    if store_key and store_key in unique_locations:
                        is_duplicate = True
                    elif self._is_duplicate_location(channel, city_locations):
                        is_duplicate = True
                    
                    if not is_duplicate:
                        if store_key:
                            unique_locations.add(store_key)
                        city_locations.append(channel)
                        new_locations_count += 1
                
                print(f"  Page {page_count + 1}: {len(channels)} returned, {new_locations_count} new unique locations")
                
                # If we got fewer results than page size, we've reached the end
                if len(channels) < page_size:
                    print(f"  Reached end of results for {city['name']} (got {len(channels)} < {page_size})")
                    break
                    
            except Exception as e:
                print(f"  Error fetching page {page_count + 1} for {city['name']}: {e}")
                break
            
            # Move to next page
            offset += page_size
            page_count += 1
        
        if page_count >= max_pages:
            print(f"  Warning: Reached maximum page limit ({max_pages}) for {city['name']}")
            
        return city_locations
    
    async def fetch_pharmacy_details(self, location_id):
        """
        Fetch details for a specific pharmacy location.
        For Chemist Warehouse, all details are included in the main API call.
        
        Args:
            location_id: The ID of the location to get details for
            
        Returns:
            Location details data
        """
        # All details are included in the locations response for Chemist Warehouse
        return {"location_details": location_id}
        
    async def fetch_all_locations_details(self):
        """
        Fetch details for all locations and return as a list.
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        print("Fetching all Chemist Warehouse locations with pagination...")
        locations = await self.fetch_locations()
        if not locations:
            print("No Chemist Warehouse locations found.")
            return []
            
        print(f"Found {len(locations)} unique Chemist Warehouse locations. Processing details...")
        all_details = []
        
        for location in locations:
            try:
                extracted_details = self.extract_pharmacy_details(location)
                all_details.append(extracted_details)
            except Exception as e:
                print(f"Error processing Chemist Warehouse location {location.get('key', 'unknown')}: {e}")
        
        # Provide summary statistics
        states = {}
        for detail in all_details:
            state = detail.get('state', 'Unknown')
            states[state] = states.get(state, 0) + 1
        
        print(f"Completed processing details for {len(all_details)} Chemist Warehouse locations.")
        print("Distribution by state:")
        for state, count in sorted(states.items()):
            print(f"  {state}: {count} locations")
            
        return all_details
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract and standardize pharmacy details from new API data format.
        
        Args:
            pharmacy_data: Raw pharmacy data from the new API
            
        Returns:
            Dictionary with standardized pharmacy details
        """
        # Extract address information
        address_data = pharmacy_data.get('address', {})
        
        # Parse opening hours from new format
        trading_hours = self._parse_trading_hours_new_format(pharmacy_data.get('openingHours', {}))
        
        # Extract coordinates
        coordinates = pharmacy_data.get('coordinates', {})
        latitude = coordinates.get('latitude')
        longitude = coordinates.get('longitude')
        
        # Build full address
        street_address = address_data.get('streetNumber', '')
        full_address = f"{street_address}, {address_data.get('city', '')}, {address_data.get('state', '')} {address_data.get('postalCode', '')}"
        
        # Format the data according to our standardized structure
        result = {
            'name': pharmacy_data.get('name'),
            'address': full_address.strip(),
            'street_address': street_address,
            'suburb': address_data.get('city'),
            'state': address_data.get('state'),
            'postcode': address_data.get('postalCode'),
            'phone': address_data.get('phone'),
            'fax': address_data.get('fax'),
            'email': address_data.get('email'),
            'latitude': str(latitude) if latitude is not None else None,
            'longitude': str(longitude) if longitude is not None else None,
            'trading_hours': trading_hours,
            'store_key': pharmacy_data.get('key'),
            'capabilities': pharmacy_data.get('capabilities', [])
        }
        
        # Clean up the result by removing None values
        cleaned_result = {}
        for key, value in result.items():
            if value is not None and value != '':
                cleaned_result[key] = value
                
        return cleaned_result
        
    def _parse_trading_hours_new_format(self, hours_data):
        """
        Parse trading hours from new API format to structured format.
        
        Args:
            hours_data: Dictionary with days as keys and hours as values like
                       {'monday': {'from': '08:30:00.000', 'to': '19:30:00.000'}, ...}
            
        Returns:
            Dictionary with days as keys and hours as values formatted as
            {'Monday': {'open': '08:30 AM', 'closed': '06:00 PM'}, ...}
        """
        # Initialize all days with closed hours
        trading_hours = {
            'Monday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Tuesday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Wednesday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Thursday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Friday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Saturday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Sunday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Public Holiday': {'open': '12:00 AM', 'closed': '12:00 AM'}
        }
        
        if not hours_data:
            return trading_hours
        
        # Mapping from API day names to our format
        day_mapping = {
            'monday': 'Monday',
            'tuesday': 'Tuesday', 
            'wednesday': 'Wednesday',
            'thursday': 'Thursday',
            'friday': 'Friday',
            'saturday': 'Saturday',
            'sunday': 'Sunday'
        }
        
        # Process each day's opening hours
        for api_day, day_hours in hours_data.items():
            if api_day.lower() in day_mapping and isinstance(day_hours, dict):
                day_name = day_mapping[api_day.lower()]
                open_time = day_hours.get('from')
                close_time = day_hours.get('to')
                
                if open_time and close_time:
                    # Format the times to match the required format (HH:MM AM/PM)
                    formatted_open = self._format_time_new_format(open_time)
                    formatted_close = self._format_time_new_format(close_time)
                    
                    trading_hours[day_name] = {
                        'open': formatted_open,
                        'closed': formatted_close
                    }
        
        return trading_hours
    
    def _format_time_new_format(self, time_str):
        """
        Convert time strings like "08:30:00.000" to "08:30 AM" format
        
        Args:
            time_str: Time string in format like "08:30:00.000" or "19:30:00.000"
            
        Returns:
            Formatted time string like "08:30 AM" or "07:30 PM"
        """
        try:
            # Remove milliseconds and parse time in HH:MM:SS format
            if '.' in time_str:
                time_str = time_str.split('.')[0]
                
            if ':' in time_str:
                parts = time_str.split(':')
                if len(parts) >= 2:
                    hour = int(parts[0])
                    minute = int(parts[1])
                    
                    # Convert 24h format to 12h format with AM/PM
                    if hour == 0:
                        return "12:{:02d} AM".format(minute)
                    elif hour < 12:
                        return "{:02d}:{:02d} AM".format(hour, minute)
                    elif hour == 12:
                        return "12:{:02d} PM".format(minute)
                    else:
                        return "{:02d}:{:02d} PM".format(hour - 12, minute)
        except (ValueError, TypeError):
            pass
            
        # If parsing failed, return the original
        return time_str
    
    def _is_duplicate_location(self, channel, all_locations):
        """
        Check if a location is a duplicate based on multiple criteria.
        
        Args:
            channel: The channel data to check
            all_locations: List of existing locations to compare against
            
        Returns:
            bool: True if this is a duplicate location
        """
        current_coords = channel.get('coordinates', {})
        current_lat = current_coords.get('latitude')
        current_lng = current_coords.get('longitude')
        current_address = channel.get('address', {})
        current_street = current_address.get('streetNumber', '').strip().lower()
        current_city = current_address.get('city', '').strip().lower()
        current_postcode = current_address.get('postalCode', '').strip()
        
        for existing in all_locations:
            existing_coords = existing.get('coordinates', {})
            existing_lat = existing_coords.get('latitude')
            existing_lng = existing_coords.get('longitude')
            existing_address = existing.get('address', {})
            existing_street = existing_address.get('streetNumber', '').strip().lower()
            existing_city = existing_address.get('city', '').strip().lower()
            existing_postcode = existing_address.get('postalCode', '').strip()
            
            # Check if coordinates are very close (within ~10 meters)
            if (current_lat is not None and current_lng is not None and 
                existing_lat is not None and existing_lng is not None):
                lat_diff = abs(float(current_lat) - float(existing_lat))
                lng_diff = abs(float(current_lng) - float(existing_lng))
                
                # If coordinates are very close (within 0.0001 degrees ~ 10 meters)
                if lat_diff < 0.0001 and lng_diff < 0.0001:
                    return True
            
            # Check if address is identical
            if (current_street and existing_street and current_city and existing_city and 
                current_postcode and existing_postcode):
                if (current_street == existing_street and 
                    current_city == existing_city and 
                    current_postcode == existing_postcode):
                    return True
                    
        return False
    
    def _handle_api_response(self, response, city_name, page_num):
        """
        Handle API response and provide detailed error information.
        
        Args:
            response: The HTTP response object
            city_name: Name of the city being processed
            page_num: Current page number
            
        Returns:
            tuple: (success: bool, data: dict or None, error_msg: str or None)
        """
        if response.status_code == 200:
            try:
                data = response.json()
                return True, data, None
            except Exception as e:
                return False, None, f"Failed to parse JSON response: {e}"
        elif response.status_code == 429:
            return False, None, f"Rate limited (429) - consider increasing delay between requests"
        elif response.status_code == 404:
            return False, None, f"API endpoint not found (404) - check if URL is correct"
        elif response.status_code >= 500:
            return False, None, f"Server error ({response.status_code}) - API may be temporarily unavailable"
        else:
            return False, None, f"HTTP error {response.status_code}"