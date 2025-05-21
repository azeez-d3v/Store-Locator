from ..base_handler import BasePharmacyHandler
from rich import print

class PricelineHandler(BasePharmacyHandler):
    """Handler for Priceline Pharmacies"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "priceline"
        # Define brand-specific headers for API requests
        self.headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Pragma': 'no-cache',
            'Referer': 'https://www.priceline.com.au/store-locator',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
            'Origin': 'https://www.priceline.com.au',
        }

    async def fetch_locations(self):
        """
        Fetch all Priceline Pharmacy locations.
        
        Returns:
            List of Priceline Pharmacy locations
        """
        
        # Make API call to get location data
        response = await self.session_manager.get(
            url=self.pharmacy_locations.PRICELINE_API_URL,
            headers=self.headers
        )
        
        if response.status_code == 200:
            data = response.json()
            # The API returns a stores array within the JSON
            locations = data.get('stores', [])
            print(f"Found {len(locations)} Priceline Pharmacy locations")
            return locations
        else:
            raise Exception(f"Failed to fetch Priceline Pharmacy locations: {response.status_code}")
    
    async def fetch_pharmacy_details(self, location_id):
        """
        Fetch details for a specific pharmacy location.
        For Priceline, all details are included in the main API call.
        
        Args:
            location_id: The ID of the location to get details for
            
        Returns:
            Location details data
        """
        # All details are included in the locations response for Priceline
        return {"location_details": location_id}
        
    async def fetch_all_locations_details(self):
        """
        Fetch details for all locations and return as a list.
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        print("Fetching all Priceline Pharmacy locations...")
        locations = await self.fetch_locations()
        if not locations:
            print("No Priceline Pharmacy locations found.")
            return []
            
        print(f"Found {len(locations)} Priceline Pharmacy locations. Processing details...")
        all_details = []
        
        for location in locations:
            try:
                extracted_details = self.extract_pharmacy_details(location)
                all_details.append(extracted_details)
            except Exception as e:
                store_name = location.get('displayName', 'Unknown Store')
                print(f"Error processing Priceline Pharmacy location '{store_name}': {e}")
                
        print(f"Completed processing details for {len(all_details)} Priceline Pharmacy locations.")
        return all_details
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract and standardize pharmacy details from raw API data.
        
        Args:
            pharmacy_data: Raw pharmacy data from the API
            
        Returns:
            Dictionary with standardized pharmacy details
        """        # Extract address information
        address_data = pharmacy_data.get('address', {})
        geo_point = pharmacy_data.get('geoPoint', {})
        
        # Get latitude and longitude from geoPoint or address, geoPoint is preferred
        latitude = geo_point.get('latitude') if geo_point else address_data.get('latitude')
        longitude = geo_point.get('longitude') if geo_point else address_data.get('longitude')
        
        # Get postcode, town, region
        postcode = address_data.get('postalCode', '')
        town = address_data.get('town', '')
        region_data = address_data.get('region', {})
        state = region_data.get('isocodeShort', '') if region_data else ''
        
        # Format address
        street_address = address_data.get('line2', '')
        formatted_address = address_data.get('formattedAddress', '')
        
        # Extract contact information
        email = address_data.get('email', '')
        phone = address_data.get('phone', '')
        fax = address_data.get('fax', '')
        
        # Extract script email if available (specific to pharmacies)
        script_email = pharmacy_data.get('scriptEmail', '')
        if script_email and not email:
            email = script_email
        
        # Parse opening hours - ensure we're using correct structure
        opening_hours_data = pharmacy_data.get('openingHours', {})
        trading_hours = self._parse_trading_hours(opening_hours_data)
        
        # Format the data according to our standardized structure
        result = {
            'name': pharmacy_data.get('displayName', ''),
            'address': formatted_address,
            'email': email,
            'latitude': str(latitude) if latitude is not None else None,
            'longitude': str(longitude) if longitude is not None else None,
            'phone': phone,
            'postcode': postcode,
            'state': state,
            'street_address': street_address,
            'suburb': town,
            'trading_hours': trading_hours,
            'fax': fax
        }
        
        # Clean up the result by removing None values and empty strings
        cleaned_result = {}
        for key, value in result.items():
            if value is not None and value != '':
                cleaned_result[key] = value
                
        return cleaned_result
    def _parse_trading_hours(self, hours_data):
        """
        Parse trading hours from the Priceline API format to structured format.
        
        Args:
            hours_data: Opening hours data from the API
            
        Returns:
            Dictionary with days as keys and hours as values formatted as
            {'Monday': {'open': '08:30 AM', 'closed': '06:00 PM'}, ...}
        """
        # Initialize all days with closed hours
        trading_hours = {
            'Monday': {'open': 'Closed', 'closed': 'Closed'},
            'Tuesday': {'open': 'Closed', 'closed': 'Closed'},
            'Wednesday': {'open': 'Closed', 'closed': 'Closed'},
            'Thursday': {'open': 'Closed', 'closed': 'Closed'},
            'Friday': {'open': 'Closed', 'closed': 'Closed'},
            'Saturday': {'open': 'Closed', 'closed': 'Closed'},
            'Sunday': {'open': 'Closed', 'closed': 'Closed'},
        }
        
        # Get the weekday opening list
        week_day_opening_list = hours_data.get('weekDayOpeningList', [])
        
        if not week_day_opening_list:
            return trading_hours
        
        # Day mapping from API format (UPPERCASE) to our format (Title Case)
        day_mapping = {
            'MONDAY': 'Monday',
            'TUESDAY': 'Tuesday',
            'WEDNESDAY': 'Wednesday',
            'THURSDAY': 'Thursday',
            'FRIDAY': 'Friday',
            'SATURDAY': 'Saturday',
            'SUNDAY': 'Sunday'
        }
        
        # Process each day's opening hours
        for hours_item in week_day_opening_list:
            api_day = hours_item.get('weekDay', '')
            if not api_day:
                continue
                
            # Convert API day format to our format
            day = day_mapping.get(api_day, api_day)
            
            # Skip if day is not in our map or if store is closed on this day
            if day not in trading_hours:
                continue
                
            # Check if store is closed on this day
            if hours_item.get('closed', True):
                continue
            
            # Get opening and closing times
            opening_time = hours_item.get('openingTime', {})
            closing_time = hours_item.get('closingTime', {})
            
            # Use formatted hours from API if available, otherwise build from hour and minute
            open_formatted = opening_time.get('formattedHour', '')
            close_formatted = closing_time.get('formattedHour', '')
            
            if not open_formatted and 'hour' in opening_time and 'minute' in opening_time:
                open_formatted = self._format_time(opening_time['hour'], opening_time['minute'])
                
            if not close_formatted and 'hour' in closing_time and 'minute' in closing_time:
                close_formatted = self._format_time(closing_time['hour'], closing_time['minute'])
            
            if open_formatted and close_formatted:
                trading_hours[day] = {
                    'open': open_formatted,
                    'closed': close_formatted
                }
        
        return trading_hours
    def _format_time(self, hour, minute):
        """
        Format hour and minute to a time string like "08:30 AM"
        
        Args:
            hour: Hour value (0-23)
            minute: Minute value (0-59)
            
        Returns:
            Formatted time string
        """
        try:
            hour = int(hour)
            minute = int(minute)
            
            # Convert to 12-hour format
            if hour == 0:
                return f"12:{minute:02d} AM"
            elif hour < 12:
                return f"{hour}:{minute:02d} AM"
            elif hour == 12:
                return f"12:{minute:02d} PM"
            else:
                return f"{hour-12}:{minute:02d} PM"
        except (ValueError, TypeError) as e:
            print(f"Error formatting time: {hour}:{minute} - {e}")
            # Return a default if conversion fails
            return f"{hour}:{minute}"
