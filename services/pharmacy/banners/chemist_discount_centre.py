import logging
from rich import print
from ..base_handler import BasePharmacyHandler

class ChemistDiscountCentreHandler(BasePharmacyHandler):
    """Handler for Chemist Discount Centre pharmacies"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "chemist_discount_centre"
        self.base_url = self.pharmacy_locations.CHEMIST_DISCOUNT_CENTRE_URL
        
        # Define brand-specific headers for requests
        self.headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
            'referer': 'https://www.chemistdiscountcentre.com.au/',
            'origin': 'https://www.chemistdiscountcentre.com.au',
            'x-requested-with': 'XMLHttpRequest'
        }
        self.logger = logging.getLogger(__name__)

    async def fetch_locations(self):
        """
        Fetch all Chemist Discount Centre pharmacy locations from their API.
        
        Returns:
            List of Chemist Discount Centre pharmacy locations
        """
        try:
            # POST request payload as specified
            payload = {
                'lat': '0',
                'lng': '0', 
                'radius': '-1'
            }
            
            response = await self.session_manager.post(
                url=self.base_url,
                headers=self.headers,
                data=payload
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # The API should return a list of pharmacies
                if isinstance(data, list):
                    print(f"Found {len(data)} Chemist Discount Centre locations in API response")
                    return data
                elif isinstance(data, dict) and 'shops' in data:
                    print(f"Found {len(data['shops'])} Chemist Discount Centre locations in API response")
                    return data['shops']
                elif isinstance(data, dict) and 'data' in data:
                    print(f"Found {len(data['data'])} Chemist Discount Centre locations in API response")
                    return data['data']
                else:
                    print("No locations found in Chemist Discount Centre API response")
                    print(f"API response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
                    return []
            else:
                self.logger.error(f"Failed to fetch Chemist Discount Centre locations: {response.status_code}")
                self.logger.error(f"Response content: {response.text}")
                return []
                
        except Exception as e:
            self.logger.error(f"Error fetching Chemist Discount Centre locations: {e}")
            return []

    async def fetch_pharmacy_details(self, location_id):
        """
        For Chemist Discount Centre, we already have all the data in the locations response
        This is a placeholder for API consistency
        
        Args:
            location_id: The ID of the location to get details for
            
        Returns:
            Location data (unchanged)
        """
        return {"location_details": location_id}

    async def fetch_all_locations_details(self):
        """
        Fetch details for all Chemist Discount Centre locations and return as a list
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        print("Fetching all Chemist Discount Centre locations...")
        locations = await self.fetch_locations()
        if not locations:
            print("No Chemist Discount Centre locations found.")
            return []
            
        print(f"Found {len(locations)} Chemist Discount Centre locations. Processing details...")
        all_details = []
        
        for location in locations:
            try:
                extracted_details = self.extract_pharmacy_details(location)
                all_details.append(extracted_details)
            except Exception as e:
                print(f"Error processing Chemist Discount Centre location {location.get('ID') or location.get('id')}: {e}")
                
        print(f"Completed processing details for {len(all_details)} Chemist Discount Centre locations.")
        return all_details    
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract and format pharmacy details from the API data.
        
        Args:
            pharmacy_data: Raw pharmacy data from the API
            
        Returns:
            Dictionary with standardized pharmacy details
        """
        try:
            # Extract basic information from actual API response structure
            name = pharmacy_data.get('Name', '')
            street_number = pharmacy_data.get('StreerNumber', '')  # Note: API has typo "StreerNumber"
            address = pharmacy_data.get('Address', '')
            suburb = pharmacy_data.get('Suburb', '')
            postcode = pharmacy_data.get('Postcode', '')
            
            # Get phone numbers - ContactNumber1 is primary, ContactNumber2 is secondary
            phone1 = pharmacy_data.get('ContactNumber1', '')
            phone2 = pharmacy_data.get('ContactNumber2', '')
            phone = f"{phone1}" + (f" / {phone2}" if phone2 and phone2 != phone1 else "")
            
            # Construct full address with street number
            address_parts = []
            if street_number:
                address_parts.append(street_number)
            if address:
                address_parts.append(address)
            if suburb:
                address_parts.append(suburb)
            if postcode:
                address_parts.append(postcode)
            
            full_address = ', '.join(filter(None, address_parts))
            
            # Extract trading hours
            trading_hours = self.parse_trading_hours(pharmacy_data)
            
            return {
                'name': name,
                'address': full_address,
                'phone': phone,
                'email': pharmacy_data.get('Email', ''),
                'trading_hours': trading_hours,
                'latitude': pharmacy_data.get('Lat', ''),  # Note: API uses 'Lat' not 'Latitude'
                'longitude': pharmacy_data.get('Long', ''),  # Note: API uses 'Long' not 'Longitude'
                'suburb': suburb,
                'state': '',  # Extract state from suburb field if needed
                'postcode': postcode,
                'brand': 'Chemist Discount Centre'
            }
            
        except Exception as e:
            self.logger.error(f"Error extracting pharmacy details: {e}")
            return {
                'name': str(pharmacy_data.get('Name') or pharmacy_data.get('name', 'Unknown')),
                'address': 'Address not available',
                'phone': '',
                'email': '',
                'trading_hours': '',
                'latitude': '',
                'longitude': '',
                'suburb': '',
                'state': '',
                'postcode': '',
                'brand': 'Chemist Discount Centre'
            }

    def parse_trading_hours(self, pharmacy_data):
        """
        Parse trading hours from the API response.
        
        Args:
            pharmacy_data: Raw pharmacy data containing trading hours
            
        Returns:
            Formatted trading hours string
        """
        try:
            # Look for trading hours in various possible formats
            if 'ShopsHours' in pharmacy_data and isinstance(pharmacy_data['ShopsHours'], list):
                return self.format_shops_hours(pharmacy_data['ShopsHours'])
            elif 'OpeningHours' in pharmacy_data:
                return str(pharmacy_data['OpeningHours'])
            elif 'TradingHours' in pharmacy_data:
                return str(pharmacy_data['TradingHours'])
            elif 'Hours' in pharmacy_data:
                return str(pharmacy_data['Hours'])
            else:
                return 'Trading hours not available'
                
        except Exception as e:
            self.logger.error(f"Error parsing trading hours: {e}")
            return 'Trading hours not available'    
        
    def format_shops_hours(self, shops_hours):
        """
        Format trading hours from ShopsHours array.
        
        Args:
            shops_hours: List of trading hours data
            
        Returns:
            Formatted trading hours string
        """
        try:
            # Map weekday IDs to day names (0=Sunday, 1=Monday, etc.)
            weekdays = {
                0: 'Sunday',
                1: 'Monday', 
                2: 'Tuesday',
                3: 'Wednesday',
                4: 'Thursday',
                5: 'Friday',
                6: 'Saturday'
            }
            
            # Group hours by weekday and type - focus on Type 0 (general hours)
            weekday_hours = {}
            
            for hour_data in shops_hours:
                weekday_id = hour_data.get('Weekday')
                start_time = hour_data.get('Start', '')
                end_time = hour_data.get('End', '')
                is_available = hour_data.get('IsAvailable', False)
                hour_type = hour_data.get('Type', 0)
                
                # Focus on Type 0 (general store hours) and available times
                if weekday_id in weekdays and hour_type == 0:
                    day_name = weekdays[weekday_id]
                    
                    if not is_available or (start_time == "00:00:00" and end_time == "00:00:00"):
                        weekday_hours[weekday_id] = f"{day_name}: Closed"
                    elif start_time and end_time:
                        # Format time from 24h to 12h format
                        formatted_start = self.format_time(start_time)
                        formatted_end = self.format_time(end_time)
                        weekday_hours[weekday_id] = f"{day_name}: {formatted_start} - {formatted_end}"
                    else:
                        weekday_hours[weekday_id] = f"{day_name}: Hours not available"
            
            # Sort by weekday (Monday first)
            sorted_hours = []
            for day_id in [1, 2, 3, 4, 5, 6, 0]:  # Monday to Sunday
                if day_id in weekday_hours:
                    sorted_hours.append(weekday_hours[day_id])
            
            return '; '.join(sorted_hours) if sorted_hours else 'Trading hours not available'
            
        except Exception as e:
            self.logger.error(f"Error formatting shops hours: {e}")
            return 'Trading hours not available'

    def format_time(self, time_str):
        """
        Format time from 24h format (HH:MM:SS) to 12h format (H:MM AM/PM).
        
        Args:
            time_str: Time string in format "HH:MM:SS"
            
        Returns:
            Formatted time string in 12h format
        """
        try:
            if not time_str or time_str == "00:00:00":
                return "Closed"
                
            # Parse the time string
            time_parts = time_str.split(':')
            hour = int(time_parts[0])
            minute = int(time_parts[1])
            
            # Convert to 12h format
            if hour == 0:
                return f"12:{minute:02d} AM"
            elif hour < 12:
                return f"{hour}:{minute:02d} AM"
            elif hour == 12:
                return f"12:{minute:02d} PM"
            else:
                return f"{hour-12}:{minute:02d} PM"
                
        except Exception as e:
            self.logger.error(f"Error formatting time {time_str}: {e}")
            return time_str
