from ...base_handler import BasePharmacyHandler
import json
import re
import asyncio

class UnichemNZHandler(BasePharmacyHandler):
    """Handler for Unichem NZ pharmacies"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "unichem_nz"
        # Define brand-specific headers for API requests
        self.headers = {
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Pragma': 'no-cache',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
        }
        # The API URLs for Unichem NZ
        self.locations_url = "https://www.closeby.co/embed/60e75b93df98a16d97499b8b8512e14f/locations?bounding_box&cachable=true&isInitialLoad=true"
        self.location_details_url = "https://www.closeby.co/locations/{location_id}"
        # Maximum number of concurrent requests
        self.max_concurrent_requests = 10

    async def fetch_locations(self):
        """
        Fetch all Unichem NZ pharmacy locations.
        
        Returns:
            List of Unichem NZ pharmacy locations
        """
        # Make request to get locations data in JSON format
        response = await self.session_manager.get(
            url=self.locations_url,
            headers=self.headers
        )
        
        if response.status_code == 200:
            try:
                # Parse JSON response
                data = response.json()
                locations = data.get('locations', [])
                print(f"Found {len(locations)} Unichem NZ locations")
                return locations
            except json.JSONDecodeError as e:
                raise Exception(f"Failed to parse Unichem NZ locations response: {e}")
        else:
            raise Exception(f"Failed to fetch Unichem NZ locations: {response.status_code}")
    
    async def fetch_pharmacy_details(self, location_id):
        """
        Fetch details for a specific Unichem NZ pharmacy.
        
        Args:
            location_id: ID of the location to get details for
            
        Returns:
            Dictionary with pharmacy details
        """
        # Format the URL for the specific location
        url = self.location_details_url.format(location_id=location_id)
        
        # Make request to get location details
        response = await self.session_manager.get(
            url=url,
            headers=self.headers
        )
        
        if response.status_code == 200:
            try:
                # Parse JSON response
                data = response.json()
                if data.get('status') == 'success':
                    location_data = data.get('location', {})
                    return location_data
                else:
                    raise Exception(f"API returned error for location {location_id}")
            except json.JSONDecodeError as e:
                raise Exception(f"Failed to parse location details response: {e}")
        else:
            raise Exception(f"Failed to fetch location details: {response.status_code}")
    
    async def fetch_all_locations_details(self):
        """
        Fetch details for all Unichem NZ pharmacy locations using asyncio for concurrent processing.
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        print(f"Fetching all Unichem NZ pharmacy locations...")
        locations = await self.fetch_locations()
        
        if not locations:
            print(f"No Unichem NZ locations found.")
            return []
        
        print(f"Found {len(locations)} Unichem NZ locations. Fetching details in parallel...")
        
        # Create list to store processed pharmacy details
        all_pharmacy_details = []
        
        # Process locations in batches to avoid overwhelming the server
        location_ids = [location.get('id') for location in locations if location.get('id')]
        total_locations = len(location_ids)
        
        # Create a semaphore to limit concurrent connections
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        
        async def fetch_with_semaphore(location_id):
            """Helper function to fetch details with semaphore control"""
            async with semaphore:
                try:
                    pharmacy_details = await self.fetch_pharmacy_details(location_id)
                    if pharmacy_details:
                        processed_details = self.extract_pharmacy_details(pharmacy_details)
                        
                        return processed_details
                except Exception as e:
                    print(f"Error fetching details for location {location_id}: {e}")
                    return None
        
        # Create tasks for all locations
        tasks = [fetch_with_semaphore(location_id) for location_id in location_ids]
        
        # Process results as they complete
        print(f"Processing {total_locations} pharmacy locations in parallel...")
        results = await asyncio.gather(*tasks)
        
        # Filter out any None results (failed requests)
        all_pharmacy_details = [result for result in results if result]
        
        print(f"Successfully fetched details for {len(all_pharmacy_details)} out of {total_locations} Unichem NZ locations")
        return all_pharmacy_details
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract and standardize pharmacy details from raw data.
        
        Args:
            pharmacy_data: Raw pharmacy data from API
            
        Returns:
            Dictionary with standardized pharmacy details
        """
        # Initialize standardized data
        standardized_data = {
            "name": pharmacy_data.get('title', ''),
            "address": pharmacy_data.get('address_full', ''),
            "email": pharmacy_data.get('email', ''),
            "phone": pharmacy_data.get('phone_number', ''),
            "latitude": pharmacy_data.get('latitude', ''),
            "longitude": pharmacy_data.get('longitude', ''),
            "website": pharmacy_data.get('website', ''),
            "state": "",
            "trading_hours": {}
        }
        
        # Extract address components
        address = standardized_data["address"]
        if address:
            # Try to extract postcode (NZ postcodes are 4 digits)
            postcode_match = re.search(r'(\d{4})$', address)
            if postcode_match:
                standardized_data["postcode"] = postcode_match.group(1)
            
            # Try to extract suburb
            # New Zealand addresses typically have format: Street, Suburb, CITY Postcode
            suburb_match = re.search(r',\s*([^,]+?),\s*[A-Z\s]+\s+\d{4}$', address)
            if suburb_match:
                standardized_data["suburb"] = suburb_match.group(1).strip()
            
            # Street address is usually everything before the suburb
            if 'suburb' in standardized_data and standardized_data['suburb']:
                street_match = re.search(f'(.*),\\s*{re.escape(standardized_data["suburb"])}', address)
                if street_match:
                    standardized_data["street_address"] = street_match.group(1).strip()
        
        # Process trading hours from location_hours
        trading_hours = {}
        location_hours = pharmacy_data.get('location_hours', [])
        
        # Day mapping (API uses numerical days starting from 0 = Sunday)
        day_map = {
            0: 'Sunday',
            1: 'Monday',
            2: 'Tuesday',
            3: 'Wednesday',
            4: 'Thursday',
            5: 'Friday',
            6: 'Saturday'
        }
        
        for hour in location_hours:
            day_num = hour.get('day')
            day_name = day_map.get(day_num)
            if day_name:
                time_open = hour.get('time_open', '')
                time_close = hour.get('time_close', '')
                
                # Format times to match expected format (HH:MM AM/PM)
                if time_open and time_close:
                    open_formatted = self.format_time(time_open)
                    close_formatted = self.format_time(time_close)
                    
                    trading_hours[day_name] = {
                        'open': open_formatted,
                        'closed': close_formatted
                    }
                else:
                    # If no hours provided, set as closed
                    trading_hours[day_name] = {
                        'open': '12:00 AM',
                        'closed': '12:00 AM'
                    }
        
        # Ensure all days are represented
        for day in day_map.values():
            if day not in trading_hours:
                trading_hours[day] = {
                    'open': '12:00 AM',
                    'closed': '12:00 AM'
                }
        
        standardized_data["trading_hours"] = trading_hours
        
        return standardized_data
    
    def format_time(self, time_str):
        """
        Format time from 24-hour format (HH:MM) to 12-hour format (HH:MM AM/PM).
        
        Args:
            time_str: Time string in 24-hour format (e.g., "09:00")
            
        Returns:
            Time string in 12-hour format (e.g., "09:00 AM")
        """
        try:
            # Split the time string into hours and minutes
            hours, minutes = time_str.split(':')
            hours = int(hours)
            
            # Determine AM/PM
            if hours == 0:
                return f"12:{minutes} AM"
            elif hours < 12:
                return f"{hours:02d}:{minutes} AM"
            elif hours == 12:
                return f"12:{minutes} PM"
            else:
                return f"{hours-12:02d}:{minutes} PM"
        except (ValueError, AttributeError):
            # If parsing fails, return the original string
            return time_str