from ..base_handler import BasePharmacyHandler
import logging
import json
import re
from rich import print
from datetime import datetime
import asyncio

class DirectChemistHandler(BasePharmacyHandler):
    """Handler for Direct Chemist Outlet stores"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "direct_chemist"
        self.all_stores_url = "https://www.directchemistoutlet.com.au/graphql?query=query+getAmStoreLocatorsByState%7BamStoreLocatorsByState%7Bstate_code+state_id+items%7Bid+is_new+state+name+url_key+__typename%7D__typename%7D%7D&operationName=getAmStoreLocatorsByState&variables=%7B%7D"
        self.store_details_url_template = "https://www.directchemistoutlet.com.au/graphql?query=query+getStoreLocations%28%24locationId%3AInt%24stateId%3AInt%24distance%3AAmStoreLocatorDistanceFilterInput%24attributes%3A%5BAmStoreLocatorAttributeFilterInput%5D%24pageSize%3AInt%29%7BsearchAmStoreLocations%28filter%3A%7Blocation_id%3A%24locationId+state_id%3A%24stateId+distance%3A%24distance+attributes%3A%24attributes%7DpageSize%3A%24pageSize%29%7Bitems%7Baddress+full_address+state_code+attributes%7Battribute_code+attribute_id+entity_id+frontend_input+frontend_label+option_title_item%7Btitle+path+__typename%7Dvalue+__typename%7Daverage_rating+city+country+description+distance+email+id+images%7Bid+image_name+is_base+image_path+__typename%7Dis_new+lat+lng+main_image_name+marker_img+name+phone+schedule_string+show_schedule+state+url_key+website+working_time_today+zip+__typename%7Dpage_info%7Bcurrent_page+page_size+total_pages+__typename%7Dtotal_count+__typename%7D%7D&operationName=getStoreLocations&variables=%7B%22locationId%22:{location_id}%7D"
        
        self.headers = {
            'accept': 'application/json',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
            'referer': 'https://www.directchemistoutlet.com.au/find-store'
        }
        self.logger = logging.getLogger(__name__)
    
    async def fetch_locations(self):
        """
        Fetch all Direct Chemist Outlet locations
        
        Returns:
            List of Direct Chemist Outlet store information
        """
        try:
            # Make request to the GraphQL API to get all stores
            response = await self.session_manager.get(
                url=self.all_stores_url,
                headers=self.headers
            )
            
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch Direct Chemist Outlet locations: HTTP {response.status_code}")
                return []
            
            # Parse the JSON response
            data = response.json()
            
            if 'data' not in data or 'amStoreLocatorsByState' not in data['data']:
                self.logger.error("Invalid response format from Direct Chemist Outlet API")
                return []
            
            # Extract all store locations from the response
            all_locations = []
            
            for state_data in data['data']['amStoreLocatorsByState']:
                state_code = state_data.get('state_code')
                state_id = state_data.get('state_id')
                
                for store in state_data.get('items', []):
                    store_id = store.get('id')
                    store_name = store.get('name')
                    
                    # Create a store location object with basic details
                    store_location = {
                        'id': store_id,
                        'name': store_name,
                        'state_code': state_code,
                        'state_id': state_id,
                        'url_key': store.get('url_key'),
                        'is_new': store.get('is_new')
                    }
                    
                    all_locations.append(store_location)
            
            self.logger.info(f"Found {len(all_locations)} Direct Chemist Outlet locations")
            return all_locations
        except Exception as e:
            self.logger.error(f"Exception when fetching Direct Chemist Outlet locations: {str(e)}")
            return []
    
    async def fetch_pharmacy_details(self, location):
        """
        Fetch detailed information for a specific pharmacy location
        
        Args:
            location: Dictionary containing basic location information including the store ID
            
        Returns:
            Dictionary with detailed pharmacy information
        """
        try:
            location_id = location.get('id')
            if not location_id:
                self.logger.warning("No location ID provided for Direct Chemist Outlet store")
                return None
            
            # Construct the URL for fetching store details
            details_url = self.store_details_url_template.format(location_id=location_id)
            
            # Make request to the API
            response = await self.session_manager.get(
                url=details_url,
                headers=self.headers
            )
            
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch Direct Chemist Outlet store details for ID {location_id}: HTTP {response.status_code}")
                return None
            
            # Parse the JSON response
            data = response.json()
            
            if 'data' not in data or 'searchAmStoreLocations' not in data['data']:
                self.logger.error(f"Invalid response format from Direct Chemist Outlet details API for ID {location_id}")
                return None
            
            # Extract store details from the response
            search_results = data['data']['searchAmStoreLocations']
            items = search_results.get('items', [])
            
            if not items or len(items) == 0:
                self.logger.warning(f"No details found for Direct Chemist Outlet location ID {location_id}")
                return None
            
            # Return the first (and likely only) item from the results
            store_details = items[0]
            
            # Combine the basic location data with the detailed information
            combined_details = {**location, **store_details}
            return combined_details
        except Exception as e:
            self.logger.error(f"Exception when fetching Direct Chemist Outlet store details for ID {location.get('id', 'unknown')}: {str(e)}")
            return None
    
    async def fetch_all_locations_details(self):
        """
        Fetch details for all Direct Chemist Outlet locations
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        self.logger.info("Fetching all Direct Chemist Outlet locations...")
        locations = await self.fetch_locations()
        
        if not locations:
            self.logger.warning("No Direct Chemist Outlet locations found")
            return []
        
        self.logger.info(f"Found {len(locations)} Direct Chemist Outlet locations. Fetching details...")
        
        # Create tasks for fetching details for each location
        tasks = []
        for location in locations:
            tasks.append(self.fetch_pharmacy_details(location))
        
        # Process in batches to avoid overwhelming the server
        all_details = []
        batch_size = 5
        total_batches = (len(tasks) + batch_size - 1) // batch_size
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(tasks))
            batch_tasks = tasks[start_idx:end_idx]
            
            self.logger.info(f"Processing batch {batch_idx + 1}/{total_batches} ({start_idx+1}-{end_idx} of {len(tasks)})...")
            
            # Run the batch concurrently
            results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            # Process results
            for result in results:
                if isinstance(result, Exception):
                    self.logger.error(f"Error fetching Direct Chemist Outlet store details: {result}")
                elif result:
                    # Extract the standardized details
                    extracted_details = self.extract_pharmacy_details(result)
                    if extracted_details:
                        all_details.append(extracted_details)
        
        self.logger.info(f"Successfully processed details for {len(all_details)} Direct Chemist Outlet locations")
        return all_details
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract and standardize pharmacy details from the API response
        
        Args:
            pharmacy_data: Raw data from the API response
            
        Returns:
            Dictionary with standardized pharmacy details
        """
        if not pharmacy_data:
            return None
        
        try:
            # Extract basic information
            store_id = str(pharmacy_data.get('id', ''))
            name = pharmacy_data.get('name', 'Direct Chemist Outlet')
            
            # Extract address components
            address = pharmacy_data.get('address', '')
            full_address = pharmacy_data.get('full_address', '')
            city = pharmacy_data.get('city', '')
            state_code = pharmacy_data.get('state_code', '')
            zip_code = pharmacy_data.get('zip', '')
            
            # Extract contact information
            phone = pharmacy_data.get('phone', '')
            email = pharmacy_data.get('email', '')
            website = pharmacy_data.get('website', 'https://www.directchemistoutlet.com.au/')
            
            # Extract GPS coordinates
            latitude = pharmacy_data.get('lat', '')
            longitude = pharmacy_data.get('lng', '')
            
            # Extract and parse trading hours from schedule_string
            trading_hours = self._parse_trading_hours(pharmacy_data.get('schedule_string', ''))
            
            # Construct the standardized details dictionary
            details = {
                'brand': 'Direct Chemist Outlet',
                'name': name,
                'store_id': store_id,
                'address': full_address or f"{address}, {city} {state_code} {zip_code}".strip(),
                'street_address': address,
                'suburb': city,
                'state': state_code,
                'postcode': zip_code,
                'phone': phone,
                'email': email.lower() if email else None,
                'website': website,
                'latitude': latitude,
                'longitude': longitude,
                'trading_hours': trading_hours,
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Remove any empty values to keep the data clean
            return {k: v for k, v in details.items() if v not in (None, '', {}, [])}
        except Exception as e:
            self.logger.error(f"Error extracting Direct Chemist Outlet store details: {str(e)}")
            return {
                'brand': 'Direct Chemist Outlet',
                'name': pharmacy_data.get('name', 'Direct Chemist Outlet'),
                'store_id': str(pharmacy_data.get('id', '')),
                'website': 'https://www.directchemistoutlet.com.au/',
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
    
    def _parse_trading_hours(self, schedule_string):
        """
        Parse the schedule_string from the API response into a structured trading hours format
        
        Args:
            schedule_string: JSON string containing trading hours from the API
            
        Returns:
            Dictionary with days as keys and hours as values
        """
        # Initialize with default closed hours for all days
        trading_hours = {
            'Monday': {'open': 'Closed', 'closed': 'Closed'},
            'Tuesday': {'open': 'Closed', 'closed': 'Closed'},
            'Wednesday': {'open': 'Closed', 'closed': 'Closed'},
            'Thursday': {'open': 'Closed', 'closed': 'Closed'},
            'Friday': {'open': 'Closed', 'closed': 'Closed'},
            'Saturday': {'open': 'Closed', 'closed': 'Closed'},
            'Sunday': {'open': 'Closed', 'closed': 'Closed'},
            'Public Holiday': {'open': 'Closed', 'closed': 'Closed'}
        }
        
        if not schedule_string:
            return trading_hours
        
        try:
            # Parse the JSON string into a Python dictionary
            schedule_data = json.loads(schedule_string)
            
            # Map the days from the API format to our standard format
            day_mapping = {
                'monday': 'Monday',
                'tuesday': 'Tuesday',
                'wednesday': 'Wednesday',
                'thursday': 'Thursday',
                'friday': 'Friday',
                'saturday': 'Saturday',
                'sunday': 'Sunday',
                'public': 'Public Holiday'
            }
            
            # Process each day in the schedule data
            for api_day, day_name in day_mapping.items():
                if api_day in schedule_data:
                    day_data = schedule_data[api_day]
                    
                    # Get the status of the day (0 = closed, 1 = open)
                    day_status = day_data.get(f'{api_day}_status')
                    
                    # Skip if the day is closed
                    if day_status == '0' or day_status is False or day_status == 0:
                        continue
                    
                    # For public holidays, handle special case
                    if api_day == 'public':
                        # Public holidays might have a text value instead of hours
                        public_status = day_data
                        if isinstance(public_status, str) and 'closed' in public_status.lower():
                            continue
                        
                        # Otherwise, try to extract hours if available
                        # This would need custom parsing based on the format
                        continue
                    
                    # Get opening and closing hours
                    from_hours = day_data.get('from', {}).get('hours', '00')
                    from_minutes = day_data.get('from', {}).get('minutes', '00')
                    to_hours = day_data.get('to', {}).get('hours', '00')
                    to_minutes = day_data.get('to', {}).get('minutes', '00')
                    
                    # Format the hours
                    open_time = f"{int(from_hours):02d}:{int(from_minutes):02d}"
                    close_time = f"{int(to_hours):02d}:{int(to_minutes):02d}"
                    
                    # Convert to 12-hour format with AM/PM
                    open_time = self._format_time_24h(open_time)
                    close_time = self._format_time_24h(close_time)
                    
                    # Add to the trading hours dictionary
                    trading_hours[day_name] = {
                        'open': open_time,
                        'closed': close_time
                    }
            
            return trading_hours
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing trading hours JSON: {str(e)}")
            return trading_hours
        except Exception as e:
            self.logger.error(f"Unexpected error parsing trading hours: {str(e)}")
            return trading_hours
    
    def _format_time_24h(self, time_str):
        """
        Convert 24-hour time format to 12-hour format with AM/PM
        
        Args:
            time_str: Time string in 24-hour format (e.g., "14:30")
            
        Returns:
            Time string in 12-hour format (e.g., "02:30 PM")
        """
        try:
            hours, minutes = map(int, time_str.split(':'))
            
            am_pm = "AM"
            if hours >= 12:
                am_pm = "PM"
                if hours > 12:
                    hours -= 12
            elif hours == 0:
                hours = 12
            
            return f"{hours:02d}:{minutes:02d} {am_pm}"
        except Exception:
            return time_str