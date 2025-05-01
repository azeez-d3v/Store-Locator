from ..base_handler import BasePharmacyHandler
import re
from datetime import datetime
from rich import print

class AliveHandler(BasePharmacyHandler):
    """Handler for Alive Pharmacy Warehouse"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "alive"
        self.headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0'
        }
        
    async def fetch_locations(self):
        """
        Fetch all Alive Pharmacy locations.
        
        Returns:
            List of Alive Pharmacy locations
        """
        # Make API call to get location data
        response = await self.session_manager.get(
            url=self.pharmacy_locations.ALIVE_URL,
            headers=self.headers
        )
        
        if response.status_code == 200:
            # The API returns a list directly
            locations = response.json()
            print(f"Found {len(locations)} Alive Pharmacy locations")
            return locations
        else:
            raise Exception(f"Failed to fetch Alive Pharmacy locations: {response.status_code}")
    
    async def fetch_pharmacy_details(self, location_id):
        """
        Fetch details for a specific pharmacy location.
        For Alive Pharmacy, all details are included in the main API call.
        
        Args:
            location_id: The ID of the location to get details for
            
        Returns:
            Location details data
        """
        # All details are included in the locations response for Alive Pharmacy
        return {"location_details": location_id}
        
    async def fetch_all_locations_details(self):
        """
        Fetch details for all locations and return as a list.
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        print(f"Fetching all Alive Pharmacy locations...")
        locations = await self.fetch_locations()
        if not locations:
            print(f"No Alive Pharmacy locations found.")
            return []
            
        print(f"Found {len(locations)} Alive Pharmacy locations. Processing details...")
        all_details = []
        
        for location in locations:
            try:
                extracted_details = self.extract_pharmacy_details(location)
                all_details.append(extracted_details)
            except Exception as e:
                print(f"Error processing Alive Pharmacy location {location.get('id')}: {e}")
                
        print(f"Completed processing details for {len(all_details)} Alive Pharmacy locations.")
        return all_details
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract and standardize pharmacy details from raw API data.
        
        Args:
            pharmacy_data: Raw pharmacy data from the API
            
        Returns:
            Dictionary with standardized pharmacy details
        """
        # Parse opening hours from custom fields
        trading_hours = {}
        for custom_field in pharmacy_data.get('custom_fields', []):
            if custom_field.get('name') == 'Opening Hours':
                # Extract the opening hours value
                hours_text = custom_field.get('value', '')
                # Parse the opening hours text into our trading_hours format
                trading_hours = self._parse_trading_hours(hours_text)
        
        # Construct the complete address
        address_parts = []
        if pharmacy_data.get('address_line_1'):
            address_parts.append(pharmacy_data.get('address_line_1'))
        if pharmacy_data.get('address_line_2'):
            address_parts.append(pharmacy_data.get('address_line_2'))
        if pharmacy_data.get('city'):
            address_parts.append(pharmacy_data.get('city'))
        if pharmacy_data.get('state'):
            address_parts.append(pharmacy_data.get('state'))
        if pharmacy_data.get('postal_code'):
            address_parts.append(pharmacy_data.get('postal_code'))
        
        address = ', '.join(filter(None, address_parts))
        
        # Format the data according to our standardized structure
        result = {
            'name': pharmacy_data.get('name'),
            'address': address,
            'email': pharmacy_data.get('email'),
            'latitude': pharmacy_data.get('latitude'),
            'longitude': pharmacy_data.get('longitude'),
            'phone': pharmacy_data.get('phone'),
            'postcode': pharmacy_data.get('postal_code'),
            'state': pharmacy_data.get('state'),
            'street_address': pharmacy_data.get('address_line_1'),
            'suburb': pharmacy_data.get('city'),
            'trading_hours': trading_hours,
            'website': pharmacy_data.get('website')
        }
        
        # Clean up the result by removing None values
        cleaned_result = {}
        for key, value in result.items():
            if value is not None:
                cleaned_result[key] = value
                
        return cleaned_result
        
    def _parse_trading_hours(self, hours_text):
        """
        Parse trading hours from Alive Pharmacy's text format to structured format.
        
        Args:
            hours_text: String containing trading hours information like
                     "8am - 6pm Monday to Friday 8:30am - 6pm Saturday 10am - 4pm Sunday"
            
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
        
        if not hours_text:
            return trading_hours
            
        # Define day patterns including variations
        day_patterns = {
            'monday': 'Monday',
            'mon': 'Monday',
            'tuesday': 'Tuesday',
            'tue': 'Tuesday',
            'wednesday': 'Wednesday',
            'wed': 'Wednesday',
            'thursday': 'Thursday',
            'thu': 'Thursday',
            'thurs': 'Thursday',
            'friday': 'Friday',
            'fri': 'Friday',
            'saturday': 'Saturday',
            'sat': 'Saturday',
            'sunday': 'Sunday',
            'sun': 'Sunday'
        }
        
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        
        # Normalize text for easier parsing
        text = hours_text.lower().replace('.', ' ').replace(',', ' ')
        
        # Split input by potential segments
        segments = []
        
        # First, try to split by days
        pattern = r'((?:\d+(?::\d+)?(?:am|pm)\s*[-–—]\s*\d+(?::\d+)?(?:am|pm))(?:\s+[a-zA-Z]+(?:\s+to\s+[a-zA-Z]+)?(?:day|DAY))?)'
        matches = re.findall(pattern, text)
        
        if matches:
            # Process each segment that contains a time range
            for segment in matches:
                segment = segment.strip()
                
                # Extract time range
                time_match = re.search(r'(\d+(?::\d+)?(?:am|pm))\s*[-–—]\s*(\d+(?::\d+)?(?:am|pm))', segment)
                if not time_match:
                    continue
                    
                open_time = self._format_time(time_match.group(1))
                close_time = self._format_time(time_match.group(2))
                
                # Extract day range or individual days that follow this time range
                remaining_text = text[text.find(segment) + len(segment):].strip()
                
                # Check if there's a day range pattern right after the segment
                day_range_match = re.search(r'([a-z]+day)\s+to\s+([a-z]+day)', remaining_text)
                
                days_for_this_time = []
                
                if day_range_match:
                    # Handle "Monday to Friday" pattern
                    start_day = day_range_match.group(1).capitalize()
                    end_day = day_range_match.group(2).capitalize()
                    
                    # Map to full names if needed
                    if start_day.lower() in day_patterns:
                        start_day = day_patterns[start_day.lower()]
                    if end_day.lower() in day_patterns:
                        end_day = day_patterns[end_day.lower()]
                    
                    # Find indices in day_order
                    if start_day in day_order and end_day in day_order:
                        start_idx = day_order.index(start_day)
                        end_idx = day_order.index(end_day)
                        days_for_this_time = day_order[start_idx:end_idx+1]
                        
                        # Update text to avoid double counting
                        next_time_pos = remaining_text.find('am -')
                        if next_time_pos == -1:
                            next_time_pos = remaining_text.find('pm -')
                        
                        if next_time_pos != -1:
                            text = text[:text.find(segment) + len(segment)] + remaining_text[next_time_pos:]
                        else:
                            text = text[:text.find(segment) + len(segment)]
                else:
                    # Check for individual days after this time segment
                    for day_pattern, day_name in day_patterns.items():
                        if re.search(r'\b' + day_pattern + r'\b', remaining_text):
                            days_for_this_time.append(day_name)
                            
                    # If no days found after, look in the segment itself
                    if not days_for_this_time:
                        for day_pattern, day_name in day_patterns.items():
                            if re.search(r'\b' + day_pattern + r'\b', segment):
                                days_for_this_time.append(day_name)
                
                # Apply this time range to the identified days
                for day in days_for_this_time:
                    trading_hours[day] = {
                        'open': open_time,
                        'closed': close_time
                    }
        
        # If we couldn't parse the text using the segment approach, try parsing line by line
        if all(trading_hours[day] == {'open': '12:00 AM', 'closed': '12:00 AM'} for day in day_order):
            # Special case for common patterns in Alive Pharmacy data
            time_day_patterns = [
                # Pattern: "8am - 6pm Monday to Friday"
                r'(\d+(?::\d+)?(?:am|pm))\s*[-–—]\s*(\d+(?::\d+)?(?:am|pm))\s+([a-z]+day)\s+to\s+([a-z]+day)',
                
                # Pattern: "9am - 5pm Saturday"
                r'(\d+(?::\d+)?(?:am|pm))\s*[-–—]\s*(\d+(?::\d+)?(?:am|pm))\s+([a-z]+day)',
                
                # Pattern: "Monday to Friday 8am - 6pm"
                r'([a-z]+day)\s+to\s+([a-z]+day)\s+(\d+(?::\d+)?(?:am|pm))\s*[-–—]\s*(\d+(?::\d+)?(?:am|pm))',
                
                # Pattern: "Saturday 8am - 5pm"
                r'([a-z]+day)\s+(\d+(?::\d+)?(?:am|pm))\s*[-–—]\s*(\d+(?::\d+)?(?:am|pm))',
            ]
            
            for pattern in time_day_patterns:
                matches = re.finditer(pattern, text)
                for match in matches:
                    groups = match.groups()
                    
                    if len(groups) == 4 and 'to' in match.group(0):
                        # This is either "8am - 6pm Monday to Friday" or "Monday to Friday 8am - 6pm"
                        if 'day' in groups[2]:  # "8am - 6pm Monday to Friday"
                            open_time = self._format_time(groups[0])
                            close_time = self._format_time(groups[1])
                            start_day = groups[2].capitalize()
                            end_day = groups[3].capitalize()
                        else:  # "Monday to Friday 8am - 6pm"
                            start_day = groups[0].capitalize()
                            end_day = groups[1].capitalize()
                            open_time = self._format_time(groups[2])
                            close_time = self._format_time(groups[3])
                        
                        # Map to full names
                        if start_day.lower() in day_patterns:
                            start_day = day_patterns[start_day.lower()]
                        if end_day.lower() in day_patterns:
                            end_day = day_patterns[end_day.lower()]
                            
                        # Get days in range
                        if start_day in day_order and end_day in day_order:
                            start_idx = day_order.index(start_day)
                            end_idx = day_order.index(end_day)
                            for day in day_order[start_idx:end_idx+1]:
                                trading_hours[day] = {
                                    'open': open_time,
                                    'closed': close_time
                                }
                    elif len(groups) == 3:
                        if 'day' in groups[2]:  # "8am - 6pm Saturday"
                            open_time = self._format_time(groups[0])
                            close_time = self._format_time(groups[1])
                            day_name = groups[2].capitalize()
                        else:  # "Saturday 8am - 5pm"
                            day_name = groups[0].capitalize()
                            open_time = self._format_time(groups[1])
                            close_time = self._format_time(groups[2])
                            
                        # Map to full name
                        if day_name.lower() in day_patterns:
                            day_name = day_patterns[day_name.lower()]
                            
                        if day_name in trading_hours:
                            trading_hours[day_name] = {
                                'open': open_time,
                                'closed': close_time
                            }
        
        # Final check: If no trading hours were found for specific days, leave as default
        return trading_hours
    
    def _format_time(self, time_str):
        """
        Convert time strings like "8am", "8:30am" to "08:00 AM", "08:30 AM" format
        
        Args:
            time_str: Time string like "8am", "6pm", "8:30am"
            
        Returns:
            Formatted time string like "08:00 AM", "06:00 PM", "08:30 AM"
        """
        time_str = time_str.lower().strip()
        
        # Handle various formats
        if ':' in time_str:
            # Format like "8:30am"
            hour_part, minute_part = time_str.split(':')
            minute_num = re.search(r'\d+', minute_part).group(0)
            am_pm = 'AM' if 'am' in time_str.lower() else 'PM'
            hour_num = int(hour_part)
            hour_12 = hour_num if 1 <= hour_num <= 12 else hour_num % 12
            if hour_12 == 0:
                hour_12 = 12
            return f"{hour_12:02d}:{int(minute_num):02d} {am_pm}"
        else:
            # Format like "8am"
            hour_num = int(re.search(r'\d+', time_str).group(0))
            am_pm = 'AM' if 'am' in time_str.lower() else 'PM'
            hour_12 = hour_num if 1 <= hour_num <= 12 else hour_num % 12
            if hour_12 == 0:
                hour_12 = 12
            return f"{hour_12:02d}:00 {am_pm}"