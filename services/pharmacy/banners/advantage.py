from ..base_handler import BasePharmacyHandler
from rich import print
import json

class AdvantagePharmacyHandler(BasePharmacyHandler):
    """Handler for Advantage Pharmacies"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "advantage"
        # Define payload for the API request
        self.payload = {
            "businessid": "18",
            "source": "IPA-Website",
            "status": "1,2,3,4,5,6,7",
            "type": "findAStore"
        }
        # Define brand-specific headers for API requests
        self.headers = {
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Origin': 'https://independentpharmacies.com.au',
            'Referer': 'https://independentpharmacies.com.au/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'cross-site',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
        }    
    
    async def fetch_locations(self):
        """
        Fetch all Advantage Pharmacy locations.
        
        Returns:
            List of Advantage Pharmacy locations
        """
        
        # Make API call to get location data
        response = await self.session_manager.post(
            url=self.pharmacy_locations.ADVANTAGE_API_URL,
            headers=self.headers,
            json=self.payload
        )
        
        if response.status_code == 200:
            # The API returns a list of locations
            locations = response.json()
            print(f"Found {len(locations)} Advantage Pharmacy locations")
            return locations
        else:
            raise Exception(f"Failed to fetch Advantage Pharmacy locations: {response.status_code}")
    
    async def fetch_pharmacy_details(self, location_id):
        """
        Fetch details for a specific pharmacy location.
        For Advantage Pharmacy, all details are included in the main API call.
        
        Args:
            location_id: The ID of the location to get details for
            
        Returns:
            Location details data
        """
        # All details are included in the locations response for Advantage Pharmacy
        # We would need to filter the main list to find the specific location
        locations = await self.fetch_locations()
        
        # Find the location with the matching ID
        for location in locations:
            if str(location.get('locationid')) == str(location_id):
                return self.extract_pharmacy_details(location)
                
        return {"error": f"Location ID {location_id} not found"}
        
    async def fetch_all_locations_details(self):
        """
        Fetch details for all locations and return as a list.
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        print("Fetching all Advantage Pharmacy locations...")
        locations = await self.fetch_locations()
        if not locations:
            print("No Advantage Pharmacy locations found.")
            return []
            
        print(f"Found {len(locations)} Advantage Pharmacy locations. Processing details...")
        all_details = []
        
        for location in locations:
            try:
                extracted_details = self.extract_pharmacy_details(location)
                all_details.append(extracted_details)
            except Exception as e:
                location_name = location.get('locationname', 'Unknown Store')
                print(f"Error processing Advantage Pharmacy location '{location_name}': {e}")
                
        print(f"Completed processing details for {len(all_details)} Advantage Pharmacy locations.")
        return all_details
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract and standardize pharmacy details from raw API data.
        
        Args:
            pharmacy_data: Raw pharmacy data from the API
            
        Returns:
            Dictionary with standardized pharmacy details
        """
        # Extract core location details
        location_name = pharmacy_data.get('locationname', '')
        address = pharmacy_data.get('address', '')
        suburb = pharmacy_data.get('suburb', '')
        state = pharmacy_data.get('state', '')
        postcode = pharmacy_data.get('postcode', '')
        phone = pharmacy_data.get('phone', '')
        fax = pharmacy_data.get('fax_number', '')
        email = pharmacy_data.get('email', '')
        website = pharmacy_data.get('website', '')
        
        # Extract geo data
        latitude = pharmacy_data.get('latitude')
        longitude = pharmacy_data.get('longitude')
        
        # Parse opening hours
        trading_hours = self._parse_trading_hours(pharmacy_data.get('timings'))
        
        # Format the data according to our standardized structure
        result = {
            'name': location_name,
            'address': address,
            'street_address': pharmacy_data.get('streetaddress', ''),
            'suburb': suburb,
            'state': state,
            'postcode': postcode,
            'email': email,
            'phone': phone,
            'fax': fax,
            'website': website,
            'latitude': latitude,
            'longitude': longitude,
            'trading_hours': trading_hours
        }
        
        # Clean up the result by removing None values and empty strings
        cleaned_result = {}
        for key, value in result.items():
            if value is not None and value != '':
                cleaned_result[key] = value                
        return cleaned_result
    
    def _parse_trading_hours(self, hours_data):
        """
        Parse trading hours from the Advantage Pharmacy API format to structured format.
        Format examples: 
        - "9.00am-5.30pm Mon to Fri. 9am-12noon Sat"
        - "Mon-Fri: 8.15am to 7.30pm, Sat: 8.30am to 6.30pm, Sun: 9am to 5.30pm"
        - "Mon-Fri 9am-5:30pm, Sat 9am-1pm, Sun CLOSED"
        
        Args:
            hours_data: Opening hours data from the 'timings' field in the API
            
        Returns:
            Dictionary with days as keys and hours as values formatted as
            {'Monday': {'open': '08:30 AM', 'closed': '06:00 PM'}, ...}
        """
        # Initialize all days with closed hours
        trading_hours = {
            'Monday': {'open': 'Closed', 'closed': 'Closed'},
            'Tuesday': {'open': 'Closed', 'closed': 'Closed'},
            'Wednesday': {'open': 'Closed', 'closed': 'Closed'},            'Thursday': {'open': 'Closed', 'closed': 'Closed'},
            'Friday': {'open': 'Closed', 'closed': 'Closed'},
            'Saturday': {'open': 'Closed', 'closed': 'Closed'},
            'Sunday': {'open': 'Closed', 'closed': 'Closed'},
        }
        
        # If timings data is None, empty, or contains invalid data, return the default closed hours
        if not hours_data or not isinstance(hours_data, str):
            return trading_hours
            
        # Special case handling for invalid or irrelevant timings
        invalid_timings = ["tbc", ".", "sydney nsw", "gmt"]
        hours_data_lower = hours_data.lower()
        if any(invalid in hours_data_lower for invalid in invalid_timings):
            # Check if there's any valid time pattern
            if not any(x in hours_data_lower for x in ["am", "pm", ":"]) or hours_data_lower.strip() in [".", "tbc"]:
                return trading_hours
        
        try:
            # Mapping of day abbreviations and full names to standardized day names
            day_mapping = {
                'mon': 'Monday',
                'tue': 'Tuesday',
                'wed': 'Wednesday',
                'thu': 'Thursday',
                'fri': 'Friday',
                'sat': 'Saturday',
                'sun': 'Sunday',
                'monday': 'Monday',
                'tuesday': 'Tuesday',
                'wednesday': 'Wednesday',
                'thursday': 'Thursday',
                'friday': 'Friday',
                'saturday': 'Saturday',
                'sunday': 'Sunday'
            }
            
            day_order = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
            day_abbr_order = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
            
            # Split by comma to separate different day groups
            if ',' in hours_data:
                schedule_parts = hours_data.split(',')
            # If no comma, try splitting by period
            elif '.' in hours_data:
                schedule_parts = hours_data.split('.')
            else:
                # If neither comma nor period, treat the whole string as one part
                schedule_parts = [hours_data]
            
            for part in schedule_parts:
                part = part.strip()
                if not part:
                    continue
                
                # Skip parts that don't contain day information
                lower_part = part.lower()
                if all(day_abbr not in lower_part for day_abbr in day_abbr_order):
                    # Special case for time-only formats like "9 to 17:30"
                    if any(x in lower_part for x in ["am", "pm", ":"]) and ("to" in lower_part or "-" in lower_part):
                        # Apply to all weekdays (Mon-Fri) as a default
                        time_parts = lower_part.replace(" to ", "-").split("-")
                        if len(time_parts) >= 2:
                            open_time = self._format_time_from_string(time_parts[0].strip())
                            close_time = self._format_time_from_string(time_parts[1].strip())
                            
                            # Apply to weekdays only
                            for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']:
                                trading_hours[day] = {
                                    'open': open_time,
                                    'closed': close_time
                                }
                    continue
                
                # Handle "CLOSED" days
                if "closed" in lower_part and any(day in lower_part for day in day_abbr_order):
                    for day_abbr in day_abbr_order:
                        if day_abbr in lower_part or day_abbr.replace('thu', 'thur') in lower_part:
                            full_day = day_mapping[day_abbr]
                            trading_hours[full_day] = {'open': 'Closed', 'closed': 'Closed'}
                    continue
                
                # Extract days first (they usually come before times)
                days = []
                
                # Handle day ranges like "Mon-Fri:" or "Mon-Fri"
                day_pattern = r'(mon|tue|wed|thu|thur|fri|sat|sun)(?:\s*-\s*|\s+to\s+)(mon|tue|wed|thu|thur|fri|sat|sun)'
                import re
                day_ranges = re.findall(day_pattern, lower_part, re.IGNORECASE)
                
                if day_ranges:
                    for start_day, end_day in day_ranges:
                        # Standardize the day abbreviations
                        if 'thur' in start_day:
                            start_day = 'thu'
                        if 'thur' in end_day:
                            end_day = 'thu'
                        
                        # Find the indices in the day order
                        start_idx = -1
                        end_idx = -1
                        
                        for i, day in enumerate(day_abbr_order):
                            if day.startswith(start_day) or start_day.startswith(day):
                                start_idx = i
                            if day.startswith(end_day) or end_day.startswith(day):
                                end_idx = i
                        
                        if start_idx != -1 and end_idx != -1 and start_idx <= end_idx:
                            for i in range(start_idx, end_idx + 1):
                                days.append(day_mapping[day_abbr_order[i]])
                else:
                    # Handle individual days like "Sat:"
                    for abbr in day_abbr_order:
                        pattern = fr'{abbr}\s*:'
                        if re.search(pattern, lower_part, re.IGNORECASE) or f' {abbr} ' in f' {lower_part} ':
                            days.append(day_mapping[abbr])
                
                # If no days were extracted, skip this part
                if not days:
                    continue
                
                # Extract time ranges
                time_patterns = [
                    # Format: "9am-5:30pm" or "9am to 5:30pm"
                    r'(\d+(?:\.\d+)?(?::\d+)?(?:am|pm)?)\s*(?:to|-)\s*(\d+(?:\.\d+)?(?::\d+)?(?:am|pm)?)',
                    # Format: "9am" (single time, assume full day)
                    r'(\d+(?:\.\d+)?(?::\d+)?(?:am|pm))\s*$'
                ]
                
                time_ranges = []
                for pattern in time_patterns:
                    matches = re.findall(pattern, lower_part, re.IGNORECASE)
                    if matches:
                        time_ranges.extend(matches)
                        break
                
                # Process the time ranges
                if time_ranges:
                    for time_range in time_ranges:
                        # If it's a tuple, it's a range
                        if isinstance(time_range, tuple) and len(time_range) >= 2:
                            open_time = self._format_time_from_string(time_range[0])
                            close_time = self._format_time_from_string(time_range[1])
                        else:
                            # Single time, assume 9-hour workday
                            open_time = self._format_time_from_string(time_range)
                            close_time = "5:00 PM"  # Default closing
                        
                        # Update trading hours for each day
                        for day in days:
                            if day in trading_hours:
                                trading_hours[day] = {
                                    'open': open_time,
                                    'closed': close_time
                                }
                else:
                    # No time range found but we have days, check for "CLOSED"
                    if "closed" in lower_part:
                        for day in days:
                            trading_hours[day] = {'open': 'Closed', 'closed': 'Closed'}
                        
        except Exception as e:
            print(f"Error parsing trading hours '{hours_data}': {e}")
            # In case of error, return the default closed hours            
        return trading_hours
    
    def _format_time_from_string(self, time_str):
        """
        Format a time string like "9.00am" or "12noon" to standardized "9:00 AM" format
        Handles various formats including:
        - "9.00am", "9:00am"
        - "12noon"
        - "17:30" (24-hour format)
        - "5:30pm"
        
        Args:
            time_str: Time string which might be in various formats
            
        Returns:
            Formatted time string in 12-hour format with AM/PM
        """
        try:
            # Handle empty or None inputs
            if not time_str:
                return "Closed"
                
            # Clean the input
            time_str = str(time_str).strip().lower()
            
            # Handle special cases
            if "closed" in time_str:
                return "Closed"
            if "noon" in time_str:
                return "12:00 PM"
            
            # Extract AM/PM indicator
            is_pm = "pm" in time_str
            is_am = "am" in time_str
            
            # Remove am/pm suffix
            time_str = time_str.replace("am", "").replace("pm", "").strip()
            
            # Parse hours and minutes
            hour = 0
            minute = 0
            
            if "." in time_str:
                # Handle format like "9.00"
                parts = time_str.split(".")
                hour = int(parts[0]) if parts[0].isdigit() else 0
                minute = int(parts[1]) if parts[1].isdigit() else 0
            elif ":" in time_str:
                # Handle format like "9:00" or "17:30" (24-hour format)
                parts = time_str.split(":")
                hour = int(parts[0]) if parts[0].isdigit() else 0
                minute = int(parts[1]) if parts[1].isdigit() else 0
                
                # Check if this is 24-hour format (no am/pm specified and hour > 12)
                if not is_am and not is_pm and hour > 12:
                    is_pm = True
                    hour = hour
            else:
                # Handle format like "9"
                try:
                    hour = int(time_str)
                    minute = 0
                except ValueError:
                    # If conversion fails, return original
                    return time_str
            
            # Apply AM/PM conversion
            if is_pm and hour < 12:
                hour += 12
            elif is_am and hour == 12:
                hour = 0
            
            # Format to 12-hour time
            if hour == 0:
                return f"12:{minute:02d} AM"
            elif hour < 12:
                return f"{hour}:{minute:02d} AM"
            elif hour == 12:
                return f"12:{minute:02d} PM"
            else:
                return f"{hour-12}:{minute:02d} PM"
                
        except (ValueError, TypeError) as e:
            print(f"Error formatting time: {time_str} - {e}")
            return time_str  # Return original on error
