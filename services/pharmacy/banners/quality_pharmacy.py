import re
import logging
from bs4 import BeautifulSoup
from ..base_handler import BasePharmacyHandler

class QualityPharmacyHandler(BasePharmacyHandler):
    """Handler for Quality Pharmacy stores"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "quality_pharmacy"
        self.base_url = self.pharmacy_locations.QUALITY_PHARMACY_URL
        
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
        }
        self.logger = logging.getLogger(__name__)
        
    async def fetch_locations(self):
        """
        Fetch all Quality Pharmacy locations.
        
        Returns:
            List of Quality Pharmacy locations
        """
        try:
            response = await self.session_manager.get(
                url=self.base_url,
                headers=self.headers
            )
            
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch Quality Pharmacy locations: HTTP {response.status_code}")
                return []
                
            html_content = response.text
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find all location list items with the specific class
            location_items = soup.find_all('li', class_='location__item')
            
            if not location_items:
                self.logger.warning("No location items found with class 'location__item'")
                return []
            
            locations = []
            for i, location_item in enumerate(location_items):
                try:
                    location_data = self._extract_store_info(location_item, i)
                    if location_data:
                        locations.append(location_data)
                except Exception as e:
                    self.logger.warning(f"Error extracting store {i}: {str(e)}")
                    continue
            
            self.logger.info(f"Found {len(locations)} Quality Pharmacy locations")
            return locations
            
        except Exception as e:
            self.logger.error(f"Exception when fetching Quality Pharmacy locations: {str(e)}")
            return []
    
    def _extract_store_info(self, location_item, index):
        """
        Extract store information from a location item element.
        
        Args:
            location_item: BeautifulSoup element containing store information
            index: Index of the store for ID generation
            
        Returns:
            Dictionary containing store information
        """
        try:
            store_data = {
                'id': f"quality_pharmacy_{index}",
                'brand': 'Quality Pharmacy'
            }
            
            # Extract the data-location attribute which contains address
            data_location = location_item.get('data-location', '')
            if data_location:
                store_data['address'] = data_location
            
            # Find the content area within the location item
            content_div = location_item.find('div', class_='location__content')
            if not content_div:
                self.logger.warning(f"No content div found in location {index}")
                return None
            
            # Extract store name from h5 tag
            name_element = content_div.find('h5')
            if name_element:
                store_data['name'] = name_element.get_text(strip=True)
            
            # Extract phone number
            phone_element = content_div.find('p', class_='location__phone')
            if phone_element:
                phone_link = phone_element.find('a')
                if phone_link:
                    # Extract phone from the link text, clean it up
                    phone_text = phone_link.get_text(strip=True)
                    # Remove any extra text after the phone number
                    phone_clean = re.sub(r'\s*-.*$', '', phone_text)  # Remove anything after " -"
                    store_data['phone'] = phone_clean
            
            # Extract address (also available in location__address class)
            address_element = content_div.find('p', class_='location__address')
            if address_element and not store_data.get('address'):
                store_data['address'] = address_element.get_text(strip=True)
            
            # Extract opening hours
            hours_list = content_div.find('ul', class_='opening-hours')
            if hours_list:
                hours_items = hours_list.find_all('li')
                if hours_items:
                    # Combine all hours into a single string
                    hours_text = ', '.join([item.get_text(strip=True) for item in hours_items])
                    store_data['trading_hours'] = self._parse_trading_hours(hours_text)
            
            # Extract state and postcode from address for geocoding
            if 'address' in store_data:
                address = store_data['address']
                # Extract state from address (VIC, NSW, etc.)
                state_match = re.search(r'\b(VIC|NSW|QLD|SA|WA|TAS|NT|ACT)\b', address)
                if state_match:
                    store_data['state'] = state_match.group(1)
                
                # Extract postcode
                postcode_match = re.search(r'\b(\d{4})\b', address)
                if postcode_match:
                    store_data['postcode'] = postcode_match.group(1)
            
            return store_data
            
        except Exception as e:
            self.logger.error(f"Error extracting store info: {str(e)}")
            return None
    
    def _parse_trading_hours(self, hours_text):
        """
        Parse trading hours from Quality Pharmacy format to structured format.
        Format examples: 
        - "Monday - Friday 9:00am - 5:30pm, Saturday 9:00am - 5:00pm, Sunday 10:00am - 5:00pm"
        - "Monday - Wednesday 9:00am - 6:00pm, Thursday - Friday 9:00am - 9:00pm, Saturday 9:00am - 5:00pm, Sunday 10:00am - 5:00pm"
        
        Args:
            hours_text: Opening hours string from Quality Pharmacy
            
        Returns:
            Dictionary with days as keys and hours as values formatted as
            {'Monday': {'open': '09:00 AM', 'closed': '05:30 PM'}, ...}
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
        
        if not hours_text or not isinstance(hours_text, str):
            return trading_hours
            
        try:
            # Split by comma to handle multiple day ranges
            segments = [seg.strip() for seg in hours_text.split(',')]
            
            for segment in segments:
                if not segment:
                    continue
                    
                # Handle different day range formats
                days = []
                
                if "Monday - Friday" in segment or "monday - friday" in segment.lower():
                    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
                elif "Monday - Wednesday" in segment or "monday - wednesday" in segment.lower():
                    days = ['Monday', 'Tuesday', 'Wednesday']
                elif "Thursday - Friday" in segment or "thursday - friday" in segment.lower():
                    days = ['Thursday', 'Friday']
                elif "Saturday" in segment.lower():
                    days = ['Saturday']
                elif "Sunday" in segment.lower():
                    days = ['Sunday']
                elif "Monday" in segment.lower():
                    days = ['Monday']
                elif "Tuesday" in segment.lower():
                    days = ['Tuesday']
                elif "Wednesday" in segment.lower():
                    days = ['Wednesday']
                elif "Thursday" in segment.lower():
                    days = ['Thursday']
                elif "Friday" in segment.lower():
                    days = ['Friday']
                
                if not days:
                    continue
                
                # Extract time range from this segment
                # Look for patterns like "9:00am - 5:30pm"
                time_match = re.search(r'(\d{1,2}:\d{2}(?:am|pm))\s*-\s*(\d{1,2}:\d{2}(?:am|pm))', segment, re.IGNORECASE)
                if time_match:
                    open_time = self._format_time_from_string(time_match.group(1))
                    close_time = self._format_time_from_string(time_match.group(2))
                    
                    for day in days:
                        trading_hours[day] = {'open': open_time, 'closed': close_time}
                else:
                    # Check for "CLOSED" keyword
                    if "closed" in segment.lower():
                        for day in days:
                            trading_hours[day] = {'open': 'Closed', 'closed': 'Closed'}
                            
        except Exception as e:
            self.logger.error(f"Error parsing trading hours '{hours_text}': {e}")
            
        return trading_hours
    
    def _format_time_from_string(self, time_str):
        """
        Format a time string like "9:00am" or "5:30pm" to standardized "9:00 AM" format
        
        Args:
            time_str: Time string which might be in various formats
            
        Returns:
            Formatted time string in 12-hour format with AM/PM
        """
        try:
            if not time_str:
                return "Closed"
                
            # Clean the input
            time_str = str(time_str).strip().upper()
            
            if "CLOSED" in time_str:
                return "Closed"
            
            # Extract AM/PM indicator
            is_pm = "PM" in time_str
            is_am = "AM" in time_str
            
            # Remove AM/PM suffix and clean
            time_str = time_str.replace("AM", "").replace("PM", "").strip()
            
            # Parse hours and minutes
            if ":" in time_str:
                # Handle format like "9:00"
                parts = time_str.split(":")
                hour = int(parts[0]) if parts[0].isdigit() else 0
                minute = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
            else:
                # Handle format like "9"
                hour = int(time_str) if time_str.isdigit() else 0
                minute = 0
            
            # Apply AM/PM conversion for 24-hour format
            if is_pm and hour < 12:
                hour += 12
            elif is_am and hour == 12:
                hour = 0
            
            # Format to 12-hour time
            if hour == 0:
                formatted_time = f"12:{minute:02d} AM"
            elif hour < 12:
                formatted_time = f"{hour}:{minute:02d} AM"
            elif hour == 12:
                formatted_time = f"12:{minute:02d} PM"
            else:
                formatted_time = f"{hour - 12}:{minute:02d} PM"
                
            return formatted_time
                
        except (ValueError, TypeError) as e:
            self.logger.error(f"Error formatting time: {time_str} - {e}")
            return time_str  # Return original on error
    
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
        
        # For Quality Pharmacy, the data is already extracted in the right format
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
        Fetch details for all Quality Pharmacy locations
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        self.logger.info("Fetching all Quality Pharmacy locations...")
        
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
            
            self.logger.info(f"Successfully processed {len(all_details)} Quality Pharmacy locations")
            return all_details
            
        except Exception as e:
            self.logger.error(f"Exception when fetching all Quality Pharmacy location details: {str(e)}")
            return []
