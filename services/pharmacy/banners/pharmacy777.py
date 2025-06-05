import re
import logging
from bs4 import BeautifulSoup
from ..base_handler import BasePharmacyHandler

class Pharmacy777Handler(BasePharmacyHandler):
    """Handler for Pharmacy 777 stores"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "pharmacy777"
        self.base_url = "https://www.pharmacy777.com.au/find-a-pharmacy"
        
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
        }
        self.logger = logging.getLogger(__name__)
        
    async def fetch_locations(self):
        """
        Fetch all Pharmacy 777 locations.
        
        Returns:
            List of Pharmacy 777 locations
        """
        try:
            response = await self.session_manager.get(
                url=self.base_url,
                headers=self.headers
            )
            
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch Pharmacy 777 locations: HTTP {response.status_code}")
                return []
                
            html_content = response.text
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find all store divs
            store_divs = soup.find_all('div', class_='store')
            
            if not store_divs:
                self.logger.warning("No store divs found with class 'store'")
                return []
            
            locations = []
            for i, store_div in enumerate(store_divs):
                try:
                    location_data = self._extract_store_info(store_div, i)
                    if location_data:
                        locations.append(location_data)
                except Exception as e:
                    self.logger.warning(f"Error extracting store {i}: {str(e)}")
                    continue
            
            self.logger.info(f"Found {len(locations)} Pharmacy 777 locations")
            return locations
            
        except Exception as e:
            self.logger.error(f"Exception when fetching Pharmacy 777 locations: {str(e)}")
            return []
    
    def _extract_coordinates_from_url(self, url):
        """
        Extract latitude and longitude from various Google Maps URL formats.
        
        Args:
            url: Google Maps URL string
            
        Returns:
            Tuple of (latitude, longitude) or (None, None) if not found
        """
        if not url:
            return None, None
            
        # Multiple patterns to try for different Google Maps URL formats
        patterns = [
            # Pattern 1: daddr=@lat,lng (your current format)
            r'daddr=@(-?\d+\.?\d*),(-?\d+\.?\d*)',
            # Pattern 2: ll=lat,lng
            r'[?&]ll=(-?\d+\.?\d*),(-?\d+\.?\d*)',
            # Pattern 3: @lat,lng,zoom
            r'@(-?\d+\.?\d*),(-?\d+\.?\d*),\d+',
            # Pattern 4: destination=lat,lng
            r'destination=(-?\d+\.?\d*),(-?\d+\.?\d*)',
            # Pattern 5: center=lat,lng
            r'center=(-?\d+\.?\d*),(-?\d+\.?\d*)',
            # Pattern 6: q=lat,lng
            r'[?&]q=(-?\d+\.?\d*),(-?\d+\.?\d*)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                try:
                    lat = float(match.group(1))
                    lng = float(match.group(2))
                    # Basic validation for Australian coordinates
                    if -45 <= lat <= -10 and 110 <= lng <= 155:
                        self.logger.info(f"Extracted coordinates using pattern '{pattern}': {lat}, {lng}")
                        return lat, lng
                    else:
                        self.logger.warning(f"Coordinates outside Australia bounds: {lat}, {lng}")
                except (ValueError, IndexError) as e:
                    self.logger.error(f"Error converting coordinates to float: {e}")
                    continue
        
        self.logger.warning(f"No coordinates found in URL: {url}")
        return None, None
    
    def _extract_store_info(self, store_div, index):
        """
        Extract store information from a store div element.
        
        Args:
            store_div: BeautifulSoup element containing store information
            index: Index of the store for ID generation
            
        Returns:
            Dictionary containing store information
        """
        try:
            store_data = {
                'id': f"pharmacy777_{index}",
                'brand': 'Pharmacy 777'
            }
            
            # Extract name from title div
            title_div = store_div.find('div', class_='title')
            if title_div:
                link = title_div.find('a')
                if link:
                    store_data['name'] = link.get_text(strip=True)
                    store_data['url'] = f"https://www.pharmacy777.com.au{link.get('href', '')}"
            
            # Extract info from info divs
            info_divs = store_div.find_all('div', class_='info')
            
            for info_div in info_divs:
                info_text = info_div.get_text(strip=True)
                
                # Extract address (first info div without specific labels)
                if not any(label in info_text for label in ['Phone:', 'Fax:', 'Email:', 'Opening Hours:']):
                    if 'address' not in store_data:
                        store_data['address'] = info_text
                
                # Extract phone
                elif info_text.startswith('Phone:'):
                    phone = info_text.replace('Phone:', '').strip()
                    store_data['phone'] = phone
                
                # Extract fax
                elif info_text.startswith('Fax:'):
                    fax = info_text.replace('Fax:', '').strip()
                    store_data['fax'] = fax
                    
                # Extract email
                elif info_text.startswith('Email:'):
                    email_link = info_div.find('a')
                    if email_link and email_link.get('href', '').startswith('mailto:'):
                        store_data['email'] = email_link.get('href').replace('mailto:', '')
                
                # Extract trading hours
                elif info_text.startswith('Opening Hours:'):
                    hours = info_text.replace('Opening Hours:', '').strip()
                    store_data['trading_hours'] = self._parse_trading_hours(hours)
            
            # Extract coordinates from directions link (search in all info divs)
            coordinates_found = False
            for info_div in info_divs:
                if coordinates_found:
                    break
                    
                # Look for any link that might contain directions
                all_links = info_div.find_all('a')
                for link in all_links:
                    href = link.get('href', '')
                    if any(domain in href for domain in ['maps.google.com', 'google.com/maps', 'goo.gl']):
                        self.logger.info(f"Found directions URL: {href}")
                        
                        lat, lng = self._extract_coordinates_from_url(href)
                        if lat is not None and lng is not None:
                            store_data['latitude'] = lat
                            store_data['longitude'] = lng
                            coordinates_found = True
                            self.logger.info(f"Successfully extracted coordinates: {lat}, {lng}")
                            break
                        else:
                            self.logger.warning(f"Could not extract coordinates from URL: {href}")
            
            # Log if coordinates were not found
            if not coordinates_found:
                self.logger.warning(f"No coordinates found for store: {store_data.get('name', 'Unknown')}")
            
            return store_data
            
        except Exception as e:
            self.logger.error(f"Error extracting store info: {str(e)}")
            return None
    
    def _parse_trading_hours(self, hours_data):
        """
        Parse trading hours from Pharmacy 777 format to structured format.
        Format examples: 
        - "MON TO FRI 8.30AM – 6PM, SAT 9AM – 12PM"
        - "EVERY DAY 8AM – 10PM"
        - "MON TO FRI 8AM - 7PM, SAT 8AM - 4PM, SUN 9AM - 1PM"
        
        Args:
            hours_data: Opening hours string from Pharmacy 777
            
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
        
        if not hours_data or not isinstance(hours_data, str):
            return trading_hours
            
        try:
            # Clean up the hours string
            hours_data = hours_data.strip().upper()
            
            # Handle "EVERY DAY" format
            if "EVERY DAY" in hours_data:
                # Extract time range
                time_match = re.search(r'(\d+(?:\.\d+)?AM?)\s*[–-]\s*(\d+(?:\.\d+)?PM?)', hours_data)
                if time_match:
                    open_time = self._format_time_from_string(time_match.group(1))
                    close_time = self._format_time_from_string(time_match.group(2))
                    
                    for day in trading_hours.keys():
                        trading_hours[day] = {'open': open_time, 'closed': close_time}
                
                return trading_hours
            
            # Split by comma to handle multiple day ranges
            segments = [seg.strip() for seg in hours_data.split(',')]
            
            for segment in segments:
                if not segment:
                    continue
                    
                # Handle different day range formats
                if "MON TO FRI" in segment or "MON-FRI" in segment:
                    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
                elif "SAT" in segment and "SUN" not in segment:
                    days = ['Saturday']
                elif "SUN" in segment:
                    days = ['Sunday']
                elif "MON" in segment and "TO" not in segment and "FRI" not in segment:
                    days = ['Monday']
                elif "TUE" in segment:
                    days = ['Tuesday']
                elif "WED" in segment:
                    days = ['Wednesday']
                elif "THU" in segment:
                    days = ['Thursday']
                elif "FRI" in segment and "TO" not in segment:
                    days = ['Friday']
                else:
                    continue
                
                # Extract time range from this segment
                time_match = re.search(r'(\d+(?:\.\d+)?AM?)\s*[–-]\s*(\d+(?:\.\d+)?PM?)', segment)
                if time_match:
                    open_time = self._format_time_from_string(time_match.group(1))
                    close_time = self._format_time_from_string(time_match.group(2))
                    
                    for day in days:
                        trading_hours[day] = {'open': open_time, 'closed': close_time}
                else:
                    # Check for "CLOSED" keyword
                    if "CLOSED" in segment:
                        for day in days:
                            trading_hours[day] = {'open': 'Closed', 'closed': 'Closed'}
                            
        except Exception as e:
            self.logger.error(f"Error parsing trading hours '{hours_data}': {e}")
            
        return trading_hours
    
    def _format_time_from_string(self, time_str):
        """
        Format a time string like "8.30AM" or "6PM" to standardized "8:30 AM" format
        
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
            if "NOON" in time_str:
                return "12:00 PM"
            
            # Extract AM/PM indicator
            is_pm = "PM" in time_str
            is_am = "AM" in time_str
            
            # Remove AM/PM suffix
            time_str = time_str.replace("AM", "").replace("PM", "").strip()
            
            # Parse hours and minutes
            hour = 0
            minute = 0
            
            if "." in time_str:
                # Handle format like "8.30"
                parts = time_str.split(".")
                hour = int(parts[0]) if parts[0].isdigit() else 0
                minute = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
            elif ":" in time_str:
                # Handle format like "8:30"
                parts = time_str.split(":")
                hour = int(parts[0]) if parts[0].isdigit() else 0
                minute = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
            else:
                # Handle format like "8"
                hour = int(time_str) if time_str.isdigit() else 0
                minute = 0
            
            # Apply AM/PM conversion
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
        
        # For Pharmacy 777, the data is already extracted in the right format
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
        Fetch details for all Pharmacy 777 locations
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        self.logger.info("Fetching all Pharmacy 777 locations...")
        
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
            
            self.logger.info(f"Successfully processed {len(all_details)} Pharmacy 777 locations")
            return all_details
            
        except Exception as e:
            self.logger.error(f"Exception when fetching all Pharmacy 777 location details: {str(e)}")
            return []
