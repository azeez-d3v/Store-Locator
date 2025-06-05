import re
import logging
from bs4 import BeautifulSoup
from ..base_handler import BasePharmacyHandler

class VitalityPharmacyHandler(BasePharmacyHandler):
    """Handler for Vitality Pharmacy stores"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "vitality_pharmacy"
        self.base_url = self.pharmacy_locations.VITALITY_PHARMACY_URL
        
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
        }
        self.logger = logging.getLogger(__name__)
        
    async def fetch_locations(self):
        """
        Fetch all Vitality Pharmacy locations.
        
        Returns:
            List of Vitality Pharmacy locations
        """
        try:
            response = await self.session_manager.get(
                url=self.base_url,
                headers=self.headers
            )
            
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch Vitality Pharmacy locations: HTTP {response.status_code}")
                return []
                
            html_content = response.text
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Fixed: Use proper class selector - look for divs with these specific classes
            location_divs = soup.find_all('div', class_=['et_pb_column', 'et_pb_column_1_4'])
            
            # Alternative approach: Use CSS selector for more precise matching
            if not location_divs:
                location_divs = soup.select('div.et_pb_column.et_pb_column_1_4')
            
            # Another fallback: Look for divs that contain pharmacy location information
            if not location_divs:
                # Look for divs that contain the contact information pattern
                location_divs = soup.find_all('div', id=re.compile(r'.*contact.*'))
                if location_divs:
                    # Get the parent containers
                    location_divs = [div.find_parent('div', class_='et_pb_column') for div in location_divs]
                    location_divs = [div for div in location_divs if div is not None]
            
            if not location_divs:
                self.logger.warning("No location divs found with any of the attempted selectors")
                # Debug: Print some of the HTML structure to understand the layout
                self.logger.debug(f"HTML structure sample: {soup.prettify()[:1000]}")
                return []
            
            locations = []
            for i, location_div in enumerate(location_divs):
                try:
                    location_data = self._extract_store_info(location_div, i)
                    if location_data:
                        locations.append(location_data)
                except Exception as e:
                    self.logger.warning(f"Error extracting store {i}: {str(e)}")
                    continue
            
            self.logger.info(f"Found {len(locations)} Vitality Pharmacy locations")
            return locations
            
        except Exception as e:
            self.logger.error(f"Exception when fetching Vitality Pharmacy locations: {str(e)}")
            return []
    
    def _extract_store_info(self, location_div, index):
        """
        Extract store information from a location div element.
        
        Args:
            location_div: BeautifulSoup element containing store information
            index: Index of the store for ID generation
            
        Returns:
            Dictionary containing store information
        """
        try:
            store_data = {
                'id': f"vitality_pharmacy_{index}",
                'brand': 'Vitality Pharmacy'
            }
            
            # Extract store name and address from the first blurb element with id containing contact
            contact_blurb = location_div.find('div', id=re.compile(r'.*contact.*'))
            if contact_blurb:
                # Extract store name from h4 header
                header = contact_blurb.find('h4', class_='et_pb_module_header')
                if header:
                    store_name = header.get_text(strip=True)
                    store_data['name'] = f"Vitality Pharmacy {store_name}"
                
                # Extract address from the description
                description = contact_blurb.find('div', class_='et_pb_blurb_description')
                if description:
                    address_text = description.get_text(strip=True)
                    # Clean up the address by replacing line breaks with commas
                    address_clean = re.sub(r'\n+', ', ', address_text).strip()
                    store_data['address'] = address_clean
                    
                    # Extract state and postcode from address
                    state_match = re.search(r'\b(VIC|NSW|QLD|SA|WA|TAS|NT|ACT)\b', address_clean)
                    if state_match:
                        store_data['state'] = state_match.group(1)
                    
                    postcode_match = re.search(r'\b(\d{4})\b', address_clean)
                    if postcode_match:
                        store_data['postcode'] = postcode_match.group(1)
            
            # Extract phone number from phone link
            phone_link = location_div.find('a', href=re.compile(r'^tel:'))
            if phone_link:
                phone_href = phone_link.get('href', '')
                # Extract phone number from tel: link, decode URL encoding
                phone_number = phone_href.replace('tel:', '').replace('%20', ' ')
                store_data['phone'] = phone_number
            
            # Extract email from email link
            email_link = location_div.find('a', href=re.compile(r'^mailto:'))
            if email_link:
                email_href = email_link.get('href', '')
                email_address = email_href.replace('mailto:', '').replace('%20', ' ')
                store_data['email'] = email_address
            
            # Extract trading hours
            trading_hours = self._extract_trading_hours(location_div)
            if trading_hours:
                store_data['trading_hours'] = trading_hours
            
            # Only return store data if we have at least a name or address
            if store_data.get('name') or store_data.get('address'):
                return store_data
            else:
                self.logger.debug(f"No valid store data found for index {index}")
                return None
            
        except Exception as e:
            self.logger.error(f"Error extracting store info: {str(e)}")
            return None
    
    def _extract_trading_hours(self, location_div):
        """
        Extract trading hours from the location div.
        
        Args:
            location_div: BeautifulSoup element containing the location information
            
        Returns:
            Dictionary with structured trading hours
        """
        try:
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
            
            # Find all blurb elements that contain trading hours
            blurb_elements = location_div.find_all('div', class_='et_pb_blurb')
            
            for blurb in blurb_elements:
                header = blurb.find('h4', class_='et_pb_module_header')
                description = blurb.find('div', class_='et_pb_blurb_description')
                
                if header and description:
                    day_text = header.get_text(strip=True).lower()
                    hours_text = description.get_text(strip=True)
                    
                    # Parse different day formats
                    if 'monday to friday' in day_text or 'monday - friday' in day_text:
                        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
                    elif 'saturday' in day_text:
                        days = ['Saturday']
                    elif 'sunday' in day_text:
                        days = ['Sunday']
                    else:
                        continue
                    
                    # Parse hours or check for closed
                    if 'closed' in hours_text.lower():
                        for day in days:
                            trading_hours[day] = {'open': 'Closed', 'closed': 'Closed'}
                    else:
                        # Extract time range (e.g., "8:30am – 5:30pm")
                        time_match = re.search(r'(\d{1,2}:\d{2}(?:am|pm))\s*[–-]\s*(\d{1,2}:\d{2}(?:am|pm))', hours_text, re.IGNORECASE)
                        if time_match:
                            open_time = self._format_time_from_string(time_match.group(1))
                            close_time = self._format_time_from_string(time_match.group(2))
                            
                            for day in days:
                                trading_hours[day] = {'open': open_time, 'closed': close_time}
            
            return trading_hours
            
        except Exception as e:
            self.logger.error(f"Error extracting trading hours: {str(e)}")
            return None
    
    def _format_time_from_string(self, time_str):
        """
        Format a time string like "8:30am" or "5:30pm" to standardized "8:30 AM" format
        
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
                # Handle format like "8:30"
                parts = time_str.split(":")
                hour = int(parts[0]) if parts[0].isdigit() else 0
                minute = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
            else:
                # Handle format like "8"
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
        
        # For Vitality Pharmacy, the data is already extracted in the right format
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
        Fetch details for all Vitality Pharmacy locations
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        self.logger.info("Fetching all Vitality Pharmacy locations...")
        
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
            
            self.logger.info(f"Successfully processed {len(all_details)} Vitality Pharmacy locations")
            return all_details
            
        except Exception as e:
            self.logger.error(f"Exception when fetching all Vitality Pharmacy location details: {str(e)}")
            return []
