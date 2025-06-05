import re
import logging
import urllib.parse
from bs4 import BeautifulSoup
from ..base_handler import BasePharmacyHandler

class WholelifePharmacyHandler(BasePharmacyHandler):
    """Handler for Wholelife Pharmacy & Healthfoods stores"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "wholelife_pharmacy"
        self.base_url = self.pharmacy_locations.WHOLELIFE_PHARMACY_URL
        
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
        }
        self.logger = logging.getLogger(__name__)
        
    async def fetch_locations(self):
        """
        Fetch all Wholelife Pharmacy & Healthfoods locations.
        
        Returns:
            List of Wholelife Pharmacy locations
        """
        try:
            response = await self.session_manager.get(
                url=self.base_url,
                headers=self.headers
            )
            
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch Wholelife Pharmacy locations: HTTP {response.status_code}")
                return []
                
            html_content = response.text
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find all directory-store divs
            store_divs = soup.find_all('div', class_='directory-store')
            
            if not store_divs:
                self.logger.warning("No store divs found with class 'directory-store'")
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
            
            self.logger.info(f"Found {len(locations)} Wholelife Pharmacy locations")
            return locations
            
        except Exception as e:
            self.logger.error(f"Exception when fetching Wholelife Pharmacy locations: {str(e)}")
            return []
    
    def _extract_store_info(self, store_div, index):
        """
        Extract store information from a directory-store div element.
        
        Args:
            store_div: BeautifulSoup element containing store information
            index: Index of the store for ID generation
            
        Returns:
            Dictionary containing store information
        """
        try:
            store_data = {
                'id': f"wholelife_pharmacy_{index}",
                'brand': 'Wholelife Pharmacy & Healthfoods'
            }
            
            # Extract store name from h3 tag
            h3_element = store_div.find('h3')
            if h3_element:
                store_name = h3_element.get_text(strip=True)
                store_data['name'] = f"Wholelife Pharmacy & Healthfoods {store_name}"
            
            # Get all text content from the store div to parse it properly
            all_text = store_div.get_text(separator='\n', strip=True)
            
            # Extract phone number - multiple patterns to handle different formats
            phone_number = self._extract_phone_number(store_div, all_text)
            if phone_number:
                store_data['phone'] = phone_number
            
            # Extract email
            email_address = self._extract_email(store_div, all_text)
            if email_address:
                store_data['email'] = email_address
            
            # Extract address - this needs to be done carefully to avoid including phone/email
            address = self._extract_address(store_div, all_text)
            if address:
                store_data['address'] = address
                
                # Extract state and postcode from address
                state_match = re.search(r'\b(VIC|NSW|QLD|SA|WA|TAS|NT|ACT)\b', address, re.IGNORECASE)
                if state_match:
                    store_data['state'] = state_match.group(1).upper()
                
                postcode_match = re.search(r'\b(\d{4})\b', address)
                if postcode_match:
                    store_data['postcode'] = postcode_match.group(1)
            
            # Extract trading hours from table
            trading_hours = self._extract_trading_hours(store_div)
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
    
    def _extract_phone_number(self, store_div, all_text):
        """Extract phone number from various sources."""
        try:
            # First try to find phone link
            phone_link = store_div.find('a', href=re.compile(r'^tel:'))
            if phone_link:
                phone_href = phone_link.get('href', '')
                # Properly decode URL encoding
                phone_number = urllib.parse.unquote(phone_href.replace('tel:', ''))
                return phone_number.strip()
            
            # If no phone link, try to extract from text using multiple patterns
            phone_patterns = [
                r'\((\d{2})\)\s*(\d{4})\s*(\d{4})',  # (07) 4091 1027
                r'(\d{2})\s*(\d{4})\s*(\d{4})',      # 07 4091 1027
                r'Ph:\s*(\d{2})\s*(\d{4})\s*(\d{4})', # Ph: 07 5462 3333
                r'Phone:\s*(\d{2})\s*(\d{4})\s*(\d{4})', # Phone: 07 5462 3333
                r'\b(\d{2})\s*(\d{4})\s*(\d{4})\b',   # General pattern
            ]
            
            for pattern in phone_patterns:
                match = re.search(pattern, all_text)
                if match:
                    if len(match.groups()) == 3:
                        return f"({match.group(1)}) {match.group(2)} {match.group(3)}"
                    else:
                        return match.group(0).strip()
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error extracting phone number: {e}")
            return None
    
    def _extract_email(self, store_div, all_text):
        """Extract email address from various sources."""
        try:
            # First try to find email link
            email_link = store_div.find('a', href=re.compile(r'^mailto:'))
            if email_link:
                email_href = email_link.get('href', '')
                # Properly decode URL encoding and clean up
                email_address = urllib.parse.unquote(email_href.replace('mailto:', ''))
                # Remove any trailing spaces or unwanted characters
                email_address = email_address.strip().rstrip(' \t\n\r')
                return email_address
            
            # If no email link, try to extract from text
            email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
            email_match = re.search(email_pattern, all_text)
            if email_match:
                email_address = email_match.group(0)
                # Clean up any URL encoding that might be in the text
                email_address = urllib.parse.unquote(email_address)
                return email_address.strip()
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error extracting email: {e}")
            return None
    
    def _extract_address(self, store_div, all_text):
        """Extract address from the store div, being careful to exclude phone and email."""
        try:
            # Get all p elements and filter out the ones with contact info
            p_elements = store_div.find_all('p')
            address_parts = []
            
            for p in p_elements:
                p_text = p.get_text(strip=True)
                
                # Skip empty paragraphs
                if not p_text or p_text in ['&nbsp;', ' ']:
                    continue
                
                # Skip paragraphs that contain links to store pages
                if 'Visit store page' in p_text:
                    continue
                
                # Skip paragraphs that only contain phone numbers or emails
                if re.match(r'^\(?[\d\s\-\(\)]+$', p_text):  # Only phone number
                    continue
                if re.match(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$', p_text):  # Only email
                    continue
                if p_text.startswith('Ph:') or p_text.startswith('Phone:'):
                    continue
                
                # Check if this paragraph contains mixed content (address + contact info)
                # For example: "G011/200 Boat Harbour Dr,\nPialba QLD 4655\n(07) 4128 1680\nherveybay@wholelife.com.au"
                if p.find('a', href=re.compile(r'^(tel:|mailto:)')):
                    # This paragraph contains contact links, extract only the address part
                    # Split by <br> tags and take only address-like lines
                    lines = []
                    for content in p.contents:
                        if hasattr(content, 'get_text'):
                            text = content.get_text(strip=True)
                        else:
                            text = str(content).strip()
                        
                        if text and text not in ['', '\n']:
                            # Check if this line looks like an address (contains letters and possibly numbers)
                            if re.search(r'[A-Za-z]', text) and not re.match(r'^\(?[\d\s\-\(\)]+$', text):
                                # Not just a phone number
                                if not re.match(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$', text):
                                    # Not just an email
                                    lines.append(text)
                    
                    if lines:
                        # Join the address lines
                        address_text = ' '.join(lines)
                        # Clean up the address by removing phone numbers and emails
                        address_text = re.sub(r'\(?\d{2}\)?\s*\d{4}\s*\d{4}', '', address_text)
                        address_text = re.sub(r'Ph:\s*\d{2}\s*\d{4}\s*\d{4}', '', address_text)
                        address_text = re.sub(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}', '', address_text)
                        address_text = address_text.strip()
                        if address_text:
                            address_parts.append(address_text)
                else:
                    # This paragraph doesn't contain contact links, likely pure address
                    # But still check if it's not just a phone number in text form
                    if not re.match(r'^\(?[\d\s\-\(\)Ph:]+$', p_text):
                        address_parts.append(p_text)
            
            if address_parts:
                # Join address parts and clean up
                full_address = ', '.join(address_parts)
                # Clean up multiple commas and spaces
                full_address = re.sub(r',+', ',', full_address)  # Remove multiple commas
                full_address = re.sub(r',\s*,', ',', full_address)  # Remove comma-space-comma
                full_address = re.sub(r'^\s*,\s*|\s*,\s*$', '', full_address)  # Remove leading/trailing commas
                full_address = re.sub(r'\s+', ' ', full_address)  # Normalize spaces
                
                return full_address.strip()
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error extracting address: {e}")
            return None
    
    def _extract_trading_hours(self, store_div):
        """
        Extract trading hours from the table in the store div.
        
        Args:
            store_div: BeautifulSoup element containing the store information
            
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
            
            # Find the table containing trading hours
            table = store_div.find('table')
            if not table:
                return trading_hours
            
            # Find all rows in the table
            rows = table.find_all('tr')
            
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 2:
                    day_text = cells[0].get_text(strip=True)
                    hours_text = cells[1].get_text(strip=True)
                    
                    # Normalize day name
                    day_text = day_text.strip().capitalize()
                    
                    # Check if it's a valid day
                    if day_text in trading_hours:
                        # Parse hours or check for closed
                        if 'closed' in hours_text.lower():
                            trading_hours[day_text] = {'open': 'Closed', 'closed': 'Closed'}
                        else:
                            # Extract time range (e.g., "8:00am – 6:00pm")
                            time_match = re.search(r'(\d{1,2}:\d{2}(?:am|pm))\s*[–-]\s*(\d{1,2}:\d{2}(?:am|pm))', hours_text, re.IGNORECASE)
                            if time_match:
                                open_time = self._format_time_from_string(time_match.group(1))
                                close_time = self._format_time_from_string(time_match.group(2))
                                trading_hours[day_text] = {'open': open_time, 'closed': close_time}
            
            return trading_hours
            
        except Exception as e:
            self.logger.error(f"Error extracting trading hours: {e}")
            return None
    
    def _format_time_from_string(self, time_str):
        """
        Format a time string like "8:00am" or "6:00pm" to standardized "8:00 AM" format
        
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
                # Handle format like "8:00"
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
        
        # For Wholelife Pharmacy, the data is already extracted in the right format
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
        Fetch details for all Wholelife Pharmacy & Healthfoods locations
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        self.logger.info("Fetching all Wholelife Pharmacy & Healthfoods locations...")
        
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
            
            self.logger.info(f"Successfully processed {len(all_details)} Wholelife Pharmacy & Healthfoods locations")
            return all_details
            
        except Exception as e:
            self.logger.error(f"Exception when fetching all Wholelife Pharmacy & Healthfoods location details: {str(e)}")
            return []
