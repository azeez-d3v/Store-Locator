import re
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from ..base_handler import BasePharmacyHandler
from ..utils import decode_cloudflare_email

class ChemistWorksHandler(BasePharmacyHandler):
    """Handler for Chemist Works pharmacy stores"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "chemist_works"
        self.base_url = "https://www.chemistworks.com.au"
        self.store_locator_url = self.pharmacy_locations.CHEMIST_WORKS_URL
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
        }
        self.logger = logging.getLogger(__name__)
        self.max_concurrent_requests = 5
        
    async def fetch_locations(self):
        """
        Fetch all Chemist Works pharmacy locations from the store locator page
        
        Returns:
            List of dictionaries containing basic pharmacy information with URLs
        """
        self.logger.info("Fetching Chemist Works pharmacy locations")
        
        try:
            response = await self.session_manager.get(
                url=self.store_locator_url,
                headers=self.headers
            )
            
            if response.status_code == 200:
                all_locations = self._extract_locations_from_html(response.text)
                self.logger.info(f"Found {len(all_locations)} Chemist Works pharmacy locations")
                return all_locations
            else:
                self.logger.error(f"Failed to fetch locations. Status code: {response.status_code}")
                return []
        except Exception as e:
            self.logger.error(f"Error fetching Chemist Works locations: {str(e)}")
            return []
    
    def _extract_locations_from_html(self, html_content):
        """
        Extract pharmacy locations from the contact page HTML
        
        Args:
            html_content: HTML content of the contact page
            
        Returns:
            List of pharmacy location dictionaries with complete details
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        locations = []
        
        # Find all location containers using the specific class
        location_containers = soup.find_all('div', class_='contactPageLocation-list-item')
        
        self.logger.info(f"Found {len(location_containers)} location containers")
        
        for i, container in enumerate(location_containers):
            try:
                location_data = {}
                  # Extract store name from h4 tag (remove counter span)
                name_elem = container.find('h4')
                if name_elem:
                    # Remove any span elements (like counter numbers) from the name
                    for span in name_elem.find_all('span'):
                        span.decompose()
                    location_data['name'] = name_elem.text.strip()
                else:
                    location_data['name'] = f"Chemist Works Store {i+1}"
                
                # Extract address from specific paragraph class
                address_elem = container.find('p', class_='contactPageLocation-list-item-details-address')
                if address_elem:
                    location_data['address'] = address_elem.text.strip()
                else:
                    location_data['address'] = "Address not available"
                
                # Extract phone and email from structured list items
                location_data['phone'] = None
                location_data['email'] = None
                  # Look for list items with contact details
                list_items = container.find_all('li')
                for li in list_items:
                    strong_elem = li.find('strong')
                    span_elem = li.find('span')
                    
                    if strong_elem and span_elem:
                        strong_text = strong_elem.text.strip().lower()
                        span_text = span_elem.text.strip()
                        
                        if 'phone' in strong_text:
                            location_data['phone'] = span_text
                        elif 'email' in strong_text:
                            # Check if this is a Cloudflare-protected email
                            cf_email_elem = span_elem.find('a', {'data-cfemail': True})
                            if cf_email_elem:
                                encoded_email = cf_email_elem.get('data-cfemail')
                                location_data['email'] = decode_cloudflare_email(encoded_email)
                            else:
                                location_data['email'] = span_text
                
                # Fallback: look for phone/email in any text if not found in structured format
                if not location_data['phone']:
                    phone_pattern = r'\b(?:\(\d{2}\)\s?|\d{2}\s?)\d{4}\s?\d{4}\b'
                    phone_match = re.search(phone_pattern, container.text)
                    if phone_match:
                        location_data['phone'] = phone_match.group(0).strip()                
                if not location_data['email']:
                    # First try to find Cloudflare-protected email
                    cf_email_elem = container.find('a', {'data-cfemail': True})
                    if cf_email_elem:
                        encoded_email = cf_email_elem.get('data-cfemail')
                        location_data['email'] = decode_cloudflare_email(encoded_email)
                    else:
                        # Fallback to regex pattern matching
                        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
                        email_match = re.search(email_pattern, container.text)
                        if email_match:
                            location_data['email'] = email_match.group(0).strip()
                  # Extract trading hours from structured list
                location_data['hours'] = self._extract_trading_hours(container)
                
                # Set default values for missing fields
                if not location_data.get('phone'):
                    location_data['phone'] = "Phone not available"
                if not location_data.get('email'):
                    location_data['email'] = "Email not available"
                
                # Generate unique ID
                location_data['id'] = f"chemist-works-{i+1}"
                location_data['brand'] = 'Chemist Works'
                
                # Since this is from the contact page, we don't have individual store URLs
                # The data is already complete
                location_data['url'] = self.store_locator_url
                
                locations.append(location_data)
                self.logger.debug(f"Extracted location: {location_data['name']}")
                
            except Exception as e:
                self.logger.warning(f"Error extracting location {i}: {str(e)}")
                continue
        
        return locations
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract standardized pharmacy details from the raw data
        
        Args:
            pharmacy_data: Raw pharmacy data containing HTML content and location info
            
        Returns:
            Dictionary with standardized pharmacy details
        """
        if not pharmacy_data:
            return None
            
        location = pharmacy_data.get('location', {})
        html_content = pharmacy_data.get('html_content', '')
        
        if not html_content:
            return None
        
        return self._parse_detail_page(location, html_content)
    
    async def fetch_pharmacy_details(self, location):
        """
        Fetch detailed information for a specific Chemist Works pharmacy
        
        Args:
            location: Location dictionary containing basic pharmacy information
            
        Returns:
            Dictionary with detailed pharmacy information
        """
        if not location or not location.get('url'):
            self.logger.warning("No URL provided for location")
            return None
            
        url = location['url']
        
        try:
            response = await self.session_manager.get(
                url=url,
                headers=self.headers
            )
            
            if response.status_code == 200:
                return {
                    'location': location,
                    'html_content': response.text
                }
            else:
                self.logger.warning(f"Failed to fetch details for {url}. Status code: {response.status_code}")
                return None
                
        except Exception as e:
            self.logger.error(f"Exception when fetching details for {url}: {str(e)}")
            return None
    async def fetch_all_locations_details(self):
        """
        Fetch detailed information for all Chemist Works pharmacy locations.
        Since the contact page already contains all details, we return the locations directly.
        
        Returns:
            List of detailed pharmacy information dictionaries
        """
        self.logger.info("Fetching all Chemist Works pharmacy details from contact page")
        
        # Get locations from the contact page (they already have all details)
        locations = await self.fetch_locations()
        
        if not locations:
            self.logger.warning("No locations found")
            return []
        
        detailed_locations = []
        for location in locations:
            # Convert to standardized format
            detailed_location = {
                'brand': 'Chemist Works',
                'name': location.get('name', ''),
                'store_id': location.get('id', ''),
                'address': location.get('address', ''),
                'phone': self._format_phone(location.get('phone', '')),
                'email': location.get('email', ''),
                'website': self.store_locator_url,
                'trading_hours': location.get('hours', {}),
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Parse address components
            if detailed_location['address']:
                address_components = self._parse_address(detailed_location['address'])
                detailed_location.update({
                    'street_address': address_components.get('street', ''),
                    'suburb': address_components.get('suburb', ''),
                    'state': address_components.get('state', ''),
                    'postcode': address_components.get('postcode', '')
                })
            
            # Standardize state abbreviation
            detailed_location = self._standardize_state(detailed_location)
            
            # Clean up the result - remove empty values
            cleaned_location = {k: v for k, v in detailed_location.items() if v not in (None, '', {}, [])}
            detailed_locations.append(cleaned_location)
        
        self.logger.info(f"Successfully processed {len(detailed_locations)} Chemist Works pharmacies")
        return detailed_locations
    
    def _parse_detail_page(self, location, html_content):
        """
        Parse the HTML content of a Chemist Works store detail page
        
        Args:
            location: Location dictionary with basic info
            html_content: HTML content of the pharmacy detail page
            
        Returns:
            Dictionary with pharmacy details
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Initialize variables
        store_id = location.get('id', '')
        name = location.get('name', '')
        address = ""
        street_address = ""
        suburb = ""
        state = ""
        postcode = ""
        phone = ""
        email = ""
        trading_hours = {}
        latitude = None
        longitude = None
        
        # Extract store name from page if not in location data
        if not name:
            title_elem = soup.find('h1') or soup.find('title')
            if title_elem:
                name = title_elem.text.strip()
        
        # Extract address information - try multiple selectors
        address_container = soup.find('div', class_='address') or \
                           soup.find('div', class_='contact-details') or \
                           soup.find('div', class_='store-details') or \
                           soup.find('section', class_='contact')
        
        if address_container:
            # Look for specific address elements
            address_elem = address_container.find('p', class_='address') or \
                          address_container.find('div', class_='address-text') or \
                          address_container.find('address')
            
            if address_elem:
                address = address_elem.text.strip()
                # Parse address components
                address_components = self._parse_address(address)
                street_address = address_components.get('street', '')
                suburb = address_components.get('suburb', '')
                state = address_components.get('state', '')
                postcode = address_components.get('postcode', '')
        
        # Extract contact information
        contact_section = soup.find('div', class_='contact') or \
                         soup.find('div', class_='contact-info') or \
                         soup.find('section', class_='contact-details')
        
        if contact_section:
            # Extract phone number
            phone_elem = contact_section.find('a', href=lambda x: x and x.startswith('tel:')) or \
                        contact_section.find('span', class_='phone') or \
                        contact_section.find('div', class_='phone')
            
            if phone_elem:
                phone = phone_elem.text.strip().replace('Phone:', '').replace('Tel:', '').strip()
            
            # Extract email
            email_elem = contact_section.find('a', href=lambda x: x and x.startswith('mailto:'))
            if email_elem:
                email = email_elem.text.strip()
          # Extract trading hours
        hours_section = soup.find('div', class_='hours') or \
                       soup.find('div', class_='opening-hours') or \
                       soup.find('div', class_='trading-hours') or \
                       soup.find('section', class_='hours')
        
        if hours_section:
            trading_hours = self._extract_trading_hours_legacy(hours_section)
        
        # Look for coordinates in script tags or data attributes
        latitude, longitude = self._extract_coordinates(soup)
        
        # Create the final result
        result = {
            'brand': 'Chemist Works',
            'name': name,
            'store_id': store_id,
            'address': address,
            'street_address': street_address,
            'suburb': suburb,
            'state': state,
            'postcode': postcode,
            'phone': self._format_phone(phone),
            'email': email,
            'latitude': latitude,
            'longitude': longitude,
            'website': location.get('url', ''),
            'trading_hours': trading_hours,
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Standardize state abbreviation
        result = self._standardize_state(result)
        
        # Clean up the result - remove empty values
        return {k: v for k, v in result.items() if v not in (None, '', {}, [])}
    
    def _parse_address(self, address_text):
        """
        Parse address text into components
        
        Args:
            address_text: Full address string
            
        Returns:
            Dictionary with address components
        """
        if not address_text:
            return {}
        
        # Clean up the address
        address = address_text.strip().replace('\n', ' ').replace('\r', ' ')
        address = re.sub(r'\s+', ' ', address)  # Normalize whitespace
        
        components = {}
        
        # Try to extract postcode (4 digits at the end)
        postcode_match = re.search(r'\b(\d{4})\b\s*$', address)
        if postcode_match:
            components['postcode'] = postcode_match.group(1)
            address = address[:postcode_match.start()].strip()
        
        # Try to extract state (common Australian states)
        state_pattern = r'\b(NSW|VIC|QLD|WA|SA|TAS|ACT|NT|New South Wales|Victoria|Queensland|Western Australia|South Australia|Tasmania|Australian Capital Territory|Northern Territory)\b'
        state_match = re.search(state_pattern, address, re.IGNORECASE)
        if state_match:
            components['state'] = state_match.group(1)
            address = address[:state_match.start()].strip().rstrip(',').strip()
        
        # Split remaining address into parts
        address_parts = [part.strip() for part in address.split(',') if part.strip()]
        
        if len(address_parts) >= 2:
            components['street'] = address_parts[0]
            components['suburb'] = address_parts[1]
        elif len(address_parts) == 1:
            components['street'] = address_parts[0]
        
        return components
      # Deprecated: Use the newer _extract_trading_hours method that handles contact page structure
    def _extract_trading_hours_legacy(self, hours_section):
        """
        Legacy method for extracting trading hours from individual store detail pages.
        Now redirects to the new parsing methods for consistency.
        
        Args:
            hours_section: BeautifulSoup element containing hours info
            
        Returns:
            Dictionary with trading hours in nested format
        """
        if not hours_section:
            return {}
        
        # Convert the hours section to text and parse using new methods
        hours_text = hours_section.text.strip()
        parsed_hours = self._parse_day_time_text(hours_text)
        
        if not parsed_hours:
            # Initialize default closed hours
            parsed_hours = {
                'Monday': {'open': 'Closed', 'closed': 'Closed'},
                'Tuesday': {'open': 'Closed', 'closed': 'Closed'},
                'Wednesday': {'open': 'Closed', 'closed': 'Closed'},
                'Thursday': {'open': 'Closed', 'closed': 'Closed'},
                'Friday': {'open': 'Closed', 'closed': 'Closed'},
                'Saturday': {'open': 'Closed', 'closed': 'Closed'},
                'Sunday': {'open': 'Closed', 'closed': 'Closed'},
                'Public Holiday': {'open': 'Closed', 'closed': 'Closed'}
            }
        
        return parsed_hours    # Note: The old _parse_day_hours and _parse_hours_text methods have been 
    # replaced by _parse_day_time_text and _parse_time_range for better consistency

    def _format_time(self, time_str):
        """
        Format time string consistently
        
        Args:
            time_str: Raw time string
            
        Returns:
            Formatted time string
        """
        if not time_str:
            return ''
        
        time_str = time_str.strip().lower()
        
        # Handle common time formats
        if 'am' in time_str or 'pm' in time_str:
            return time_str.upper()
        
        # Try to parse numeric time
        time_match = re.match(r'(\d{1,2})(?:\.(\d{2}))?', time_str)
        if time_match:
            hour = int(time_match.group(1))
            minute = int(time_match.group(2)) if time_match.group(2) else 0
            
            # Assume AM/PM based on hour
            if hour < 8:
                hour += 12  # Likely PM
            
            if hour == 12:
                return f"12:{minute:02d} PM"
            elif hour > 12:
                return f"{hour-12}:{minute:02d} PM"
            else:
                return f"{hour}:{minute:02d} AM"
        
        return time_str
    
    def _extract_coordinates(self, soup):
        """
        Extract latitude and longitude from the page
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Tuple of (latitude, longitude) or (None, None)
        """
        latitude = None
        longitude = None
        
        # Look for coordinates in data attributes
        map_elem = soup.find(attrs={'data-lat': True, 'data-lng': True}) or \
                  soup.find(attrs={'data-latitude': True, 'data-longitude': True})
        
        if map_elem:
            try:
                latitude = float(map_elem.get('data-lat') or map_elem.get('data-latitude'))
                longitude = float(map_elem.get('data-lng') or map_elem.get('data-longitude'))
            except (ValueError, TypeError):
                pass
        
        # Look for coordinates in script tags
        if latitude is None or longitude is None:
            script_tags = soup.find_all('script')
            for script in script_tags:
                if script.string:
                    # Look for Google Maps or other coordinate patterns
                    coord_pattern = r'lat[itude]*["\s]*[:=]\s*([+-]?\d+\.?\d*)'
                    lat_match = re.search(coord_pattern, script.string, re.IGNORECASE)
                    
                    coord_pattern = r'lng|lon[gitude]*["\s]*[:=]\s*([+-]?\d+\.?\d*)'
                    lng_match = re.search(coord_pattern, script.string, re.IGNORECASE)
                    
                    if lat_match and lng_match:
                        try:
                            latitude = float(lat_match.group(1))
                            longitude = float(lng_match.group(1))
                            break                        
                        except (ValueError, TypeError):
                            continue
        
        return latitude, longitude
    
    def _extract_trading_hours(self, container):
        """
        Extract trading hours from the structured list format.
        
        Args:
            container: BeautifulSoup element containing the location data
            
        Returns:
            Dictionary with trading hours in nested format
        """
        # Initialize default closed hours for all days
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
        
        # Day name standardization mapping
        day_mapping = {
            'mon': 'Monday', 'monday': 'Monday',
            'tue': 'Tuesday', 'tuesday': 'Tuesday', 'tues': 'Tuesday',
            'wed': 'Wednesday', 'wednesday': 'Wednesday',
            'thu': 'Thursday', 'thursday': 'Thursday', 'thur': 'Thursday', 'thurs': 'Thursday',
            'fri': 'Friday', 'friday': 'Friday',
            'sat': 'Saturday', 'saturday': 'Saturday',
            'sun': 'Sunday', 'sunday': 'Sunday',
            'public holiday': 'Public Holiday', 'public holidays': 'Public Holiday'
        }
        
        # First, try to find the structured hours list
        hours_list = container.find('ul', class_='contactPageLocation-list-item-details-list')
        
        if hours_list:
            for li in hours_list.find_all('li'):
                # Look for strong/span pattern in each li
                strong_elem = li.find('strong')
                span_elem = li.find('span')
                
                if strong_elem and span_elem:
                    day_text = strong_elem.text.strip()
                    time_text = span_elem.text.strip()
                    
                    # Normalize day name
                    day_lower = day_text.lower()
                    standard_day = day_mapping.get(day_lower)
                    
                    if standard_day:
                        # Parse the time range
                        parsed_hours = self._parse_time_range(time_text)
                        if parsed_hours:
                            trading_hours[standard_day] = parsed_hours
                elif li.text.strip():
                    # If no strong/span structure, try to parse the full text
                    li_text = li.text.strip()
                    parsed_day_hours = self._parse_day_time_text(li_text)
                    if parsed_day_hours:
                        trading_hours.update(parsed_day_hours)
          # Fallback: look for day patterns in any text within the container
        if all(hours['open'] == 'Closed' for hours in trading_hours.values()):
            for elem in container.find_all(['p', 'div', 'span', 'li']):
                elem_text = elem.text.strip()
                parsed_day_hours = self._parse_day_time_text(elem_text)
                if parsed_day_hours:
                    trading_hours.update(parsed_day_hours)
        
        return trading_hours

    def _parse_time_range(self, time_text):
        """
        Parse a time range string into open/closed format.
        
        Args:
            time_text: Time range text like "8am - 9pm" or "8:00 AM - 9:00 PM"
            
        Returns:
            Dictionary with 'open' and 'closed' keys or None if parsing fails
        """
        if not time_text or 'closed' in time_text.lower():
            return {'open': 'Closed', 'closed': 'Closed'}
        
        # Handle various time range formats
        # Pattern for formats like "8am - 9pm", "8:00am - 9:00pm", "8.00am - 9.00pm"
        time_pattern = r'(\d{1,2}(?:[:.]\d{2})?)\s*([ap]m)?\s*[-–—to]\s*(\d{1,2}(?:[:.]\d{2})?)\s*([ap]m)?'
        time_match = re.search(time_pattern, time_text.lower())
        
        if time_match:
            start_time = time_match.group(1)
            start_period = time_match.group(2) or ''
            end_time = time_match.group(3)
            end_period = time_match.group(4) or ''
            
            # Format both times
            open_time = self._format_time_12h(f"{start_time}{start_period}")
            close_time = self._format_time_12h(f"{end_time}{end_period}")
            
            if open_time and close_time:
                return {'open': open_time, 'closed': close_time}
        
        # If parsing fails, return as is but in proper format
        return {'open': time_text.strip(), 'closed': time_text.strip()}

    def _parse_day_time_text(self, text):
        """
        Parse text that might contain day and time information.
        
        Args:
            text: Text that might contain "Monday: 8am - 9pm" format
            
        Returns:
            Dictionary with day mapped to hours or empty dict if no match
        """
        if not text:
            return {}
        
        # Day name standardization mapping
        day_mapping = {
            'mon': 'Monday', 'monday': 'Monday',
            'tue': 'Tuesday', 'tuesday': 'Tuesday', 'tues': 'Tuesday',
            'wed': 'Wednesday', 'wednesday': 'Wednesday',
            'thu': 'Thursday', 'thursday': 'Thursday', 'thur': 'Thursday', 'thurs': 'Thursday',
            'fri': 'Friday', 'friday': 'Friday',
            'sat': 'Saturday', 'saturday': 'Saturday',
            'sun': 'Sunday', 'sunday': 'Sunday',
            'public holiday': 'Public Holiday', 'public holidays': 'Public Holiday'
        }
        
        result = {}
        
        # Pattern to match "Day: time" or "Day time" format
        day_time_pattern = r'\b(monday|tuesday|wednesday|thursday|friday|saturday|sunday|mon|tue|wed|thu|fri|sat|sun|public\s+holidays?)\s*:?\s*(.+?)(?=\b(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday|mon|tue|wed|thu|fri|sat|sun|public\s+holidays?)|$)'
        
        matches = re.finditer(day_time_pattern, text.lower(), re.IGNORECASE)
        
        for match in matches:
            day_text = match.group(1).strip()
            time_text = match.group(2).strip()
            
            # Normalize day name
            standard_day = day_mapping.get(day_text.lower())
            
            if standard_day and time_text:
                # Check if time_text contains actual time information
                if re.search(r'\d', time_text) and not re.search(r'phone|email|fax', time_text.lower()):
                    parsed_hours = self._parse_time_range(time_text)
                    if parsed_hours:
                        result[standard_day] = parsed_hours
        
        return result

    def _format_time_12h(self, time_str):
        """
        Format time string to 12-hour format with AM/PM.
        
        Args:
            time_str: Raw time string like "8am", "8:30pm", "8.30am"
            
        Returns:
            Formatted time string like "8:00 AM" or "8:30 PM"
        """
        if not time_str:
            return ''
        
        time_str = time_str.strip().lower()
        
        # Handle "closed" case
        if 'closed' in time_str:
            return 'Closed'
        
        # Extract time components
        # Handle formats: 8am, 8:30pm, 8.30am, 08:30, etc.
        time_pattern = r'(\d{1,2})(?:[:.:](\d{2}))?\s*(am|pm)?'
        match = re.search(time_pattern, time_str)
        
        if not match:
            return time_str.title()  # Return as-is if we can't parse it
        
        hour = int(match.group(1))
        minute = int(match.group(2)) if match.group(2) else 0
        period = match.group(3)
        
        # If no AM/PM specified, make educated guess
        if not period:
            if hour < 8:  # Likely PM for closing hours
                period = 'pm'
            elif hour >= 12:
                period = 'pm'
            else:
                period = 'am'
        
        # Handle 24-hour format conversion
        if period == 'pm' and hour != 12:
            hour += 12
        elif period == 'am' and hour == 12:
            hour = 0
        
        # Convert back to 12-hour format for display
        display_hour = hour
        display_period = 'AM'
        
        if hour == 0:
            display_hour = 12
            display_period = 'AM'
        elif hour == 12:
            display_hour = 12
            display_period = 'PM'
        elif hour > 12:
            display_hour = hour - 12
            display_period = 'PM'
        
        return f"{display_hour}:{minute:02d} {display_period}"

    def _format_phone(self, phone):
        """
        Format phone number consistently
        
        Args:
            phone: Raw phone number string
            
        Returns:
            Formatted phone number
        """
        if not phone:
            return ''
        
        # Remove common prefixes and clean up
        phone = phone.replace('Phone:', '').replace('Tel:', '').replace('T:', '').strip()
        
        # Remove non-digit characters except + and spaces
        phone = re.sub(r'[^\d\s+()-]', '', phone)
        
        # Basic formatting for Australian numbers
        phone = re.sub(r'\s+', ' ', phone).strip()
        
        return phone
    
    def _standardize_state(self, result):
        """
        Convert full state names to standard abbreviations
        
        Args:
            result: Dictionary containing pharmacy details
            
        Returns:
            Updated dictionary with standardized state
        """
        if not result.get('state'):
            return result
        
        state_mapping = {
            'NEW SOUTH WALES': 'NSW',
            'VICTORIA': 'VIC',
            'QUEENSLAND': 'QLD',
            'WESTERN AUSTRALIA': 'WA',
            'SOUTH AUSTRALIA': 'SA',
            'TASMANIA': 'TAS',
            'AUSTRALIAN CAPITAL TERRITORY': 'ACT',
            'NORTHERN TERRITORY': 'NT'
        }
        
        state_upper = result['state'].upper()
        if state_upper in state_mapping:
            result['state'] = state_mapping[state_upper]
        
        return result