import re
import logging
from rich import print
from bs4 import BeautifulSoup
from datetime import datetime
from ..base_handler import BasePharmacyHandler

class CaremoreHandler(BasePharmacyHandler):
    """Handler for Caremore Pharmacy stores"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "caremore"
        self.base_url = self.pharmacy_locations.CAREMORE_URL
        
        # Define brand-specific headers for requests
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
            'referer': 'https://caremore.com.au/',
            'origin': 'https://caremore.com.au'
        }
        self.logger = logging.getLogger(__name__)

    async def fetch_locations(self):
        """
        Fetch all Caremore pharmacy locations from the store locator page.
        
        Returns:
            List of Caremore pharmacy locations
        """
        try:
            response = await self.session_manager.get(
                url=self.base_url,
                headers=self.headers
            )
            
            if response.status_code == 200:
                html_content = response.text
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Find the sidebar container first
                sidebar_container = soup.find('div', class_='str-lctr-sidebar')
                
                if not sidebar_container:
                    self.logger.warning("No sidebar container found with class 'str-lctr-sidebar'")
                    return []
                
                # Find all individual location divs within the sidebar
                location_divs = sidebar_container.find_all('div', class_='location')
                
                if not location_divs:
                    self.logger.warning("No location divs found with class 'location'")
                    return []
                
                locations = []
                for i, div in enumerate(location_divs):
                    try:
                        location_data = self._extract_location_data(div, i+1)
                        if location_data:
                            locations.append(location_data)
                    except Exception as e:
                        self.logger.error(f"Error parsing location div {i+1}: {str(e)}")
                        continue
                
                self.logger.info(f"Found {len(locations)} Caremore locations")
                return locations
            else:
                self.logger.error(f"Failed to fetch Caremore locations: HTTP {response.status_code}")
                return []
                
        except Exception as e:
            self.logger.error(f"Exception when fetching Caremore locations: {str(e)}")
            return []
            
    def _extract_location_data(self, location_div, index):
        """
        Extract pharmacy data from a location div element.
        
        Args:
            location_div: BeautifulSoup div element containing location data
            index: Index for generating unique IDs
            
        Returns:
            Dictionary with pharmacy location data
        """
        try:
            location_data = {}
            
            # Extract pharmacy name from h4 > a element
            name_element = location_div.find('h4')
            if name_element:
                # Look for anchor tag inside h4
                anchor_tag = name_element.find('a', class_='marker-button')
                if anchor_tag:
                    location_data['name'] = anchor_tag.get_text(strip=True)
                else:
                    location_data['name'] = name_element.get_text(strip=True)
            
            # Extract address components from the paragraph elements
            address_parts = []
            paragraphs = location_div.find_all('p')
            
            for p in paragraphs:
                text = p.get_text(strip=True)
                # Skip paragraphs that contain phone, email, or fax info
                if (text and 
                    not text.startswith('Ph:') and 
                    not text.startswith('Phone:') and
                    not text.startswith('Email:') and
                    not text.startswith('Fax:')):
                    address_parts.append(text)
            
            # Combine address parts
            if address_parts:
                location_data['address'] = ', '.join(address_parts)
            
            # Extract phone number
            phone_link = location_div.find('a', href=lambda x: x and x.startswith('tel:'))
            if phone_link:
                location_data['phone'] = phone_link.get_text(strip=True)
            else:
                # Look for phone in paragraph text
                for p in paragraphs:
                    text = p.get_text(strip=True)
                    if text.startswith('Ph:'):
                        phone_text = text.replace('Ph:', '').strip()
                        location_data['phone'] = phone_text
                        break
            
            # Extract email
            email_link = location_div.find('a', href=lambda x: x and x.startswith('mailto:'))
            if email_link:
                email = email_link.get('href').replace('mailto:', '').strip()
                location_data['email'] = email
            else:
                # Look for email in paragraph text
                for p in paragraphs:
                    text = p.get_text(strip=True)
                    if text.startswith('Email:'):
                        email_text = text.replace('Email:', '').strip()
                        location_data['email'] = email_text
                        break
            
            # Extract trading hours from the details section
            trading_hours = self._extract_trading_hours(location_div)
            if trading_hours:
                location_data['trading_hours'] = trading_hours
            
            # Parse address components
            if 'address' in location_data:
                address_components = self._parse_address(location_data['address'])
                location_data.update(address_components)
            
            # Add additional metadata
            location_data['brand'] = 'Caremore'
            location_data['id'] = f"caremore_{index}"
            location_data['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            return location_data
            
        except Exception as e:
            self.logger.error(f"Error extracting location data: {str(e)}")
            return None
    def _extract_trading_hours(self, location_div):
        """
        Extract trading hours from the location div.
        
        Args:
            location_div: BeautifulSoup div element containing location data
            
        Returns:
            Dictionary with trading hours by day
        """
        trading_hours = {}
        
        # Look for ul.hours element within the details div
        details_div = location_div.find('div', class_='details')
        if details_div:
            hours_list = details_div.find('ul', class_='hours')
            if hours_list:
                for li in hours_list.find_all('li'):
                    # Extract day and time from separate span elements
                    spans = li.find_all('span')
                    if len(spans) >= 2:
                        day = spans[0].get_text(strip=True)
                        hours = spans[1].get_text(strip=True)
                        
                        # Normalize day names
                        day_mapping = {
                            'Mon': 'Monday', 'Tue': 'Tuesday', 'Wed': 'Wednesday',
                            'Thu': 'Thursday', 'Fri': 'Friday', 'Sat': 'Saturday', 'Sun': 'Sunday'
                        }
                        full_day = day_mapping.get(day, day)
                        
                        # Parse hours
                        if hours.lower() == 'closed':
                            trading_hours[full_day] = {'open': 'Closed', 'close': 'Closed'}
                        elif '–' in hours or '-' in hours:
                            # Handle both em dash (–) and regular dash (-)
                            separator = '–' if '–' in hours else '-'
                            time_parts = hours.split(separator)
                            if len(time_parts) == 2:
                                open_time = time_parts[0].strip()
                                close_time = time_parts[1].strip()
                                trading_hours[full_day] = {'open': open_time, 'close': close_time}
                        else:
                            # Single time format or other format
                            trading_hours[full_day] = {'open': hours, 'close': hours}
                    else:
                        # Fallback: try to parse from combined text
                        text = li.get_text(strip=True)
                        # Parse day and hours (e.g., "Monday 8:30 AM – 6:00 PM")
                        parts = text.split(None, 1)  # Split on whitespace, max 2 parts
                        if len(parts) == 2:
                            day = parts[0]
                            hours = parts[1]
                            
                            # Normalize day names
                            day_mapping = {
                                'Mon': 'Monday', 'Tue': 'Tuesday', 'Wed': 'Wednesday',
                                'Thu': 'Thursday', 'Fri': 'Friday', 'Sat': 'Saturday', 'Sun': 'Sunday'
                            }
                            full_day = day_mapping.get(day, day)
                            
                            # Parse hours
                            if hours.lower() == 'closed':
                                trading_hours[full_day] = {'open': 'Closed', 'close': 'Closed'}
                            elif '–' in hours or '-' in hours:
                                separator = '–' if '–' in hours else '-'
                                time_parts = hours.split(separator)
                                if len(time_parts) == 2:
                                    open_time = time_parts[0].strip()
                                    close_time = time_parts[1].strip()
                                    trading_hours[full_day] = {'open': open_time, 'close': close_time}
        else:
            # Fallback: look for ul.hours directly in location_div
            hours_list = location_div.find('ul', class_='hours')
            if hours_list:
                for li in hours_list.find_all('li'):
                    # Extract day and time from separate span elements
                    spans = li.find_all('span')
                    if len(spans) >= 2:
                        day = spans[0].get_text(strip=True)
                        hours = spans[1].get_text(strip=True)
                        
                        # Normalize day names
                        day_mapping = {
                            'Mon': 'Monday', 'Tue': 'Tuesday', 'Wed': 'Wednesday',
                            'Thu': 'Thursday', 'Fri': 'Friday', 'Sat': 'Saturday', 'Sun': 'Sunday'
                        }
                        full_day = day_mapping.get(day, day)
                        
                        # Parse hours
                        if hours.lower() == 'closed':
                            trading_hours[full_day] = {'open': 'Closed', 'close': 'Closed'}
                        elif '–' in hours or '-' in hours:
                            separator = '–' if '–' in hours else '-'
                            time_parts = hours.split(separator)
                            if len(time_parts) == 2:
                                open_time = time_parts[0].strip()
                                close_time = time_parts[1].strip()
                                trading_hours[full_day] = {'open': open_time, 'close': close_time}
                        else:
                            trading_hours[full_day] = {'open': hours, 'close': hours}
        
        return trading_hours
    
    def _parse_address(self, address):
        """
        Parse address string into components.
        
        Args:
            address: Full address string
            
        Returns:
            Dictionary with address components
        """
        components = {
            'street_address': '',
            'suburb': '',
            'state': '',
            'postcode': ''
        }
        
        if not address:
            return components
        
        # Split address by commas
        parts = [part.strip() for part in address.split(',')]
        
        if len(parts) >= 1:
            components['street_address'] = parts[0]
        
        if len(parts) >= 2:
            components['suburb'] = parts[1]
        
        # Look for state and postcode in the last parts
        for part in reversed(parts):
            # Check for postcode (4 digits)
            postcode_match = re.search(r'\b(\d{4})\b', part)
            if postcode_match and not components['postcode']:
                components['postcode'] = postcode_match.group(1)
            
            # Check for Australian state codes
            state_match = re.search(r'\b(NSW|VIC|QLD|WA|SA|TAS|ACT|NT)\b', part.upper())
            if state_match and not components['state']:
                components['state'] = state_match.group(1)
        
        return components
    
    async def fetch_pharmacy_details(self, location):
        """
        Fetch detailed information for a specific pharmacy.
        Since Caremore uses HTML parsing, the details are already in the location data.
        
        Args:
            location: Location dictionary from fetch_locations
            
        Returns:
            Detailed pharmacy data
        """
        # For HTML parsing banners, all details are usually already available
        # Just return the location data processed through extract_pharmacy_details
        return self.extract_pharmacy_details(location)
    
    async def fetch_all_locations_details(self):
        """
        Fetch details for all Caremore locations.
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        try:
            # Get all locations first
            locations = await self.fetch_locations()
            
            if not locations:
                self.logger.warning("No Caremore locations found")
                return []
            
            # Process each location to extract standardized details
            detailed_locations = []
            for location in locations:
                try:
                    details = self.extract_pharmacy_details(location)
                    if details:
                        detailed_locations.append(details)
                except Exception as e:
                    self.logger.error(f"Error processing location {location.get('name', 'Unknown')}: {str(e)}")
                    continue
            
            self.logger.info(f"Processed {len(detailed_locations)} Caremore locations")
            return detailed_locations
            
        except Exception as e:
            self.logger.error(f"Exception when fetching all Caremore locations details: {str(e)}")
            return []
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract and standardize pharmacy details.
        
        Args:
            pharmacy_data: Raw pharmacy data from location parsing
            
        Returns:
            Dictionary with standardized pharmacy details
        """
        if not pharmacy_data:
            return None
        
        # Get trading hours and format them properly
        trading_hours = pharmacy_data.get('trading_hours', {})
        
        # Convert trading hours to the expected format for consistency with other banners
        working_hours = {}
        if trading_hours:
            for day, hours in trading_hours.items():
                if isinstance(hours, dict):
                    if hours.get('open') == 'Closed' or hours.get('close') == 'Closed':
                        working_hours[day] = 'Closed'
                    else:
                        open_time = hours.get('open', '')
                        close_time = hours.get('close', '')
                        if open_time and close_time:
                            working_hours[day] = f"{open_time} - {close_time}"
                        else:
                            working_hours[day] = open_time or close_time
                else:
                    working_hours[day] = str(hours)
          # Standardize the data format to match the field mapping in core.py
        result = {
            'name': pharmacy_data.get('name', ''),
            'brand': 'Caremore',
            'address': pharmacy_data.get('address', ''),
            'street_address': pharmacy_data.get('street_address', ''),
            'suburb': pharmacy_data.get('suburb', ''),
            'state': pharmacy_data.get('state', ''),
            'postcode': pharmacy_data.get('postcode', ''),
            'phone': self._format_phone(pharmacy_data.get('phone', '')),
            'email': pharmacy_data.get('email', ''),
            'fax': pharmacy_data.get('fax', ''),
            'trading_hours': working_hours if working_hours else {},
            'latitude': pharmacy_data.get('latitude'),
            'longitude': pharmacy_data.get('longitude'),
            'website': 'https://caremore.com.au/',
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Clean up the result by removing None values and empty strings
        cleaned_result = {}
        for key, value in result.items():
            if value is not None and value != '' and value != {}:
                cleaned_result[key] = value
        
        return cleaned_result
    
    def _format_phone(self, phone):
        """
        Format phone number consistently.
        
        Args:
            phone: Raw phone number string
            
        Returns:
            Formatted phone number
        """
        if not phone:
            return ''
        
        # Remove common prefixes and clean up
        phone = phone.replace('Phone:', '').replace('Ph:', '').strip()
        
        # Remove extra whitespace and standardize format
        phone = re.sub(r'\s+', ' ', phone)
        
        return phone
