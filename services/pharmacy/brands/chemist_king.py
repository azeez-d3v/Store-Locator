from ..base_handler import BasePharmacyHandler
import re
from datetime import datetime
import logging
import asyncio
from bs4 import BeautifulSoup

class ChemistKingHandler(BasePharmacyHandler):
    """Handler for Chemist King Pharmacy stores"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "chemist_king"
        self.headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
            'referer': 'https://www.chemistking.com.au/',
            'origin': 'https://www.chemistking.com.au'
        }
        self.logger = logging.getLogger(__name__)

    async def fetch_locations(self):
        """
        Use the provided store links instead of making an API call
        
        Returns:
            List of Chemist King locations
        """
        locations = []
        for i, url in enumerate(self.pharmacy_locations.CHEMIST_KING_URLS):
            store_id = url.split('/')[-1]
            locations.append({
                'id': i + 1,
                'url': url,
                'store_id': store_id,
                'name': f"Chemist King {store_id.replace('-', ' ').title()}"
            })
            
        self.logger.info(f"Using {len(locations)} provided Chemist King store links")
        return locations
    
    async def fetch_pharmacy_details(self, location):
        """
        Fetch details for a specific pharmacy location using the store's URL
        
        Args:
            location: The location object with store URL
            
        Returns:
            Location details data
        """
        try:
            url = location.get('url')
            if not url:
                return None
                
            # Make request to the store page to get details
            response = await self.session_manager.get(
                url=url,
                headers=self.headers,
            )
            
            if response.status_code == 200:
                return {
                    'id': location.get('id'),
                    'store_id': location.get('store_id'),
                    'name': location.get('name'),
                    'url': url,
                    'html_content': response.text
                }
            else:
                self.logger.error(f"Failed to fetch details for {url}: HTTP {response.status_code}")
                return None
        except Exception as e:
            self.logger.error(f"Exception when fetching details for {location.get('url', 'unknown')}: {str(e)}")
            return None
        
    async def fetch_all_locations_details(self):
        """
        Fetch details for all locations and return as a list.
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        self.logger.info("Fetching all Chemist King locations...")
        locations = await self.fetch_locations()
        
        if not locations:
            self.logger.warning("No Chemist King locations found.")
            return []
            
        self.logger.info(f"Processing details for {len(locations)} Chemist King locations...")
        all_details = []
        
        # Process each location - fetch details in parallel to improve performance
        tasks = []
        for location in locations:
            tasks.append(self.fetch_pharmacy_details(location))
        
        # Wait for all requests to complete
        location_details = await asyncio.gather(*tasks)
        
        # Process the results
        for details in location_details:
            if details:
                try:
                    extracted_details = self.extract_pharmacy_details(details)
                    if extracted_details:  # Only add if we got valid details
                        all_details.append(extracted_details)
                except Exception as e:
                    location_id = details.get('id', 'unknown')
                    self.logger.error(f"Error processing Chemist King location {location_id}: {str(e)}")
                
        self.logger.info(f"Successfully processed {len(all_details)} Chemist King locations")
        return all_details
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract and standardize pharmacy details from store page HTML content
        using BeautifulSoup for more reliable parsing
        
        Args:
            pharmacy_data: Data including HTML content from the store page
            
        Returns:
            Dictionary with standardized pharmacy details
        """
        if not pharmacy_data or 'html_content' not in pharmacy_data:
            return None
            
        html_content = pharmacy_data['html_content']
        store_id = pharmacy_data['store_id']
        
        # Use BeautifulSoup for more reliable HTML parsing
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract store name (typically in h1 tag)
        name_element = soup.find('h1', attrs={'style': 'font-size:56px; line-height:normal; text-align:center;'})
        name = name_element.get_text(strip=True) if name_element else ''
        
        # Extract address - typically in a paragraph element near a location icon or specific container
        address = ""
        address_container = None
        
        # Look for address in elements with location icon class
        location_elements = soup.select('div[class*="icon-location"] p')
        if location_elements:
            address_container = location_elements[0]
            
        # If not found, try alternate methods to find the address
        if not address_container:
            # Try to find address in any element that might contain location info
            address_candidates = soup.find_all('p', string=lambda t: t and re.search(r'\b[A-Z]{2}\s+\d{4}\b', t))
            if address_candidates:
                address_container = address_candidates[0]
                
        if address_container:
            address = address_container.get_text().strip()
            # Clean up the address text
            address = re.sub(r'\s+', ' ', address)
        
        # Extract phone number - typically in a link with "tel:" prefix
        phone = ""
        phone_element = soup.select_one('a[href^="tel:"]')
        if phone_element:
            phone = phone_element.get('href').replace('tel:', '').strip()
        
        # Extract fax number - typically in a button or element with "Fax:" text
        fax = None
        fax_elements = soup.find_all(string=lambda t: t and 'Fax:' in t)
        for element in fax_elements:
            fax_text = element.strip()
            # Extract the fax number from the text
            fax_match = re.search(r'Fax:\s*(\(?\d+\)?\s*\d+\s*\d+)', fax_text)
            if fax_match:
                fax = fax_match.group(1).strip()
                break
        
        # If not found via string search, try to find it through button aria-label
        if not fax:
            fax_buttons = soup.find_all('button', attrs={'aria-label': lambda v: v and 'Fax:' in v})
            if fax_buttons:
                fax_text = fax_buttons[0].get('aria-label', '')
                fax_match = re.search(r'Fax:\s*(\(?\d+\)?\s*\d+\s*\d+)', fax_text)
                if fax_match:
                    fax = fax_match.group(1).strip()
        
        # If still not found, try to find by data-semantic-classname or class containing "fax"
        if not fax:
            fax_elements = soup.find_all(['div', 'span', 'p', 'button'], 
                                        attrs={'class': lambda c: c and ('fax' in c.lower() or 'Fax' in c)})
            for element in fax_elements:
                fax_text = element.get_text().strip()
                fax_match = re.search(r'Fax:\s*(\(?\d+\)?\s*\d+\s*\d+)', fax_text)
                if fax_match:
                    fax = fax_match.group(1).strip()
                    break
        
        # Extract email if available - typically in a link with "mailto:" prefix
        email = None
        email_element = soup.select_one('a[href^="mailto:"]')
        if email_element:
            email = email_element.get('href').replace('mailto:', '').strip()
        
        # Extract trading hours
        trading_hours = {}
        
        # Find trading hours section - specifically looking for elements with Opening Hours text
        hours_heading = soup.find(lambda tag: tag.name and tag.get_text() and 'Opening Hours' in tag.get_text())
        hours_container = None
        
        if hours_heading:
            # Look for the div that contains the actual hours
            # First try to find it using class or id-based selectors
            parent_element = hours_heading.parent
            
            # Look for nearby elements with specific structure matching the hours layout
            potential_containers = []
            
            # Try to find elements with "Mon:", "Tues:", etc. text patterns
            for element in soup.find_all(['p', 'div']):
                text = element.get_text()
                if re.search(r'(Mon|Tues|Wed|Thurs|Fri|Sat|Sun):', text):
                    potential_containers.append(element)
            
            # If we found potential containers, use the one with the most day mentions
            if potential_containers:
                best_container = max(potential_containers, 
                                     key=lambda e: len(re.findall(r'(Mon|Tues|Wed|Thurs|Fri|Sat|Sun):', e.get_text())))
                hours_container = best_container
        
        # If we still don't have a container, try another approach using specific classes or IDs
        if not hours_container:
            # Try to find the element with class containing comp-kzw0cxzd based on the HTML example
            hours_containers = soup.find_all(class_=lambda c: c and 'comp-kzw0cxzd' in c)
            if hours_containers:
                hours_container = hours_containers[0]
        
        # If we found a container with hours, extract the day and time information
        if hours_container:
            # Parse the trading hours using a more specific approach for the provided HTML structure
            trading_hours = self._parse_specific_trading_hours_format(hours_container)
        else:
            # Fallback to general text-based extraction
            trading_hours = self._extract_trading_hours_from_text(html_content)
        
        # Parse address to components
        address_parts = self._parse_address(address)
        
        # Format the data according to our standardized structure
        details = {
            'brand': 'Chemist King',
            'name': name,
            'address': address,
            'email': email.lower() if email else None,
            'phone': self._format_phone(phone),
            'fax': self._format_phone(fax),
            'postcode': address_parts.get('postcode', ''),
            'state': address_parts.get('state', 'SA'),  # Most stores are in South Australia
            'street_address': address_parts.get('street', ''),
            'suburb': address_parts.get('suburb', ''),
            'trading_hours': trading_hours,
            'website': pharmacy_data.get('url', ''),
            'store_id': store_id,
            'store_location': store_id,
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Clean up the result by removing None or empty values
        return {k: v for k, v in details.items() if v not in (None, '', {}, [])}
    
    def _parse_trading_hours(self, hours_container):
        """
        Parse trading hours from the hours container element
        
        Args:
            hours_container: BeautifulSoup element containing the hours
            
        Returns:
            Dictionary with days as keys and hours as values
        """
        # Initialize all days with closed hours
        trading_hours = {
            'Monday': {'open': 'Closed', 'close': 'Closed'},
            'Tuesday': {'open': 'Closed', 'close': 'Closed'},
            'Wednesday': {'open': 'Closed', 'close': 'Closed'},
            'Thursday': {'open': 'Closed', 'close': 'Closed'},
            'Friday': {'open': 'Closed', 'close': 'Closed'},
            'Saturday': {'open': 'Closed', 'close': 'Closed'},
            'Sunday': {'open': 'Closed', 'close': 'Closed'},
            'Public Holiday': {'open': 'Closed', 'close': 'Closed'}
        }
        
        if not hours_container:
            return trading_hours
            
        # Get the text content of the hours container
        hours_text = hours_container.get_text()
        
        # Find all day patterns with their corresponding hours
        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday', 'Public Holiday']
        day_abbreviations = {
            'Mon': 'Monday',
            'Tues': 'Tuesday',
            'Wed': 'Wednesday',
            'Thurs': 'Thursday',
            'Fri': 'Friday',
            'Sat': 'Saturday',
            'Sun': 'Sunday'
        }
        
        # Process each line of the hours text
        lines = hours_text.split('\n')
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Check for day patterns
            day_found = None
            
            # Try to match full day names
            for day in days:
                if day.lower() in line.lower():
                    day_found = day
                    break
            
            # If no full day name found, try abbreviations
            if not day_found:
                for abbr, full_day in day_abbreviations.items():
                    if abbr.lower() in line.lower():
                        day_found = full_day
                        break
            
            if day_found:
                # Extract hours using regex
                hours_match = re.search(r'(\d+(?::\d+)?\s*(?:am|pm))\s*[-–—]\s*(\d+(?::\d+)?\s*(?:am|pm))', line, re.IGNORECASE)
                if hours_match:
                    open_time = self._format_time(hours_match.group(1).strip())
                    close_time = self._format_time(hours_match.group(2).strip())
                    trading_hours[day_found] = {'open': open_time, 'close': close_time}
                elif 'closed' in line.lower():
                    trading_hours[day_found] = {'open': 'Closed', 'close': 'Closed'}
        
        return trading_hours
        
    def _extract_trading_hours_from_text(self, html_content):
        """
        Extract trading hours from HTML content when structured container isn't found
        
        Args:
            html_content: Raw HTML content
            
        Returns:
            Dictionary with days as keys and hours as values
        """
        # Initialize all days with closed hours
        trading_hours = {
            'Monday': {'open': 'Closed', 'close': 'Closed'},
            'Tuesday': {'open': 'Closed', 'close': 'Closed'},
            'Wednesday': {'open': 'Closed', 'close': 'Closed'},
            'Thursday': {'open': 'Closed', 'close': 'Closed'},
            'Friday': {'open': 'Closed', 'close': 'Closed'},
            'Saturday': {'open': 'Closed', 'close': 'Closed'},
            'Sunday': {'open': 'Closed', 'close': 'Closed'},
            'Public Holiday': {'open': 'Closed', 'close': 'Closed'}
        }
        
        if not html_content:
            return trading_hours
            
        # Remove HTML tags and normalize whitespace
        cleaned_html = re.sub(r'<[^>]+>', ' ', html_content)
        cleaned_html = re.sub(r'\s+', ' ', cleaned_html).strip()
        
        # Look for day patterns
        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday', 'Public Holiday']
        day_abbreviations = {
            'Mon': 'Monday',
            'Tues': 'Tuesday',
            'Wed': 'Wednesday',
            'Thurs': 'Thursday',
            'Fri': 'Friday',
            'Sat': 'Saturday',
            'Sun': 'Sunday'
        }
        
        # Process full day names
        for day in days:
            pattern = rf'{day}\s*[:]\s*(.*?)(?=(?:{"|".join(days)})|$)'
            day_match = re.search(pattern, cleaned_html, re.IGNORECASE)
            
            if day_match:
                hours_text = day_match.group(1).strip()
                self._process_hours_text(trading_hours, day, hours_text)
        
        # Process abbreviations
        for abbr, full_day in day_abbreviations.items():
            pattern = rf'{abbr}\s*[:]\s*(.*?)(?=(?:{"|".join(list(day_abbreviations.keys()) + days)})|$)'
            day_match = re.search(pattern, cleaned_html, re.IGNORECASE)
            
            if day_match:
                hours_text = day_match.group(1).strip()
                self._process_hours_text(trading_hours, full_day, hours_text)
        
        return trading_hours
        
    def _process_hours_text(self, trading_hours, day, hours_text):
        """Helper method to process hours text for a specific day"""
        if 'closed' in hours_text.lower():
            trading_hours[day] = {'open': 'Closed', 'close': 'Closed'}
        else:
            # Look for time ranges
            time_pattern = r'(\d+(?::\d+)?\s*(?:am|pm))\s*[-–—]\s*(\d+(?::\d+)?\s*(?:am|pm))'
            time_match = re.search(time_pattern, hours_text, re.IGNORECASE)
            
            if time_match:
                open_time = self._format_time(time_match.group(1).strip())
                close_time = self._format_time(time_match.group(2).strip())
                trading_hours[day] = {'open': open_time, 'close': close_time}
    
    def _parse_specific_trading_hours_format(self, hours_container):
        """
        Parse trading hours from the specific HTML structure provided in the example
        
        Args:
            hours_container: BeautifulSoup element containing the hours (matching comp-kzw0cxzd format)
            
        Returns:
            Dictionary with days as keys and hours as values
        """
        # Initialize all days with closed hours
        trading_hours = {
            'Monday': {'open': 'Closed', 'close': 'Closed'},
            'Tuesday': {'open': 'Closed', 'close': 'Closed'},
            'Wednesday': {'open': 'Closed', 'close': 'Closed'},
            'Thursday': {'open': 'Closed', 'close': 'Closed'},
            'Friday': {'open': 'Closed', 'close': 'Closed'},
            'Saturday': {'open': 'Closed', 'close': 'Closed'},
            'Sunday': {'open': 'Closed', 'close': 'Closed'},
            'Public Holiday': {'open': 'Closed', 'close': 'Closed'}
        }
        
        if not hours_container:
            return trading_hours
            
        # Get direct text content for easier parsing
        hours_text = hours_container.get_text()
        
        # Map abbreviated day names to full day names
        day_mapping = {
            'Mon': 'Monday',
            'Tues': 'Tuesday',
            'Wed': 'Wednesday',
            'Thurs': 'Thursday',
            'Fri': 'Friday',
            'Sat': 'Saturday',
            'Sun': 'Sunday',
            'Public Holiday': 'Public Holiday'  # Include this for completeness
        }
        
        # Use a simpler approach directly using regular expressions
        # This pattern specifically matches the format in the example: "Mon: 8.30am – 9pm"
        day_time_pattern = r'(Mon|Tues|Wed|Thurs|Fri|Sat|Sun):\s*([\d\.]+(?:am|pm))\s*[–-]\s*([\d\.]+(?:am|pm))'
        
        # Find all matches in the text
        matches = re.findall(day_time_pattern, hours_text, re.IGNORECASE)
        
        # Process each match
        for match in matches:
            day_abbr = match[0].strip()
            open_time = match[1].strip()
            close_time = match[2].strip()
            
            # Convert day abbreviation to full day name
            full_day = day_mapping.get(day_abbr, day_abbr)
            
            # Format times consistently
            formatted_open = self._format_time(open_time)
            formatted_close = self._format_time(close_time)
            
            # Store in our dictionary
            trading_hours[full_day] = {
                'open': formatted_open, 
                'close': formatted_close
            }
            
            # Log for debugging
            self.logger.debug(f"Parsed {full_day}: open={formatted_open}, close={formatted_close}")
        
        # If no matches were found using the normal pattern, try a more detailed raw HTML approach
        if not any('Closed' != v['open'] for v in trading_hours.values()):
            # Access each paragraph directly
            paragraphs = hours_container.find_all('p')
            for paragraph in paragraphs:
                # Get text and look for day patterns
                paragraph_text = paragraph.get_text()
                for day_abbr, full_day in day_mapping.items():
                    if day_abbr + ':' in paragraph_text:
                        # Look for time pattern
                        time_match = re.search(r'([\d\.]+(?:am|pm))\s*[–-]\s*([\d\.]+(?:am|pm))', 
                                              paragraph_text, re.IGNORECASE)
                        if time_match:
                            open_time = time_match.group(1).strip()
                            close_time = time_match.group(2).strip()
                            
                            # Format times consistently  
                            formatted_open = self._format_time(open_time)
                            formatted_close = self._format_time(close_time)
                            
                            # Store in our dictionary
                            trading_hours[full_day] = {
                                'open': formatted_open,
                                'close': formatted_close
                            }
        
        return trading_hours
    
    def _parse_address(self, address):
        """Parse address string into components"""
        result = {'street': '', 'suburb': '', 'state': 'SA', 'postcode': ''}
        
        if not address:
            return result
            
        # Try to match Australian address format
        # Example: 123 Main St, Suburb SA 5000
        address_pattern = r'(.*?)(?:,\s*|\s+)([^,]+?)(?:\s+([A-Z]{2,3}))?\s+(\d{4})?$'
        match = re.search(address_pattern, address)
        
        if match:
            street = match.group(1)
            suburb = match.group(2)
            state = match.group(3) or 'SA'  # Default to SA if not specified
            postcode = match.group(4) or ''
            
            result = {
                'street': street.strip(),
                'suburb': suburb.strip(),
                'state': state.strip(),
                'postcode': postcode.strip()
            }
        else:
            # If pattern doesn't match, split by comma
            parts = address.split(',')
            if len(parts) >= 2:
                result['street'] = parts[0].strip()
                # Try to extract postcode from the last part
                last_part = parts[-1].strip()
                postcode_match = re.search(r'(\d{4})', last_part)
                if postcode_match:
                    result['postcode'] = postcode_match.group(1)
                    # Remove postcode from the suburb part
                    suburb_state = re.sub(r'\d{4}', '', last_part).strip()
                    # Try to extract state
                    state_match = re.search(r'([A-Z]{2,3})$', suburb_state)
                    if state_match:
                        result['state'] = state_match.group(1)
                        result['suburb'] = re.sub(r'[A-Z]{2,3}$', '', suburb_state).strip()
                    else:
                        result['suburb'] = suburb_state
                else:
                    result['suburb'] = last_part
            
        return result
        
    def _safe_float(self, value):
        """Convert value to float safely, returning None if not possible"""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
            
    def _format_phone(self, phone):
        """Format phone number consistently"""
        if not phone:
            return None
            
        # Remove non-numeric characters
        digits_only = re.sub(r'\D', '', phone)
        
        # Handle Australian phone number formats
        if len(digits_only) == 10 and digits_only.startswith('0'):
            # Format as 0X XXXX XXXX
            return f"{digits_only[0:2]} {digits_only[2:6]} {digits_only[6:10]}"
        elif len(digits_only) == 8:
            # Local number, no area code - add default area code (08) for South Australia
            return f"08 {digits_only[0:4]} {digits_only[4:8]}"
        else:
            # Return original if we can't standardize
            return phone.strip()
    
    def _format_time(self, time_str):
        """
        Convert time strings to a standardized format.
        
        Args:
            time_str: Time string like "8am", "6pm", "8:30am", "8.30am"
            
        Returns:
            Formatted time string like "08:00 AM", "06:00 PM", "08:30 AM"
        """
        if not time_str or 'closed' in time_str.lower():
            return 'Closed'
            
        time_str = time_str.strip().upper()
        
        # If already in correct format, return as is
        if re.match(r'\d{2}:\d{2}\s+[AP]M', time_str):
            return time_str
            
        # Handle decimal points in time (e.g., "8.30am")
        if '.' in time_str:
            # Convert decimal point format to colon format
            parts = time_str.split('.')
            if len(parts) == 2:
                hour_part = parts[0]
                minute_part_with_ampm = parts[1]
                
                # Extract minutes and AM/PM
                minute_match = re.match(r'(\d+)(AM|PM)', minute_part_with_ampm)
                if minute_match:
                    minutes = minute_match.group(1)
                    am_pm = minute_match.group(2)
                    
                    # Format hour to 2 digits
                    hour_num = int(hour_part)
                    hour_12 = hour_num if 1 <= hour_num <= 12 else hour_num % 12
                    if hour_12 == 0:
                        hour_12 = 12
                        
                    return f"{hour_12:02d}:{int(minutes):02d} {am_pm}"
        
        # Handle standard formats with colon
        if ':' in time_str:
            # Format like "8:30 AM" or "8:30AM"
            hour_part, minute_part = time_str.split(':')
            if 'AM' in minute_part or 'PM' in minute_part:
                minute_num = re.search(r'\d+', minute_part).group(0)
                am_pm = 'AM' if 'AM' in minute_part else 'PM'
            else:
                minute_num = minute_part
                am_pm = 'AM'  # default if not specified
            
            hour_num = int(hour_part)
            hour_12 = hour_num if 1 <= hour_num <= 12 else hour_num % 12
            if hour_12 == 0:
                hour_12 = 12
            return f"{hour_12:02d}:{int(minute_num):02d} {am_pm}"
        else:
            # Format like "8AM" or "8 AM" or simply "9pm"
            match = re.match(r'(\d+)\s*([AP]M)?', time_str)
            if match:
                hour_num = int(match.group(1))
                am_pm = match.group(2) or 'AM'  # default to AM if not specified
                
                hour_12 = hour_num if 1 <= hour_num <= 12 else hour_num % 12
                if hour_12 == 0:
                    hour_12 = 12
                return f"{hour_12:02d}:00 {am_pm}"
            else:
                # If no pattern match, return as is
                return time_str