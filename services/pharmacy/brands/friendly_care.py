from ..base_handler import BasePharmacyHandler
import re
from datetime import datetime
import logging
import asyncio
from bs4 import BeautifulSoup

class FriendlyCareHandler(BasePharmacyHandler):
    """Handler for FriendlyCare Pharmacy stores"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "friendly_care"
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
            'referer': 'https://www.friendlycare.com.au/',
            'origin': 'https://www.friendlycare.com.au'
        }
        self.logger = logging.getLogger(__name__)
        # Store the provided store links
        self.store_links = [
            "https://www.friendlycare.com.au/headoffice",
            "https://www.friendlycare.com.au/ayr",
            "https://www.friendlycare.com.au/booval",
            "https://www.friendlycare.com.au/burleigh",
            "https://www.friendlycare.com.au/ipswichcbd",
            "https://www.friendlycare.com.au/jacobswell",
            "https://www.friendlycare.com.au/nundah",
            "https://www.friendlycare.com.au/sandgate"
        ]
        
    async def fetch_locations(self):
        """
        Use the provided store links to create location objects
        
        Returns:
            List of FriendlyCare locations
        """
        locations = []
        for i, url in enumerate(self.store_links):
            store_id = url.split('/')[-1]
            locations.append({
                'id': i + 1,
                'url': url,
                'store_id': store_id,
                'name': f"FriendlyCare Pharmacy {store_id.replace('-', ' ').title()}"
            })
            
        self.logger.info(f"Using {len(locations)} provided FriendlyCare Pharmacy store links")
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
        self.logger.info("Fetching all FriendlyCare Pharmacy locations...")
        locations = await self.fetch_locations()
        
        if not locations:
            self.logger.warning("No FriendlyCare Pharmacy locations found.")
            return []
            
        self.logger.info(f"Processing details for {len(locations)} FriendlyCare Pharmacy locations...")
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
                    self.logger.error(f"Error processing FriendlyCare Pharmacy location {location_id}: {str(e)}")
                
        self.logger.info(f"Successfully processed {len(all_details)} FriendlyCare Pharmacy locations")
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
        
        # Use BeautifulSoup for reliable HTML parsing
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract store name
        name = "FriendlyCare Pharmacy"
        store_heading = soup.find('form', attrs={'id': 'aspnetForm'}).find('h2')
        if store_heading:
            store_location = store_heading.get_text().strip()
            name = f"{name} {store_location}"
        
        # Extract address from the store details table
        address = ""
        address_row = soup.find('th', string=lambda s: s and 'Address:' in s)
        if address_row and address_row.find_next('td'):
            address_cell = address_row.find_next('td')
            address = address_cell.get_text().strip().replace('\n', ', ')
        
        # Extract phone number
        phone = ""
        phone_row = soup.find('th', string=lambda s: s and 'Phone:' in s)
        if phone_row and phone_row.find_next('td'):
            phone_cell = phone_row.find_next('td')
            phone_link = phone_cell.find('a')
            if phone_link:
                phone = phone_link.get_text().strip()
            else:
                phone = phone_cell.get_text().strip()
        
        # Extract fax number
        fax = None
        fax_row = soup.find('th', string=lambda s: s and 'Fax:' in s)
        if fax_row and fax_row.find_next('td'):
            fax_cell = fax_row.find_next('td')
            fax = fax_cell.get_text().strip()
        
        # Extract email
        email = None
        email_row = soup.find('th', string=lambda s: s and 'Email:' in s)
        if email_row and email_row.find_next('td'):
            email_cell = email_row.find_next('td')
            email = email_cell.get_text().strip()
        
        # Extract trading hours
        trading_hours = {}
        hours_row = soup.find('th', string=lambda s: s and 'Opening Hours:' in s)
        if hours_row and hours_row.find_next('td'):
            hours_cell = hours_row.find_next('td')
            hours_table = hours_cell.find('table', class_='opening-hours')
            if hours_table:
                trading_hours = self._parse_trading_hours(hours_table)
        
        # Parse address to components
        address_parts = self._parse_address(address)
        
        # Format the data according to our standardized structure
        details = {
            'brand': 'FriendlyCare Pharmacy',
            'name': name,
            'address': address,
            'email': email.lower() if email else None,
            'phone': self._format_phone(phone),
            'fax': self._format_phone(fax),
            'postcode': address_parts.get('postcode', ''),
            'state': address_parts.get('state', 'QLD'),  # Default to QLD for FriendlyCare
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
    
    def _parse_trading_hours(self, hours_table):
        """
        Parse trading hours from the hours table element
        
        Args:
            hours_table: BeautifulSoup element containing the hours table
            
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
        
        if not hours_table:
            return trading_hours
        
        # Process each row in the hours table
        hours_rows = hours_table.find_all('tr')
        for row in hours_rows:
            # Get the day and hours cells
            day_cell = row.find('th')
            hours_cell = row.find('td')
            
            if day_cell and hours_cell:
                day_text = day_cell.get_text().strip()
                hours_text = hours_cell.get_text().strip()
                
                # Map the day text to standard day names
                days = self._map_day_range(day_text)
                
                # Extract opening and closing hours
                hours_match = re.search(r'(\d+:\d+\s*(?:am|pm))\s*-\s*(\d+:\d+\s*(?:am|pm))', hours_text, re.IGNORECASE)
                
                if hours_match:
                    open_time = self._format_time(hours_match.group(1).strip())
                    close_time = self._format_time(hours_match.group(2).strip())
                    
                    # Update each day in the range
                    for day in days:
                        if day in trading_hours:
                            trading_hours[day] = {'open': open_time, 'close': close_time}
        
        return trading_hours
    
    def _map_day_range(self, day_text):
        """
        Map day range text like "Mon-Fri" to a list of individual days
        
        Args:
            day_text: Text representing a day or range of days
            
        Returns:
            List of standard day names
        """
        day_text = day_text.lower()
        
        # Map day abbreviations to full names
        day_map = {
            'mon': 'Monday',
            'tue': 'Tuesday',
            'tues': 'Tuesday',
            'wed': 'Wednesday',
            'thu': 'Thursday',
            'thurs': 'Thursday',
            'fri': 'Friday',
            'sat': 'Saturday',
            'sun': 'Sunday'
        }
        
        # Handle specific patterns
        if 'mon-fri' in day_text or 'monday-friday' in day_text:
            return ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
        elif 'weekdays' in day_text:
            return ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
        elif 'weekend' in day_text:
            return ['Saturday', 'Sunday']
        
        # Check for day ranges with dash
        range_match = re.match(r'(\w+)-(\w+)', day_text)
        if range_match:
            start_day = range_match.group(1).lower()
            end_day = range_match.group(2).lower()
            
            # Map to standard names if possible
            start_day = next((v for k, v in day_map.items() if start_day.startswith(k)), start_day.capitalize())
            end_day = next((v for k, v in day_map.items() if end_day.startswith(k)), end_day.capitalize())
            
            # Get the day indices
            day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            try:
                start_idx = day_order.index(start_day)
                end_idx = day_order.index(end_day)
                return day_order[start_idx:end_idx+1]
            except (ValueError, IndexError):
                # If we can't determine the range, just return the text capitalized
                return [day_text.capitalize()]
        
        # Single day
        for abbr, full_day in day_map.items():
            if abbr in day_text:
                return [full_day]
        
        # If we can't match, just return the text capitalized
        return [day_text.capitalize()]
    
    def _parse_address(self, address):
        """Parse address string into components"""
        result = {'street': '', 'suburb': '', 'state': 'QLD', 'postcode': ''}
        
        if not address:
            return result
            
        # Try to match Australian address format
        # Example: Enter via 15 East Street, Ipswich QLD 4305
        address_pattern = r'(.*?)(?:,\s*|\s+)([^,]+?)(?:\s+([A-Z]{2,3}))?\s+(\d{4})?$'
        match = re.search(address_pattern, address)
        
        if match:
            street = match.group(1)
            suburb = match.group(2)
            state = match.group(3) or 'QLD'  # Default to QLD if not specified
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
                    state_match = re.search(r'([A-Z]{2,3})', suburb_state)
                    if state_match:
                        result['state'] = state_match.group(1)
                        result['suburb'] = re.sub(r'[A-Z]{2,3}', '', suburb_state).strip()
                    else:
                        result['suburb'] = suburb_state
                else:
                    result['suburb'] = last_part
            
        return result
        
    def _format_time(self, time_str):
        """
        Convert time strings to a standardized format.
        
        Args:
            time_str: Time string like "8:00 am", "6:30 pm"
            
        Returns:
            Formatted time string like "08:00 AM", "06:30 PM"
        """
        if not time_str or 'closed' in time_str.lower():
            return 'Closed'
            
        time_str = time_str.strip().upper()
        
        # If already in correct format, return as is
        if re.match(r'\d{2}:\d{2}\s+[AP]M', time_str):
            return time_str
            
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
            # Format like "8AM" or "8 AM"
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
            # Local number, no area code - add default area code (07) for Queensland
            return f"07 {digits_only[0:4]} {digits_only[4:8]}"
        else:
            # Return original if we can't standardize
            return phone.strip()