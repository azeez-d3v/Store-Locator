from ..base_handler import BasePharmacyHandler
import logging
import re
from rich import print
from datetime import datetime
from bs4 import BeautifulSoup

class FullifeHandler(BasePharmacyHandler):
    """Handler for Fullife Pharmacy stores"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "fullife"

        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
            'referer': 'https://www.fullife.com.au/'
        }
        self.logger = logging.getLogger(__name__)
        
    async def fetch_locations(self):
        """
        Fetch all Fullife Pharmacy store locations with complete details from the main page
        
        Returns:
            List of Fullife locations with complete details
        """
        try:
            # Make request to the locations page
            response = await self.session_manager.get(
                url=self.pharmacy_locations.FULLIFE_URL,
                headers=self.headers
            )
            
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch Fullife locations: HTTP {response.status_code}")
                return []
            
            # Parse the HTML content
            soup = BeautifulSoup(response.text, 'html.parser')
              # Find all pharmacy items
            pharmacy_items = soup.find_all('div', {'role': 'listitem', 'class': 'T7n0L6'})
            if not pharmacy_items:
                # Try alternative selector
                pharmacy_items = soup.find_all('div', {'class': 'cGWabE'})
            
            # Initialize the list for storing complete pharmacy details
            all_details = []
            
            for i, item in enumerate(pharmacy_items):
                try:
                    # Extract store details
                    store_details = self._extract_store_details_from_item(item, i)
                    if store_details:
                        all_details.append(store_details)
                except Exception as e:
                    self.logger.warning(f"Error extracting Fullife location item {i}: {str(e)}")
            
            self.logger.info(f"Found {len(all_details)} Fullife Pharmacy locations")
            return all_details
        except Exception as e:
            self.logger.error(f"Exception when fetching Fullife locations: {str(e)}")
            return []
    
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
            
        # For Fullife, our data is already in the right format from _extract_store_details_from_item
        # Just return it as is, or perform any additional standardization if needed
        return pharmacy_data
    
    def _extract_store_details_from_item(self, item, index):
        """
        Extract all store details from a single pharmacy item on the page
        
        Args:
            item: BeautifulSoup element representing a pharmacy location
            index: Index for generating a unique ID
            
        Returns:
            Dictionary with complete pharmacy details
        """        # If item is a container, find the actual pharmacy item inside
        pharmacy_item = item.find('div', {'role': 'listitem'}) or item
        
        # Find the store link which contains the store ID - look for "Learn More" button
        learn_more_link = pharmacy_item.find('a', {'aria-label': 'Learn More'})
        if not learn_more_link:
            # Try finding by class
            learn_more_div = pharmacy_item.find('div', {'class': 'comp-klrg2zs3'})
            if learn_more_div:
                learn_more_link = learn_more_div.find('a')
        
        if not learn_more_link:
            return None
            
        # Extract store URL
        store_url = learn_more_link.get('href')
        if not store_url:
            return None
            
        # Extract store ID from URL
        store_id = store_url.split('/')[-1]
          # Find store name heading
        store_name = None
        
        # Try finding in the h4 element in comp-klrg2zrz1 div (as seen in the HTML)
        store_name_container = pharmacy_item.find('div', {'class': 'comp-klrg2zrz1'})
        if store_name_container:
            store_name_elem = store_name_container.find('h4', class_='font_4')
            if store_name_elem:
                store_name = store_name_elem.get_text().strip()
        
        # If not found, try other methods
        if not store_name:
            # Try to find the button with the store name
            store_button = pharmacy_item.find('a', {'aria-label': lambda x: x and x != 'Learn More'})
            if store_button and store_button.find('span'):
                store_name = store_button.find('span').get_text().strip()
                # Format the store name
                store_name = f"Fullife {store_name}"
            else:
                # Use fallback naming
                store_name = f"Fullife Pharmacy {store_id.title()}"
        
        # Find the rich text element containing store details
        details_elem = pharmacy_item.find('div', {'class': 'comp-klrg2zrn'})
        
        if not details_elem:
            # Try alternative class structures
            for div in pharmacy_item.find_all('div', {'class': lambda x: x and 'richText' in x or 'wixui-rich-text' in x}):
                if div.find_all('p'):
                    details_elem = div
                    break
        if not details_elem:
            return None
            
        # Get all paragraphs in the details
        paragraphs = details_elem.find_all('p', class_='font_4') or details_elem.find_all('p')
        
        # If still no paragraphs found, try more generic search
        if not paragraphs:
            for element in details_elem.find_all(['p', 'span']):
                if element.get_text().strip():
                    paragraphs.append(element)
        
        # Initialize variables for store details
        address_lines = []
        phone = None
        fax = None
        email = None
        hours = {}
        
        # Process each paragraph to extract details
        for para in paragraphs:
            text = para.get_text().strip()
            
            # Skip empty paragraphs
            if not text or text == '\u200B':  # Zero-width space
                continue
            
            # Extract phone
            if 'Phone:' in text:
                phone_match = re.search(r'Phone:\s*(.+)', text)
                if phone_match:
                    phone = phone_match.group(1).strip()
            
            # Extract fax
            elif 'Fax:' in text:
                fax_match = re.search(r'Fax:\s*(.+)', text)
                if fax_match:
                    fax = fax_match.group(1).strip()
            
            # Extract email
            elif any(keyword in text for keyword in ['Email:', 'Enquiries:', 'Prescriptions:', 'Email/scripts:']):
                # Find the email link inside this paragraph
                email_link = para.find('a', {'data-auto-recognition': 'true'})
                if email_link and email_link.get('href', '').startswith('mailto:'):
                    if not email:  # Only store the first email we find
                        email = email_link.get('href')[7:]  # Remove 'mailto:'
            
            # Extract hours - look for patterns like "Monday - Friday: 9:00am - 6:00pm"
            elif any(day in text for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']):
                # Check for combined days like "Monday - Friday: 9:00am - 6:00pm"
                days_range_match = re.search(r'(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s*-\s*(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday):\s*(.+)', text)
                if days_range_match:
                    start_day = days_range_match.group(1)
                    end_day = days_range_match.group(2)
                    time_str = days_range_match.group(3).strip()
                    
                    # Map the range to individual days
                    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                    try:
                        start_idx = day_order.index(start_day)
                        end_idx = day_order.index(end_day)
                        days_in_range = day_order[start_idx:end_idx+1]
                        
                        # Extract opening and closing hours
                        hours_match = re.search(r'(\d+:\d+(?:am|pm))\s*-\s*(\d+:\d+(?:am|pm))', time_str, re.IGNORECASE)
                        if hours_match:
                            open_time = hours_match.group(1)
                            close_time = hours_match.group(2)
                            
                            # Add hours for each day in the range
                            for day in days_in_range:
                                hours[day] = {'open': open_time, 'close': close_time}
                    except (ValueError, IndexError):
                        pass
                else:
                    # Check for single day format like "Saturday: 9:00am - 2:00pm"
                    single_day_match = re.search(r'(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday):\s*(.+)', text)
                    if single_day_match:
                        day = single_day_match.group(1)
                        time_str = single_day_match.group(2).strip()
                        
                        # Extract opening and closing hours
                        hours_match = re.search(r'(\d+:\d+(?:am|pm))\s*-\s*(\d+:\d+(?:am|pm))', time_str, re.IGNORECASE)
                        if hours_match:
                            open_time = hours_match.group(1)
                            close_time = hours_match.group(2)
                            hours[day] = {'open': open_time, 'close': close_time}
            
            # Extract "7 days a week" hours
            elif "7 days a week" in text:
                # Look for previous paragraph with time range
                if len(address_lines) > 0 and 'days' not in address_lines[-1]:
                    time_str = address_lines[-1]
                    address_lines.pop()  # Remove this from address lines
                    
                    # Extract opening and closing hours
                    hours_match = re.search(r'(\d+:\d+(?:am|pm))\s*-\s*(\d+:\d+(?:am|pm))', time_str, re.IGNORECASE)
                    if hours_match:
                        open_time = hours_match.group(1)
                        close_time = hours_match.group(2)
                        
                        # Set for all days
                        for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']:
                            hours[day] = {'open': open_time, 'close': close_time}
            
            # Check if this is an address line (doesn't contain specific keywords)
            elif not any(keyword in text for keyword in ['Phone:', 'Fax:', 'Email:', 'Instagram:', 'Script', 'Compounding:']):
                # Check if it's an hours line (may not have day prefix)
                if any(time_pattern in text for time_pattern in ['am', 'pm']) and '-' in text:
                    # This could be hours information without day prefix
                    hours_match = re.search(r'(\d+:\d+(?:am|pm))\s*-\s*(\d+:\d+(?:am|pm))', text, re.IGNORECASE)
                    if hours_match:
                        # Store temporarily in address_lines, may process later with "7 days" info
                        address_lines.append(text)
                    else:
                        # This is likely an address line
                        address_lines.append(text)
                else:
                    # This is likely an address line
                    address_lines.append(text)
        
        # Clean up address lines (remove any lines that look like hours)
        cleaned_address_lines = []
        for line in address_lines:
            if not any(time_pattern in line for time_pattern in ['am', 'pm']) or 'roster' in line.lower():
                cleaned_address_lines.append(line)
        
        # Parse address into components
        address = ', '.join(cleaned_address_lines)
        address_components = self._parse_address(address)
        
        # Formulate trading hours
        trading_hours = {}
        for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']:
            if day in hours:
                trading_hours[day] = hours[day]
            else:
                trading_hours[day] = {'open': 'Closed', 'close': 'Closed'}
        
        # Add public holiday if not available
        if 'Public Holiday' not in trading_hours:
            trading_hours['Public Holiday'] = {'open': 'Closed', 'close': 'Closed'}
        
        # Create the final pharmacy details object
        result = {
            'brand': 'Fullife Pharmacy',
            'name': store_name,
            'store_id': store_id,
            'address': address,
            'street_address': address_components.get('street', ''),
            'suburb': address_components.get('suburb', ''),
            'state': address_components.get('state', ''),
            'postcode': address_components.get('postcode', ''),
            'phone': self._format_phone(phone),
            'fax': self._format_phone(fax),
            'email': email,
            'website': store_url,
            'trading_hours': trading_hours,
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Remove any None or empty values
        return {k: v for k, v in result.items() if v}
        
    async def fetch_pharmacy_details(self, location):
        """
        Get details for a specific pharmacy location
        The details are already included in location from fetch_locations
        
        Args:
            location: Dict containing complete pharmacy details
            
        Returns:
            The same location dict (for API compatibility)
        """
        # Simply return the location as it already contains all details
        return location
    
    async def fetch_all_locations_details(self):
        """
        Fetch details for all Fullife Pharmacy locations
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        self.logger.info("Fetching all Fullife Pharmacy locations...")
        
        # Since fetch_locations now returns complete details, we can just use that
        all_details = await self.fetch_locations()
        
        if not all_details:
            self.logger.warning("No Fullife Pharmacy locations found")
            return []
        
        self.logger.info(f"Successfully processed {len(all_details)} Fullife Pharmacy locations")
        return all_details
    
    def _parse_address(self, address):
        """
        Parse address string into components
        
        Args:
            address: Full address string
            
        Returns:
            Dictionary with address components
        """
        result = {'street': '', 'suburb': '', 'state': '', 'postcode': ''}
        
        if not address:
            return result
        
        # Normalize address - replace multiple whitespace with single space
        normalized_address = re.sub(r'\s+', ' ', address)
        
        # Direct pattern match for "TAS" - specifically for Ulverstone
        if "Ulverstone TAS" in normalized_address:
            tas_match = re.search(r'(\d+\s+[^,]+),?\s+Ulverstone\s+TAS\s+(\d{4})', normalized_address)
            if tas_match:
                result['street'] = tas_match.group(1).strip()
                result['suburb'] = 'Ulverstone'
                result['state'] = 'TAS'
                result['postcode'] = tas_match.group(2)
                return result
        
        # Try additional patterns for other address formats
        # Pattern for addresses with clear state codes like "VIC", "NSW", etc.
        state_pattern = r'([A-Z]{2,3})\s+(\d{4})'
        state_match = re.search(state_pattern, normalized_address)
        
        if state_match:
            # Extract state and postcode
            result['state'] = state_match.group(1)
            result['postcode'] = state_match.group(2)
            
            # Remove state and postcode from address to process the rest
            address_without_state = re.sub(state_pattern, '', normalized_address).strip()
            
            # Try to split into street and suburb
            parts = address_without_state.split(',')
            if len(parts) >= 2:
                result['street'] = parts[0].strip()
                result['suburb'] = parts[-1].strip()
            else:
                # If no comma, try to find the last word group as suburb
                words = address_without_state.split()
                if len(words) > 2:
                    result['street'] = ' '.join(words[:-1]).strip()
                    result['suburb'] = words[-1].strip()
                else:
                    # Just use the whole thing as street address
                    result['street'] = address_without_state
            
            return result
                
        # Australian address pattern - fallback
        # Example: "24 King Edward Street, Ulverstone TAS 7315"
        address_pattern = r'(.+?)(?:,\s*|\s+)([^,]+?)(?:\s+([A-Z]{2,3}))?\s+(\d{4})?$'
        match = re.search(address_pattern, normalized_address)
        
        if match:
            street = match.group(1)
            suburb = match.group(2)
            state = match.group(3) or ''
            postcode = match.group(4) or ''
            
            result = {
                'street': street.strip(),
                'suburb': suburb.strip(),
                'state': state.strip(),
                'postcode': postcode.strip()
            }
        else:
            # If pattern doesn't match, do best effort parsing
            parts = normalized_address.split(',')
            if len(parts) >= 2:
                result['street'] = parts[0].strip()
                
                # Last part might have suburb, state, postcode
                last_part = parts[-1].strip()
                
                # Try to extract postcode
                postcode_match = re.search(r'(\d{4})', last_part)
                if postcode_match:
                    result['postcode'] = postcode_match.group(1)
                    # Remove postcode
                    last_part = re.sub(r'\d{4}', '', last_part).strip()
                
                # Try to extract state
                state_match = re.search(r'\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b', last_part)
                if state_match:
                    result['state'] = state_match.group(1)
                    # Remove state
                    last_part = re.sub(r'\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b', '', last_part).strip()
                
                # Remaining part is likely suburb
                result['suburb'] = last_part.strip()
        
        return result
    
    def _format_phone(self, phone):
        """
        Format phone number consistently
        
        Args:
            phone: Raw phone number string
            
        Returns:
            Formatted phone number
        """
        if not phone:
            return None
        
        # Remove non-numeric characters
        digits_only = re.sub(r'\D', '', phone)
        
        # Handle Australian phone number formats
        if len(digits_only) == 10 and digits_only.startswith('0'):
            # Format as 0X XXXX XXXX
            return f"{digits_only[0:2]} {digits_only[2:6]} {digits_only[6:10]}"
        elif len(digits_only) == 8:
            # Local number, no area code - keep as is
            return phone.strip()
        else:
            # Return original if we can't standardize
            return phone.strip()