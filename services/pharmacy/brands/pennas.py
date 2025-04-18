from ..base_handler import BasePharmacyHandler
import logging
import re
import json
from datetime import datetime
from bs4 import BeautifulSoup
import urllib.parse

class PennasPharmacyHandler(BasePharmacyHandler):
    """Handler for Pennas Pharmacy stores"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "pennas"
        self.base_url = self.pharmacy_locations.PENNAS_URLS
        

        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
        }
        self.logger = logging.getLogger(__name__)
        
    async def fetch_locations(self):
        """
        Return the hardcoded list of Pennas Pharmacy store locations
        
        Returns:
            List of Pennas Pharmacy locations with basic details
        """
        try:
            # Initialize the list for storing basic pharmacy information
            all_locations = []
            
            # Create location objects from the hardcoded URLs
            for i, url in enumerate(self.base_url):
                try:
                    # Extract the store name from the URL
                    store_id = url.rstrip('/').split('/')[-1]
                    
                    # Format the name nicer (convert hyphens to spaces and capitalize)
                    name_part = store_id.replace('pennas-discount-pharmacy-', '')
                    location_name = f"Penna's Discount Pharmacy {name_part.replace('-', ' ').title()}"
                    
                    # Create basic location info
                    location = {
                        'id': store_id,
                        'name': location_name,
                        'url': url,
                        'brand': 'Pennas Pharmacy'
                    }
                    
                    all_locations.append(location)
                except Exception as e:
                    self.logger.warning(f"Error creating Pennas Pharmacy location item {i}: {str(e)}")
            
            self.logger.info(f"Found {len(all_locations)} Pennas Pharmacy locations")
            return all_locations
        except Exception as e:
            self.logger.error(f"Exception when creating Pennas Pharmacy locations: {str(e)}")
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
            
        # For Pennas Pharmacy, data is already in the right format from fetch_pharmacy_details
        # Just return it as is
        return pharmacy_data
    
    async def fetch_pharmacy_details(self, location):
        """
        Get details for a specific pharmacy location
        
        Args:
            location: Dict containing basic pharmacy location info
            
        Returns:
            Complete pharmacy details
        """
        try:
            # Get the location URL
            store_url = location.get('url', '')
            
            if not store_url:
                self.logger.error(f"No URL provided for Pennas Pharmacy location: {location.get('name', '')}")
                return {}
            
            # Make request to the store page
            response = await self.session_manager.get(
                url=store_url,
                headers=self.headers
            )
            
            self.logger.info(f"Fetching details for {location.get('name', '')} from {store_url}")
            
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch Pennas Pharmacy details: HTTP {response.status_code}")
                return {}
            
            # Parse the HTML content
            try:
                # Use BeautifulSoup with 'html.parser'
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extract detailed store information
                store_details = self._extract_store_details(soup, location)
                
                # Debug logging to see what we're getting
                self.logger.info(f"Extracted details for {location.get('name', '')}: {list(store_details.keys())}")
                
                return store_details
            except Exception as e:
                self.logger.error(f"HTML parsing error for Pennas Pharmacy details: {str(e)}")
                self.logger.error(f"Store URL: {store_url}")
                return {}
        except Exception as e:
            self.logger.error(f"Exception when fetching Pennas Pharmacy details: {str(e)}")
            return {}
    
    async def fetch_all_locations_details(self):
        """
        Fetch details for all Pennas Pharmacy locations
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        self.logger.info("Fetching all Pennas Pharmacy locations...")
        
        try:
            # First get all basic location data
            locations = await self.fetch_locations()
            if not locations:
                return []
            
            # Initialize the list for storing complete pharmacy details
            all_details = []
            
            # Fetch details for each location
            for i, location in enumerate(locations):
                try:
                    self.logger.info(f"Processing details for Pennas Pharmacy location {i+1}/{len(locations)}: {location.get('name', '')}")
                    store_details = await self.fetch_pharmacy_details(location)
                    if store_details:
                        all_details.append(store_details)
                except Exception as e:
                    self.logger.warning(f"Error processing Pennas Pharmacy location {i}: {str(e)}")
            
            self.logger.info(f"Successfully processed {len(all_details)} Pennas Pharmacy locations")
            return all_details
        except Exception as e:
            self.logger.error(f"Exception when fetching all Pennas Pharmacy locations: {str(e)}")
            return []
    
    def _extract_store_details(self, soup, location):
        """
        Extract all store details from the pharmacy page
        
        Args:
            soup: BeautifulSoup object of the store page
            location: Basic location information
            
        Returns:
            Dictionary with complete pharmacy details
        """
        try:
            # Extract store information from HTML
            store_id = location.get('id', '')
            store_name = location.get('name', '')
            store_url = location.get('url', '')
            
            # Look for the rich text content section that contains the store details
            rich_text_div = soup.find('div', {'class': 'richTextWithImage'})
            
            if not rich_text_div:
                self.logger.error(f"Could not find the rich text content for {store_name}")
                return {
                    'brand': 'Pennas Pharmacy',
                    'name': store_name,
                    'store_id': store_id,
                    'website': store_url,
                    'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            
            # Find the rich text content that contains address and contact information
            rich_text = rich_text_div.find('div', {'class': 'richText'})
            if not rich_text:
                self.logger.error(f"Could not find the rich text content for {store_name}")
                return {
                    'brand': 'Pennas Pharmacy',
                    'name': store_name,
                    'store_id': store_id,
                    'website': store_url,
                    'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            
            # Extract address, which is typically in the first paragraph that contains "Shop" and/or postal code
            address = None
            address_para = rich_text.find('p', {'class': 'large'})
            if address_para and address_para.find('strong'):
                # Get all text within the strong tag, which should contain the address
                address_text = address_para.find('strong').get_text(separator='\n').strip()
                
                # Clean up any extra whitespace
                address = re.sub(r'\s+', ' ', address_text.replace('\n', ', ')).strip()
            
            # Extract phone, fax, and email from the text
            phone = None
            fax = None
            email = None
            
            # Find paragraphs that contain contact information
            contact_paras = rich_text.find_all('p')
            for para in contact_paras:
                para_text = para.get_text()
                
                # Extract phone number
                if "Phone:" in para_text:
                    phone_match = re.search(r'Phone:\s*(.+?)(?:\s*<br|\s*$|\s*Fax:)', str(para))
                    if phone_match:
                        phone = phone_match.group(1).strip()
                        # Clean up any HTML entities or tags
                        phone = re.sub(r'<[^>]+>', '', phone).strip()
                        phone = re.sub(r'&nbsp;', ' ', phone).strip()
                
                # Extract fax number
                if "Fax:" in para_text:
                    fax_match = re.search(r'Fax:\s*(.+?)(?:\s*<br|\s*$|\s*Email:)', str(para))
                    if fax_match:
                        fax = fax_match.group(1).strip()
                        # Clean up any HTML entities or tags
                        fax = re.sub(r'<[^>]+>', '', fax).strip()
                        fax = re.sub(r'&nbsp;', ' ', fax).strip()
                
                # Extract email
                email_link = para.find('a', href=lambda h: h and 'mailto:' in h)
                if email_link:
                    email = email_link.text.strip()
            
            # If the phone/fax extraction didn't work from regex, try another approach
            if not phone or not fax:
                for para in contact_paras:
                    para_text = para.get_text()
                    if "Phone:" in para_text and not phone:
                        lines = para_text.split('\n')
                        for line in lines:
                            if "Phone:" in line:
                                phone = line.replace("Phone:", "").strip()
                    if "Fax:" in para_text and not fax:
                        lines = para_text.split('\n')
                        for line in lines:
                            if "Fax:" in line:
                                fax = line.replace("Fax:", "").strip()
            
            # Parse address into components
            address_components = self._parse_address(address)
            
            # Extract trading hours
            trading_hours = self._extract_trading_hours(rich_text)
            
            # Create the final pharmacy details object
            result = {
                'brand': 'Pennas Pharmacy',
                'name': store_name,
                'store_id': store_id,
                'address': address,
                'street_address': address_components.get('street', ''),
                'suburb': address_components.get('suburb', ''),
                'state': address_components.get('state', ''),
                'postcode': address_components.get('postcode', ''),
                'phone': phone,
                'fax': fax,
                'email': email,
                'website': store_url,
                'trading_hours': trading_hours,
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Remove any None values
            return {k: v for k, v in result.items() if v is not None}
        except Exception as e:
            self.logger.error(f"Error extracting store details for {location.get('name', '')}: {str(e)}")
            return {
                'brand': 'Pennas Pharmacy',
                'name': store_name,
                'store_id': store_id,
                'website': store_url,
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
    
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
        
        try:
            # Split the address by commas or line breaks
            parts = [p.strip() for p in re.split(r',|\n', address)]
            
            # For NSW addresses, the last part typically has the format "SUBURB NSW POSTCODE"
            if len(parts) > 0:
                # The last part should contain state and postcode
                last_part = parts[-1]
                
                # Extract postcode (4 digits)
                postcode_match = re.search(r'(\d{4})', last_part)
                if postcode_match:
                    result['postcode'] = postcode_match.group(1)
                    
                    # Extract state code (2-3 letters)
                    state_match = re.search(r'\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b', last_part)
                    if state_match:
                        result['state'] = state_match.group(1)
                        
                        # Calculate suburb (everything else in the last part)
                        suburb_text = last_part
                        suburb_text = suburb_text.replace(result['postcode'], '').replace(result['state'], '').strip()
                        result['suburb'] = suburb_text
                
                # Street is everything before the last part
                if len(parts) > 1:
                    result['street'] = ' '.join(parts[:-1])
        except Exception as e:
            self.logger.error(f"Error parsing address: {str(e)}")
        
        return result
    
    def _extract_trading_hours(self, rich_text):
        """
        Extract trading hours from rich text content
        
        Args:
            rich_text: BeautifulSoup object containing the rich text content
            
        Returns:
            Dictionary with days as keys and hours as values
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
            'Public Holiday': {'open': 'Closed', 'closed': 'Closed'}
        }
        
        try:
            # Find all paragraphs that might contain trading hours
            paragraphs = rich_text.find_all('p')
            
            for p in paragraphs:
                p_text = p.get_text().strip()
                
                # Skip paragraphs that contain contact information
                if "Phone:" in p_text or "Fax:" in p_text or "Email:" in p_text:
                    continue
                
                # Look for lines with day and time patterns - common format in Pennas pages
                if "Monday" in p_text and "Friday" in p_text and any(x in p_text.lower() for x in ["am", "pm"]):
                    # This might be a trading hours paragraph
                    lines = p_text.split('\n')
                    
                    for line in lines:
                        line = line.strip()
                        if not line:
                            continue
                            
                        # Parse different day formats
                        days_to_update = []
                        
                        # Monday - Friday format
                        if "Monday" in line and "Friday" in line and "-" in line:
                            days_to_update = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
                        # Saturday specific
                        elif "Saturday" in line:
                            days_to_update = ['Saturday']
                        # Sunday & Public Holidays
                        elif "Sunday" in line and ("Public" in line or "&" in line):
                            days_to_update = ['Sunday', 'Public Holiday']
                        # Sunday only
                        elif "Sunday" in line:
                            days_to_update = ['Sunday']
                        # Public Holiday only
                        elif "Public Holiday" in line or "Public Holidays" in line:
                            days_to_update = ['Public Holiday']
                            
                        # Look for time pattern in the line
                        if days_to_update:
                            time_match = re.search(r'(\d{1,2}(?::\d{2})?(?:am|pm))\s*-\s*(\d{1,2}(?::\d{2})?(?:am|pm))', 
                                                 line, re.IGNORECASE)
                            if time_match:
                                open_time = self._standardize_time(time_match.group(1))
                                close_time = self._standardize_time(time_match.group(2))
                                
                                # Apply to all matched days
                                for day in days_to_update:
                                    trading_hours[day] = {
                                        'open': open_time,
                                        'closed': close_time
                                    }
        except Exception as e:
            self.logger.error(f"Error extracting trading hours: {str(e)}")
        
        return trading_hours
    
    def _standardize_time(self, time_str):
        """
        Standardize time format to "HH:MM AM/PM"
        
        Args:
            time_str: Time string like "8:00am" or "9pm"
            
        Returns:
            Standardized time string like "08:00 AM" or "09:00 PM"
        """
        # Remove any whitespace
        time_str = time_str.strip().lower()
        
        # Extract hours, minutes, and AM/PM
        if ':' in time_str:
            # Format like "8:00am"
            time_parts = re.match(r'(\d{1,2}):(\d{2})(am|pm)', time_str)
            if time_parts:
                hour = int(time_parts.group(1))
                minute = int(time_parts.group(2))
                am_pm = time_parts.group(3).upper()
            else:
                # Return as is if we can't parse
                return time_str
        else:
            # Format like "9pm"
            time_parts = re.match(r'(\d{1,2})(am|pm)', time_str)
            if time_parts:
                hour = int(time_parts.group(1))
                minute = 0
                am_pm = time_parts.group(2).upper()
            else:
                # Return as is if we can't parse
                return time_str
        
        # Format with leading zeros
        return f"{hour:02d}:{minute:02d} {am_pm}"