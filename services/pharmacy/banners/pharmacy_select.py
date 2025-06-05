import re
import logging
from bs4 import BeautifulSoup
from ..base_handler import BasePharmacyHandler

class PharmacySelectHandler(BasePharmacyHandler):
    """Handler for Pharmacy Select stores"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "pharmacy_select"
        self.base_url = self.pharmacy_locations.PHARMACY_SELECT_URL
        
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
        }
        self.logger = logging.getLogger(__name__)
        
    async def fetch_locations(self):
        """
        Fetch all Pharmacy Select locations.
        
        Returns:
            List of Pharmacy Select locations
        """
        try:
            response = await self.session_manager.get(
                url=self.base_url,
                headers=self.headers
            )
            
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch Pharmacy Select locations: HTTP {response.status_code}")
                return []
                
            html_content = response.text
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find all store divs with the specific class
            store_divs = soup.find_all('div', class_='wp-block-media-text alignwide has-media-on-the-right is-stacked-on-mobile')
            
            if not store_divs:
                self.logger.warning("No store divs found with target class")
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
            
            self.logger.info(f"Found {len(locations)} Pharmacy Select locations")
            return locations
            
        except Exception as e:
            self.logger.error(f"Exception when fetching Pharmacy Select locations: {str(e)}")
            return []
    
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
                'id': f"pharmacy_select_{index}",
                'brand': 'Pharmacy Select'
            }
            
            # Find the content area within the media text block
            content_div = store_div.find('div', class_='wp-block-media-text__content')
            if not content_div:
                self.logger.warning(f"No content div found in store {index}")
                return None
            
            # Get the text content - all the info is in paragraph tags
            content_text = content_div.get_text(separator='\n', strip=True)
            
            if not content_text:
                self.logger.warning(f"No text content found in store {index}")
                return None
            
            # Parse the content line by line
            lines = [line.strip() for line in content_text.split('\n') if line.strip()]
            
            if not lines:
                return None
            
            # Extract store name (first line, usually ends with "Pharmacy Select")
            store_name = lines[0] if lines else ""
            if store_name:
                store_data['name'] = store_name
            
            # Parse the rest of the information
            address_parts = []
            current_line_idx = 1
            
            # Collect address lines until we hit phone/fax/email
            while current_line_idx < len(lines):
                line = lines[current_line_idx]
                
                # Check if this line contains phone, fax, or email
                if any(keyword in line.lower() for keyword in ['ph:', 'phone:', 'fax:', 'email:']):
                    break
                    
                # If it's not contact info, it's part of the address
                address_parts.append(line)
                current_line_idx += 1
            
            # Join address parts
            if address_parts:
                store_data['address'] = ' '.join(address_parts)
            
            # Parse contact information from remaining lines
            while current_line_idx < len(lines):
                line = lines[current_line_idx]
                
                # Extract phone number
                if line.lower().startswith('ph:') or line.lower().startswith('phone:'):
                    # Extract phone and fax from the same line if present
                    parts = line.split('Fax:')
                    if len(parts) == 2:
                        # Phone and fax on same line
                        phone_part = parts[0].replace('Ph:', '').replace('Phone:', '').strip()
                        fax_part = parts[1].strip()
                        store_data['phone'] = phone_part
                        store_data['fax'] = fax_part
                    else:
                        # Only phone on this line
                        phone = line.replace('Ph:', '').replace('Phone:', '').strip()
                        store_data['phone'] = phone
                
                # Extract fax (if not already extracted)
                elif line.lower().startswith('fax:') and 'fax' not in store_data:
                    fax = line.replace('Fax:', '').strip()
                    store_data['fax'] = fax
                
                # Extract email
                elif line.lower().startswith('email:'):
                    # Look for email link in the original HTML
                    email_link = content_div.find('a', href=lambda x: x and 'mailto:' in x)
                    if email_link:
                        email = email_link.get('href').replace('mailto:', '')
                        store_data['email'] = email
                    else:
                        # Fallback: extract from text
                        email_text = line.replace('Email:', '').strip()
                        store_data['email'] = email_text
                
                current_line_idx += 1
            
            # Extract state/location from address for geocoding (basic)
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
        
        # For Pharmacy Select, the data is already extracted in the right format
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
        Fetch details for all Pharmacy Select locations
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        self.logger.info("Fetching all Pharmacy Select locations...")
        
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
            
            self.logger.info(f"Successfully processed {len(all_details)} Pharmacy Select locations")
            return all_details
            
        except Exception as e:
            self.logger.error(f"Exception when fetching all Pharmacy Select location details: {str(e)}")
            return []
