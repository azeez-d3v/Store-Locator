from ..base_handler import BasePharmacyHandler
import logging
import re
import json
from datetime import datetime
from bs4 import BeautifulSoup

class HealthyWorldPharmacyHandler(BasePharmacyHandler):
    """Handler for Healthy World Pharmacy stores"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "healthy_world"
        self.base_url = "https://healthyworldpharmacy.com.au/pages/locations"
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
        }
        self.logger = logging.getLogger(__name__)
        
    async def fetch_locations(self):
        """
        Fetch all Healthy World Pharmacy store locations from the locations page
        
        Returns:
            List of Healthy World Pharmacy locations with basic details
        """
        try:
            # Make request to the locations page
            response = await self.session_manager.get(
                url=self.base_url,
                headers=self.headers
            )
            
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch Healthy World Pharmacy locations: HTTP {response.status_code}")
                return []
            
            # Parse the HTML content
            try:
                # Use BeautifulSoup with 'html.parser'
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Find the content div containing the store information
                content_div = soup.find('div', {'class': 'page__content rte'})
                if not content_div:
                    self.logger.error("Could not find the content div on the Healthy World Pharmacy locations page")
                    return []
                
                # Define the store name regions
                regions = []
                current_region = None
                
                # First pass: identify all regions (Brisbane, Gold Coast, etc.)
                for div in content_div.find_all('div', {'style': 'text-align: center;'}):
                    region_span = div.find('span', style=lambda s: s and 'color: #2b00ff' in s)
                    if region_span and region_span.find('b'):
                        region_name = region_span.find('b').text.strip()
                        if region_name:
                            current_region = region_name
                            regions.append((current_region, []))
                
                # If no regions found, fallback to old approach
                if not regions:
                    # Store names are in bold red text
                    store_name_elements = content_div.find_all('span', style=lambda s: s and 'color: #ff2a00' in s)
                    
                    # Initialize the list for storing basic pharmacy information
                    all_locations = []
                    
                    for i, store_element in enumerate(store_name_elements):
                        try:
                            # Extract store name from the bold element inside the span
                            bold_element = store_element.find('b')
                            if not bold_element:
                                continue
                                
                            store_name = bold_element.text.strip()
                            
                            # Create a unique ID based on the name
                            store_id = f"hw-{i+1}"
                            
                            # Create basic location info
                            location = {
                                'id': store_id,
                                'name': store_name,
                                'url': self.base_url,
                                'brand': 'Healthy World Pharmacy'
                            }
                            
                            all_locations.append(location)
                        except Exception as e:
                            self.logger.warning(f"Error extracting Healthy World Pharmacy location item {i}: {str(e)}")
                    
                    self.logger.info(f"Found {len(all_locations)} Healthy World Pharmacy locations")
                    return all_locations
                
                # Second pass: find pharmacy names in each region
                current_region_idx = 0
                for div in content_div.find_all('div', {'style': 'text-align: center;'}):
                    # Check if this div contains a red-colored span (store name)
                    store_span = div.find('span', style=lambda s: s and 'color: #ff2a00' in s)
                    
                    if store_span:
                        # Find all bold elements within this span - some store names are split across multiple tags
                        bold_elements = store_span.find_all('b')
                        
                        if bold_elements:
                            # Combine all bold text to get the full store name
                            store_name_parts = [b.text.strip() for b in bold_elements]
                            store_name = ' '.join(store_name_parts).strip()
                            
                            # Clean up any double spaces or trailing spaces
                            store_name = re.sub(r'\s+', ' ', store_name).strip()
                            
                            # Make sure we have a valid store name
                            if "Healthyworld Pharmacy" in store_name:
                                # Determine which region this belongs to based on div position
                                region_found = False
                                for i, (region_name, store_list) in enumerate(regions):
                                    if i > current_region_idx:
                                        # We're in a new region
                                        if region_found:
                                            break
                                        current_region_idx = i
                                        region_found = True
                                    
                                    # Add the store to the current region
                                    regions[current_region_idx][1].append(store_name)
                                    break
                
                # Finally, create location objects for all stores
                all_locations = []
                store_counter = 1
                
                for region_name, store_names in regions:
                    for store_name in store_names:
                        store_id = f"hw-{store_counter}"
                        store_counter += 1
                        
                        location = {
                            'id': store_id,
                            'name': store_name,
                            'url': self.base_url,
                            'brand': 'Healthy World Pharmacy',
                            'region': region_name
                        }
                        
                        all_locations.append(location)
                
                self.logger.info(f"Found {len(all_locations)} Healthy World Pharmacy locations")
                return all_locations
            except Exception as e:
                self.logger.error(f"HTML parsing error for Healthy World Pharmacy locations: {str(e)}")
                return []
        except Exception as e:
            self.logger.error(f"Exception when fetching Healthy World Pharmacy locations: {str(e)}")
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
            
        # For Healthy World Pharmacy, data is already in the right format from _extract_store_details
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
            # For Healthy World Pharmacy, all details are on a single page
            # We'll reuse the locations page URL
            store_url = self.base_url
            
            # Make request to the store page
            response = await self.session_manager.get(
                url=store_url,
                headers=self.headers
            )
            
            self.logger.info(f"Fetching details for {location.get('name', '')} from {store_url}")
            
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch Healthy World Pharmacy details: HTTP {response.status_code}")
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
                self.logger.error(f"HTML parsing error for Healthy World Pharmacy details: {str(e)}")
                self.logger.error(f"Store URL: {store_url}")
                return {}
        except Exception as e:
            self.logger.error(f"Exception when fetching Healthy World Pharmacy details: {str(e)}")
            return {}
    
    async def fetch_all_locations_details(self):
        """
        Fetch details for all Healthy World Pharmacy locations
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        self.logger.info("Fetching all Healthy World Pharmacy locations...")
        
        try:
            # First get all basic location data
            locations = await self.fetch_locations()
            if not locations:
                return []
            
            # Initialize the list for storing complete pharmacy details
            all_details = []
            
            # Make a single request to get the page content with all store details
            response = await self.session_manager.get(
                url=self.base_url,
                headers=self.headers
            )
            
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch Healthy World Pharmacy details: HTTP {response.status_code}")
                return []
            
            # Parse the HTML content once
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Process all locations at once to extract the complete details properly
            store_details_map = self._extract_all_store_details(soup, locations)
            
            # Add all valid store details to the results
            for store_id, details in store_details_map.items():
                if details:
                    all_details.append(details)
            
            self.logger.info(f"Successfully processed {len(all_details)} Healthy World Pharmacy locations")
            return all_details
        except Exception as e:
            self.logger.error(f"Exception when fetching all Healthy World Pharmacy locations: {str(e)}")
            return []
    
    def _extract_all_store_details(self, soup, locations):
        """
        Extract details for all stores at once to prevent duplicates
        
        Args:
            soup: BeautifulSoup object of the store page
            locations: List of basic location information
            
        Returns:
            Dictionary mapping store_ids to their complete details
        """
        try:
            # Create a dictionary to store results by store_id
            result_map = {}
            
            # Find the content div containing the store information
            content_div = soup.find('div', {'class': 'page__content rte'})
            if not content_div:
                self.logger.error("Could not find the content div on the Healthy World Pharmacy locations page")
                return {}
            
            # Keep track of processed store names to avoid duplicates
            processed_names = set()
            
            # Create a dictionary to map store names to their location data
            store_location_map = {}
            for location in locations:
                store_location_map[location.get('name', '')] = location
            
            # Scan the HTML by regions to find all stores
            regions = []
            current_region = None
            region_divs = []
            
            # First collect all divs by region
            for div in content_div.find_all('div', {'style': 'text-align: center;'}):
                # Check if this is a region header (blue text)
                region_span = div.find('span', style=lambda s: s and 'color: #2b00ff' in s)
                if region_span and region_span.find('b'):
                    # Found a new region, save the previous one if it exists
                    if current_region and region_divs:
                        regions.append((current_region, region_divs))
                    
                    # Start a new region
                    current_region = region_span.find('b').text.strip()
                    region_divs = [div]
                elif current_region:
                    # Add this div to the current region
                    region_divs.append(div)
            
            # Add the last region if we have one
            if current_region and region_divs:
                regions.append((current_region, region_divs))
            
            # Process each region to find stores
            for region_name, region_divs in regions:
                i = 0
                while i < len(region_divs):
                    # Check if this div is a store name (red text)
                    div = region_divs[i]
                    store_span = div.find('span', style=lambda s: s and 'color: #ff2a00' in s)
                    
                    if store_span:
                        # Get all bold elements - handle cases where the name is split across multiple tags
                        bold_elements = store_span.find_all('b')
                        if bold_elements:
                            # Combine all bold text to get the full store name
                            store_name_parts = [b.text.strip() for b in bold_elements]
                            store_name = ' '.join(store_name_parts).strip()
                            
                            # Clean up the store name
                            store_name = re.sub(r'\s+', ' ', store_name).strip()
                            
                            # Only process valid store names
                            if store_name and "Healthyworld Pharmacy" in store_name:
                                # Look for this store in our locations list
                                location = None
                                for loc in locations:
                                    # Try exact match first
                                    if loc.get('name', '') == store_name:
                                        location = loc
                                        break
                                    
                                    # Try partial match if exact match fails
                                    if not location and store_name in loc.get('name', '') or loc.get('name', '') in store_name:
                                        location = loc
                                
                                # Create a new location if we couldn't find an existing one
                                if not location:
                                    store_id = f"hw-new-{len(locations) + 1}"
                                    location = {
                                        'id': store_id,
                                        'name': store_name,
                                        'url': self.base_url,
                                        'brand': 'Healthy World Pharmacy',
                                        'region': region_name
                                    }
                                
                                # Collect store details from following divs
                                store_divs = [div]
                                j = i + 1
                                while j < len(region_divs):
                                    next_div = region_divs[j]
                                    # Stop if we find another store name or region header
                                    if (next_div.find('span', style=lambda s: s and 'color: #ff2a00' in s) or 
                                        next_div.find('span', style=lambda s: s and 'color: #2b00ff' in s)):
                                        break
                                    store_divs.append(next_div)
                                    j += 1
                                
                                # Process store details
                                address_text = []
                                phone = None
                                email = None
                                
                                for store_div in store_divs:
                                    # Skip the store name div itself for address extraction
                                    if store_div == div:
                                        continue
                                        
                                    # Extract address from divs that don't contain email or phone
                                    div_text = store_div.text.strip()
                                    if div_text and "Email:" not in div_text and "Phone" not in div_text:
                                        if div_text and not div_text.isspace():
                                            address_text.append(div_text)
                                    
                                    # Extract email
                                    email_link = store_div.find('a', href=lambda h: h and 'mailto:' in h)
                                    if email_link:
                                        email = email_link.text.strip()
                                    
                                    # Extract phone
                                    if "Phone" in div_text:
                                        # Various phone patterns
                                        phone_match = re.search(r'\(0\d\)\s*\d{4}\s*\d{4}', div_text)
                                        if phone_match:
                                            phone = phone_match.group(0)
                                        else:
                                            # Try another pattern for mobile numbers
                                            phone_match = re.search(r'0\d{3}\s*\d{3}\s*\d{3}', div_text)
                                            if phone_match:
                                                phone = phone_match.group(0)
                                        
                                        # If still not found, look for digits after "Phone"
                                        if not phone:
                                            phone_match = re.search(r'Phone[^\d]*(\d[\d\s]+)', div_text)
                                            if phone_match:
                                                phone = phone_match.group(1).strip()
                                
                                # Combine address parts and clean up
                                address = ', '.join(address_text).strip()
                                
                                # Parse address into components
                                address_components = self._parse_address(address)
                                
                                # Create the final pharmacy details object
                                result = {
                                    'brand': 'Healthy World Pharmacy',
                                    'name': store_name,
                                    'store_id': location.get('id', ''),
                                    'address': address,
                                    'street_address': address_components.get('street', ''),
                                    'suburb': address_components.get('suburb', ''),
                                    'state': address_components.get('state', ''),
                                    'postcode': address_components.get('postcode', ''),
                                    'phone': self._format_phone(phone),
                                    'email': email,
                                    'website': self.base_url,
                                    'region': region_name,
                                    'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                }
                                
                                # Remove any None values
                                clean_result = {k: v for k, v in result.items() if v is not None}
                                
                                # Add to results
                                result_map[location.get('id', '')] = clean_result
                                
                                # Skip the divs we've processed
                                i = j - 1
                    
                    # Move to the next div
                    i += 1
            
            return result_map
        except Exception as e:
            self.logger.error(f"Error extracting all store details: {str(e)}")
            return {}
    
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
            
            # Initialize variables for store details
            address = None
            phone = None
            email = None
            
            # Find the content div containing the store information
            content_div = soup.find('div', {'class': 'page__content rte'})
            if not content_div:
                self.logger.error("Could not find the content div on the Healthy World Pharmacy locations page")
                return {
                    'brand': 'Healthy World Pharmacy',
                    'name': store_name,
                    'store_id': store_id,
                    'website': store_url,
                    'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            
            # Find the specific pharmacy section by looking for the store name
            store_section = None
            for span in content_div.find_all('span', style=lambda s: s and 'color: #ff2a00' in s):
                if span.find('b') and store_name in span.text:
                    # Found the matching store section
                    store_section = span.parent
                    
                    # Get all divs following this one until the next store section or end
                    store_details_divs = []
                    current_div = store_section
                    while current_div and current_div.next_sibling:
                        current_div = current_div.next_sibling
                        if current_div.name == 'div':
                            # Stop if we've reached another store name (red text)
                            if current_div.find('span', style=lambda s: s and 'color: #ff2a00' in s):
                                break
                            store_details_divs.append(current_div)
                    
                    # Extract address from the divs following the store name
                    address_text = []
                    for div in store_details_divs:
                        # Skip divs with "Email:" or "Phone" as they contain contact info, not address
                        if div.text and "Email:" not in div.text and "Phone" not in div.text:
                            address_text.append(div.text.strip())
                        
                        # Extract email
                        email_link = div.find('a', href=lambda h: h and 'mailto:' in h)
                        if email_link:
                            email = email_link.text.strip()
                        
                        # Extract phone
                        if "Phone" in div.text:
                            # Use regex to extract phone number
                            phone_match = re.search(r'\(0\d\)\s*\d{4}\s*\d{4}', div.text)
                            if phone_match:
                                phone = phone_match.group(0)
                            else:
                                # Try another pattern for mobile numbers
                                phone_match = re.search(r'0\d{3}\s*\d{3}\s*\d{3}', div.text)
                                if phone_match:
                                    phone = phone_match.group(0)
                            
                            # If still not found, look for digits after "Phone"
                            if not phone:
                                phone_match = re.search(r'Phone[^\d]*(\d[\d\s]+)', div.text)
                                if phone_match:
                                    phone = phone_match.group(1).strip()
                    
                    # Combine address parts
                    address = ', '.join(address_text).strip()
                    break
            
            # If no specific section found for this store, return basic information
            if not store_section:
                self.logger.warning(f"Could not find specific section for store: {store_name}")
                return {
                    'brand': 'Healthy World Pharmacy',
                    'name': store_name,
                    'store_id': store_id,
                    'website': store_url,
                    'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            
            # Parse address into components
            address_components = self._parse_address(address)
            
            # Create the final pharmacy details object
            result = {
                'brand': 'Healthy World Pharmacy',
                'name': store_name,
                'store_id': store_id,
                'address': address,
                'street_address': address_components.get('street', ''),
                'suburb': address_components.get('suburb', ''),
                'state': address_components.get('state', ''),
                'postcode': address_components.get('postcode', ''),
                'phone': self._format_phone(phone),
                'email': email,
                'website': store_url,
                # Note: Trading hours are not available on the website
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Remove any None values
            return {k: v for k, v in result.items() if v is not None}
        except Exception as e:
            self.logger.error(f"Error extracting store details for {location.get('name', '')}: {str(e)}")
            return {
                'brand': 'Healthy World Pharmacy',
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
        
        # Normalize address - replace multiple whitespace with single space
        normalized_address = re.sub(r'\s+', ' ', address)
        
        # Australian full state names and their abbreviations
        state_mapping = {
            'NEW SOUTH WALES': 'NSW',
            'VICTORIA': 'VIC',
            'QUEENSLAND': 'QLD',
            'SOUTH AUSTRALIA': 'SA',
            'WESTERN AUSTRALIA': 'WA',
            'TASMANIA': 'TAS',
            'NORTHERN TERRITORY': 'NT',
            'AUSTRALIAN CAPITAL TERRITORY': 'ACT',
            # Keep abbreviations for backward compatibility
            'NSW': 'NSW',
            'VIC': 'VIC',
            'QLD': 'QLD',
            'SA': 'SA',
            'WA': 'WA',
            'TAS': 'TAS',
            'NT': 'NT',
            'ACT': 'ACT'
        }
        
        # For Healthy World Pharmacy, most addresses will end with ", Australia"
        # Strip this part to make parsing easier
        if normalized_address.endswith(', Australia'):
            normalized_address = normalized_address[:-10].strip()
        
        # Try to match patterns like "Street, Suburb, State Postcode"
        address_patterns = [
            # Pattern for: Street, Suburb, State Postcode
            r'(.*?),\s*([^,]+?),\s*([A-Za-z]{2,3})\s+(\d{4})$',
            # Pattern for: Street, Suburb Postcode (missing state)
            r'(.*?),\s*([^,]+?)\s+(\d{4})$',
            # Pattern for: Street, Suburb, Postcode (missing state)
            r'(.*?),\s*([^,]+?),\s*(\d{4})$'
        ]
        
        for pattern in address_patterns:
            match = re.search(pattern, normalized_address)
            if match:
                if len(match.groups()) == 4:  # Full pattern with state
                    street = match.group(1).strip()
                    suburb = match.group(2).strip()
                    state = match.group(3).strip().upper()
                    postcode = match.group(4).strip()
                    
                    # Validate and convert state
                    state_abbr = state_mapping.get(state, state)
                    
                    result = {
                        'street': street,
                        'suburb': suburb,
                        'state': state_abbr,
                        'postcode': postcode
                    }
                    return result
                elif len(match.groups()) == 3:  # Pattern missing state
                    street = match.group(1).strip()
                    suburb = match.group(2).strip()
                    postcode = match.group(3).strip()
                    
                    # For Healthy World Pharmacy, all locations are in QLD
                    # Based on the provided HTML structure
                    state = 'QLD'
                    
                    result = {
                        'street': street,
                        'suburb': suburb,
                        'state': state,
                        'postcode': postcode
                    }
                    return result
        
        # If no patterns matched, try a simpler approach by looking for the postcode
        postcode_match = re.search(r'(\d{4})', normalized_address)
        if postcode_match:
            postcode = postcode_match.group(1)
            result['postcode'] = postcode
            
            # For Healthy World, assume QLD for all locations
            result['state'] = 'QLD'
            
            # Try to extract the suburb and street
            before_postcode = normalized_address[:postcode_match.start()].strip()
            if ',' in before_postcode:
                parts = before_postcode.split(',')
                result['street'] = ','.join(parts[:-1]).strip()
                result['suburb'] = parts[-1].strip()
            else:
                result['street'] = before_postcode
        else:
            # If all else fails, just use the whole address as the street
            result['street'] = normalized_address
            result['state'] = 'QLD'  # Assume QLD for Healthy World
        
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
            
        # Remove non-numeric characters except for the leading + if present
        if phone.startswith('+'):
            digits_only = '+' + re.sub(r'\D', '', phone[1:])
        else:
            digits_only = re.sub(r'\D', '', phone)
        
        # Handle Australian phone number formats
        if len(digits_only) == 10 and digits_only.startswith('0'):
            # Format as 0X XXXX XXXX
            return f"{digits_only[0:2]} {digits_only[2:6]} {digits_only[6:10]}"
        elif len(digits_only) == 8:
            # Local number, no area code - add QLD area code
            return f"07 {digits_only[0:4]} {digits_only[4:8]}"
        else:
            # Return original if we can't standardize
            return phone.strip()