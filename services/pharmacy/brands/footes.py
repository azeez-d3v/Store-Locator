import asyncio
import re
from rich import print
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import warnings
from ..base_handler import BasePharmacyHandler
from ..utils import decode_cloudflare_email, extract_state_postcode, extract_trading_hours

# Filter out the XML parsed as HTML warning
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

class FootesHandler(BasePharmacyHandler):
    """Handler for Footes Pharmacies"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "footes"
        # Define Footes-specific headers
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'sec-ch-ua': '"Microsoft Edge";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0'
        }
        
    async def fetch_locations(self):
        """
        Fetch all Footes Pharmacy locations using the sitemap XML.
        
        Returns:
            List of Footes Pharmacy locations with basic information
        """
        # First fetch the sitemap XML to get all store URLs
        response = await self.session_manager.get(
            url=self.pharmacy_locations.FOOTES_SITEMAP_URL,
            headers=self.headers
        )
        
        if response.status_code == 200:
            xml_content = response.text
            
            # Parse as XML with correct features
            soup = BeautifulSoup(xml_content, 'xml' if 'xml' in 'lxml' else 'html.parser')
            
            # Find all store URLs in the XML sitemap format
            store_links = []
            url_elements = soup.find_all('url')
            
            if url_elements:
                for url_elem in url_elements:
                    loc_elem = url_elem.find('loc')
                    if loc_elem:
                        store_url = loc_elem.text
                        if store_url and '/stores/' in store_url and not store_url.endswith('/stores/'):
                            store_links.append(store_url)
                
                if not store_links:
                    print("No store links found in sitemap")
                    return []
                
                print(f"Found {len(store_links)} store links in sitemap")
                
                # Process each store URL to extract basic information
                locations = []
                for store_url in store_links:
                    try:
                        # Extract store name from URL
                        store_name = store_url.rstrip('/').split('/')[-1].replace('-', ' ').title()
                        
                        pharmacy_data = {
                            'name': f"Footes Pharmacy {store_name}",
                            'detail_url': store_url,
                            'id': f"footes_{store_name.lower().replace(' ', '_')}"
                        }
                        
                        locations.append(pharmacy_data)
                    except Exception as e:
                        print(f"Error processing Footes Pharmacy location URL {store_url}: {e}")
                
                if locations:
                    # Now fetch additional details for each location
                    return await self.enrich_locations(locations)
                else:
                    print("No pharmacy data could be extracted from the sitemap links")
                    raise Exception("No pharmacy data could be extracted from Footes Pharmacy sitemap")
            else:
                # Try alternative approach by directly parsing the XML with regex if parsing fails
                print("No URL elements found in sitemap XML, trying regex approach")
                url_pattern = r'<loc>(https://footespharmacies\.com/stores/[^/]+/)</loc>'
                matches = re.findall(url_pattern, xml_content)
                
                if matches:
                    print(f"Found {len(matches)} store links with regex")
                    store_links = matches
                    
                    # Process each store URL just as above
                    locations = []
                    for store_url in store_links:
                        try:
                            store_name = store_url.rstrip('/').split('/')[-1].replace('-', ' ').title()
                            
                            pharmacy_data = {
                                'name': f"Footes Pharmacy {store_name}",
                                'detail_url': store_url,
                                'id': f"footes_{store_name.lower().replace(' ', '_')}"
                            }
                            
                            locations.append(pharmacy_data)
                        except Exception as e:
                            print(f"Error processing Footes Pharmacy location URL {store_url}: {e}")
                    
                    if locations:
                        return await self.enrich_locations(locations)
                
                print("No store links found in sitemap with any method")
                print(f"Sitemap XML preview: {xml_content[:300]}...")
                raise Exception("No pharmacy locations found in Footes Pharmacy sitemap")
        else:
            raise Exception(f"Failed to fetch Footes Pharmacy sitemap: {response.status_code}")
    
    async def enrich_locations(self, locations):
        """
        Fetch additional details for Footes Pharmacy locations.
        
        Args:
            locations: List of location dictionaries with store URLs
            
        Returns:
            List of locations with additional details
        """
        enriched_locations = []
        
        # Process 5 locations at a time to avoid overwhelming the server
        batch_size = 5
        for i in range(0, len(locations), batch_size):
            batch = locations[i:i+batch_size]
            print(f"Processing Footes locations batch {i//batch_size + 1}/{(len(locations) + batch_size - 1)//batch_size}")
            
            # Create tasks for fetching details
            tasks = []
            for location in batch:
                task = self.fetch_store_details(location)
                tasks.append(task)
            
            # Run tasks concurrently
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for result in results:
                if isinstance(result, Exception):
                    print(f"Error fetching Footes store details: {result}")
                elif result:
                    enriched_locations.append(result)
        
        print(f"Successfully processed {len(enriched_locations)} out of {len(locations)} Footes locations")
        return enriched_locations
    
    async def fetch_store_details(self, location):
        """
        Fetch details for a specific Footes Pharmacy location.
        
        Args:
            location: Dictionary containing basic location information including detail_url
            
        Returns:
            Dictionary with enriched location data
        """
        try:
            store_url = location.get('detail_url')
            if not store_url:
                return location
            
            response = await self.session_manager.get(
                url=store_url,
                headers=self.headers
            )
            
            if response.status_code != 200:
                print(f"Failed to fetch details for {location.get('name')}: {response.status_code}")
                return location
            
            html_content = response.text
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract address from heading element
            address_element = soup.select_one('.elementor-element-d9bbb9b .elementor-heading-title, [data-id="d9bbb9b"] .elementor-heading-title')
            if address_element:
                address = address_element.text.strip()
                location['address'] = address
                
                # Try to extract state and postcode from address
                state, postcode = extract_state_postcode(address)
                if state:
                    location['state'] = state
                if postcode:
                    location['postcode'] = postcode
            
            # Extract phone number from store-phone element
            phone_element = soup.select_one('.store-phone a')
            if phone_element:
                phone = phone_element.text.strip()
                location['phone'] = phone
            
            # Extract fax number - using the class name or finding text that contains "Fx:"
            fax_element = soup.select_one('.elementor-element-2008741, [data-id="2008741"]')
            if fax_element:
                fax_text = fax_element.get_text(strip=True)
                if "Fx:" in fax_text:
                    location['fax'] = fax_text.replace("Fx:", "").strip()
                else:
                    location['fax'] = fax_text
            
            # Extract email from store-email element - handling Cloudflare email protection
            email_element = soup.select_one('.store-email a, a.store-email, a[href^="/cdn-cgi/l/email-protection"]')
            if email_element:
                # Check if email is protected by Cloudflare
                cf_email_span = email_element.select_one('span.__cf_email__')
                if cf_email_span and cf_email_span.has_attr('data-cfemail'):
                    # Get the encoded email
                    encoded_email = cf_email_span.get('data-cfemail')
                    # Decode the email
                    try:
                        email = decode_cloudflare_email(encoded_email)
                        location['email'] = email
                    except Exception as e:
                        print(f"Error decoding Cloudflare email: {e}")
                else:
                    # Regular email extraction
                    email = email_element.text.strip()
                    location['email'] = email
            
            # Extract trading hours - new structure with days and hours in separate columns
            trading_hours = {}
            
            # Days are in one column, hours in another 
            day_elements = soup.select('.elementor-element-fb1522c .elementor-widget-text-editor')
            hour_elements = soup.select('.elementor-element-b96bcb7 .elementor-widget-text-editor')
            
            # Map each day to its hours
            for i in range(min(len(day_elements), len(hour_elements))):
                day_text = day_elements[i].text.strip()
                hour_text = hour_elements[i].text.strip()
                
                # Process trading hours using the utility function
                day_hours = extract_trading_hours(f"{day_text}: {hour_text}", 'range')
                if day_hours:
                    trading_hours.update(day_hours)
            
            # If we found trading hours, add them to the location
            if trading_hours:
                location['trading_hours'] = trading_hours
                
            # Add website
            location['website'] = 'https://footespharmacies.com/'
            
            # If we still don't have basic fields, search more broadly
            if not location.get('phone') or not location.get('address') or not location.get('email') or not location.get('fax'):
                # Try more general selectors for missing information
                
                # Try to find phone by looking for tel: links if not found yet
                if not location.get('phone'):
                    phone_links = soup.select('a[href^="tel:"]')
                    if phone_links:
                        location['phone'] = phone_links[0].text.strip()
                
                # Try to find email by looking for all potential CloudFlare protected emails
                if not location.get('email'):
                    all_cf_emails = soup.select('span.__cf_email__')
                    for cf_email in all_cf_emails:
                        if cf_email.has_attr('data-cfemail'):
                            try:
                                encoded_email = cf_email.get('data-cfemail')
                                email = decode_cloudflare_email(encoded_email)
                                location['email'] = email
                                break
                            except Exception as e:
                                print(f"Error decoding additional CloudFlare email: {e}")
                        
                # Try to find fax in any text containing "Fx:" if not found yet
                if not location.get('fax'):
                    for element in soup.select('.elementor-text-editor'):
                        text = element.get_text(strip=True)
                        if 'Fx:' in text:
                            location['fax'] = text.replace('Fx:', '').strip()
                            break
                
                # Try to find address in any heading if not found yet
                if not location.get('address'):
                    for element in soup.select('.elementor-heading-title'):
                        text = element.text.strip()
                        # Check for address pattern (look for postcode)
                        if re.search(r'\b\d{4}\b', text):
                            location['address'] = text
                            # Extract state and postcode
                            state, postcode = extract_state_postcode(text)
                            if state:
                                location['state'] = state
                            if postcode:
                                location['postcode'] = postcode
                            break
            
            return location
        except Exception as e:
            print(f"Error fetching details for Footes store {location.get('name')}: {e}")
            return location
            
    async def fetch_pharmacy_details(self, location_id):
        """
        For Footes, we already have all the data in the locations response
        This is a placeholder for API consistency
        
        Args:
            location_id: The ID of the location to get details for
            
        Returns:
            Location data (unchanged)
        """
        return {"location_details": location_id}
    
    async def fetch_all_locations_details(self):
        """
        Fetch details for all Footes locations and return as a list
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        # For Footes, all details are included in the locations endpoint
        print(f"Fetching all Footes Pharmacy locations...")
        locations = await self.fetch_locations()
        if not locations:
            print(f"No Footes Pharmacy locations found.")
            return []
            
        print(f"Found {len(locations)} Footes Pharmacy locations. Processing details...")
        all_details = []
        
        for location in locations:
            try:
                extracted_details = self.extract_pharmacy_details(location)
                all_details.append(extracted_details)
            except Exception as e:
                print(f"Error processing Footes Pharmacy location {location.get('id')}: {e}")
                
        print(f"Completed processing details for {len(all_details)} Footes Pharmacy locations.")
        return all_details
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract specific fields from Footes pharmacy location data
        
        Args:
            pharmacy_data: The raw pharmacy data from the API
            
        Returns:
            Dictionary containing standardized pharmacy details
        """
        # Extract address, state, and postcode
        address = pharmacy_data.get('address', '')
        state = pharmacy_data.get('state', '')
        postcode = pharmacy_data.get('postcode', '')
        
        # Try to extract suburb from address
        suburb = None
        if address:
            # Try to parse out the suburb from the address
            # First remove state and postcode if present
            address_without_state = re.sub(r'\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b', '', address)
            address_without_postcode = re.sub(r'\b\d{4}\b', '', address_without_state)
            
            # Check if there's a comma in the address that might separate street and suburb
            address_parts = address_without_postcode.split(',')
            if len(address_parts) > 1:
                suburb = address_parts[-1].strip()
        
        # Parse trading hours
        trading_hours = pharmacy_data.get('trading_hours', {})
        
        # Using fixed column order
        result = {
            'name': pharmacy_data.get('name'),
            'address': address,
            'email': pharmacy_data.get('email'),
            'fax': pharmacy_data.get('fax'),
            'latitude': None,  # No coordinates in the data
            'longitude': None, # No coordinates in the data
            'phone': pharmacy_data.get('phone'),
            'postcode': postcode,
            'state': state,
            'street_address': address,
            'suburb': suburb,
            'trading_hours': trading_hours,
            'website': pharmacy_data.get('website', 'https://footespharmacies.com/')
        }
        
        # Remove any None values to keep the data clean
        cleaned_result = {}
        for key, value in result.items():
            if value is not None:
                cleaned_result[key] = value
                
        return cleaned_result