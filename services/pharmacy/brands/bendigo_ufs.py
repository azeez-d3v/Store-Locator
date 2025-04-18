import re
import logging
from bs4 import BeautifulSoup
from ..base_handler import BasePharmacyHandler

class BendigoUfsHandler(BasePharmacyHandler):
    """Handler for Bendigo UFS Pharmacies"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "bendigo_ufs"
        # Define Bendigo UFS-specific headers
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
        Fetch Bendigo UFS pharmacies from their sitemap XML
        
        Returns:
            List of pharmacy URLs to scrape
        """
        logging.info("Fetching Bendigo UFS pharmacy URLs from sitemap")
        response = await self.session_manager.get(
            url=self.pharmacy_locations.BENDIGO_UFS_SITEMAP_URL,
            headers=self.headers
        )
        
        pharmacy_urls = []
        if response.status_code == 200:
            xml_content = response.text
            # Parse XML with lxml parser
            soup = BeautifulSoup(xml_content, 'lxml')
            
            # Find all URLs in the sitemap
            url_tags = soup.find_all('url')
            
            # Filter for pharmacy location pages
            for url_tag in url_tags:
                loc_tag = url_tag.find('loc')
                if loc_tag:
                    url = loc_tag.text
                    # Only include URLs that match the pharmacy location pattern
                    if 'locate-us-' in url:
                        pharmacy_urls.append(url)
            
            logging.info(f"Found {len(pharmacy_urls)} Bendigo UFS pharmacy URLs")
            return pharmacy_urls
        else:
            logging.error(f"Failed to fetch Bendigo UFS sitemap: {response.status_code}")
            raise Exception(f"Failed to fetch Bendigo UFS sitemap: {response.status_code}")
    
    async def fetch_pharmacy_details(self, location_url):
        """
        Fetch details for a specific Bendigo UFS pharmacy
        
        Args:
            location_url: URL of the pharmacy location page
            
        Returns:
            Dictionary containing pharmacy details
        """
        logging.info(f"Fetching pharmacy details from: {location_url}")
        response = await self.session_manager.get(
            url=location_url,
            headers=self.headers
        )
        
        if response.status_code == 200:
            html_content = response.text
            # Parse HTML content
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract pharmacy data from the page
            pharmacy_data = {
                'url': location_url
            }
            
            # Extract name from the page title or URL
            page_title = soup.find('title')
            if page_title:
                title_text = page_title.text.strip()
                # Extract pharmacy name from title
                name_match = re.search(r'Locate Us - (.+?) \|', title_text)
                if name_match:
                    pharmacy_data['name'] = name_match.group(1).strip()
                else:
                    # Extract from URL as fallback
                    name_from_url = location_url.split('locate-us-')[1].replace('/', '').replace('-', ' ').title()
                    pharmacy_data['name'] = name_from_url
            else:
                # Extract from URL as fallback
                name_from_url = location_url.split('locate-us-')[1].replace('/', '').replace('-', ' ').title()
                pharmacy_data['name'] = name_from_url
            
            # Find pharmacy details container
            content_containers = soup.select('div.elementor-widget-container')
            
            for container in content_containers:
                # Process each container
                
                # Check for address section
                address_header = container.find('h3', string=lambda t: t and 'address' in t.lower())
                if address_header:
                    address_p = address_header.find_next('p')
                    if address_p:
                        pharmacy_data['address'] = address_p.text.strip()
                
                # Check for contact section
                contact_header = container.find('h3', string=lambda t: t and 'contact' in t.lower())
                if contact_header:
                    contact_p = contact_header.find_next('p')
                    if contact_p:
                        contact_text = contact_p.text.strip()
                        
                        # Extract phone
                        phone_match = re.search(r'Tel:?\s*(.+?)(?:\s*<br>|\s*\n|$)', str(contact_p))
                        if phone_match:
                            pharmacy_data['phone'] = phone_match.group(1).strip()
                        
                        # Extract fax
                        fax_match = re.search(r'Fax:?\s*(.+?)(?:\s*<br>|\s*\n|$)', str(contact_p))
                        if fax_match:
                            pharmacy_data['fax'] = fax_match.group(1).strip()
                        
                        # Extract email
                        email_tag = contact_p.find('a', href=lambda h: h and h.startswith('mailto:'))
                        if email_tag:
                            pharmacy_data['email'] = email_tag.text.strip()
                
                # Check for trading hours section
                hours_header = container.find('h3', string=lambda t: t and 'trading hours' in t.lower())
                if hours_header:
                    hours_p = hours_header.find_next('p')
                    if hours_p:
                        hours_text = hours_p.text.strip()
                        
                        # Parse trading hours
                        trading_hours = {}
                        
                        # Monday-Friday
                        mon_fri_match = re.search(r'Monday\s*[–\-]\s*Friday\s*(\d+[:.]\d+\s*(?:am|pm)?)\s*to\s*(\d+[:.]\d+\s*(?:am|pm)?)', hours_text, re.IGNORECASE)
                        if mon_fri_match:
                            open_time = mon_fri_match.group(1).strip()
                            close_time = mon_fri_match.group(2).strip()
                            
                            # Ensure AM/PM is present
                            if 'am' not in open_time.lower() and 'pm' not in open_time.lower():
                                if 'am' in close_time.lower():
                                    open_time += 'am'
                                elif 'pm' in close_time.lower():
                                    # Determine if opening time should be AM or PM based on value
                                    hours_value = float(re.search(r'(\d+)[:.]\d+', open_time).group(1))
                                    open_time += 'am' if hours_value < 12 else 'pm'
                            
                            if 'am' not in close_time.lower() and 'pm' not in close_time.lower():
                                # Assume PM for closing time if it's a single-digit hour or if it's 12
                                hours_value = float(re.search(r'(\d+)[:.]\d+', close_time).group(1))
                                close_time += 'pm' if hours_value <= 12 else 'am'
                            
                            for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']:
                                trading_hours[day] = {
                                    'open': open_time,
                                    'closed': close_time
                                }
                        
                        # Saturday
                        sat_match = re.search(r'Saturday\s*[–\-]\s*(\d+[:.]\d+)(?:\s*(?:am|pm)?)?\s*to\s*(\d+[:.]\d+\s*(?:am|pm)?)', hours_text, re.IGNORECASE)
                        if sat_match:
                            open_time = sat_match.group(1).strip()
                            close_time = sat_match.group(2).strip()
                            
                            # Ensure AM/PM is present
                            if 'am' not in open_time.lower() and 'pm' not in open_time.lower():
                                # Assume AM for opening time on Saturday
                                open_time += 'am'
                            
                            if 'am' not in close_time.lower() and 'pm' not in close_time.lower():
                                # For Saturday closing time, check the hour
                                hours_value = float(re.search(r'(\d+)[:.]\d+', close_time).group(1))
                                # If it's early (1-5), likely PM. If late (6-12), could be AM for early closing
                                close_time += 'pm' if hours_value < 6 else 'am'
                            
                            trading_hours['Saturday'] = {
                                'open': open_time,
                                'closed': close_time
                            }
                        elif 'Saturday' in hours_text and 'closed' in hours_text.lower():
                            trading_hours['Saturday'] = {
                                'open': 'Closed',
                                'closed': 'Closed'
                            }
                        
                        # Sunday and public holidays
                        if 'Sunday' in hours_text and 'closed' in hours_text.lower():
                            trading_hours['Sunday'] = {
                                'open': 'Closed',
                                'closed': 'Closed'
                            }
                            # Also add public holiday if mentioned together
                            if 'public holiday' in hours_text.lower():
                                trading_hours['Public Holiday'] = {
                                    'open': 'Closed',
                                    'closed': 'Closed'
                                }
                        else:
                            sun_match = re.search(r'Sunday\s*(?:and)?\s*(?:Public)?\s*(?:Holidays)?\s*[–\-]?\s*(\d+[:.]\d+\s*(?:am|pm))\s*to\s*(\d+[:.]\d+\s*(?:am|pm))', hours_text, re.IGNORECASE)
                            if sun_match:
                                open_time = sun_match.group(1).strip()
                                close_time = sun_match.group(2).strip()
                                trading_hours['Sunday'] = {
                                    'open': open_time,
                                    'closed': close_time
                                }
                                # Also add public holiday if mentioned together
                                if 'public holiday' in hours_text.lower():
                                    trading_hours['Public Holiday'] = {
                                        'open': open_time,
                                        'closed': close_time
                                    }
                        
                        pharmacy_data['trading_hours'] = trading_hours
            
            # Extract state and postcode from address if available
            if 'address' in pharmacy_data:
                from ..utils import extract_state_postcode
                state, postcode = extract_state_postcode(pharmacy_data['address'])
                if state:
                    pharmacy_data['state'] = state
                if postcode:
                    pharmacy_data['postcode'] = postcode
            
            return pharmacy_data
        else:
            logging.error(f"Failed to fetch Bendigo UFS pharmacy details: {response.status_code}")
            raise Exception(f"Failed to fetch Bendigo UFS pharmacy details: {response.status_code}")
    
    async def fetch_all_locations_details(self):
        """
        Fetch details for all Bendigo UFS pharmacy locations
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        logging.info("Fetching all Bendigo UFS pharmacy locations")
        
        # Get all pharmacy URLs
        pharmacy_urls = await self.fetch_locations()
        
        # If no pharmacy URLs found, return empty list
        if not pharmacy_urls:
            logging.warning("No Bendigo UFS pharmacy URLs found")
            return []
        
        # Fetch details for each pharmacy
        all_pharmacy_details = []
        for url in pharmacy_urls:
            try:
                pharmacy_data = await self.fetch_pharmacy_details(url)
                if pharmacy_data:
                    # Extract standardized details
                    extracted_details = self.extract_pharmacy_details(pharmacy_data)
                    all_pharmacy_details.append(extracted_details)
            except Exception as e:
                logging.error(f"Error fetching Bendigo UFS pharmacy details from {url}: {e}")
        
        logging.info(f"Successfully fetched details for {len(all_pharmacy_details)} Bendigo UFS pharmacies")
        return all_pharmacy_details
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract standardized pharmacy details from raw data
        
        Args:
            pharmacy_data: Raw pharmacy data
            
        Returns:
            Dictionary with standardized pharmacy details
        """
        # Extract address components
        address = pharmacy_data.get('address', '')
        
        # Try to extract suburb from address if state and postcode are available
        suburb = None
        state = pharmacy_data.get('state')
        postcode = pharmacy_data.get('postcode')
        
        if address and state:
            # Try to extract suburb from address
            parts = address.split(',')
            if len(parts) > 1:
                suburb_part = parts[-1].strip() if len(parts) == 2 else parts[-2].strip()
                # Remove state and postcode
                suburb_part = re.sub(r'\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b', '', suburb_part)
                suburb_part = re.sub(r'\b\d{4}\b', '', suburb_part)
                suburb = suburb_part.strip()
        
        # Standardized result with fixed column order
        result = {
            'name': pharmacy_data.get('name', '').strip(),
            'address': address,
            'email': pharmacy_data.get('email'),
            'fax': pharmacy_data.get('fax'),
            'latitude': None,  # Bendigo UFS doesn't provide coordinates
            'longitude': None, # Bendigo UFS doesn't provide coordinates
            'phone': pharmacy_data.get('phone'),
            'postcode': postcode,
            'state': state,
            'street_address': address,
            'suburb': suburb,
            'trading_hours': pharmacy_data.get('trading_hours', {}),
            'website': "https://www.bendigoufs.com.au/"  # Default website
        }
        
        # Remove None values
        return {k: v for k, v in result.items() if v is not None}