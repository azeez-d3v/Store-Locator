import re
import json
import asyncio
import logging
from rich import print
from bs4 import BeautifulSoup
from ...base_handler import BasePharmacyHandler

class WoolworthsPharmacyNZHandler(BasePharmacyHandler):
    """Handler for Woolworths Pharmacy NZ"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "woolworths_nz"
        # Define brand-specific headers for API requests
        self.headers = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Referer': 'https://www.healthpoint.co.nz/search?q=woolworths%20pharmacy&types=services',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
            'X-Requested-With': 'XMLHttpRequest',
            'sec-ch-ua': '"Microsoft Edge";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }
        
        # Base API URL for Healthpoint
        self.healthpoint_api_url = "https://www.healthpoint.co.nz/geo.do?zoom=28&minLat=-43.542297156318796&maxLat=-43.54229601711441&minLng=172.7399945598083&maxLng=172.7400002856959&lat=&lng=&q=woolworths%20pharmacy&region=&addr=&branch=&types=services"
        self.healthpoint_base_url = "https://www.healthpoint.co.nz"
        
        # Maximum number of concurrent requests
        self.max_concurrent_requests = 5
        
        # Logger for this handler
        self.logger = logging.getLogger(__name__)
    
    async def fetch_locations(self):
        """
        Fetch all Woolworths Pharmacy NZ locations from Healthpoint.
        
        Returns:
            List of Woolworths Pharmacy NZ locations
        """
        # Make request to get locations data
        response = await self.session_manager.get(
            url=self.healthpoint_api_url,
            headers=self.headers
        )
        
        if response.status_code == 200:
            try:
                # Parse JSON response
                data = response.json()
                
                if not data or 'results' not in data:
                    self.logger.error("No results found in Woolworths Pharmacy NZ API response")
                    return []
                
                locations = []
                for result in data.get('results', []):
                    # Extract location ID from URL
                    if 'url' in result:
                        location_id = result['url'].split('/')[-2] if 'url' in result else None
                        
                        if not location_id:
                            continue
                        
                        location = {
                            'id': location_id,
                            'name': result.get('name', ''),
                            'url': f"{self.healthpoint_base_url}{result.get('url')}" if 'url' in result else None,
                            'latitude': result.get('lat'),
                            'longitude': result.get('lng'),
                            'branch': result.get('branch', '')
                        }
                        
                        locations.append(location)
                
                print(f"Found {len(locations)} Woolworths Pharmacy NZ locations")
                return locations
            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to parse Woolworths Pharmacy NZ locations response: {e}")
                return []
        else:
            self.logger.error(f"Failed to fetch Woolworths Pharmacy NZ locations: {response.status_code}")
            return []
    
    async def fetch_pharmacy_details(self, location):
        """
        Fetch details for a specific Woolworths Pharmacy NZ location.
        
        Args:
            location: Dictionary containing location info including URL
            
        Returns:
            Dictionary with pharmacy details
        """
        if not location or 'url' not in location:
            self.logger.error("Missing URL in location data")
            return None
        
        # Make request to get the detailed HTML page
        response = await self.session_manager.get(
            url=location['url'],
            headers=self.headers
        )
        
        if response.status_code == 200:
            try:
                # Extract detailed pharmacy information from HTML
                pharmacy_data = self._extract_pharmacy_data_from_html(response.text, location)
                return pharmacy_data
            except Exception as e:
                self.logger.error(f"Failed to parse Woolworths Pharmacy NZ location details: {e}")
                return None
        else:
            self.logger.error(f"Failed to fetch details for {location.get('name')}: {response.status_code}")
            return None
    
    def _extract_pharmacy_data_from_html(self, html_content, location):
        """
        Extract pharmacy details from HTML content.
        
        Args:
            html_content: HTML content of the pharmacy detail page
            location: Basic location data
            
        Returns:
            Dictionary with complete pharmacy details
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Initialize with data we already have
        pharmacy_data = dict(location)
        
        # Extract street address and region
        street_address = ''
        suburb = ''
        region = ''
        postcode = ''
        
        # Find the service-location div which contains the address
        service_location = soup.select_one('div.service-location')
        if service_location:
            # Extract the main address from h3
            h3_elem = service_location.select_one('h3')
            if h3_elem:
                address_text = h3_elem.text.strip()
                if address_text:
                    street_address = address_text
            
            # Extract region from p element
            p_elems = service_location.select('p')
            if p_elems:
                for p in p_elems:
                    text = p.text.strip()
                    if text:
                        region = text
                        break
        
        # Find street address details (more detailed)
        street_address_heading = soup.find('h4', class_='label-text', string='Street Address')
        if street_address_heading:
            address_div = street_address_heading.find_next('div', {'itemprop': 'address'})
            if address_div:
                address_text = address_div.get_text('\n').strip()
                address_lines = address_text.split('\n')
                
                if len(address_lines) >= 1:
                    street_address = address_lines[0].strip()
                
                if len(address_lines) >= 2:
                    suburb = address_lines[1].strip()
                
                if len(address_lines) >= 3:
                    # Try to extract region and postcode from the third line
                    third_line = address_lines[2].strip()
                    postcode_match = re.search(r'(\d{4})$', third_line)
                    if postcode_match:
                        postcode = postcode_match.group(1)
                        region = third_line.replace(postcode, '').strip()
                    else:
                        region = third_line
                
                if len(address_lines) >= 4 and not postcode:
                    # If we didn't get postcode from the third line, try fourth line
                    fourth_line = address_lines[3].strip()
                    postcode_match = re.search(r'(\d{4})$', fourth_line)
                    if postcode_match:
                        postcode = postcode_match.group(1)
        
        # Extract contact details from contact-list
        phone = ''
        fax = ''
        email = ''
        website = ''
        healthlink_edi = ''
        prescription_email = ''
        
        # Find the contact-list
        contact_list = soup.select_one('ul.contact-list')
        if contact_list:
            # Process each list item in the contact list
            for li in contact_list.select('li'):
                # Get the label text
                label = li.select_one('h4.label-text')
                if not label:
                    continue
                
                label_text = label.text.strip()
                
                # Based on the label, extract the appropriate information
                if label_text == 'Phone':
                    phone_elem = li.select_one('p[itemprop="telephone"]')
                    if phone_elem:
                        phone = phone_elem.text.strip()
                
                elif label_text == 'Fax':
                    fax_elem = li.select_one('p[itemprop="faxNumber"]')
                    if fax_elem:
                        fax = fax_elem.text.strip()
                
                elif label_text == 'Healthlink EDI':
                    edi_elem = li.select_one('p')
                    if edi_elem:
                        healthlink_edi = edi_elem.text.strip()
                
                elif label_text == 'Email':
                    email_elem = li.select_one('a')
                    if email_elem:
                        email = email_elem.text.strip()
                
                elif label_text == 'Website':
                    website_elem = li.select_one('a')
                    if website_elem:
                        website = website_elem.get('href', '')
                
                elif label_text == 'Prescription Email':
                    prescription_email_elem = li.select_one('a')
                    if prescription_email_elem:
                        prescription_email = prescription_email_elem.text.strip()
            
            # If no regular email but we have prescription email, use that
            if not email and prescription_email:
                email = prescription_email
        
        # Extract trading hours
        trading_hours = self._parse_trading_hours(html_content)
        
        # Complete pharmacy data
        pharmacy_data.update({
            'brand': 'Woolworths Pharmacy',
            'name': location.get('name', ''),
            'address': street_address,
            'street_address': street_address,
            'suburb': suburb,
            'state': region,  # In NZ context, this is the region
            'country': 'NZ',
            'postcode': postcode,
            'phone': phone,
            'fax': fax,
            'email': email,
            'website': website,
            'trading_hours': trading_hours,
            'healthlink_edi': healthlink_edi
        })
        
        return pharmacy_data
    
    def _parse_trading_hours(self, html_content):
        """
        Parse trading hours from HTML content.
        
        Args:
            html_content: HTML content of the pharmacy detail page
            
        Returns:
            Dictionary with trading hours by day
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Initialize trading hours with default closed values
        trading_hours = {
            'Monday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Tuesday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Wednesday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Thursday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Friday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Saturday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Sunday': {'open': '12:00 AM', 'closed': '12:00 AM'}
        }
        
        # Find the hours section
        hours_section = soup.select_one('div#section-hours2')
        if not hours_section:
            return trading_hours
        
        # Find the hours table
        hours_table = hours_section.select_one('table.hours')
        if not hours_table:
            return trading_hours
        
        # Process each row in the table
        for row in hours_table.select('tr'):
            cells = row.select('th, td')
            if len(cells) < 2:
                continue
            
            days_text = cells[0].text.strip()
            hours_text = cells[1].text.strip()
            
            # Parse hours range using regex
            hours_match = re.search(r'(\d+(?::\d+)?)\s*(?:AM|PM)?\s*–\s*(\d+(?::\d+)?)\s*(?:AM|PM)', hours_text)
            if hours_match:
                open_time = self._format_time(hours_match.group(1))
                close_time = self._format_time(hours_match.group(2))
                
                # Handle different day formats
                if 'Mon – Fri' in days_text:
                    for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']:
                        trading_hours[day] = {'open': open_time, 'closed': close_time}
                elif 'Sat – Sun' in days_text:
                    for day in ['Saturday', 'Sunday']:
                        trading_hours[day] = {'open': open_time, 'closed': close_time}
                else:
                    # Try to match individual days
                    day_matches = re.findall(r'(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)', days_text)
                    for day in day_matches:
                        trading_hours[day] = {'open': open_time, 'closed': close_time}
        
        # Check for holidays information
        holidays_elem = hours_section.select_one('p.hours-holidays')
        if holidays_elem:
            holidays_text = holidays_elem.text.strip()
            if 'Closed' in holidays_text:
                trading_hours['Public Holidays'] = {'open': 'Closed', 'closed': 'Closed'}
        
        return trading_hours
    
    def _format_time(self, time_str):
        """
        Format time string to standard format (HH:MM AM/PM).
        
        Args:
            time_str: Time string to format
            
        Returns:
            Formatted time string
        """
        try:
            # Clean the time string
            time_str = time_str.strip()
            
            # Check if AM/PM is specified
            has_am_pm = 'AM' in time_str.upper() or 'PM' in time_str.upper()
            
            # Extract hours and minutes
            time_parts = time_str.upper().replace('AM', '').replace('PM', '').strip().split(':')
            hours = int(time_parts[0])
            minutes = int(time_parts[1]) if len(time_parts) > 1 else 0
            
            # Determine AM/PM
            is_pm = 'PM' in time_str.upper() or (not has_am_pm and hours >= 12)
            
            # Convert to 12-hour format
            if is_pm and hours < 12:
                hours += 12
            elif not is_pm and hours == 12:
                hours = 0
            
            # Format as HH:MM AM/PM
            if hours == 0:
                return f"12:{minutes:02d} AM"
            elif hours == 12:
                return f"12:{minutes:02d} PM"
            elif hours > 12:
                return f"{hours-12}:{minutes:02d} PM"
            else:
                return f"{hours}:{minutes:02d} AM"
        except Exception as e:
            self.logger.error(f"Error formatting time {time_str}: {e}")
            return time_str
    
    async def fetch_all_locations_details(self):
        """
        Fetch details for all Woolworths Pharmacy NZ locations.
        
        Returns:
            List of pharmacy details
        """
        # First get all locations
        locations = await self.fetch_locations()
        if not locations:
            self.logger.warning("No Woolworths Pharmacy NZ locations found")
            return []
        
        # Create a semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        
        async def fetch_with_semaphore(location):
            """Helper function to fetch details with semaphore control"""
            async with semaphore:
                try:
                    pharmacy_details = await self.fetch_pharmacy_details(location)
                    if pharmacy_details:
                        processed_details = self.extract_pharmacy_details(pharmacy_details)
                        return processed_details
                except Exception as e:
                    self.logger.error(f"Error fetching details for {location.get('name')}: {e}")
                    return None
        
        # Create tasks for all locations
        tasks = [fetch_with_semaphore(location) for location in locations]
        
        # Process results as they complete
        print(f"Processing {len(locations)} Woolworths Pharmacy NZ locations in parallel...")
        results = await asyncio.gather(*tasks)
        
        # Filter out any None results (failed requests)
        all_pharmacy_details = [result for result in results if result]
        
        print(f"Successfully fetched details for {len(all_pharmacy_details)} out of {len(locations)} Woolworths Pharmacy NZ locations")
        return all_pharmacy_details
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract and standardize pharmacy details from raw data.
        
        Args:
            pharmacy_data: Raw pharmacy data
            
        Returns:
            Dictionary with standardized pharmacy details
        """
        # Clean up the result by removing None values
        cleaned_result = {}
        for key, value in pharmacy_data.items():
            if value is not None and value != '':
                cleaned_result[key] = value
                
        return cleaned_result