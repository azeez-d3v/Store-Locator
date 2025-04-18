from ...base_handler import BasePharmacyHandler
import json
import re
import asyncio
from bs4 import BeautifulSoup

class BargainChemistNZHandler(BasePharmacyHandler):
    """Handler for Bargain Chemist NZ pharmacies"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "bargain_chemist_nz"
        # Define brand-specific headers for API requests
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Pragma': 'no-cache',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
        }
        # The URL for the store locator page
        self.store_locator_url = "https://www.bargainchemist.co.nz/pages/find-a-store"
        # Base URL for store detail pages
        self.base_url = "https://www.bargainchemist.co.nz"
        # Maximum number of concurrent requests
        self.max_concurrent_requests = 5

    async def fetch_locations(self):
        """
        Fetch all Bargain Chemist NZ pharmacy locations.
        
        Returns:
            List of Bargain Chemist NZ pharmacy location info
        """
        # Make request to get HTML data
        response = await self.session_manager.get(
            url=self.store_locator_url,
            headers=self.headers
        )
        
        if response.status_code == 200:
            try:
                # Parse HTML to extract basic store info and detail page links
                locations = self._extract_locations_from_html(response.text)
                print(f"Found {len(locations)} Bargain Chemist NZ locations")
                return locations
            except Exception as e:
                raise Exception(f"Failed to parse Bargain Chemist NZ locations: {e}")
        else:
            raise Exception(f"Failed to fetch Bargain Chemist NZ locations: {response.status_code}")
    
    def _extract_locations_from_html(self, html_content):
        """
        Extract location data from the store locator HTML page.
        
        Args:
            html_content: HTML content from the store locator page
            
        Returns:
            List of pharmacy location information
        """
        locations = []
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find all location blocks
        location_blocks = soup.find_all('div', class_='location-block')
        
        for block in location_blocks:
            try:
                # Extract store name
                name_elem = block.find('h3', class_='location-name')
                name = name_elem.text.strip() if name_elem else ""
                
                # Extract address
                address_lines = block.find_all('span', class_='store-address')
                address = ", ".join([line.text.strip() for line in address_lines]) if address_lines else ""
                
                # Extract phone number
                phone_elem = block.find('span', class_='store-number', string=lambda text: 'Ph:' in text if text else False)
                phone = phone_elem.text.replace('Ph:', '').strip() if phone_elem else ""
                
                # Extract fax number
                fax_elem = block.find('span', class_='store-number', string=lambda text: 'Fax:' in text if text else False)
                fax = fax_elem.text.replace('Fax:', '').strip() if fax_elem else ""
                
                # Extract detail page URL
                detail_link = block.find('a', class_='details')
                detail_url = f"{self.base_url}{detail_link['href']}" if detail_link and 'href' in detail_link.attrs else None
                
                # Extract JSON data if available in script tag
                json_data = {}
                json_script = block.find('script', class_='location-json')
                if json_script and json_script.string:
                    try:
                        json_data = json.loads(json_script.string)
                    except json.JSONDecodeError:
                        json_data = {}
                
                # Compile basic location data
                location_data = {
                    "name": f"Bargain Chemist {name}",
                    "store_name": name,
                    "address": address,
                    "phone": phone,
                    "fax": fax,
                    "detail_url": detail_url,
                    "json_data": json_data
                }
                
                locations.append(location_data)
            except Exception as e:
                print(f"Error parsing location block: {e}")
                continue
        
        return locations
    
    async def fetch_pharmacy_details(self, location_data):
        """
        Fetch details for a specific Bargain Chemist NZ pharmacy.
        
        Args:
            location_data: Dictionary containing basic location data and detail URL
            
        Returns:
            Dictionary with complete pharmacy details
        """
        detail_url = location_data.get('detail_url')
        if not detail_url:
            # If we already have JSON data from the script tag, use that
            if location_data.get('json_data'):
                return self._parse_json_data(location_data)
            else:
                raise Exception(f"No detail URL or JSON data found for {location_data.get('name')}")
        
        # Make request to get details HTML
        response = await self.session_manager.get(
            url=detail_url,
            headers=self.headers
        )
        
        if response.status_code == 200:
            try:
                # Combine location_data with additional data from detail page
                detailed_data = self._extract_details_from_html(response.text, location_data)
                return detailed_data
            except Exception as e:
                raise Exception(f"Failed to parse details for {location_data.get('name')}: {e}")
        else:
            # Fall back to JSON data if available
            if location_data.get('json_data'):
                return self._parse_json_data(location_data)
            else:
                raise Exception(f"Failed to fetch details for {location_data.get('name')}: {response.status_code}")
    
    def _extract_details_from_html(self, html_content, location_data):
        """
        Extract detailed pharmacy information from the store detail page.
        
        Args:
            html_content: HTML content from the store detail page
            location_data: Basic location data from the store locator page
            
        Returns:
            Dictionary with complete pharmacy details
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # If we already have JSON data from the script tag, use that as a base
        # and supplement with additional data from the detail page
        if location_data.get('json_data'):
            return self._parse_json_data(location_data)
        
        # Otherwise, extract details from the HTML
        detailed_data = dict(location_data)
        
        # Extract email
        email_elem = soup.find('a', class_='selected__email')
        if email_elem and 'href' in email_elem.attrs and email_elem['href'].startswith('mailto:'):
            detailed_data['email'] = email_elem['href'].replace('mailto:', '').strip()
        
        # Extract trading hours
        hours = {}
        
        # Days of the week
        for day in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']:
            day_elem = soup.find('span', class_=f'selected__{day}')
            if day_elem:
                hours[day.capitalize()] = day_elem.text.strip()
        
        # Public holidays
        holidays_elem = soup.find('span', class_='selected__holidays')
        if holidays_elem:
            hours['Public Holidays'] = holidays_elem.text.strip()
        
        detailed_data['hours'] = hours
        
        return detailed_data
    
    def _parse_json_data(self, location_data):
        """
        Parse JSON data from script tag for a pharmacy location.
        
        Args:
            location_data: Location data with JSON data from script tag
            
        Returns:
            Dictionary with complete pharmacy details
        """
        json_data = location_data.get('json_data', {})
        
        # Create detailed data dict with all available information
        detailed_data = dict(location_data)
        
        # Remove the original JSON data from the result
        if 'json_data' in detailed_data:
            del detailed_data['json_data']
        
        if json_data:
            # Extract address components
            detailed_data['address_line_1'] = json_data.get('store_address_1', '')
            detailed_data['address_line_2'] = json_data.get('store_address_2', '')
            
            # Create a complete address
            if detailed_data.get('address_line_1') and detailed_data.get('address_line_2'):
                detailed_data['address'] = f"{detailed_data['address_line_1']}, {detailed_data['address_line_2']}"
            
            # Extract contact info
            detailed_data['email'] = json_data.get('email', '')
            detailed_data['phone'] = json_data.get('phone', '')
            detailed_data['fax'] = json_data.get('fax', '')
            
            # Extract trading hours
            hours = json_data.get('hours', {})
            if hours:
                detailed_data['hours'] = hours
        
        return detailed_data
    
    async def fetch_all_locations_details(self):
        """
        Fetch details for all Bargain Chemist NZ pharmacy locations using asyncio for concurrent processing.
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        print("Fetching all Bargain Chemist NZ pharmacy locations...")
        locations = await self.fetch_locations()
        
        if not locations:
            print("No Bargain Chemist NZ locations found.")
            return []
        
        print(f"Found {len(locations)} Bargain Chemist NZ locations. Fetching details in parallel...")
        
        # Create a semaphore to limit concurrent connections
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        
        async def fetch_with_semaphore(location_data):
            """Helper function to fetch details with semaphore control"""
            async with semaphore:
                try:
                    pharmacy_details = await self.fetch_pharmacy_details(location_data)
                    if pharmacy_details:
                        processed_details = self.extract_pharmacy_details(pharmacy_details)
                        return processed_details
                except Exception as e:
                    print(f"Error fetching details for {location_data.get('name')}: {e}")
                    return None
        
        # Create tasks for all locations
        tasks = [fetch_with_semaphore(location) for location in locations]
        
        # Process results as they complete
        print(f"Processing {len(locations)} pharmacy locations in parallel...")
        results = await asyncio.gather(*tasks)
        
        # Filter out any None results (failed requests)
        all_pharmacy_details = [result for result in results if result]
        
        print(f"Successfully fetched details for {len(all_pharmacy_details)} out of {len(locations)} Bargain Chemist NZ locations")
        return all_pharmacy_details
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract and standardize pharmacy details from raw data.
        
        Args:
            pharmacy_data: Raw pharmacy data (combined from fetch_locations and fetch_pharmacy_details)
            
        Returns:
            Dictionary with standardized pharmacy details
        """
        # Initialize standardized data
        standardized_data = {
            "name": pharmacy_data.get('name', ''),
            "address": pharmacy_data.get('address', ''),
            "email": pharmacy_data.get('email', ''),
            "phone": pharmacy_data.get('phone', ''),
            "fax": pharmacy_data.get('fax', ''),
            "latitude": '',
            "longitude": '',
            "website": "https://www.bargainchemist.co.nz",
            "state": "",  # New Zealand doesn't use states
            "country": "NZ",
            "trading_hours": {}
        }
        
        # Extract address components
        address = standardized_data["address"]
        if address:
            # Try to extract postcode (NZ postcodes are 4 digits)
            postcode_match = re.search(r'(\d{4})$', address)
            if postcode_match:
                standardized_data["postcode"] = postcode_match.group(1)
            
            # Try to extract suburb and city
            address_parts = address.split(',')
            if len(address_parts) >= 2:
                # Last part typically contains city and possibly postcode
                city_part = address_parts[-1].strip()
                # Remove postcode if present
                city = re.sub(r'\d{4}$', '', city_part).strip()
                standardized_data["suburb"] = city
                
                # Street address is typically everything before the last part
                standardized_data["street_address"] = ", ".join(address_parts[:-1]).strip()
        
        # Process trading hours
        hours = pharmacy_data.get('hours', {})
        trading_hours = {}
        
        # Process regular days
        for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']:
            # Get hours string for the day (convert to lowercase to match JSON keys)
            day_lower = day.lower()
            hours_str = hours.get(day_lower, '')
            
            if hours_str:
                # Parse hours string (e.g., "8am - 8pm")
                times = hours_str.split('-')
                
                if len(times) == 2:
                    open_time = times[0].strip()
                    close_time = times[1].strip()
                    
                    # Format times to HH:MM AM/PM format
                    open_formatted = self._format_time(open_time)
                    close_formatted = self._format_time(close_time)
                    
                    trading_hours[day] = {
                        'open': open_formatted,
                        'closed': close_formatted
                    }
                else:
                    # Handle special case like "Closed"
                    if hours_str.lower() == 'closed':
                        trading_hours[day] = {
                            'open': '12:00 AM',
                            'closed': '12:00 AM'
                        }
                    else:
                        # Default to closed if format is unknown
                        trading_hours[day] = {
                            'open': '12:00 AM',
                            'closed': '12:00 AM'
                        }
            else:
                # Default to closed if no hours provided
                trading_hours[day] = {
                    'open': '12:00 AM',
                    'closed': '12:00 AM'
                }
        
        standardized_data["trading_hours"] = trading_hours
        
        return standardized_data
    
    def _format_time(self, time_str):
        """
        Format time strings like "8am" to "08:00 AM" format
        
        Args:
            time_str: Time string in various formats
            
        Returns:
            Formatted time string like "08:00 AM"
        """
        try:
            # Convert to lowercase for consistency
            time_str = time_str.lower().strip()
            
            # Handle noon and midnight special cases
            if time_str == 'noon':
                return "12:00 PM"
            if time_str == 'midnight':
                return "12:00 AM"
            
            # Extract hours, minutes, and am/pm using regex
            time_match = re.match(r'(\d+)(?::(\d+))?\s*(am|pm)', time_str)
            
            if time_match:
                hours = int(time_match.group(1))
                minutes = int(time_match.group(2)) if time_match.group(2) else 0
                am_pm = time_match.group(3).upper()
                
                # Adjust hours for 12-hour format
                if am_pm == "PM" and hours < 12:
                    hours += 12
                elif am_pm == "AM" and hours == 12:
                    hours = 0
                
                # Format as HH:MM AM/PM
                if am_pm == "AM":
                    if hours == 0:
                        return f"12:{minutes:02d} AM"
                    else:
                        return f"{hours:02d}:{minutes:02d} AM"
                else:  # PM
                    if hours == 12:
                        return f"12:{minutes:02d} PM"
                    else:
                        return f"{hours-12:02d}:{minutes:02d} PM"
            
            return time_str
        except (ValueError, AttributeError) as e:
            print(f"Error formatting time '{time_str}': {e}")
            return time_str