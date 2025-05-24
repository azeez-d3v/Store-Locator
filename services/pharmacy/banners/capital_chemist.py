from ..base_handler import BasePharmacyHandler
from bs4 import BeautifulSoup
import json
import re
import asyncio
from rich import print

class CapitalChemistHandler(BasePharmacyHandler):
    """Handler for Capital Chemist"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "capital_chemist"
        self.base_url = self.pharmacy_locations.CAPITAL_CHEMIST_URL + '/stores'
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
        }
    
    async def fetch_locations(self):
        """
        Fetch all Capital Chemist locations from stores.aspx page.
        
        Returns:
            List of Capital Chemist locations with branch codes and coordinates
        """
        
        print("Fetching Capital Chemist stores from stores.aspx page...")
        response = await self.session_manager.get(
            url=self.pharmacy_locations.CAPITAL_CHEMIST_URL + 'stores.aspx',
            headers=self.headers
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to fetch Capital Chemist locations: {response.status_code}")
        
        # Parse the HTML to extract store information from the script
        soup = BeautifulSoup(response.content, 'html.parser')
        extra_script = soup.find('script', {'id': 'extra_script'})
        
        if not extra_script:
            print("Failed to find store script in Capital Chemist page")
            return []
            
        # Extract the JSON data from the script
        script_text = extra_script.string
        if not script_text:
            print("Empty script content in Capital Chemist page")
            return []
            
        # Extract branches data from the script
        branches_data = self._extract_branches_from_script(script_text)
        
        if not branches_data:
            print("No branches data found in JSON, trying HTML extraction")
            return self._extract_stores_from_html(soup)
        
        # Process the branches data
        locations = self._process_branches_data(branches_data)
        print(f"Found {len(locations)} Capital Chemist locations with coordinates")
        return locations
    
    def _extract_branches_from_script(self, script_text):
        """
        Extract branches data from the JavaScript content.
        
        Args:
            script_text: The JavaScript content as string
            
        Returns:
            List of branch data or empty list if extraction fails
        """
        branches_data = []
        
        # First try to extract the full 'window.extras' object
        json_match = re.search(r'window\.extras\s*=\s*({.*?})(?:;|$)', script_text, re.DOTALL)
        
        if json_match:
            try:
                # Extract the JSON part and fix common issues
                json_str = json_match.group(1)
                json_str = re.sub(r',\s*}', '}', json_str)
                json_str = re.sub(r',\s*]', ']', json_str)
                
                # Parse the JSON
                data = json.loads(json_str)
                
                # Get branches array
                branches_data = data.get('branches', [])
                if not branches_data and 'stores' in data:
                    branches_data = data.get('stores', [])
                    
            except json.JSONDecodeError as e:
                print(f"Error parsing Capital Chemist JSON data: {e}")
        
        # If we couldn't extract from the full JSON, try to extract just the branches array
        if not branches_data:
            branches_match = re.search(r'"branches"\s*:\s*(\[.*?\])', script_text, re.DOTALL)
            if branches_match:
                try:
                    # Wrap the branches array in a JSON object
                    json_str = f'{{"branches": {branches_match.group(1)}}}'
                    # Clean potential issues with the JSON
                    json_str = re.sub(r',\s*]', ']', json_str)
                    data = json.loads(json_str)
                    branches_data = data.get('branches', [])
                except json.JSONDecodeError as e:
                    print(f"Error parsing Capital Chemist branches data: {e}")
        
        return branches_data
    
    def _process_branches_data(self, branches_data):
        """
        Process branches data into standardized location format.
        
        Args:
            branches_data: List of branch dictionaries from JSON
            
        Returns:
            List of processed location dictionaries
        """
        locations = []
        
        print(f"Processing {len(branches_data)} Capital Chemist branches from JSON data")
        
        for store in branches_data:
            # Skip test stores or stores with invalid coordinates
            name = store.get('name', '')
            if ('test' in name.lower() or 
                (store.get('Latitude') == 0 and store.get('Longitude') == 0) or
                not name):
                continue
                
            store_id = store.get('id') or store.get('branchcode')
            branch_code = store.get('branchcode') or store.get('url')
            
            # Extract location details
            address = store.get('address', '')
            suburb = store.get('suburb', '')
            state = store.get('state', '')
            region = store.get('region', '')
            
            # Get coordinates from JSON (this is the key improvement - we get lat/lng here)
            latitude = store.get('Latitude') or store.get('latitude')
            longitude = store.get('Longitude') or store.get('longitude')
            
            # Convert coordinates to float
            try:
                latitude = float(latitude) if latitude is not None else None
                longitude = float(longitude) if longitude is not None else None
            except (ValueError, TypeError):
                latitude = None
                longitude = None
            
            if not branch_code:
                # Generate branch code from name
                branch_code = name.lower().replace(' ', '-').replace('capital-chemist-', '') if name else None
            
            location = {
                'id': store_id or f"capital-chemist-{branch_code}",
                'name': name,
                'branch_code': branch_code,
                'address': address,
                'suburb': suburb,
                'state': state or region,
                'region': region,
                'latitude': latitude,
                'longitude': longitude,
                'raw_data': store
            }

            locations.append(location)
        
        return locations
    
    def _extract_stores_from_html(self, soup):
        """
        Fallback method to extract store information from the HTML structure
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            List of stores extracted from the HTML
        """
        store_links = soup.select('a[href^="/"]')
        locations = []
        
        for i, link in enumerate(store_links):
            href = link.get('href', '')
            # Skip if it's not a store link
            if not href or href == '/' or href.startswith('/stores'):
                continue
                
            branch_code = href.strip('/').lower()
            name = link.get_text(strip=True) or f"Capital Chemist {branch_code.replace('-', ' ').title()}"
            
            if not name or len(name) < 3:
                continue
                
            location = {
                'id': f"capital-chemist-{i+1}",
                'name': name,
                'branch_code': branch_code
            }
            locations.append(location)
        
        print(f"Extracted {len(locations)} Capital Chemist locations from HTML")
        return locations
    
    async def fetch_pharmacy_details(self, location_id):
        """
        Fetch details for a specific pharmacy location.
        
        Args:
            location_id: The ID of the location to get details for
            
        Returns:
            Location details data
        """
        locations = await self.fetch_locations()
        
        # Find the target location
        target_location = next(
            (loc for loc in locations if str(loc.get('id')) == str(location_id)), 
            None
        )
        
        if not target_location:
            return {"error": f"Location ID {location_id} not found"}
        
        branch_code = target_location.get('branch_code')
        if not branch_code:
            return {"error": f"Branch code not found for location ID {location_id}"}
        
        # Fetch detailed page for the location
        response = await self.session_manager.get(
            url=f"{self.pharmacy_locations.CAPITAL_CHEMIST_URL}{branch_code}",
            headers=self.headers,
        )
        
        if response.status_code == 200:
            return self.extract_pharmacy_details({
                'location': target_location,
                'html_content': response.content
            })
        else:
            return {"error": f"Failed to fetch details for {target_location.get('name', 'Unknown Store')}: {response.status_code}"}
    
    async def fetch_all_locations_details(self):
        """
        Fetch details for all locations with enhanced async processing.
        Always fetches individual store pages to get complete details including phone, email, fax, and hours.
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        print("Fetching all Capital Chemist locations...")
        locations = await self.fetch_locations()
        
        if not locations:
            print("No Capital Chemist locations found.")
            return []
            
        print(f"Found {len(locations)} Capital Chemist locations. Processing details...")
        
        async def fetch_location_details(location):
            """Fetch details for a single location"""
            try:
                branch_code = location.get('branch_code')
                if not branch_code:
                    print(f"Branch code not found for {location.get('name', 'Unknown Store')}")
                    return None
                
                # Always fetch the individual store page to get complete details
                # Even if we have basic data from JSON, we need phone, email, fax, hours from HTML
                detail_url = f"{self.pharmacy_locations.CAPITAL_CHEMIST_URL}{branch_code}"
                response = await self.session_manager.get(
                    url=detail_url,
                    headers=self.headers,
                )
                
                if response.status_code == 200:
                    # Extract details from the HTML content
                    details = self.extract_pharmacy_details({
                        'location': location,
                        'html_content': response.content
                    })
                    details['source'] = 'individual_page'
                    return details
                else:
                    print(f"Failed to fetch details for {location.get('name', 'Unknown Store')}: {response.status_code}")
                    
                    # Fallback: return basic data from JSON if HTML fetch fails
                    if (location.get('latitude') and location.get('longitude') and 
                        location.get('address') and location.get('name')):
                        details = {
                            'name': location.get('name', ''),
                            'address': location.get('address', ''),
                            'suburb': location.get('suburb', ''),
                            'state': location.get('state', ''),
                            'latitude': location.get('latitude'),
                            'longitude': location.get('longitude'),
                            'branch_code': branch_code,
                            'region': location.get('region', ''),
                            'source': 'stores.aspx_json_fallback'
                        }
                        return details
                    
                    return None
                    
            except Exception as e:
                location_name = location.get('name', 'Unknown Store')
                print(f"Error processing Capital Chemist location '{location_name}': {e}")
                return None
        
        # Process locations in batches to avoid overwhelming the server
        batch_size = 10
        all_details = []
        
        for i in range(0, len(locations), batch_size):
            batch = locations[i:i + batch_size]
            print(f"Processing batch {i//batch_size + 1}/{(len(locations) + batch_size - 1)//batch_size}")
            
            # Process batch concurrently
            tasks = [fetch_location_details(location) for location in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Filter out None results and exceptions
            valid_results = [
                result for result in batch_results 
                if result is not None and not isinstance(result, Exception)
            ]
            
            all_details.extend(valid_results)
            
            # Small delay between batches to be respectful to the server
            if i + batch_size < len(locations):
                await asyncio.sleep(1.0)  # Increased delay since we're making more requests
        
        print(f"Completed processing details for {len(all_details)} Capital Chemist locations.")
        return all_details
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract pharmacy details from HTML content.
        Enhanced to correctly parse email, fax, phone and working hours.
        
        Args:
            pharmacy_data: Dictionary containing location data and HTML content
            
        Returns:
            Dictionary with standardized pharmacy details
        """
        location = pharmacy_data.get('location', {})
        html_content = pharmacy_data.get('html_content', '')
        
        if not html_content:
            return {"error": "No HTML content provided"}
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Initialize with data from location (from JSON) - use as fallback
        name = location.get('name', '')
        address = location.get('address', '')
        state = location.get('state', '')
        suburb = location.get('suburb', '')
        postcode = ''
        phone = ''
        fax = ''
        email = ''
        text_script = ''
        trading_hours = {}
        
        # Prioritize coordinates from JSON data
        latitude = location.get('latitude') or location.get('Latitude')
        longitude = location.get('longitude') or location.get('Longitude')
        
        # Extract details from HTML store-details-table
        details_table = soup.find('table', class_='store-details-table')
        
        if details_table:
            print(f"Found store details table for {name}")
            rows = details_table.find_all('tr')
            
            for row in rows:
                header = row.find('th')
                data_cell = row.find('td')
                
                if not header or not data_cell:
                    continue
                    
                header_text = header.get_text(strip=True).lower().replace(':', '').strip()
                
                if 'address' in header_text:
                    # Extract address from td, handling br tags properly
                    address_parts = []
                    
                    # Get all text content, replacing <br> with newlines
                    for br in data_cell.find_all('br'):
                        br.replace_with('\n')
                    
                    address_text = data_cell.get_text(separator='\n', strip=True)
                    address_lines = [line.strip() for line in address_text.split('\n') if line.strip()]
                    
                    # Filter out "Country" line and join the rest
                    filtered_lines = [line for line in address_lines if 'country' not in line.lower()]
                    address = ', '.join(filtered_lines)
                    
                    # Extract postcode, state, suburb from the last line
                    if filtered_lines:
                        last_line = filtered_lines[-1]
                        # Look for pattern like "Calwell ACT 2905"
                        location_match = re.search(r'([A-Za-z\s]+?)\s+([A-Z]{2,3})\s+(\d{4})$', last_line)
                        if location_match:
                            suburb = location_match.group(1).strip()
                            state = location_match.group(2).strip()
                            postcode = location_match.group(3).strip()
                            print(f"Extracted: suburb={suburb}, state={state}, postcode={postcode}")
                
                elif 'phone' in header_text:
                    # Extract phone from link or plain text
                    phone_link = data_cell.find('a', href=re.compile(r'^tel:'))
                    if phone_link:
                        phone = phone_link.get_text(strip=True)
                        print(f"Found phone from link: {phone}")
                    else:
                        phone = data_cell.get_text(strip=True)
                        print(f"Found phone from text: {phone}")
                
                elif 'fax' in header_text:
                    fax = data_cell.get_text(strip=True)
                    print(f"Found fax: {fax}")
                
                elif 'text your script' in header_text or 'text script' in header_text:
                    text_script = data_cell.get_text(strip=True)
                    print(f"Found text script: {text_script}")
                
                elif 'email' in header_text:
                    # Extract email from link or plain text
                    email_link = data_cell.find('a', href=re.compile(r'^mailto:'))
                    if email_link:
                        email = email_link.get_text(strip=True)
                        print(f"Found email from link: {email}")
                    else:
                        # Look for email pattern in text
                        email_text = data_cell.get_text(strip=True)
                        email_match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', email_text)
                        if email_match:
                            email = email_match.group(0)
                        else:
                            email = email_text
                            print(f"Found email from text: {email}")
                
                elif 'opening hours' in header_text or 'hours' in header_text:
                    # Extract opening hours from nested table
                    hours_table = data_cell.find('table', class_='opening-hours')
                    if hours_table:
                        print("Found opening hours table")
                        trading_hours = self._parse_trading_hours(hours_table)
                    else:
                        print("No opening hours table found")
        else:
            print(f"No store details table found for {name}")
        
        # Only extract coordinates from HTML if not available from JSON
        if not latitude or not longitude:
            latitude, longitude = self._extract_coordinates_from_html(soup)
        
        # If still no coordinates from HTML, check raw_data from location
        if (not latitude or not longitude) and 'raw_data' in location:
            raw_data = location.get('raw_data', {})
            if not latitude:
                latitude = raw_data.get('Latitude') or raw_data.get('latitude')
            if not longitude:
                longitude = raw_data.get('Longitude') or raw_data.get('longitude')
        
        # Format the result
        result = {
            'name': name,
            'address': address,
            'suburb': suburb,
            'state': state,
            'postcode': postcode,
            'email': email,
            'phone': phone,
            'fax': fax,
            'text_script': text_script,
            'latitude': latitude,
            'longitude': longitude,
            'trading_hours': trading_hours
        }
        
        
        # Clean up the result by removing None values and empty strings
        return {k: v for k, v in result.items() if v is not None and v != ''}
    
    def _extract_coordinates_from_html(self, soup):
        """
        Extract latitude and longitude from HTML/JavaScript content.
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            Tuple of (latitude, longitude) or (None, None)
        """
        latitude = None
        longitude = None
        
        for script in soup.find_all('script'):
            if not script.string:
                continue
            
            script_text = script.string
            
            # Try different patterns for lat/lng
            patterns = [
                (r'lat\s*:\s*([-\d.]+)', r'lng\s*:\s*([-\d.]+)'),
                (r'new\s+google\.maps\.LatLng\s*\(\s*([-\d.]+)\s*,\s*([-\d.]+)\s*\)', None),
                (r'[\'"]?[Ll]at(?:itude)?[\'"]?\s*[:=]\s*([-\d.]+)', r'[\'"]?[Ll](?:on|ng|ong)(?:itude)?[\'"]?\s*[:=]\s*([-\d.]+)')
            ]
            
            for lat_pattern, lng_pattern in patterns:
                if lng_pattern is None:
                    # LatLng constructor format
                    match = re.search(lat_pattern, script_text)
                    if match:
                        try:
                            latitude = float(match.group(1))
                            longitude = float(match.group(2))
                            break
                        except (ValueError, TypeError, IndexError):
                            continue
                else:
                    # Separate lat/lng patterns
                    lat_match = re.search(lat_pattern, script_text)
                    lng_match = re.search(lng_pattern, script_text)
                    if lat_match and lng_match:
                        try:
                            latitude = float(lat_match.group(1))
                            longitude = float(lng_match.group(1))
                            break
                        except (ValueError, TypeError):
                            continue
            
            if latitude and longitude:
                break
        
        return latitude, longitude
    
    def _parse_trading_hours(self, hours_table):
        """
        Parse trading hours from the hours table with improved handling.
        
        Args:
            hours_table: BeautifulSoup object containing the hours table
            
        Returns:
            Dictionary with days as keys and hours as values
        """
        trading_hours = {
            'Monday': {'open': 'Closed', 'closed': 'Closed'},
            'Tuesday': {'open': 'Closed', 'closed': 'Closed'},
            'Wednesday': {'open': 'Closed', 'closed': 'Closed'},
            'Thursday': {'open': 'Closed', 'closed': 'Closed'},
            'Friday': {'open': 'Closed', 'closed': 'Closed'},
            'Saturday': {'open': 'Closed', 'closed': 'Closed'},
            'Sunday': {'open': 'Closed', 'closed': 'Closed'},
        }
        
        day_mapping = {
            'mon': 'Monday', 'tue': 'Tuesday', 'wed': 'Wednesday', 'thu': 'Thursday',
            'fri': 'Friday', 'sat': 'Saturday', 'sun': 'Sunday',
            'monday': 'Monday', 'tuesday': 'Tuesday', 'wednesday': 'Wednesday',
            'thursday': 'Thursday', 'friday': 'Friday', 'saturday': 'Saturday',
            'sunday': 'Sunday'
        }
        
        try:
            hours_rows = hours_table.find_all('tr')
            
            for row in hours_rows:
                day_cell = row.find('th')
                hours_cell = row.find('td')
                
                if not day_cell or not hours_cell:
                    continue
                
                day_text = day_cell.get_text(strip=True).lower()
                hours_text = hours_cell.get_text(strip=True)
                
                if not hours_text:
                    continue
                
                # Handle different day formats
                day_range = []
                
                # Handle "Mon-Fri" format
                if 'mon-fri' in day_text or 'monday-friday' in day_text:
                    day_range = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
                
                # Handle "Sat-Sun" or "Weekend" format
                elif 'sat-sun' in day_text or 'saturday-sunday' in day_text or 'weekend' in day_text:
                    day_range = ['Saturday', 'Sunday']
                
                # Handle individual days
                elif day_text in day_mapping:
                    day_range = [day_mapping[day_text]]
                
                # Handle partial matches for individual days
                else:
                    for key, value in day_mapping.items():
                        if key in day_text:
                            day_range = [value]
                            break
                
                # Skip public holidays as they're not regular trading hours
                if 'public' in day_text or 'holiday' in day_text:
                    continue
                
                # Parse hours
                if hours_text.lower() in ['closed', 'close']:
                    opening = 'Closed'
                    closing = 'Closed'
                else:
                    # Handle formats like "8:30 am - 8:00 pm"
                    time_parts = hours_text.split('-')
                    if len(time_parts) == 2:
                        opening = self._format_time(time_parts[0].strip())
                        closing = self._format_time(time_parts[1].strip())
                    else:
                        # Single time or unexpected format
                        opening = self._format_time(hours_text)
                        closing = 'Unknown'
                
                # Update trading hours for each day in the range
                for day in day_range:
                    if day in trading_hours:
                        trading_hours[day] = {
                            'open': opening,
                            'closed': closing
                        }
        
        except Exception as e:
            print(f"Error parsing trading hours: {e}")
        
        return trading_hours
    
    def _format_time(self, time_str):
        """
        Format a time string to a standardized format with improved parsing.
        
        Args:
            time_str: A string representing a time (e.g., "8:30 am")
            
        Returns:
            Formatted time string (e.g., "8:30 AM")
        """
        if not time_str or time_str.lower() in ['closed', 'close']:
            return 'Closed'
        
        # Handle special cases
        lower_time = time_str.lower().strip()
        special_cases = {
            'tba': 'TBA',
            'to be announced': 'TBA',
            'call': 'Call for hours',
            'phone': 'Call for hours',
            'contact': 'Call for hours',
            'varies': 'Variable hours',
            'variable': 'Variable hours',
            '24 hours': '24 Hours',
            '24hr': '24 Hours',
            '24/7': '24 Hours'
        }
        
        for key, value in special_cases.items():
            if key in lower_time:
                return value
        
        try:
            # Clean the input
            time_str = time_str.strip().lower()
            
            # Extract AM/PM indicator
            is_pm = 'pm' in time_str
            is_am = 'am' in time_str
            
            # Remove am/pm and extra spaces
            time_str = re.sub(r'\s*(am|pm)\s*', '', time_str).strip()
            
            # Handle edge cases in time format
            time_str = time_str.replace('.:',':').replace('.', ':')
            if time_str.endswith(':'):
                time_str = time_str[:-1]
            
            # Check if contains digits
            if not any(c.isdigit() for c in time_str):
                return time_str.capitalize()
            
            # Parse hours and minutes
            hour = 0
            minute = 0
            
            if ':' in time_str:
                parts = time_str.split(':')
                if len(parts) >= 2:
                    try:
                        hour = int(parts[0].strip())
                        minute_str = re.sub(r'[^\d]', '', parts[1])  # Remove non-digits
                        minute = int(minute_str) if minute_str else 0
                    except ValueError:
                        return time_str.capitalize()
                else:
                    return time_str.capitalize()
            else:
                # No colon, assume it's just hours
                try:
                    hour = int(re.sub(r'[^\d]', '', time_str))
                    minute = 0
                except ValueError:
                    return time_str.capitalize()
            
            # Validate hour and minute ranges
            if not (0 <= hour <= 24) or not (0 <= minute <= 59):
                return time_str.capitalize()
            
            # Handle 24-hour format conversion
            if hour == 24:
                hour = 0
                is_am = True
            
            # Apply AM/PM logic
            if is_pm and hour < 12:
                hour += 12
            elif is_am and hour == 12:
                hour = 0
            elif not is_am and not is_pm:
                # No AM/PM specified, make educated guess
                if hour < 7:  # Early hours likely PM (closing time)
                    if hour != 0:  # Don't change midnight
                        hour += 12
            
            # Format to 12-hour time
            if hour == 0:
                return f"12:{minute:02d} AM"
            elif hour < 12:
                return f"{hour}:{minute:02d} AM"
            elif hour == 12:
                return f"12:{minute:02d} PM"
            else:
                return f"{hour-12}:{minute:02d} PM"
                
        except Exception as e:
            print(f"Error formatting time '{time_str}': {e}")
            return time_str.strip().capitalize() if time_str else "Unknown time"