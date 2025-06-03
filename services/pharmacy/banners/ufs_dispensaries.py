from ..base_handler import BasePharmacyHandler
from rich import print
import json
import re
from datetime import datetime

class UfsDispensariesHandler(BasePharmacyHandler):
    """Handler for UFS Dispensaries Pharmacies"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "ufs_dispensaries"
        # Define brand-specific headers for API requests
        self.headers = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
            'Referer': 'https://ufs-pharmacies.myshopify.com/',
            'Origin': 'https://ufs-pharmacies.myshopify.com'
        }
        
    async def fetch_locations(self):
        """
        Fetch all UFS Dispensaries locations from the API
        
        Returns:
            List of basic location data
        """
        try:
            response = await self.session_manager.get(
                self.pharmacy_locations.UFS_DISPENSARIES_URL,
                headers=self.headers
            )
            
            if response.status_code == 200:
                response_text = response.text
                
                # The response is wrapped in a JSONP callback, so we need to extract the JSON
                # Format: eqfeed_callback({...json data...})
                json_start = response_text.find('({') + 1
                json_end = response_text.rfind('})')
                
                if json_start > 0 and json_end > json_start:
                    json_str = response_text[json_start:json_end + 1]
                    data = json.loads(json_str)
                    
                    print(f"Raw data from UFS Dispensaries API: {json.dumps(data, indent=2)[:500]}...")
                    
                    # Extract features from GeoJSON
                    if isinstance(data, dict) and 'features' in data:
                        features = data['features']
                        print(f"Found {len(features)} UFS Dispensaries locations")
                        return features
                    else:
                        print("No valid feature data returned from UFS Dispensaries API")
                        return []
                else:
                    print("Could not extract JSON from JSONP response")
                    return []
            else:
                print(f"Failed to fetch UFS Dispensaries locations: HTTP {response.status_code}")
                return []
                
        except Exception as e:
            print(f"Exception when fetching UFS Dispensaries locations: {str(e)}")
            return []
    
    async def fetch_pharmacy_details(self, location_id):
        """
        For UFS Dispensaries, the basic locations data already contains all details
        
        Args:
            location_id: The location ID or feature ID
            
        Returns:
            Dictionary with pharmacy details
        """
        # Since the main API returns all details, we can use the basic location data
        locations = await self.fetch_locations()
        
        for feature in locations:
            properties = feature.get('properties', {})
            if str(properties.get('id')) == str(location_id):
                return self.extract_pharmacy_details(feature)
        
        return {}
    
    async def fetch_all_locations_details(self):
        """
        Fetch details for all UFS Dispensaries locations
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        try:
            locations = await self.fetch_locations()
            
            if not locations:
                print("No UFS Dispensaries locations found")
                return []
            
            all_details = []
            for feature in locations:
                try:
                    details = self.extract_pharmacy_details(feature)
                    if details:
                        all_details.append(details)
                except Exception as e:
                    print(f"Error processing UFS Dispensaries location: {str(e)}")
                    continue
            
            print(f"Successfully processed {len(all_details)} UFS Dispensaries locations")
            return all_details
            
        except Exception as e:
            print(f"Exception when fetching all UFS Dispensaries locations: {str(e)}")
            return []
    
    def extract_pharmacy_details(self, feature_data):
        """
        Extract and standardize pharmacy details from the GeoJSON feature
        
        Args:
            feature_data: Raw feature data from the GeoJSON API
            
        Returns:
            Dictionary with standardized pharmacy details
        """
        if not feature_data or 'properties' not in feature_data:
            return {}
        
        try:
            properties = feature_data.get('properties', {})
            geometry = feature_data.get('geometry', {})
            coordinates = geometry.get('coordinates', [])
            
            # Extract basic information
            name = properties.get('name', '')
            address = properties.get('address', '')
            phone = properties.get('phone', '')
            email = properties.get('email', '')
            
            # Extract coordinates (GeoJSON format is [longitude, latitude])
            longitude = coordinates[0] if len(coordinates) > 0 else None
            latitude = coordinates[1] if len(coordinates) > 1 else None
            
            # Extract location details
            city = properties.get('city', '')
            region = properties.get('region', '')  # This is the state
            country = properties.get('country', '')
            
            # Parse address components if not already provided
            address_parts = self._parse_address(address)
            suburb = city or address_parts.get('suburb', '')
            state = region or address_parts.get('state', '')
            postcode = address_parts.get('postcode', '')
            street_address = address_parts.get('street', address)
            
            # Parse trading hours from schedule HTML
            schedule_html = properties.get('schedule', '')
            trading_hours = self._parse_trading_hours(schedule_html)
            
            # Extract web/appointment booking URL
            web_html = properties.get('web', '')
            appointment_url = self._extract_appointment_url(web_html)
            
            # Format the data according to our standardized structure
            result = {
                'name': name,
                'brand': 'UFS Dispensaries',
                'address': address,
                'street_address': street_address,
                'suburb': suburb,
                'state': state,
                'postcode': postcode,
                'phone': self._format_phone(phone),
                'email': email.lower() if email else None,
                'latitude': str(latitude) if latitude is not None else None,
                'longitude': str(longitude) if longitude is not None else None,
                'trading_hours': trading_hours,
                'feature_id': properties.get('id', ''),
                'url': properties.get('url', ''),
                'thumbnail': properties.get('thumbnail', ''),
                'appointment_url': appointment_url,
                'country': country,
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Clean up the result by removing None values and empty strings
            cleaned_result = {}
            for key, value in result.items():
                if value is not None and value != '' and value != {}:
                    cleaned_result[key] = value
            
            return cleaned_result
            
        except Exception as e:
            print(f"Error extracting UFS Dispensaries pharmacy details: {str(e)}")
            return {}
    
    def _extract_appointment_url(self, web_html):
        """
        Extract appointment booking URL from web HTML
        
        Args:
            web_html: HTML string containing the web link
            
        Returns:
            URL string or None
        """
        if not web_html:
            return None
        
        try:
            # Extract href URL from HTML anchor tag
            href_match = re.search(r'href="([^"]+)"', web_html)
            if href_match:
                return href_match.group(1)
        except Exception as e:
            print(f"Error extracting appointment URL: {str(e)}")
        
        return None
    
    def _parse_trading_hours(self, schedule_html):
        """
        Parse trading hours from the schedule HTML template
        
        Args:
            schedule_html: HTML template string with trading hours
            
        Returns:
            Dictionary with standardized trading hours
        """
        trading_hours = {}
        
        if not schedule_html:
            return trading_hours
        
        try:
            # Define day mappings
            day_mappings = {
                'row-mon': 'Monday',
                'row-tue': 'Tuesday', 
                'row-wed': 'Wednesday',
                'row-thu': 'Thursday',
                'row-fri': 'Friday',
                'row-sat': 'Saturday',
                'row-sun': 'Sunday'
            }
            
            # Find all table rows for each day
            for row_class, day_name in day_mappings.items():
                # Look for the specific row pattern
                row_pattern = rf'<tr class="{row_class}">.*?<td>(.*?)</td></tr>'
                match = re.search(row_pattern, schedule_html, re.DOTALL)
                
                if match:
                    hours_text = match.group(1).strip()
                    
                    # Replace template variables with actual values
                    hours_text = hours_text.replace('{{ am }}', 'AM')
                    hours_text = hours_text.replace('{{ pm }}', 'PM')
                    hours_text = hours_text.replace('{{ closed }}', 'Closed')
                    
                    if 'closed' in hours_text.lower():
                        trading_hours[day_name] = {
                            'open': 'Closed',
                            'closed': 'Closed'
                        }
                    elif ' - ' in hours_text:
                        # Split on dash to get open and close times
                        time_parts = hours_text.split(' - ')
                        if len(time_parts) == 2:
                            open_time = time_parts[0].strip()
                            close_time = time_parts[1].strip()
                            
                            # Format times consistently
                            open_formatted = self._format_time(open_time)
                            close_formatted = self._format_time(close_time)
                            
                            trading_hours[day_name] = {
                                'open': open_formatted,
                                'closed': close_formatted
                            }
                    else:
                        # Fallback for any other format
                        trading_hours[day_name] = {
                            'open': hours_text,
                            'closed': hours_text
                        }
        
        except Exception as e:
            print(f"Error parsing UFS Dispensaries trading hours: {str(e)}")
        
        return trading_hours
    
    def _format_time(self, time_str):
        """
        Format time string to standardized format
        
        Args:
            time_str: Time string from API
            
        Returns:
            Formatted time string
        """
        if not time_str:
            return time_str
        
        try:
            # Remove any extra whitespace
            time_clean = time_str.strip()
            
            # If already in proper format, return as-is
            if re.match(r'\d{1,2}:\d{2}\s*[AP]M', time_clean, re.IGNORECASE):
                return time_clean
            
            # Convert from 24-hour to 12-hour format if needed
            if re.match(r'\d{1,2}:\d{2}$', time_clean):
                hour, minute = map(int, time_clean.split(':'))
                
                if hour == 0:
                    return f"12:{minute:02d} AM"
                elif hour < 12:
                    return f"{hour}:{minute:02d} AM"
                elif hour == 12:
                    return f"12:{minute:02d} PM"
                else:
                    return f"{hour - 12}:{minute:02d} PM"
            
            return time_clean
            
        except Exception as e:
            print(f"Error formatting time '{time_str}': {str(e)}")
            return time_str
    
    def _parse_address(self, address):
        """
        Parse address string into components
        
        Args:
            address: Full address string
            
        Returns:
            Dictionary with address components
        """
        result = {
            'street': '',
            'suburb': '',
            'state': '',
            'postcode': ''
        }
        
        if not address:
            return result
        
        try:
            # Remove "Australia" if present
            address_clean = address.replace(', Australia', '').strip()
            
            # Split by commas
            parts = [part.strip() for part in address_clean.split(',')]
            
            if len(parts) >= 3:
                # Typical format: Street, Suburb, State, Postcode
                result['street'] = parts[0]
                
                # Last part usually contains state and postcode
                last_part = parts[-1]
                
                # Extract postcode (4 digits)
                postcode_match = re.search(r'\b(\d{4})\b', last_part)
                if postcode_match:
                    result['postcode'] = postcode_match.group(1)
                    
                    # Extract state (2-3 letter code before postcode)
                    state_match = re.search(r'\b([A-Z][a-zA-Z]+)\s+\d{4}\b', last_part)
                    if state_match:
                        result['state'] = state_match.group(1)
                
                # Suburb is usually the second part
                if len(parts) >= 2:
                    result['suburb'] = parts[1]
            
            elif len(parts) == 2:
                result['street'] = parts[0]
                # Try to extract components from the second part
                second_part = parts[1]
                
                postcode_match = re.search(r'\b(\d{4})\b', second_part)
                if postcode_match:
                    result['postcode'] = postcode_match.group(1)
                    
                    state_match = re.search(r'\b([A-Z][a-zA-Z]+)\s+\d{4}\b', second_part)
                    if state_match:
                        result['state'] = state_match.group(1)
                    
                    # Extract suburb
                    suburb_clean = re.sub(r'\b[A-Z][a-zA-Z]+\s+\d{4}\b', '', second_part).strip()
                    if suburb_clean:
                        result['suburb'] = suburb_clean
            
            # If we still don't have state, try to infer from postcode
            if result['postcode'] and not result['state']:
                result['state'] = self._infer_state_from_postcode(result['postcode'])
                
        except Exception as e:
            print(f"Error parsing address '{address}': {str(e)}")
        
        return result
    
    def _infer_state_from_postcode(self, postcode):
        """
        Infer state from postcode
        
        Args:
            postcode: 4-digit postcode string
            
        Returns:
            State abbreviation
        """
        try:
            postcode_num = int(postcode)
            
            if 1000 <= postcode_num <= 2999:
                return 'NSW'
            elif 3000 <= postcode_num <= 3999:
                return 'VIC'
            elif 4000 <= postcode_num <= 4999:
                return 'QLD'
            elif 5000 <= postcode_num <= 5999:
                return 'SA'
            elif 6000 <= postcode_num <= 6999:
                return 'WA'
            elif 7000 <= postcode_num <= 7999:
                return 'TAS'
            elif 800 <= postcode_num <= 999:
                return 'NT'
            elif 2600 <= postcode_num <= 2618 or 2900 <= postcode_num <= 2920:
                return 'ACT'
        except (ValueError, TypeError):
            pass
        
        return ''
    
    def _format_phone(self, phone):
        """
        Format phone number consistently
        
        Args:
            phone: Phone number string
            
        Returns:
            Formatted phone number
        """
        if not phone:
            return None
        
        # Remove any non-digit characters except parentheses and spaces for formatting
        phone_clean = re.sub(r'[^\d\s\(\)\-\+]', '', phone.strip())
        
        # Return cleaned phone number
        return phone_clean if phone_clean else None
