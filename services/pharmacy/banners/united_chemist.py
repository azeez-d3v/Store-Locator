from ..base_handler import BasePharmacyHandler
from rich import print
import json
import re
from datetime import datetime

class UnitedChemistHandler(BasePharmacyHandler):
    """Handler for United Chemist Pharmacies"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "united_chemist"
        # Define brand-specific headers for API requests
        self.headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
            'Referer': 'https://www.unitedchemists.net.au/',
            'Origin': 'https://www.unitedchemists.net.au'
        }
        
    async def fetch_locations(self):
        """
        Fetch all United Chemist locations from the API
        
        Returns:
            List of basic location data
        """
        try:
            response = await self.session_manager.get(
                self.pharmacy_locations.UNITED_CHEMIST_URL,
                headers=self.headers
            )
            
            if response.status_code == 200:
                data = response.json()
                
                print(f"Raw data from United Chemist API: {json.dumps(data[:2], indent=2) if data else 'No data'}")
                
                if isinstance(data, list) and data:
                    print(f"Found {len(data)} United Chemist locations")
                    return data
                else:
                    print("No valid location data returned from United Chemist API")
                    return []
            else:
                print(f"Failed to fetch United Chemist locations: HTTP {response.status_code}")
                return []
                
        except Exception as e:
            print(f"Exception when fetching United Chemist locations: {str(e)}")
            return []
    
    async def fetch_pharmacy_details(self, location_id):
        """
        For United Chemist, the basic locations data already contains all details
        
        Args:
            location_id: The store code or location ID
            
        Returns:
            Dictionary with pharmacy details
        """
        # Since the main API returns all details, we can use the basic location data
        locations = await self.fetch_locations()
        
        for location in locations:
            if (str(location.get('store_code')) == str(location_id) or 
                str(location.get('business_name')) == str(location_id)):
                return self.extract_pharmacy_details(location)
        
        return {}
    
    async def fetch_all_locations_details(self):
        """
        Fetch details for all United Chemist locations
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        try:
            locations = await self.fetch_locations()
            
            if not locations:
                print("No United Chemist locations found")
                return []
            
            all_details = []
            for location in locations:
                try:
                    details = self.extract_pharmacy_details(location)
                    if details:
                        all_details.append(details)
                except Exception as e:
                    print(f"Error processing United Chemist location: {str(e)}")
                    continue
            
            print(f"Successfully processed {len(all_details)} United Chemist locations")
            return all_details
            
        except Exception as e:
            print(f"Exception when fetching all United Chemist locations: {str(e)}")
            return []
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract and standardize pharmacy details from the API response
        
        Args:
            pharmacy_data: Raw pharmacy data from the API
            
        Returns:
            Dictionary with standardized pharmacy details
        """
        if not pharmacy_data:
            return {}
        
        try:
            # Extract basic information
            name = pharmacy_data.get('business_name', '')
            address = pharmacy_data.get('address', '')
            phone = pharmacy_data.get('phone', '')
            fax = pharmacy_data.get('fax', '')
            email = pharmacy_data.get('email', '')
            
            # Extract coordinates
            latitude = pharmacy_data.get('lat')
            longitude = pharmacy_data.get('lng')
            
            # Extract additional details
            store_code = pharmacy_data.get('store_code', '')
            pharmacist = pharmacy_data.get('pharmacist', '')
            
            # Parse address components
            address_parts = self._parse_address(address)
            suburb = address_parts.get('suburb', '')
            state = address_parts.get('state', '')
            postcode = address_parts.get('postcode', '')
            street_address = address_parts.get('street', address)
            
            # Parse trading hours
            hours_data = pharmacy_data.get('hours', {})
            trading_hours = self._parse_trading_hours(hours_data)
            
            # Extract logos/services information
            logos = pharmacy_data.get('logos', {})
            services = self._parse_services(logos)
            
            # Format the data according to our standardized structure
            result = {
                'name': name,
                'brand': 'United Chemist',
                'address': address,
                'street_address': street_address,
                'suburb': suburb,
                'state': state,
                'postcode': postcode,
                'phone': self._format_phone(phone),
                'fax': self._format_phone(fax),
                'email': email.lower() if email else None,
                'latitude': str(latitude) if latitude is not None else None,
                'longitude': str(longitude) if longitude is not None else None,
                'trading_hours': trading_hours,
                'store_code': store_code,
                'pharmacist': pharmacist,
                'services': services,
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Clean up the result by removing None values and empty strings
            cleaned_result = {}
            for key, value in result.items():
                if value is not None and value != '' and value != {}:
                    cleaned_result[key] = value
            
            return cleaned_result
            
        except Exception as e:
            print(f"Error extracting United Chemist pharmacy details: {str(e)}")
            return {}
    
    def _parse_services(self, logos):
        """
        Parse services from logos data
        
        Args:
            logos: Dictionary containing service flags
            
        Returns:
            List of available services
        """
        services = []
        
        try:
            # Map logo flags to service names
            service_mapping = {
                'ndss': 'NDSS (National Diabetes Services Scheme)',
                'rimmel': 'Rimmel Cosmetics',
                'gluten': 'Gluten Free Products'
            }
            
            for logo_key, logo_value in logos.items():
                if logo_value == "1" and logo_key in service_mapping:
                    services.append(service_mapping[logo_key])
                    
        except Exception as e:
            print(f"Error parsing services: {str(e)}")
        
        return services
    
    def _parse_trading_hours(self, hours_data):
        """
        Parse trading hours from the hours dictionary
        
        Args:
            hours_data: Dictionary with day:time mappings
            
        Returns:
            Dictionary with standardized trading hours
        """
        trading_hours = {}
        
        if not hours_data:
            return trading_hours
        
        try:
            for day, hours in hours_data.items():
                if not hours:
                    continue
                
                if 'closed' in hours.lower():
                    trading_hours[day] = {
                        'open': 'Closed',
                        'closed': 'Closed'
                    }
                elif ' - ' in hours:
                    # Split on dash to get open and close times
                    time_parts = hours.split(' - ')
                    if len(time_parts) == 2:
                        open_time = time_parts[0].strip()
                        close_time = time_parts[1].strip()
                        
                        # Format times consistently
                        open_formatted = self._format_time(open_time)
                        close_formatted = self._format_time(close_time)
                        
                        trading_hours[day] = {
                            'open': open_formatted,
                            'closed': close_formatted
                        }
                else:
                    # Fallback for any other format
                    trading_hours[day] = {
                        'open': hours,
                        'closed': hours
                    }
        
        except Exception as e:
            print(f"Error parsing United Chemist trading hours: {str(e)}")
        
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
            if re.match(r'\d{1,2}:\d{2}[AP]M', time_clean, re.IGNORECASE):
                return time_clean
            
            # Convert from 24-hour to 12-hour format if needed
            if re.match(r'\d{1,2}:\d{2}$', time_clean):
                hour, minute = map(int, time_clean.split(':'))
                
                if hour == 0:
                    return f"12:{minute:02d}AM"
                elif hour < 12:
                    return f"{hour}:{minute:02d}AM"
                elif hour == 12:
                    return f"12:{minute:02d}PM"
                else:
                    return f"{hour - 12}:{minute:02d}PM"
            
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
                # Typical format: Street, State, Postcode or Street, Suburb, State, Postcode
                result['street'] = parts[0]
                
                # Find state and postcode in the parts
                for i, part in enumerate(parts):
                    # Look for state abbreviation and postcode
                    state_postcode_match = re.search(r'\b([A-Z]{2,3})\s*,?\s*(\d{4})\b', part)
                    if state_postcode_match:
                        result['state'] = state_postcode_match.group(1)
                        result['postcode'] = state_postcode_match.group(2)
                        
                        # Remove state and postcode from this part to get suburb
                        suburb_part = re.sub(r'\b[A-Z]{2,3}\s*,?\s*\d{4}\b', '', part).strip()
                        if suburb_part:
                            result['suburb'] = suburb_part
                        
                        # If this is not the first part and suburb is empty, 
                        # check previous parts for suburb
                        if not result['suburb'] and i > 1:
                            result['suburb'] = parts[i-1]
                        break
                
                # If we didn't find state/postcode in a combined part, look separately
                if not result['state'] or not result['postcode']:
                    for part in parts:
                        if not result['postcode']:
                            postcode_match = re.search(r'\b(\d{4})\b', part)
                            if postcode_match:
                                result['postcode'] = postcode_match.group(1)
                        
                        if not result['state']:
                            state_match = re.search(r'\b([A-Z]{2,3})\b', part)
                            if state_match and state_match.group(1) in ['NSW', 'VIC', 'QLD', 'SA', 'WA', 'TAS', 'NT', 'ACT']:
                                result['state'] = state_match.group(1)
            
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
