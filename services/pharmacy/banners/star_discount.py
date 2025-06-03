from ..base_handler import BasePharmacyHandler
from rich import print
import json
import re
from datetime import datetime

class StarDiscountHandler(BasePharmacyHandler):
    """Handler for Star Discount Chemist Pharmacies"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "star_discount"
        # Define brand-specific headers for API requests
        self.headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
            'Referer': 'https://sdc-frontend-fmd6ahcwgxhbcshd.z01.azurefd.net/',
            'Origin': 'https://sdc-frontend-fmd6ahcwgxhbcshd.z01.azurefd.net'
        }
    async def fetch_locations(self):
        """
        Fetch all Star Discount Chemist locations from the API
        
        Returns:
            List of basic location data
        """
        try:
            response = await self.session_manager.get(
                self.pharmacy_locations.STAR_DISCOUNT_URL,
                headers=self.headers
            )
            
            if response.status_code == 200:
                data = response.json()
                
                print(f"Raw data from Star Discount Chemist API: {json.dumps(data[:2], indent=2) if data else 'No data'}")
                
                if isinstance(data, list) and data:
                    print(f"Found {len(data)} Star Discount Chemist locations")
                    return data
                else:
                    print("No valid location data returned from Star Discount Chemist API")
                    return []
            else:
                print(f"Failed to fetch Star Discount Chemist locations: HTTP {response.status_code}")
                return []
                
        except Exception as e:
            print(f"Exception when fetching Star Discount Chemist locations: {str(e)}")
            return []
    
    async def fetch_pharmacy_details(self, location_id):
        """
        For Star Discount Chemist, the basic locations data already contains all details
        
        Args:
            location_id: The location ID or store ID
            
        Returns:
            Dictionary with pharmacy details
        """
        # Since the main API returns all details, we can use the basic location data
        locations = await self.fetch_locations()
        
        for location in locations:
            if (str(location.get('locationId')) == str(location_id) or 
                str(location.get('storeId')) == str(location_id)):
                return self.extract_pharmacy_details(location)
        
        return {}
    
    async def fetch_all_locations_details(self):
        """
        Fetch details for all Star Discount Chemist locations
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        try:
            locations = await self.fetch_locations()
            
            if not locations:
                print("No Star Discount Chemist locations found")
                return []
            
            all_details = []
            for location in locations:
                try:
                    details = self.extract_pharmacy_details(location)
                    if details:
                        all_details.append(details)
                except Exception as e:
                    print(f"Error processing Star Discount Chemist location: {str(e)}")
                    continue
            
            print(f"Successfully processed {len(all_details)} Star Discount Chemist locations")
            return all_details
            
        except Exception as e:
            print(f"Exception when fetching all Star Discount Chemist locations: {str(e)}")
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
            name = pharmacy_data.get('name', '')
            address = pharmacy_data.get('formattedAddress', '')
            phone = pharmacy_data.get('phoneNumber', '')
            email = pharmacy_data.get('email', '')
            
            # Extract coordinates
            latitude = pharmacy_data.get('latitude')
            longitude = pharmacy_data.get('longitude')
            
            # Extract location details
            suburb = pharmacy_data.get('suburb', '')
            state = pharmacy_data.get('state', '')
            
            # Parse address components if not already provided
            address_parts = self._parse_address(address)
            if not suburb and address_parts.get('suburb'):
                suburb = address_parts['suburb']
            if not state and address_parts.get('state'):
                state = address_parts['state']
            
            postcode = address_parts.get('postcode', '')
            street_address = address_parts.get('street', address)
            
            # Parse trading hours
            trading_hours = self._parse_trading_hours(pharmacy_data.get('openingPeriod', []))
            
            # Format the data according to our standardized structure
            result = {
                'name': name,
                'brand': 'Star Discount Chemist',
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
                'store_id': pharmacy_data.get('storeId', ''),
                'location_id': pharmacy_data.get('locationId', ''),
                'google_place_id': pharmacy_data.get('googlePlaceId', ''),
                'is_active': pharmacy_data.get('isActive', True),
                'is_delivery_store': pharmacy_data.get('isDeliveryStore', False),
                'is_click_collect': pharmacy_data.get('isClickCollect', False),
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Clean up the result by removing None values and empty strings
            cleaned_result = {}
            for key, value in result.items():
                if value is not None and value != '' and value != {}:
                    cleaned_result[key] = value
            
            return cleaned_result
            
        except Exception as e:
            print(f"Error extracting Star Discount Chemist pharmacy details: {str(e)}")
            return {}
    
    def _parse_trading_hours(self, opening_period):
        """
        Parse trading hours from the openingPeriod array
        
        Args:
            opening_period: List of strings with day and hours information
            
        Returns:
            Dictionary with standardized trading hours
        """
        trading_hours = {}
        
        if not opening_period:
            return trading_hours
        
        try:
            for period in opening_period:
                if not isinstance(period, str):
                    continue
                
                # Remove HTML entities and extra whitespace
                period_clean = period.replace('\u202F', ' ').replace('\u2009', ' ').strip()
                
                # Split by colon to get day and hours
                if ':' in period_clean:
                    day_part, hours_part = period_clean.split(':', 1)
                    day = day_part.strip()
                    hours = hours_part.strip()
                    
                    if 'closed' in hours.lower():
                        trading_hours[day] = {
                            'open': 'Closed',
                            'closed': 'Closed'
                        }
                    elif '–' in hours or '-' in hours:
                        # Split on different dash characters
                        separator = '–' if '–' in hours else '-'
                        time_parts = hours.split(separator)
                        if len(time_parts) == 2:
                            open_time = time_parts[0].strip()
                            close_time = time_parts[1].strip()
                            
                            # Convert to standard 12-hour format
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
            print(f"Error parsing Star Discount Chemist trading hours: {str(e)}")
        
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
            # Remove any extra whitespace and unicode characters
            time_clean = time_str.replace('\u202F', ' ').replace('\u2009', ' ').strip()
            
            # If already in proper format, return as-is
            if re.match(r'\d{1,2}:\d{2}\s*[AP]M', time_clean, re.IGNORECASE):
                return time_clean
            
            # Convert from 24-hour to 12-hour format if needed
            if re.match(r'\d{1,2}:\d{2}', time_clean):
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
                # Typical format: Street, Suburb STATE POSTCODE
                result['street'] = parts[0]
                
                # Last part usually contains state and postcode
                last_part = parts[-1]
                
                # Extract postcode (4 digits)
                postcode_match = re.search(r'\b(\d{4})\b', last_part)
                if postcode_match:
                    result['postcode'] = postcode_match.group(1)
                    
                    # Extract state (2-3 letter code before postcode)
                    state_match = re.search(r'\b([A-Z]{2,3})\s+\d{4}\b', last_part)
                    if state_match:
                        result['state'] = state_match.group(1)
                
                # Suburb is usually the second-to-last part or part of the last part
                if len(parts) >= 2:
                    suburb_part = parts[-2] if len(parts) > 2 else parts[-1]
                    # Remove state and postcode from suburb
                    suburb_clean = re.sub(r'\b[A-Z]{2,3}\s+\d{4}\b', '', suburb_part).strip()
                    if suburb_clean:
                        result['suburb'] = suburb_clean
            
            elif len(parts) == 2:
                result['street'] = parts[0]
                # Try to extract components from the second part
                second_part = parts[1]
                
                postcode_match = re.search(r'\b(\d{4})\b', second_part)
                if postcode_match:
                    result['postcode'] = postcode_match.group(1)
                    
                    state_match = re.search(r'\b([A-Z]{2,3})\s+\d{4}\b', second_part)
                    if state_match:
                        result['state'] = state_match.group(1)
                    
                    # Extract suburb
                    suburb_clean = re.sub(r'\b[A-Z]{2,3}\s+\d{4}\b', '', second_part).strip()
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
