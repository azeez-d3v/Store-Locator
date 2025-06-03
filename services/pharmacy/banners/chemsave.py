from ..base_handler import BasePharmacyHandler
import logging
import json
from datetime import datetime
from rich import print

class ChemsaveHandler(BasePharmacyHandler):
    """Handler for Chemsave Pharmacy stores"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "chemsave"
        self.headers = {
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest',
            'referer': 'https://www.chemsave.com.au/'
        }
        self.logger = logging.getLogger(__name__)
        
    async def fetch_locations(self):
        """
        Fetch all Chemsave Pharmacy store locations from the API endpoint
        
        Returns:
            List of Chemsave Pharmacy locations
        """
        try:
            # Make POST request to the locations endpoint
            response = await self.session_manager.post(
                url=self.pharmacy_locations.CHEMSAVE_URL,
                headers=self.headers,
                data={
                    'lat': '',
                    'lng': '',
                    'radius': ''
                }
            )
            
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch Chemsave locations: HTTP {response.status_code}")
                return []
            
            # Parse the JSON response
            try:
                json_data = response.json()
                if 'items' not in json_data:
                    self.logger.error("No 'items' key in Chemsave API response")
                    return []
                
                locations = json_data['items']
                self.logger.info(f"Found {len(locations)} Chemsave Pharmacy locations")
                return locations
                
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON decode error for Chemsave locations: {str(e)}")
                return []
        except Exception as e:
            self.logger.error(f"Exception when fetching Chemsave locations: {str(e)}")
            return []
    
    async def fetch_pharmacy_details(self, location_id):
        """
        Fetch details for a specific pharmacy location.
        For Chemsave, all details are included in the main API call.
        
        Args:
            location_id: The ID of the location to get details for
            
        Returns:
            Location details data
        """
        # All details are included in the locations response for Chemsave
        return {"location_details": location_id}
        
    async def fetch_all_locations_details(self):
        """
        Fetch details for all locations and return as a list.
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        print("Fetching all Chemsave Pharmacy locations...")
        locations = await self.fetch_locations()
        if not locations:
            print("No Chemsave Pharmacy locations found.")
            return []
            
        print(f"Found {len(locations)} Chemsave Pharmacy locations. Processing details...")
        all_details = []
        
        for location in locations:
            try:
                extracted_details = self.extract_pharmacy_details(location)
                all_details.append(extracted_details)
            except Exception as e:
                print(f"Error processing Chemsave Pharmacy location {location.get('id')}: {e}")
                
        print(f"Completed processing details for {len(all_details)} Chemsave Pharmacy locations.")
        return all_details
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract and standardize pharmacy details from raw API data.
        
        Args:
            pharmacy_data: Raw pharmacy data from the API
            
        Returns:
            Dictionary with standardized pharmacy details
        """
        # Parse opening hours from schedule JSON
        trading_hours = self._parse_trading_hours(pharmacy_data.get('schedule', ''))
          # Construct complete address
        address_parts = []
        if pharmacy_data.get('address'):
            address_parts.append(str(pharmacy_data.get('address')))
        if pharmacy_data.get('city'):
            address_parts.append(str(pharmacy_data.get('city')))
        if pharmacy_data.get('state'):
            address_parts.append(str(pharmacy_data.get('state')))
        if pharmacy_data.get('zip'):
            address_parts.append(str(pharmacy_data.get('zip')))
        
        complete_address = ', '.join(filter(None, address_parts))
        return {
            'name': (pharmacy_data.get('name') or '').strip(),
            'address': complete_address,
            'phone': (pharmacy_data.get('phone') or '').strip(),
            'fax': '',  # Not provided in Chemsave API
            'email': (pharmacy_data.get('email') or '').strip(),
            'trading_hours': trading_hours,
            'latitude': pharmacy_data.get('lat') or '',
            'longitude': pharmacy_data.get('lng') or '',
            'state': pharmacy_data.get('state') or '',
            'postcode': pharmacy_data.get('zip') or '',
            'city': pharmacy_data.get('city') or '',
            'country': pharmacy_data.get('country') or 'AU',
            'website': pharmacy_data.get('website') or '',
            'description': self._clean_description(pharmacy_data.get('description') or ''),
            'brand': 'Chemsave'
        }    
    
    def _parse_trading_hours(self, schedule_json):
        """
        Parse trading hours from the JSON schedule format.
        
        Args:
            schedule_json: JSON string containing schedule data
            
        Returns:
            Formatted trading hours string
        """
        if not schedule_json:
            return "Trading hours not available"
        
        try:
            schedule = json.loads(schedule_json)
            trading_hours = []
            
            days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
            day_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
            
            all_closed = True  # Track if all days are closed
            
            for day, day_name in zip(days, day_names):
                if day in schedule:
                    day_schedule = schedule[day]
                    from_time = day_schedule.get('from', ['00', '00'])
                    to_time = day_schedule.get('to', ['00', '00'])
                    
                    # Ensure from_time and to_time are lists
                    if not isinstance(from_time, list):
                        from_time = ['00', '00']
                    if not isinstance(to_time, list):
                        to_time = ['00', '00']
                        
                    # Ensure we have at least 2 elements
                    if len(from_time) < 2:
                        from_time = ['00', '00']
                    if len(to_time) < 2:
                        to_time = ['00', '00']
                    
                    # Convert to strings and strip any whitespace
                    from_hour = str(from_time[0]).strip()
                    from_min = str(from_time[1]).strip()
                    to_hour = str(to_time[0]).strip()
                    to_min = str(to_time[1]).strip()
                    
                    # Check if the store is closed (00:00 - 00:00 or empty values)
                    if ((from_hour == '00' or from_hour == '') and 
                        (from_min == '00' or from_min == '') and 
                        (to_hour == '00' or to_hour == '') and 
                        (to_min == '00' or to_min == '')):
                        trading_hours.append(f"{day_name}: Closed")
                    else:
                        all_closed = False
                        # Format time with leading zeros
                        from_hour = from_hour.zfill(2)
                        from_min = from_min.zfill(2)
                        to_hour = to_hour.zfill(2)
                        to_min = to_min.zfill(2)
                        
                        from_formatted = f"{from_hour}:{from_min}"
                        to_formatted = f"{to_hour}:{to_min}"
                        trading_hours.append(f"{day_name}: {from_formatted} - {to_formatted}")
                else:
                    trading_hours.append(f"{day_name}: Closed")
            
            # If all days are closed, it likely means the schedule data is not available
            if all_closed:
                return "Trading hours not available"
            
            return '; '.join(trading_hours) if trading_hours else "Trading hours not available"
            
        except (json.JSONDecodeError, KeyError, IndexError, TypeError) as e:
            self.logger.warning(f"Error parsing trading hours from schedule: {schedule_json[:100]}... Error: {str(e)}")
            return "Trading hours not available"
    
    def _clean_description(self, description):
        """
        Clean HTML tags and formatting from description text.
        
        Args:
            description: Raw description text that may contain HTML
            
        Returns:
            Cleaned description text
        """
        if not description:
            return ""
        
        # Remove HTML tags
        import re
        clean_text = re.sub(r'<[^>]+>', '', description)
        # Clean up extra whitespace
        clean_text = ' '.join(clean_text.split())
        return clean_text.strip()
