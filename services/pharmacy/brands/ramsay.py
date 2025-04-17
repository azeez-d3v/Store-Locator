import asyncio
import re
from ..base_handler import BasePharmacyHandler

class RamsayHandler(BasePharmacyHandler):
    """Handler for Ramsay Pharmacies"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "ramsay"
        # Define Ramsay-specific headers
        self.headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json; charset=UTF-8',
            'Origin': 'https://www.ramsaypharmacy.com.au',
            'Referer': 'https://www.ramsaypharmacy.com.au/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
        }
        # Default payload
        self.payload = {
            "Services": None,
            "PharmacyName": "ramsay",
            "WeekDayId": None,
            "TodayId": 3,
            "TodayTime": "15:51:37",
            "IsOpenNow": False,
            "IsClickCollect": False,
            "Is24Hours": False,
            "IsOpenWeekend": False,
            "Region": None,
            "Distance": 0,
            "Latitude": 0,
            "Longitude": 0,
            "PageIndex": 1,
            "PageSize": 100,
            "OrderBy": ""
        }
        
    async def get_session_id(self):
        """
        Fetch the dynamic session ID from Ramsay Pharmacy's store finder page.
        This is needed for the API to work correctly.
        
        Returns:
            String containing the session ID
        """
        url = "https://www.ramsaypharmacy.com.au/Store-Finder"
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
        }
        
        response = await self.session_manager.get(
            url=url,
            headers=headers
        )
        
        if response.status_code == 200:
            html_content = response.text
            
            # Look for the session ID in the script tag
            session_id_pattern = r"StoreLocator\.LoadInitialData\('([^']+)', ''\);"
            match = re.search(session_id_pattern, html_content)
            
            if match:
                session_id = match.group(1)
                print(f"Successfully extracted Ramsay session ID: {session_id[:10]}...")
                return session_id
            else:
                print("Could not find session ID in Ramsay Store Finder page")
                return None
        else:
            print(f"Failed to fetch Ramsay Store Finder page: {response.status_code}")
            return None
        
    async def fetch_locations(self):
        """
        Fetch Ramsay Pharmacy locations from their API.
        
        Returns:
            List of Ramsay Pharmacy locations
        """
        # First get the dynamic session ID
        session_id = await self.get_session_id()
        
        # If we couldn't get a session ID, use a default empty payload
        payload = self.payload.copy()
        
        # If we have a session ID, add it to a special header
        headers = self.headers.copy()
        if session_id:
            headers['SessionId'] = session_id
            print(f"Using session ID for Ramsay API request")
        else:
            print("Warning: No session ID found for Ramsay API request")
            
        response = await self.session_manager.post(
            url=self.pharmacy_locations.RAMSAY_URL,
            json=payload,
            headers=headers
        )
        
        if response.status_code == 200:
            data = response.json()
            
            # Check if data is a direct array (based on sample response)
            if isinstance(data, list):
                print(f"Found {len(data)} Ramsay locations in API response (direct array)")
                return data
            # Check nested structure (older API format)
            elif 'Data' in data and 'Results' in data['Data']:
                print(f"Found {len(data['Data']['Results'])} Ramsay locations in API response (nested)")
                return data['Data']['Results']
            else:
                print("No locations found in Ramsay API response")
                print(f"API response keys: {list(data.keys()) if isinstance(data, dict) else 'array'}")
                return []
        else:
            raise Exception(f"Failed to fetch Ramsay Pharmacy locations: {response.status_code}")
    
    async def fetch_pharmacy_details(self, location_id):
        """
        For Ramsay, we already have all the data in the locations response
        This is a placeholder for API consistency
        
        Args:
            location_id: The ID of the location to get details for
            
        Returns:
            Location data (unchanged)
        """
        return {"location_details": location_id}
    
    async def fetch_all_locations_details(self):
        """
        Fetch details for all Ramsay locations and return as a list
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        # For Ramsay, all details are included in the locations endpoint
        print(f"Fetching all Ramsay locations...")
        locations = await self.fetch_locations()
        if not locations:
            print(f"No Ramsay locations found.")
            return []
            
        print(f"Found {len(locations)} Ramsay locations. Processing details...")
        all_details = []
        
        for location in locations:
            try:
                extracted_details = self.extract_pharmacy_details(location)
                all_details.append(extracted_details)
            except Exception as e:
                print(f"Error processing Ramsay location {location.get('PharmacyId')}: {e}")
                
        print(f"Completed processing details for {len(all_details)} Ramsay locations.")
        return all_details
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract specific fields from Ramsay pharmacy location data
        
        Args:
            pharmacy_data: The raw pharmacy data from the API
            
        Returns:
            Dictionary containing standardized pharmacy details
        """
        # Extract address, state, and postcode
        address = pharmacy_data.get('Address', '')
        address = address.replace('<br>', ', ')  # Replace HTML line breaks with commas
        address_parts = address.split(',')
        
        # Try to extract state and postcode
        state = None
        postcode = None
        suburb = None
        
        # Look for state and postcode in address (typically at the end)
        for part in reversed(address_parts):
            part = part.strip()
            if ',' in part:
                subparts = part.split(',')
                for subpart in subparts:
                    subpart = subpart.strip()
                    # Common Australian state abbreviations
                    if any(s in subpart for s in ['NSW', 'VIC', 'QLD', 'SA', 'WA', 'TAS', 'NT', 'ACT']):
                        state_postcode = subpart.split()
                        if len(state_postcode) >= 2:
                            state = state_postcode[0]
                            postcode = state_postcode[1]
                        break
            elif 'NSW' in part or 'VIC' in part or 'QLD' in part or 'SA' in part or 'WA' in part or 'TAS' in part or 'NT' in part or 'ACT' in part:
                state_postcode = part.split()
                if len(state_postcode) >= 2:
                    state = state_postcode[0]
                    postcode = state_postcode[1]
                break
        
        # Try to extract suburb (usually before state and postcode)
        for i, part in enumerate(address_parts):
            part = part.strip()
            if state and part.endswith(state):
                if i > 0:
                    suburb = address_parts[i - 1].strip()
                break
        
        # Parse operating hours
        trading_hours = {}
        if pharmacy_data.get('OpereatingHourDescription'):
            hours_desc = pharmacy_data.get('OpereatingHourDescription').replace('<br/>', '\n')
            for line in hours_desc.split('\n'):
                if ':' in line:
                    day_hours = line.strip().split(':')
                    if len(day_hours) >= 2:
                        day = day_hours[0].strip()
                        hours = day_hours[1].strip()
                        
                        if 'Closed' in hours:
                            # Handle closed days
                            trading_hours[day] = {
                                "open": "Closed",
                                "closed": "Closed"
                            }
                        else:
                            # Try to parse open/close times
                            try:
                                open_close = hours.split('-')
                                if len(open_close) == 2:
                                    trading_hours[day] = {
                                        "open": open_close[0].strip(),
                                        "closed": open_close[1].strip()
                                    }
                            except Exception:
                                # Fall back to storing the raw string
                                trading_hours[day] = {"raw": hours}
        
        # Using fixed column order
        result = {
            'name': pharmacy_data.get('PharmacyName'),
            'address': address,
            'email': None,  # Ramsay doesn't provide email in API
            'fax': pharmacy_data.get('FaxNumber'),
            'latitude': pharmacy_data.get('Latitude'),
            'longitude': pharmacy_data.get('Longitude'),
            'phone': pharmacy_data.get('PhoneNumber'),
            'postcode': postcode,
            'state': state,
            'street_address': address,
            'suburb': suburb,
            'trading_hours': trading_hours,
            'website': None  # Ramsay doesn't provide website in API
        }
        
        # Keep additional fields outside the standard columns
        if 'reference_id' not in result:
            result['reference_id'] = pharmacy_data.get('ReferenceId')
        if 'pharmacy_id' not in result:
            result['pharmacy_id'] = pharmacy_data.get('PharmacyId')
        if 'where_to_find' not in result:
            result['where_to_find'] = pharmacy_data.get('WhereToFind')
        
        # Remove any None values to keep the data clean
        cleaned_result = {}
        for key, value in result.items():
            if value is not None:
                cleaned_result[key] = value
                
        return cleaned_result