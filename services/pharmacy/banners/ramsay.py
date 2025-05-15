import re
from rich import print
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
            print("Using session ID for Ramsay API request")
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
        print("Fetching all Ramsay locations...")
        locations = await self.fetch_locations()
        if not locations:
            print("No Ramsay locations found.")
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
        
        # Extract state and postcode directly from the Address field
        # Typical format: "..., Suburb, STATE, POSTCODE"
        state = None
        postcode = None
        suburb = None
        
        # Look for state and postcode in the address string
        # In Ramsay data, they're typically at the end in format "STATE, POSTCODE"
        address_parts = address.split(',')
        cleaned_parts = [part.strip() for part in address_parts]
        
        # First try to extract from the raw fields if available
        if pharmacy_data.get('PostCode'):
            postcode = pharmacy_data.get('PostCode')
        if pharmacy_data.get('State'):
            state = pharmacy_data.get('State')
        if pharmacy_data.get('Suburb'):
            suburb = pharmacy_data.get('Suburb')
            
        # If not found, try to extract from address
        if not state or not postcode:
            # Australian state abbreviations
            aus_states = ['NSW', 'VIC', 'QLD', 'SA', 'WA', 'TAS', 'NT', 'ACT']
            
            # Check each part for state information
            for i, part in enumerate(cleaned_parts):
                # Look for parts containing state abbreviations
                for st in aus_states:
                    if st in part:
                        state = st
                        # Check if postcode is in the same part (common format)
                        postcode_match = re.search(r'\b\d{4}\b', part)
                        if postcode_match:
                            postcode = postcode_match.group(0)
                        
                        # If postcode wasn't in same part, check the next part
                        elif i+1 < len(cleaned_parts):
                            next_part = cleaned_parts[i+1]
                            postcode_match = re.search(r'\b\d{4}\b', next_part)
                            if postcode_match:
                                postcode = postcode_match.group(0)
                                
                        # Try to get suburb from part before state
                        if i > 0 and not suburb:
                            suburb = cleaned_parts[i-1]
                        
                        break
                
                # If we already found state, no need to continue
                if state:
                    break
                
                # If part contains 4 digits, likely a postcode
                postcode_match = re.search(r'\b\d{4}\b', part)
                if postcode_match and not postcode:
                    postcode = postcode_match.group(0)
                    # Try to get suburb from part before postcode
                    if i > 0 and not suburb:
                        suburb = cleaned_parts[i-1]
        
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