from rich import print
from ..base_handler import BasePharmacyHandler
from ..utils import extract_state_postcode

class BloomsHandler(BasePharmacyHandler):
    """Handler for Blooms The Chemist pharmacies"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "blooms"
        # Define Blooms-specific headers
        self.headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'origin': 'https://www.bloomsthechemist.com.au',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://www.bloomsthechemist.com.au/',
            'sec-ch-ua': '"Microsoft Edge";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'cross-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0'
        }
        
    async def fetch_locations(self):
        """
        Fetch Blooms The Chemist locations from their API.
        
        Returns:
            List of Blooms The Chemist locations
        """
        response = await self.session_manager.get(
            url=self.pharmacy_locations.BLOOMS_URL,
            headers=self.headers
        )
        
        if response.status_code == 200:
            data = response.json()
            # Check for the new response format where locations are under 'results'
            if 'results' in data and 'locations' in data['results']:
                print(f"Found {len(data['results']['locations'])} Blooms locations in API response")
                return data['results']['locations']
            # Check for older API formats just in case
            elif 'collection' in data and 'locations' in data['collection']:
                print(f"Found {len(data['collection']['locations'])} Blooms locations in API response (collection format)")
                return data['collection']['locations']
            elif 'locations' in data:
                print(f"Found {len(data['locations'])} Blooms locations in API response (direct format)")
                return data['locations']
            else:
                print("No locations found in Blooms API response")
                print(f"API response keys: {list(data.keys())}")
                return []
        else:
            raise Exception(f"Failed to fetch Blooms The Chemist locations: {response.status_code}")
    
    async def fetch_pharmacy_details(self, location_id):
        """
        For Blooms, we already have all the data in the locations response
        This is a placeholder for API consistency
        
        Args:
            location_id: The ID of the location to get details for
            
        Returns:
            Location data (unchanged)
        """
        return {"location_details": location_id}
    
    async def fetch_all_locations_details(self):
        """
        Fetch details for all Blooms locations and return as a list
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        # For Blooms, all details are included in the locations endpoint
        print(f"Fetching all Blooms locations...")
        locations = await self.fetch_locations()
        if not locations:
            print(f"No Blooms locations found.")
            return []
            
        print(f"Found {len(locations)} Blooms locations. Processing details...")
        all_details = []
        
        for location in locations:
            try:
                extracted_details = self.extract_pharmacy_details(location)
                all_details.append(extracted_details)
            except Exception as e:
                print(f"Error processing Blooms location {location.get('id')}: {e}")
                
        print(f"Completed processing details for {len(all_details)} Blooms locations.")
        return all_details
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract specific fields from Blooms pharmacy location data
        
        Args:
            pharmacy_data: The raw pharmacy data from the API
            
        Returns:
            Dictionary containing standardized pharmacy details
        """
        # Convert the trading hours to the same format as other pharmacies
        trading_hours = {}
        for day in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']:
            if day in pharmacy_data and pharmacy_data[day]:
                hours = pharmacy_data[day].strip()
                if hours.upper() != "CLOSED":
                    try:
                        open_close = hours.split("-")
                        if len(open_close) == 2:
                            trading_hours[day.capitalize()] = {
                                "open": open_close[0].strip(),
                                "closed": open_close[1].strip()
                            }
                    except Exception:
                        # Fall back to storing the raw string if parsing fails
                        trading_hours[day.capitalize()] = {"raw": hours}
        
        # Extract state and postcode from the address
        state = ""
        postcode = ""
        suburb = ""
        address = pharmacy_data.get('streetaddress', '')
        
        # Typical format: Street address, Suburb, STATE POSTCODE, Country
        address_parts = address.split(',')
        if len(address_parts) >= 3:
            # Try to extract state and postcode from the second last part (before country)
            state_part = address_parts[-2].strip() if len(address_parts) > 2 else ""
            state_postcode = state_part.split()
            if len(state_postcode) >= 2:
                # Assume format: NSW 2000
                state = state_postcode[0]
                postcode = state_postcode[1]
            
            # Try to extract suburb from the part before state/postcode
            if len(address_parts) > 3:
                suburb = address_parts[-3].strip()
        
        # Get coordinates from loc_lat and loc_long fields
        latitude = pharmacy_data.get('loc_lat')
        longitude = pharmacy_data.get('loc_long')
        
        # Using fixed column order
        result = {
            'name': pharmacy_data.get('name'),
            'address': address,
            'email': pharmacy_data.get('email'),
            'fax': None,  # Blooms doesn't provide fax numbers in the API
            'latitude': latitude,
            'longitude': longitude,
            'phone': pharmacy_data.get('phone'),
            'postcode': postcode,
            'state': state,
            'street_address': address,
            'suburb': suburb,
            'trading_hours': trading_hours,
            'website': pharmacy_data.get('website')
        }
        
        # Remove any None values to keep the data clean
        cleaned_result = {}
        for key, value in result.items():
            if value is not None:
                cleaned_result[key] = value
                
        return cleaned_result