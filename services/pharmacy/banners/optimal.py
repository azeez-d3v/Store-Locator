from ..base_handler import BasePharmacyHandler
from rich import print
class OptimalHandler(BasePharmacyHandler):
    """Handler for Optimal Pharmacy Plus pharmacies"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "optimal"
        # Define Optimal-specific headers
        self.headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'origin': 'https://optimalpharmacyplus.com.au',
            'referer': 'https://optimalpharmacyplus.com.au/',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
        }
        
    async def fetch_locations(self):
        """
        Fetch Optimal Pharmacy Plus locations from the Elfsight widget API.
        
        Returns:
            List of Optimal Pharmacy Plus locations
        """
        response = await self.session_manager.get(
            url=self.pharmacy_locations.OPTIMAL_URL,
            headers=self.headers
        )
        
        if response.status_code == 200:
            data = response.json()
            # Extract the widgets data from the response
            if 'status' in data and data['status'] == 1 and 'data' in data:
                widget_data = data['data'].get('widgets', {})
                if widget_data:
                    # Get the first widget's data
                    widget_id = list(widget_data.keys())[0]
                    widget = widget_data[widget_id]
                    
                    # Check for the specific path to locations based on the sample response
                    if ('data' in widget and 'settings' in widget['data'] and 
                        'locations' in widget['data']['settings']):
                        locations = widget['data']['settings']['locations']
                        print(f"Found {len(locations)} Optimal locations in widget data settings")
                        return locations
                    
                    # Check alternative paths if the specific path doesn't work
                    if 'settings' in widget:
                        if 'locations' in widget['settings']:
                            locations = widget['settings']['locations']
                            print(f"Found {len(locations)} Optimal locations in widget settings")
                            return locations
                        
                    # If we get here, dump some debug info about the structure
                    print(f"Widget data keys: {list(widget.keys())}")
                    if 'data' in widget:
                        print(f"Widget data keys: {list(widget['data'].keys())}")
                        if 'settings' in widget['data']:
                            print(f"Widget data settings keys: {list(widget['data']['settings'].keys())}")
            
            print("No locations found in Optimal Pharmacy Plus widget data")
            print("API response structure:", list(data.keys()))
            return []
        else:
            raise Exception(f"Failed to fetch Optimal Pharmacy Plus locations: {response.status_code}")
    
    async def fetch_pharmacy_details(self, location_id):
        """
        For Optimal, we already have all the data in the locations response
        This is a placeholder for API consistency
        
        Args:
            location_id: The ID of the location to get details for
            
        Returns:
            Location data (unchanged)
        """
        return {"location_details": location_id}
    
    async def fetch_all_locations_details(self):
        """
        Fetch details for all Optimal locations and return as a list
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        # For Optimal, all details are included in the locations endpoint
        print("Fetching all Optimal locations...")
        locations = await self.fetch_locations()
        if not locations:
            print("No Optimal locations found.")
            return []
            
        print(f"Found {len(locations)} Optimal locations. Processing details...")
        all_details = []
        
        for location in locations:
            try:
                extracted_details = self.extract_pharmacy_details(location)
                all_details.append(extracted_details)
            except Exception as e:
                print(f"Error processing Optimal location {location.get('id')}: {e}")
                
        print(f"Completed processing details for {len(all_details)} Optimal locations.")
        return all_details
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract specific fields from Optimal pharmacy location data
        
        Args:
            pharmacy_data: The raw pharmacy data from the API
            
        Returns:
            Dictionary containing standardized pharmacy details
        """
        # Extract coordinates from the place object
        latitude = None
        longitude = None
        if 'place' in pharmacy_data and 'coordinates' in pharmacy_data['place']:
            coordinates = pharmacy_data['place']['coordinates']
            latitude = coordinates.get('lat')
            longitude = coordinates.get('lng')
        
        # Extract address information
        address = pharmacy_data.get('address', '')
        # Extract state and postcode from address
        from ..utils import extract_state_postcode
        state, postcode = extract_state_postcode(address)
        
        # Try to extract suburb from address
        suburb = None
        address_parts = address.split()
        
        # Look for state abbreviations (usually second-to-last element)
        state_abbreviations = ['NSW', 'VIC', 'QLD', 'SA', 'WA', 'TAS', 'NT', 'ACT']
        for i, part in enumerate(address_parts):
            if part in state_abbreviations and i < len(address_parts) - 1:
                # Suburb is usually right before the state
                if i > 0:
                    # Extract suburb parts between street indicators and state
                    street_indicators = ['St', 'Rd', 'Dr', 'Ave', 'Ln', 'Cres', 'Pl', 'Ct', 'Way', 'Blvd']
                    street_end_idx = -1
                    for j, addr_part in enumerate(address_parts[:i]):
                        if addr_part in street_indicators:
                            street_end_idx = j
                            break
                    
                    if street_end_idx >= 0:
                        suburb_parts = address_parts[street_end_idx + 1:i]
                        if suburb_parts:
                            suburb = ' '.join(suburb_parts)
                break
        
        # Parse trading hours from the daily open/hours fields
        trading_hours = {}
        days = {
            'Monday': ('dayMondayOpen', 'dayMondayHours'),
            'Tuesday': ('dayTuesdayOpen', 'dayTuesdayHours'),
            'Wednesday': ('dayWednesdayOpen', 'dayWednesdayHours'),
            'Thursday': ('dayThursdayOpen', 'dayThursdayHours'),
            'Friday': ('dayFridayOpen', 'dayFridayHours'),
            'Saturday': ('daySaturdayOpen', 'daySaturdayHours'),
            'Sunday': ('daySundayOpen', 'daySundayHours')
        }
        
        for day, (open_key, hours_key) in days.items():
            # For Optimal, True means open 
            is_open = pharmacy_data.get(open_key, False)
                
            if is_open and hours_key in pharmacy_data and pharmacy_data[hours_key]:
                hours_data = pharmacy_data[hours_key]
                if hours_data and isinstance(hours_data, list) and len(hours_data) > 0:
                    time_range = hours_data[0].get('timeRange', [])
                    if len(time_range) == 2:
                        trading_hours[day] = {
                            'open': time_range[0],
                            'closed': time_range[1]
                        }
            else:
                trading_hours[day] = {'open': 'Closed', 'closed': 'Closed'}
        
        # Using fixed column order
        result = {
            'name': pharmacy_data.get('name'),
            'address': address,
            'email': pharmacy_data.get('email'),
            'fax': None,  # Optimal doesn't provide fax numbers
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