from ...base_handler import BasePharmacyHandler
import re
from rich import print
from datetime import datetime

class ChemistWarehouseNZHandler(BasePharmacyHandler):
    """Handler for Chemist Warehouse NZ Pharmacies"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "chemist_warehouse_nz"
        # Define brand-specific headers for API requests
        self.headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Pragma': 'no-cache',
            'Referer': 'https://www.chemistwarehouse.co.nz/aboutus/store-locator',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
            'X-Requested-With': 'XMLHttpRequest',
        }
        # The URL for NZ stores
        self.nz_url = self.pharmacy_locations.CHEMIST_WAREHOUSE_NZ_URL
    async def fetch_locations(self):
        """
        Fetch all Chemist Warehouse NZ locations.
        
        Returns:
            List of Chemist Warehouse NZ locations
        """
        
        # Make API call to get location data
        response = await self.session_manager.get(
            url=self.nz_url,
            headers=self.headers
        )
        
        if response.status_code == 200:
            # The API returns a list directly
            locations = response.json()
            print(f"Found {len(locations)} Chemist Warehouse NZ locations")
            return locations
        else:
            raise Exception(f"Failed to fetch Chemist Warehouse NZ locations: {response.status_code}")
    
    async def fetch_pharmacy_details(self, location_id):
        """
        Fetch details for a specific pharmacy location.
        For Chemist Warehouse NZ, all details are included in the main API call.
        
        Args:
            location_id: The ID of the location to get details for
            
        Returns:
            Location details data
        """
        # All details are included in the locations response for Chemist Warehouse NZ
        return {"location_details": location_id}
        
    async def fetch_all_locations_details(self):
        """
        Fetch details for all locations and return as a list.
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        print(f"Fetching all Chemist Warehouse NZ locations...")
        locations = await self.fetch_locations()
        if not locations:
            print(f"No Chemist Warehouse NZ locations found.")
            return []
            
        print(f"Found {len(locations)} Chemist Warehouse NZ locations. Processing details...")
        all_details = []
        
        for location in locations:
            try:
                extracted_details = self.extract_pharmacy_details(location)
                all_details.append(extracted_details)
            except Exception as e:
                print(f"Error processing Chemist Warehouse NZ location {location.get('Id')}: {e}")
                
        print(f"Completed processing details for {len(all_details)} Chemist Warehouse NZ locations.")
        return all_details
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract and standardize pharmacy details from raw API data.
        
        Args:
            pharmacy_data: Raw pharmacy data from the API
            
        Returns:
            Dictionary with standardized pharmacy details
        """
        # Parse opening hours from OpenHours array
        trading_hours = self._parse_trading_hours(pharmacy_data.get('OpenHours', []))
        
        # Extract geo data
        geo_data = pharmacy_data.get('GeoPoint', {})
        latitude = geo_data.get('Latitude')
        longitude = geo_data.get('Longitude')
        
        # Format the data according to our standardized structure
        result = {
            'name': pharmacy_data.get('Name'),
            'address': pharmacy_data.get('Address'),
            'email': pharmacy_data.get('Email'),
            'latitude': str(latitude) if latitude is not None else None,
            'longitude': str(longitude) if longitude is not None else None,
            'phone': pharmacy_data.get('Phone'),
            'postcode': pharmacy_data.get('Postcode'),
            'state': pharmacy_data.get('State'),
            'street_address': pharmacy_data.get('Address'),
            'suburb': pharmacy_data.get('Suburb'),
            'trading_hours': trading_hours,
            'fax': pharmacy_data.get('Fax'),
            'location_info': pharmacy_data.get('LocationInfo'),
            'abn': pharmacy_data.get('Abn'),
            'country': 'NZ'  # Add country code to distinguish NZ locations
        }
        
        # Clean up the result by removing None values
        cleaned_result = {}
        for key, value in result.items():
            if value is not None:
                cleaned_result[key] = value
                
        return cleaned_result
        
    def _parse_trading_hours(self, hours_data):
        """
        Parse trading hours from OpenHours array format to structured format.
        
        Args:
            hours_data: List of weekday opening hours
            
        Returns:
            Dictionary with days as keys and hours as values formatted as
            {'Monday': {'open': '08:30 AM', 'closed': '06:00 PM'}, ...}
        """
        # Initialize all days with closed hours
        trading_hours = {
            'Monday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Tuesday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Wednesday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Thursday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Friday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Saturday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Sunday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Public Holiday': {'open': '12:00 AM', 'closed': '12:00 AM'}
        }
        
        if not hours_data:
            return trading_hours
        
        # Process each day's opening hours
        for hours_item in hours_data:
            day = hours_item.get('WeekDay')
            open_time = hours_item.get('OpenTime')
            close_time = hours_item.get('CloseTime')
            
            if day and open_time and close_time:
                # Format the times to match the required format (HH:MM AM/PM)
                formatted_open = self._format_time_nz(open_time)
                formatted_close = self._format_time_nz(close_time)
                
                trading_hours[day] = {
                    'open': formatted_open,
                    'closed': formatted_close
                }
        
        return trading_hours
    
    def _format_time_nz(self, time_str):
        """
        Format NZ time strings like "8:00AM" to "08:00 AM" format
        
        Args:
            time_str: Time string in format like "8:00AM" or "12:30PM"
            
        Returns:
            Formatted time string like "08:00 AM" or "12:30 PM"
        """
        try:
            # Check if the format is already like "8:00AM" or "12:30PM"
            # This regex matches patterns like "8:00AM", "12:30PM", etc.
            nz_format = re.match(r'(\d{1,2}):(\d{2})([AP]M)', time_str)
            if nz_format:
                hour = int(nz_format.group(1))
                minute = nz_format.group(2)
                am_pm = nz_format.group(3)
                
                # Add spacing between time and AM/PM
                return f"{hour:02d}:{minute} {am_pm[0]}M"
            
            # If not in expected format, try the original formatter
            return self._format_time_24h(time_str)
            
        except (ValueError, TypeError, AttributeError):
            # If parsing failed, return the original
            return time_str
    
    def _format_time_24h(self, time_str):
        """
        Convert time strings like "08:00:00" to "08:00 AM" format
        
        Args:
            time_str: Time string in 24-hour format like "08:00:00" or "18:30:00"
            
        Returns:
            Formatted time string like "08:00 AM" or "06:30 PM"
        """
        try:
            # Parse time in HH:MM:SS format
            if ':' in time_str:
                parts = time_str.split(':')
                if len(parts) >= 2:
                    hour = int(parts[0])
                    minute = int(parts[1])
                    
                    # Convert 24h format to 12h format with AM/PM
                    if hour == 0:
                        return "12:{:02d} AM".format(minute)
                    elif hour < 12:
                        return "{:02d}:{:02d} AM".format(hour, minute)
                    elif hour == 12:
                        return "12:{:02d} PM".format(minute)
                    else:
                        return "{:02d}:{:02d} PM".format(hour - 12, minute)
        except (ValueError, TypeError):
            pass
            
        # If parsing failed, return the original
        return time_str