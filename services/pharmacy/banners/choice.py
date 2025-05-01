from ..base_handler import BasePharmacyHandler
import re
from rich import print
from bs4 import BeautifulSoup
from datetime import datetime

class ChoiceHandler(BasePharmacyHandler):
    """Handler for Choice Pharmacies"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "choice"
        # Define brand-specific headers for API requests
        self.headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
            'x-requested-with': 'XMLHttpRequest'
        }
        
    async def fetch_locations(self):
        """
        Fetch all Choice pharmacy locations.
        
        Returns:
            List of Choice locations
        """
        # Make API call to get location data
        response = await self.session_manager.get(
            url=self.pharmacy_locations.CHOICE_URL,
            headers=self.headers
        )
        
        if response.status_code == 200:
            # The API returns a list directly
            locations = response.json()
            print(f"Found {len(locations)} Choice Pharmacy locations")
            return locations
        else:
            raise Exception(f"Failed to fetch Choice Pharmacy locations: {response.status_code}")
    
    async def fetch_pharmacy_details(self, location_id):
        """
        Fetch details for a specific pharmacy location.
        For Choice Pharmacy, all details are included in the main API call.
        
        Args:
            location_id: The ID of the location to get details for
            
        Returns:
            Location details data
        """
        # All details are included in the locations response for Choice
        return {"location_details": location_id}
        
    async def fetch_all_locations_details(self):
        """
        Fetch details for all locations and return as a list.
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        print(f"Fetching all Choice Pharmacy locations...")
        locations = await self.fetch_locations()
        if not locations:
            print(f"No Choice Pharmacy locations found.")
            return []
            
        print(f"Found {len(locations)} Choice Pharmacy locations. Processing details...")
        all_details = []
        
        for location in locations:
            try:
                extracted_details = self.extract_pharmacy_details(location)
                all_details.append(extracted_details)
            except Exception as e:
                print(f"Error processing Choice Pharmacy location {location.get('id')}: {e}")
                
        print(f"Completed processing details for {len(all_details)} Choice Pharmacy locations.")
        return all_details
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract and standardize pharmacy details from raw API data.
        
        Args:
            pharmacy_data: Raw pharmacy data from the API
            
        Returns:
            Dictionary with standardized pharmacy details
        """
        # Parse opening hours from HTML
        trading_hours = self._parse_trading_hours(pharmacy_data.get('hours', ''))
        
        # Construct complete address
        address_parts = []
        if pharmacy_data.get('address'):
            address_parts.append(pharmacy_data.get('address'))
        if pharmacy_data.get('address2'):
            address_parts.append(pharmacy_data.get('address2'))
        if pharmacy_data.get('city'):
            address_parts.append(pharmacy_data.get('city'))
        if pharmacy_data.get('state'):
            address_parts.append(pharmacy_data.get('state'))
        if pharmacy_data.get('zip'):
            address_parts.append(pharmacy_data.get('zip'))
            
        full_address = ', '.join(filter(None, address_parts))
        
        # Format the data according to our standardized structure
        result = {
            'name': pharmacy_data.get('store'),
            'address': full_address,
            'email': pharmacy_data.get('email'),
            'latitude': pharmacy_data.get('lat'),
            'longitude': pharmacy_data.get('lng'),
            'phone': pharmacy_data.get('phone'),
            'postcode': pharmacy_data.get('zip'),
            'state': pharmacy_data.get('state'),
            'street_address': pharmacy_data.get('address'),
            'suburb': pharmacy_data.get('city'),
            'trading_hours': trading_hours,
            'fax': pharmacy_data.get('fax'),
            'website': pharmacy_data.get('url') or pharmacy_data.get('permalink')
        }
        
        # Clean up the result by removing None values
        cleaned_result = {}
        for key, value in result.items():
            if value is not None and value != '':
                cleaned_result[key] = value
                
        return cleaned_result
    
    def _parse_trading_hours(self, hours_html):
        """
        Parse trading hours from HTML table.
        
        Args:
            hours_html: HTML string containing trading hours table
            
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
        
        if not hours_html:
            return trading_hours
        
        # Parse the HTML table using BeautifulSoup
        try:
            soup = BeautifulSoup(hours_html, 'html.parser')
            table = soup.find('table', class_='wpsl-opening-hours')
            
            if table:
                rows = table.find_all('tr')
                
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) == 2:
                        day = cells[0].text.strip()
                        hours_text = cells[1].text.strip()
                        
                        # Check if it's closed
                        if hours_text.lower() == 'closed':
                            continue  # Keep default closed hours
                            
                        # Try to parse the time
                        time_match = re.search(r'(\d+:\d+\s*[AP]M)\s*-\s*(\d+:\d+\s*[AP]M)', hours_text)
                        if time_match:
                            open_time = self._format_time_12h(time_match.group(1))
                            close_time = self._format_time_12h(time_match.group(2))
                            
                            if day in trading_hours:
                                trading_hours[day] = {
                                    'open': open_time,
                                    'closed': close_time
                                }
        except Exception as e:
            print(f"Error parsing trading hours: {str(e)}")
            
        return trading_hours
    
    def _format_time_12h(self, time_str):
        """
        Standardize 12-hour time format.
        
        Args:
            time_str: Time string like "8:30 AM" or "6:00 PM"
            
        Returns:
            Formatted time string like "08:30 AM" or "06:00 PM"
        """
        time_str = time_str.strip()
        
        # Extract components using regex
        # This regex captures the hour, minute, and AM/PM parts
        time_match = re.search(r'(\d+):(\d+)\s*([AP]M)', time_str, re.IGNORECASE)
        if not time_match:
            return time_str
            
        hour = int(time_match.group(1))
        minute = int(time_match.group(2))
        am_pm = time_match.group(3).upper()
        
        # Format with leading zeros
        return f"{hour:02d}:{minute:02d} {am_pm}"