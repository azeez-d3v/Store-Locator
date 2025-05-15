#Your Discount Chemist

from ..base_handler import BasePharmacyHandler
import re
import json
from rich import print
from bs4 import BeautifulSoup

class YdcHandler(BasePharmacyHandler):
    """Handler for Your Discount Chemist (YDC) Pharmacies"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "ydc"  # Use a simple, lowercase identifier
        # Define brand-specific headers for API requests
        self.headers = {
            'accept': 'application/json',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0'
        }
        
    async def fetch_locations(self):
        """
        Fetch all Your Discount Chemist locations.
        
        Returns:
            List of YDC locations
        """
        # Make API call to get location data
        response = await self.session_manager.get(
            url=self.pharmacy_locations.YDC_URL,
            headers=self.headers
        )
        
        if response.status_code == 200:
            # The API returns locations in a "data" field
            data = response.json()
            locations = data.get("data", [])
            print(f"Found {len(locations)} Your Discount Chemist locations")
            return locations
        else:
            raise Exception(f"Failed to fetch Your Discount Chemist locations: {response.status_code}")
    
    async def fetch_pharmacy_details(self, location_id):
        """
        Fetch details for a specific pharmacy location.
        For YDC, all details are included in the main API call.
        
        Args:
            location_id: The ID of the location to get details for
            
        Returns:
            Location details data
        """
        # All details are included in the locations response for YDC
        return {"location_details": location_id}
        
    async def fetch_all_locations_details(self):
        """
        Fetch details for all locations and return as a list.
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        print("Fetching all Your Discount Chemist locations...")
        locations = await self.fetch_locations()
        if not locations:
            print("No Your Discount Chemist locations found.")
            return []
            
        print(f"Found {len(locations)} Your Discount Chemist locations. Processing details...")
        all_details = []
        
        for location in locations:
            try:
                extracted_details = self.extract_pharmacy_details(location)
                all_details.append(extracted_details)
            except Exception as e:
                print(f"Error processing Your Discount Chemist location {location.get('id')}: {e}")
                
        print(f"Completed processing details for {len(all_details)} Your Discount Chemist locations.")
        return all_details
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract and standardize pharmacy details from raw API data.
        
        Args:
            pharmacy_data: Raw pharmacy data from the API
            
        Returns:
            Dictionary with standardized pharmacy details
        """
        # Parse opening hours from HTML notes
        trading_hours = self._parse_trading_hours(pharmacy_data.get('notes', ''))
        
        # Handle the email field which might contain multiple comma-separated emails
        emails = pharmacy_data.get('email', '')
        primary_email = emails.split(',')[0] if emails else None
        
        # Parse website from channels if available
        website = None
        if pharmacy_data.get('channels'):
            try:
                channels = json.loads(pharmacy_data.get('channels'))
                if channels and len(channels) > 0:
                    # Use the first channel as the primary website
                    website = channels[0].get('value')
            except (json.JSONDecodeError, AttributeError):
                pass
        
        # Format the data according to our standardized structure
        result = {
            'name': f"Your Discount Chemist {pharmacy_data.get('location_name')}",
            'address': pharmacy_data.get('address'),
            'email': primary_email,
            'latitude': pharmacy_data.get('lat'),
            'longitude': pharmacy_data.get('lng'),
            'phone': pharmacy_data.get('phone'),
            'postcode': pharmacy_data.get('address_postcode'),
            'state': pharmacy_data.get('address_state'),
            'street_address': pharmacy_data.get('address_street'),
            'suburb': pharmacy_data.get('address_city'),
            'trading_hours': trading_hours,
            'website': website,
            'directions_url': pharmacy_data.get('direction_url')
        }
        
        # Clean up the result by removing None values
        cleaned_result = {}
        for key, value in result.items():
            if value is not None:
                cleaned_result[key] = value
                
        return cleaned_result
    
    def _parse_trading_hours(self, notes_html):
        """
        Parse trading hours from HTML notes.
        
        Args:
            notes_html: HTML string containing trading hours table
            
        Returns:
            Dictionary with days as keys and hours as values formatted as
            {'Monday': {'open': '08:00 AM', 'closed': '06:00 PM'}, ...}
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
        
        if not notes_html:
            return trading_hours
        
        # Use BeautifulSoup to parse the HTML
        try:
            soup = BeautifulSoup(notes_html, 'html.parser')
            
            # Find the table with class "mil-store-hours"
            hours_table = soup.find('table', class_='mil-store-hours')
            
            if hours_table:
                # Find all table rows
                rows = hours_table.find_all('tr')
                
                # Skip the header row
                for row in rows[1:]:
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        day = cells[0].text.strip()
                        hours_text = cells[1].text.strip()
                        
                        # Skip if no valid day or hours
                        if not day or not hours_text or hours_text == '&nbsp;':
                            continue
                            
                        # Parse the hours
                        hours_match = re.search(r'(\d{1,2}(?::\d{2})?\s*[ap]m)\s*-\s*(\d{1,2}(?::\d{2})?\s*[ap]m)', hours_text, re.IGNORECASE)
                        
                        if hours_match:
                            open_time = self._format_time(hours_match.group(1))
                            close_time = self._format_time(hours_match.group(2))
                            
                            # Update the trading hours dictionary
                            if day in trading_hours:
                                trading_hours[day] = {
                                    'open': open_time,
                                    'closed': close_time
                                }
        except Exception as e:
            print(f"Error parsing trading hours HTML: {str(e)}")
        
        return trading_hours
    
    def _format_time(self, time_str):
        """
        Convert time strings like "8am", "8:30am" to "08:00 AM", "08:30 AM" format
        
        Args:
            time_str: Time string like "8am", "6pm", "8:30am"
            
        Returns:
            Formatted time string like "08:00 AM", "06:00 PM", "08:30 AM"
        """
        time_str = time_str.lower().strip()
        
        # Handle various formats
        if ':' in time_str:
            # Format like "8:30am"
            hour_part, minute_part = time_str.split(':')
            minute_num = re.search(r'\d+', minute_part).group(0)
            am_pm = 'AM' if 'am' in time_str.lower() else 'PM'
            hour_num = int(hour_part)
            hour_12 = hour_num if 1 <= hour_num <= 12 else hour_num % 12
            if hour_12 == 0:
                hour_12 = 12
            return f"{hour_12:02d}:{int(minute_num):02d} {am_pm}"
        else:
            # Format like "8am"
            hour_num = int(re.search(r'\d+', time_str).group(0))
            am_pm = 'AM' if 'am' in time_str.lower() else 'PM'
            hour_12 = hour_num if 1 <= hour_num <= 12 else hour_num % 12
            if hour_12 == 0:
                hour_12 = 12
            return f"{hour_12:02d}:00 {am_pm}"