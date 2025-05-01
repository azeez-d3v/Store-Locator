from ...base_handler import BasePharmacyHandler
import re
import json
from rich import print
from bs4 import BeautifulSoup

class AntidotePharmacyNZHandler(BasePharmacyHandler):
    """Handler for Antidote Pharmacy NZ"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "antidote_nz"
        # Define brand-specific headers for API requests
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Pragma': 'no-cache',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
        }
        # The URL for Antidote Pharmacy NZ website
        self.url = self.pharmacy_locations.ANTIDOTE_NZ_URL

    async def fetch_locations(self):
        """
        Fetch all Antidote Pharmacy NZ locations.
        
        Returns:
            List of Antidote Pharmacy NZ locations
        """
        # Make request to get HTML data
        response = await self.session_manager.get(
            url=self.url,
            headers=self.headers
        )
        
        if response.status_code == 200:
            # Extract location data from HTML
            locations = self._extract_pharmacy_data_from_html(response.text)
            print(f"Found {len(locations)} Antidote Pharmacy NZ locations")
            return locations
        else:
            raise Exception(f"Failed to fetch Antidote Pharmacy NZ locations: {response.status_code}")
    
    def _extract_pharmacy_data_from_html(self, html_content):
        """
        Extract pharmacy data directly from the HTML content, targeting the specific
        footer column structure of the Antidote Pharmacy NZ website.
        
        Args:
            html_content: HTML content from the website
            
        Returns:
            List of extracted pharmacy locations
        """
        locations = []
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find the footer section - using the specific class combination
        footer_section = soup.find('section', class_=lambda c: c and 'stack--footer' in c)
        
        if not footer_section:
            print("Could not find the footer section in the HTML")
            return locations
        
        # Find the columns within the footer - there should be 4 columns, with pharmacy info in columns 2 and 3
        columns = footer_section.find_all('div', class_=lambda c: c and c.startswith('column _9b6ed661'))
        
        if len(columns) < 4:
            print(f"Expected 4 columns in footer, found {len(columns)}")
            return locations
        
        # Column 2 contains Cromwell pharmacy info
        cromwell_column = columns[1]  # 0-based index
        # Column 3 contains Lake Dunstan pharmacy info
        lake_dunstan_column = columns[2]  # 0-based index
        
        # Extract Cromwell pharmacy data
        cromwell_data = self._extract_pharmacy_data_from_column(
            cromwell_column, 
            "antidote Cromwell"
        )
        if cromwell_data:
            locations.append(cromwell_data)
        
        # Extract Lake Dunstan pharmacy data
        lake_dunstan_data = self._extract_pharmacy_data_from_column(
            lake_dunstan_column, 
            "antidote Lake Dunstan"
        )
        if lake_dunstan_data:
            locations.append(lake_dunstan_data)
        
        return locations

    def _extract_pharmacy_data_from_column(self, column, pharmacy_name):
        """
        Extract pharmacy data from a specific column in the footer.
        
        Args:
            column: BeautifulSoup element for the column
            pharmacy_name: Name of the pharmacy
            
        Returns:
            Dictionary with pharmacy data
        """
        # Initialize pharmacy data
        pharmacy_data = {
            "name": pharmacy_name,
            "address": "",
            "phone": "",
            "state": "", 
            "suburb": "Cromwell",
            "postcode": "9310",  # Default postcode for Cromwell
            "trading_hours": {}
        }
        
        # Find all text blocks in the column
        text_blocks = column.find_all('div', class_='text_block_text')
        
        for block in text_blocks:
            block_text = block.get_text()
            
            # Extract opening hours
            if "Opening Hours" in block_text:
                pharmacy_data["trading_hours"] = self._parse_specific_hours(block_text, pharmacy_name)
            
            # Extract contact information
            elif "Phone:" in block_text and "Address:" in block_text:
                # Find all paragraphs in this block
                paragraphs = block.find_all('p')
                
                for p in paragraphs:
                    p_text = p.get_text()
                    
                    if "Phone:" in p_text:
                        # Extract phone number
                        phone_match = re.search(r'Phone:.*?(\d{2}\s*\d{3}\s*\d{4})', p_text)
                        if phone_match:
                            pharmacy_data["phone"] = phone_match.group(1).strip()
                    
                    if "Address:" in p_text:
                        # Extract address
                        address_match = re.search(r'Address:(.*)', p_text)
                        if address_match:
                            pharmacy_data["address"] = address_match.group(1).strip()
        
        return pharmacy_data

    def _parse_specific_hours(self, hours_text, pharmacy_name):
        """
        Parse opening hours text based on the specific format used by each pharmacy.
        
        Args:
            hours_text: Text containing opening hours
            pharmacy_name: Name of the pharmacy to determine specific parsing rules
            
        Returns:
            Dictionary with standardized trading hours
        """
        # Default trading hours (closed)
        trading_hours = {
            'Monday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Tuesday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Wednesday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Thursday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Friday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Saturday': {'open': '12:00 AM', 'closed': '12:00 AM'},
            'Sunday': {'open': '12:00 AM', 'closed': '12:00 AM'},
        }
        
        # Check if we have opening hours text to parse
        if not hours_text:
            return trading_hours
            
        # Extract weekday hours (Monday-Friday)
        weekday_pattern = r'Monday\s*-\s*Friday.*?([\d\.]+)am\s*-\s*([\d\.:]+)pm'
        weekday_match = re.search(weekday_pattern, hours_text, re.DOTALL | re.IGNORECASE)
        
        if weekday_match:
            # Get raw opening and closing times
            open_raw = weekday_match.group(1).strip()
            close_raw = weekday_match.group(2).strip()
            
            # Add am/pm suffixes
            open_time = f"{open_raw}am"
            close_time = f"{close_raw}pm"
            
            # Format times
            open_formatted = self._format_time(open_time)
            close_formatted = self._format_time(close_time)
            
            # Apply to all weekdays
            for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']:
                trading_hours[day] = {
                    'open': open_formatted,
                    'closed': close_formatted
                }
        
        # Extract Saturday hours
        # Handle specific case of "Saturday 9am - 12noon"
        saturday_pattern = r'Saturday.*?([\d\.]+)am\s*-\s*([\d\.]*\s*noon|[\d\.]+\s*(?:am|pm))'
        saturday_match = re.search(saturday_pattern, hours_text, re.DOTALL | re.IGNORECASE)
        
        if saturday_match:
            # Get raw opening time
            open_raw = saturday_match.group(1).strip()
            close_raw = saturday_match.group(2).strip()
            
            # Format opening time
            open_time = f"{open_raw}am"
            open_formatted = self._format_time(open_time)
            
            # Format closing time - handle "noon" specially
            if "noon" in close_raw.lower():
                close_formatted = "12:00 PM"
            else:
                # If it doesn't have am/pm suffix, add it
                if not re.search(r'(am|pm)$', close_raw.lower()):
                    if int(re.search(r'(\d+)', close_raw).group(1)) < 12:
                        close_time = f"{close_raw}am"
                    else:
                        close_time = f"{close_raw}pm"
                else:
                    close_time = close_raw
                    
                close_formatted = self._format_time(close_time)
            
            trading_hours['Saturday'] = {
                'open': open_formatted,
                'closed': close_formatted
            }
        
        # Check for Sunday being closed
        if "Sunday" in hours_text and "Closed" in hours_text:
            # Sunday is already set as closed (12:00 AM - 12:00 AM)
            pass
            
        # Check for "Saturday & Sunday Closed" pattern
        if re.search(r'Saturday\s*(?:&|and)\s*Sunday.*?Closed', hours_text, re.DOTALL | re.IGNORECASE):
            # Both are already set as closed (12:00 AM - 12:00 AM)
            pass
            
        return trading_hours

    def _format_time(self, time_str):
        """
        Format time strings like "8.30am" to "08:30 AM" format
        
        Args:
            time_str: Time string in various formats
            
        Returns:
            Formatted time string like "08:30 AM"
        """
        try:
            # Special cases
            if time_str.lower() == 'noon' or time_str.lower() == '12noon':
                return "12:00 PM"
                
            # Strip all spaces
            time_str = time_str.strip()
            
            # Extract time components using regex
            time_pattern = r'(\d{1,2})(?:[\.:](\d{2}))?(?:\s*)?(am|pm)'
            match = re.match(time_pattern, time_str.lower())
            
            if match:
                hour = int(match.group(1))
                minute = int(match.group(2)) if match.group(2) else 0
                am_pm = match.group(3).upper()
                
                # Format time as HH:MM AM/PM
                return f"{hour:02d}:{minute:02d} {am_pm}"
            
            return time_str
        except (ValueError, TypeError, AttributeError) as e:
            print(f"Error formatting time '{time_str}': {e}")
            return time_str
            
    async def fetch_pharmacy_details(self, location_id):
        """
        For Antidote Pharmacy NZ, all details are included in the main fetch.
        
        Args:
            location_id: The ID of the location (not used)
            
        Returns:
            Location details 
        """
        # Return dummy data as all details are fetched during the main locations call
        return {"location_details": location_id}
        
    async def fetch_all_locations_details(self):
        """
        Fetch details for all locations and return as a list.
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        print(f"Fetching all Antidote Pharmacy NZ locations...")
        locations = await self.fetch_locations()
        
        if not locations:
            print(f"No Antidote Pharmacy NZ locations found.")
            return []
            
        print(f"Found {len(locations)} Antidote Pharmacy NZ locations.")
        return locations
        
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract and standardize pharmacy details from raw data.
        
        Args:
            pharmacy_data: Raw pharmacy data
            
        Returns:
            Dictionary with standardized pharmacy details
        """
        # For Antidote Pharmacy NZ, the data is already extracted and formatted
        # This method is here to satisfy the abstract base class requirement
        
        # Clean up the result by removing None values
        cleaned_result = {}
        for key, value in pharmacy_data.items():
            if value is not None:
                cleaned_result[key] = value
                
        return cleaned_result