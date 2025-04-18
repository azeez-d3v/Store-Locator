import asyncio
import re
from bs4 import BeautifulSoup
import json
from urllib.parse import urlparse
from ..base_handler import BasePharmacyHandler

class CompleteCareHandler(BasePharmacyHandler):
    """
    Handler for Complete Care Pharmacy locations in Australia
    """
    
    # Store locations URLs
    LOCATION_URLS = [
        "https://completecarepharmacies.com.au/locations/bairnsdale/",
        "https://completecarepharmacies.com.au/locations/bellambi/",
        "https://completecarepharmacies.com.au/locations/kurri-kurri/",
        "https://completecarepharmacies.com.au/locations/landsborough/",
        "https://completecarepharmacies.com.au/locations/penguin/",
        "https://completecarepharmacies.com.au/locations/rosny/",
        "https://completecarepharmacies.com.au/locations/south-hobart/"
    ]
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "complete_care"
        # Define headers for requests
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
        }
        # Maximum number of concurrent requests
        self.max_concurrent_requests = 5
        
    async def fetch_locations(self):
        """
        Fetch all Complete Care Pharmacy locations
        
        Returns:
            List of location dictionaries with basic information
        """
        locations = []
        
        try:
            # Process each location URL
            for location_url in self.LOCATION_URLS:
                # Extract location ID from URL
                location_id = urlparse(location_url).path.split('/')[-2]
                
                # Create basic location object
                locations.append({
                    "id": location_id,
                    "url": location_url,
                    "name": f"Complete Care Pharmacy {location_id.replace('-', ' ').title()}"
                })
                
            return locations
        except Exception as e:
            print(f"Error fetching Complete Care locations: {e}")
            return []
    
    async def fetch_pharmacy_details(self, location_id):
        """
        Fetch detailed information for a specific Complete Care Pharmacy
        
        Args:
            location_id: The ID of the location (from URL path)
            
        Returns:
            Dictionary with pharmacy details
        """
        try:
            # Find the matching URL for this location ID
            location_url = next((url for url in self.LOCATION_URLS if location_id in url), None)
            
            if not location_url:
                print(f"Unknown location ID: {location_id}")
                return None
                
            # Get the page content using the session manager's get method
            response = await self.session_manager.get(
                url=location_url,
                headers=self.headers
            )
            
            if response.status_code == 200:
                # Parse the HTML content
                return self.extract_pharmacy_details(response.text, location_id)
            else:
                print(f"Error fetching {location_url}: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"Error fetching Complete Care pharmacy details for {location_id}: {e}")
            return None
    
    async def fetch_all_locations_details(self):
        """
        Fetch details for all Complete Care Pharmacy locations
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        print("Fetching all Complete Care Pharmacy locations...")
        locations = await self.fetch_locations()
        
        if not locations:
            print("No Complete Care Pharmacy locations found.")
            return []
        
        print(f"Found {len(locations)} Complete Care Pharmacy locations. Fetching details...")
        
        # Create a semaphore to limit concurrent connections
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        
        async def fetch_with_semaphore(location):
            """Helper function to fetch details with semaphore control"""
            async with semaphore:
                try:
                    location_id = location["id"]
                    return await self.fetch_pharmacy_details(location_id)
                except Exception as e:
                    print(f"Error fetching details for {location.get('name')}: {e}")
                    return None
        
        # Create tasks for all locations
        tasks = [fetch_with_semaphore(location) for location in locations]
        
        # Process results as they complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out any None results or exceptions
        pharmacy_details = [r for r in results if not isinstance(r, Exception) and r is not None]
        
        print(f"Successfully fetched details for {len(pharmacy_details)} out of {len(locations)} Complete Care Pharmacy locations")
        return pharmacy_details
    
    def extract_pharmacy_details(self, html_content, location_id):
        """
        Extract pharmacy details from HTML content
        
        Args:
            html_content: HTML content of the pharmacy page
            location_id: The ID of the location
            
        Returns:
            Dictionary with pharmacy details
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract pharmacy name from heading
            name_element = soup.select_one('h1.elementor-heading-title')
            name = name_element.text.strip() if name_element else f"Complete Care Pharmacy {location_id.replace('-', ' ').title()}"
            
            # Initialize details dictionary
            pharmacy_details = {
                'name': name,
                'address': None,
                'phone': None,
                'fax': None,
                'email': None,
                'trading_hours': {},
                'website': f"https://completecarepharmacies.com.au/locations/{location_id}/",
                'latitude': None,
                'longitude': None,
                'state': None,
                'postcode': None,
                'suburb': None,
                'street_address': None,
                'country': 'AU'  # Set country to AU for Australia
            }
            
            # Find icon list items containing contact details
            icon_list_items = soup.select('ul.elementor-icon-list-items li.elementor-icon-list-item')
            
            for item in icon_list_items:
                item_text = item.select_one('.elementor-icon-list-text').text.strip()
                icon = item.select_one('.elementor-icon-list-icon i')
                icon_class = icon.get('class', []) if icon else []
                
                # Check icon classes to determine what type of information this is
                if any('map-marker' in cls for cls in icon_class):
                    # Address
                    pharmacy_details['address'] = item_text
                    
                    # Extract state, postcode, and suburb from address
                    address_parts = item_text.split()
                    if len(address_parts) >= 3:
                        # Assume format like "46 Nicholson St Bairnsdale VIC 3875"
                        # Last two parts are typically STATE and POSTCODE
                        pharmacy_details['state'] = address_parts[-2] if len(address_parts) >= 2 else None
                        pharmacy_details['postcode'] = address_parts[-1] if len(address_parts) >= 1 else None
                        
                        # Try to extract suburb (assuming it's before STATE)
                        if len(address_parts) >= 3:
                            pharmacy_details['suburb'] = address_parts[-3]
                        
                        # Rest is street address
                        pharmacy_details['street_address'] = ' '.join(address_parts[:-3]) if len(address_parts) >= 3 else item_text
                
                elif any('phone' in cls for cls in icon_class):
                    # Phone number
                    # Remove non-numeric characters except + for international format
                    phone_link = item.select_one('a')
                    if phone_link and phone_link.has_attr('href') and 'tel:' in phone_link['href']:
                        pharmacy_details['phone'] = phone_link['href'].replace('tel:', '').strip()
                    else:
                        pharmacy_details['phone'] = item_text
                
                elif any('fax' in cls for cls in icon_class):
                    # Fax number
                    pharmacy_details['fax'] = item_text.replace('Fax:', '').strip()
                
                elif any('envelope' in cls for cls in icon_class):
                    # Email
                    email_link = item.select_one('a')
                    if email_link and email_link.has_attr('href') and 'mailto:' in email_link['href']:
                        pharmacy_details['email'] = email_link['href'].replace('mailto:%20', '').replace('mailto:', '').strip()
                    else:
                        pharmacy_details['email'] = item_text
            
            # Extract trading hours - Try multiple selectors to find the opening hours element
            print(f"Looking for trading hours in {location_id}")
            
            # More flexible selectors to find the opening hours element
            hours_selectors = [
                '.elementor-text-editor h3:-soup-contains("Opening Hours") + p',
                '.elementor-widget-text-editor:-soup-contains("Opening Hours")',
                '.elementor-widget-container h3:-soup-contains("Opening Hours") + p',
                '.elementor-widget-container p:-soup-contains("Monday to Friday")',
                '.elementor-element-933c733 .elementor-widget-container',  # Using the specific element ID from your example
                '.elementor-widget-text-editor .elementor-widget-container',
            ]
            
            hours_element = None
            for selector in hours_selectors:
                hours_element = soup.select_one(selector)
                if hours_element:
                    break
            
            if hours_element:
                hours_text = hours_element.text.strip()
                hours_html = str(hours_element)
                
                # Try to find specific patterns in the text
                # First check for the combined pattern "Monday to Friday: 8:30am – 5:30pm"
                monday_to_friday_match = re.search(r'Monday\s+to\s+Friday\s*:\s*([\d:]+\s*(?:am|pm))\s*[-–]\s*([\d:]+\s*(?:am|pm))', hours_text, re.IGNORECASE)
                if monday_to_friday_match:
                    open_time = monday_to_friday_match.group(1).strip()
                    close_time = monday_to_friday_match.group(2).strip()
                    
                    for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']:
                        pharmacy_details['trading_hours'][day] = {
                            'open': open_time,
                            'close': close_time
                        }
                
                # Look for Saturday pattern
                saturday_match = re.search(r'Saturday\s*:\s*([\d:]+\s*(?:am|pm))\s*[-–]\s*([\d:]+\s*(?:am|pm))', hours_text, re.IGNORECASE)
                if saturday_match:
                    open_time = saturday_match.group(1).strip()
                    close_time = saturday_match.group(2).strip()
                    
                    pharmacy_details['trading_hours']['Saturday'] = {
                        'open': open_time,
                        'close': close_time
                    }
                
                # Look for 12-hour format times with am/pm
                saturday_alt_match = re.search(r'Saturday[^:]*:\s*([\d:]+(?:am|pm))[\s\-–]+to[\s\-–]+([\d:]+(?:am|pm))', hours_text, re.IGNORECASE)
                if saturday_alt_match and 'Saturday' not in pharmacy_details['trading_hours']:
                    open_time = saturday_alt_match.group(1).strip()
                    close_time = saturday_alt_match.group(2).strip()
                    
                    pharmacy_details['trading_hours']['Saturday'] = {
                        'open': open_time,
                        'close': close_time
                    }
                
                # Look for Sunday: Closed pattern
                sunday_closed_match = re.search(r'Sunday\s*:\s*Closed', hours_text, re.IGNORECASE)
                if sunday_closed_match:
                    pharmacy_details['trading_hours']['Sunday'] = {
                        'open': 'Closed',
                        'close': 'Closed'
                    }
                
                # If the hours are in a <p> tag with <br> separating the days
                if '<br>' in hours_html:
                    # Split by <br> tags and process each line
                    lines = hours_html.split('<br>')
                    for line in lines:
                        clean_line = re.sub('<[^<]+?>', '', line).strip()
                        print(f"Processing line: {clean_line}")
                        
                        # Match "Day to Day: time - time" pattern
                        day_range_match = re.search(r'(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s+to\s+(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s*:\s*([\d:]+\s*(?:am|pm))\s*[-–]\s*([\d:]+\s*(?:am|pm))', clean_line, re.IGNORECASE)
                        if day_range_match:
                            start_day = day_range_match.group(1)
                            end_day = day_range_match.group(2)
                            open_time = day_range_match.group(3).strip()
                            close_time = day_range_match.group(4).strip()
                            
                            # Define day order
                            day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                            start_idx = day_order.index(start_day.capitalize())
                            end_idx = day_order.index(end_day.capitalize())
                            
                            # Set hours for each day in the range
                            for i in range(start_idx, end_idx + 1):
                                pharmacy_details['trading_hours'][day_order[i]] = {
                                    'open': open_time,
                                    'close': close_time
                                }
                        
                        # Match "Day: time - time" pattern
                        single_day_match = re.search(r'(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s*:\s*([\d:]+\s*(?:am|pm))\s*[-–]\s*([\d:]+\s*(?:am|pm))', clean_line, re.IGNORECASE)
                        if single_day_match:
                            day = single_day_match.group(1).capitalize()
                            open_time = single_day_match.group(2).strip()
                            close_time = single_day_match.group(3).strip()
                            
                            pharmacy_details['trading_hours'][day] = {
                                'open': open_time,
                                'close': close_time
                            }
                        
                        # Match "Day: Closed" pattern
                        day_closed_match = re.search(r'(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s*:\s*Closed', clean_line, re.IGNORECASE)
                        if day_closed_match:
                            day = day_closed_match.group(1).capitalize()
                            pharmacy_details['trading_hours'][day] = {
                                'open': 'Closed',
                                'close': 'Closed'
                            }
                
                # Special handling for the exact format from your example
                exact_format_match = re.search(r'Monday to Friday: ([\d:]+(?:am|pm)) – ([\d:]+(?:am|pm))\s+Saturday: ([\d:]+(?:am|pm)) – ([\d:]+(?:am|pm))\s+Sunday: Closed', hours_text.replace('\n', ' '), re.IGNORECASE)
                if exact_format_match:
                    weekday_open = exact_format_match.group(1).strip()
                    weekday_close = exact_format_match.group(2).strip()
                    sat_open = exact_format_match.group(3).strip()
                    sat_close = exact_format_match.group(4).strip()
                    
                    # Set hours for weekdays
                    for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']:
                        pharmacy_details['trading_hours'][day] = {
                            'open': weekday_open,
                            'close': weekday_close
                        }
                    
                    # Set Saturday hours
                    pharmacy_details['trading_hours']['Saturday'] = {
                        'open': sat_open,
                        'close': sat_close
                    }
                    
                    # Set Sunday as closed
                    pharmacy_details['trading_hours']['Sunday'] = {
                        'open': 'Closed',
                        'close': 'Closed'
                    }
                
                # If trading hours are still empty, try a more aggressive approach
                if not pharmacy_details['trading_hours']:
                    # Look for any times in the format 8:30am - 5:30pm
                    time_ranges = re.findall(r'([\d:]+\s*(?:am|pm))\s*[-–]\s*([\d:]+\s*(?:am|pm))', hours_text)
                    days_found = re.findall(r'(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)', hours_text, re.IGNORECASE)
                    
                    print(f"Aggressive search - Time ranges: {time_ranges}")
                    print(f"Aggressive search - Days found: {days_found}")
                    
                    if time_ranges and days_found:
                        # If we find time ranges and days, try to match them up
                        for i, day in enumerate(days_found):
                            if i < len(time_ranges):
                                open_time, close_time = time_ranges[i]
                                pharmacy_details['trading_hours'][day.capitalize()] = {
                                    'open': open_time.strip(),
                                    'close': close_time.strip()
                                }
                
                print(f"Final trading hours: {pharmacy_details['trading_hours']}")
            else:
                print(f"No hours element found for {location_id}")
            
            # Standardize state name
            if pharmacy_details['state']:
                pharmacy_details['state'] = self._standardize_state(pharmacy_details['state'])
            
            return pharmacy_details
            
        except Exception as e:
            print(f"Error extracting Complete Care pharmacy details: {e}")
            return {
                'name': f"Complete Care Pharmacy {location_id.replace('-', ' ').title()}",
                'address': None,
                'email': None,
                'fax': None,
                'latitude': None,
                'longitude': None,
                'phone': None,
                'postcode': None,
                'state': None,
                'street_address': None,
                'suburb': None,
                'trading_hours': {},
                'website': f"https://completecarepharmacies.com.au/locations/{location_id}/",
                'country': 'AU'  # Set country to AU for Australia
            }
            
    def _standardize_state(self, state):
        """Convert full state names to standard abbreviations"""
        state_mapping = {
            'NEW SOUTH WALES': 'NSW',
            'VICTORIA': 'VIC',
            'QUEENSLAND': 'QLD',
            'SOUTH AUSTRALIA': 'SA',
            'WESTERN AUSTRALIA': 'WA',
            'TASMANIA': 'TAS',
            'NORTHERN TERRITORY': 'NT',
            'AUSTRALIAN CAPITAL TERRITORY': 'ACT',
            # Already using abbreviations
            'NSW': 'NSW',
            'VIC': 'VIC', 
            'QLD': 'QLD',
            'SA': 'SA',
            'WA': 'WA',
            'TAS': 'TAS',
            'NT': 'NT',
            'ACT': 'ACT'
        }
        
        state_upper = state.upper() if state else ''
        return state_mapping.get(state_upper, state)