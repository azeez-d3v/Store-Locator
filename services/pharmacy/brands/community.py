import re
from rich import print
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import warnings
from ..base_handler import BasePharmacyHandler

# Filter out the XML parsed as HTML warning
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

class CommunityHandler(BasePharmacyHandler):
    """Handler for Community Care Chemist pharmacies"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "community"
        # Define Community-specific headers
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'sec-ch-ua': '"Microsoft Edge";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0'
        }
        
    async def fetch_locations(self):
        """
        Fetch all Community Care Chemist locations.
        
        Returns:
            List of Community Care Chemist locations
        """
        response = await self.session_manager.get(
            url=self.pharmacy_locations.COMMUNITY_URL,
            headers=self.headers
        )
        
        if response.status_code == 200:
            html_content = response.text
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Updated selector to find all pharmacy location articles
            pharmacy_articles = soup.find_all('article', class_='stores')
            
            if not pharmacy_articles:
                # Try alternative selector if the first one doesn't work
                pharmacy_articles = soup.select('.dce-posts-wrapper article.dce-post')
            
            if pharmacy_articles:
                locations = []
                for article in pharmacy_articles:
                    try:
                        # Extract pharmacy details from the HTML structure
                        pharmacy_data = {}
                        
                        # Extract name
                        name_element = article.select_one('.elementor-heading-title')
                        if name_element:
                            pharmacy_data['name'] = name_element.text.strip()
                        
                        # Extract address
                        address_element = article.select_one('.elementor-element-5eec216 .dynamic-content-for-elementor-acf')
                        if address_element:
                            pharmacy_data['address'] = address_element.text.strip()
                        
                        # Extract phone
                        phone_element = article.select_one('.elementor-element-ba94b59 .dynamic-content-for-elementor-acf')
                        if phone_element:
                            phone_text = phone_element.text.strip()
                            # Remove "PH: " prefix if present
                            pharmacy_data['phone'] = phone_text.replace('PH:', '').strip()
                        
                        # Extract fax
                        fax_element = article.select_one('.elementor-element-0b379dd .dynamic-content-for-elementor-acf')
                        if fax_element:
                            fax_text = fax_element.text.strip()
                            # Remove "FAX:" prefix if present
                            pharmacy_data['fax'] = fax_text.replace('FAX:', '').strip()
                        
                        # Extract email from the href attribute
                        email_element = article.select_one('.dce-tokens a')
                        if email_element:
                            email_href = email_element.get('href', '')
                            if email_href.startswith('mailto:'):
                                pharmacy_data['email'] = email_href.replace('mailto:', '')
                        
                        # Extract trading hours
                        trading_hours = {}
                        
                        # Mon-Fri hours
                        mon_fri_element = article.select_one('.elementor-element-2a0c443 .dynamic-content-for-elementor-acf')
                        if mon_fri_element:
                            hours_text = mon_fri_element.text.strip()
                            hours_match = re.search(r'Mon - Fri:\s*(.*)', hours_text)
                            if hours_match:
                                hours_value = hours_match.group(1).strip()
                                # Split into open and close times
                                if '-' in hours_value:
                                    open_time, close_time = map(str.strip, hours_value.split('-'))
                                    for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']:
                                        trading_hours[day] = {'open': open_time, 'closed': close_time}
                        
                        # Saturday hours
                        sat_element = article.select_one('.elementor-element-81cd6c4 .dynamic-content-for-elementor-acf')
                        if sat_element:
                            hours_text = sat_element.text.strip()
                            hours_match = re.search(r'Sat:\s*(.*)', hours_text)
                            if hours_match:
                                hours_value = hours_match.group(1).strip()
                                if hours_value.lower() == 'closed':
                                    trading_hours['Saturday'] = {'open': 'Closed', 'closed': 'Closed'}
                                elif '-' in hours_value:
                                    open_time, close_time = map(str.strip, hours_value.split('-'))
                                    trading_hours['Saturday'] = {'open': open_time, 'closed': close_time}
                        
                        # Sunday hours
                        sun_element = article.select_one('.elementor-element-a58e969 .dynamic-content-for-elementor-acf')
                        if sun_element:
                            hours_text = sun_element.text.strip()
                            hours_match = re.search(r'Sun:\s*(.*)', hours_text)
                            if hours_match:
                                hours_value = hours_match.group(1).strip()
                                if hours_value.lower() == 'closed':
                                    trading_hours['Sunday'] = {'open': 'Closed', 'closed': 'Closed'}
                                elif '-' in hours_value:
                                    open_time, close_time = map(str.strip, hours_value.split('-'))
                                    trading_hours['Sunday'] = {'open': open_time, 'closed': close_time}
                        
                        pharmacy_data['trading_hours'] = trading_hours
                        
                        # Extract state and postcode from address if possible
                        if 'address' in pharmacy_data:
                            from ..utils import extract_state_postcode
                            state, postcode = extract_state_postcode(pharmacy_data['address'])
                            if state:
                                pharmacy_data['state'] = state
                            if postcode:
                                pharmacy_data['postcode'] = postcode
                        
                        # Add a unique identifier for this pharmacy
                        if 'name' in pharmacy_data:
                            pharmacy_data['id'] = f"community_{pharmacy_data['name'].lower().replace(' ', '_')}"
                        
                        # Add debug information to better understand the extraction
                        if 'name' not in pharmacy_data:
                            print(f"Warning: Could not extract name for a pharmacy. HTML structure might have changed.")
                            
                        locations.append(pharmacy_data)
                    except Exception as e:
                        print(f"Error processing Community Care Chemist location: {e}")
                
                if locations:
                    return locations
                else:
                    print("No pharmacy data could be extracted from the found articles")
                    print(f"Found {len(pharmacy_articles)} article elements but couldn't extract data")
                    
                    # Print the HTML of the first article for debugging
                    if pharmacy_articles:
                        print(f"First article HTML sample: {str(pharmacy_articles[0])[:200]}...")
                    
                    raise Exception("No pharmacy data could be extracted from Community Care Chemist page")
            else:
                # Print some debugging information
                print("No matching pharmacy articles found with either selector")
                
                # Try to find any articles on the page to see what's available
                all_articles = soup.find_all('article')
                if all_articles:
                    print(f"Found {len(all_articles)} articles with generic 'article' selector")
                    print(f"First article classes: {all_articles[0].get('class', [])}")
                else:
                    print("No articles found with any selector")
                
                # Try to find the main content div
                main_content = soup.select('.dce-posts-wrapper')
                if main_content:
                    print(f"Found main content wrapper with {len(main_content[0].find_all())} child elements")
                
                raise Exception("No pharmacy locations found in Community Care Chemist page")
        else:
            raise Exception(f"Failed to fetch Community Care Chemist locations: {response.status_code}")
    
    async def fetch_pharmacy_details(self, location_id):
        """
        For Community Care Chemist, we already have all the data in the locations response
        This is a placeholder for API consistency
        
        Args:
            location_id: The ID of the location to get details for
            
        Returns:
            Location data (unchanged)
        """
        return {"location_details": location_id}
    
    async def fetch_all_locations_details(self):
        """
        Fetch details for all Community Care Chemist locations and return as a list
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        # For Community Care Chemist, all details are included in the locations endpoint
        print(f"Fetching all Community Care Chemist locations...")
        locations = await self.fetch_locations()
        if not locations:
            print(f"No Community Care Chemist locations found.")
            return []
            
        print(f"Found {len(locations)} Community Care Chemist locations. Processing details...")
        all_details = []
        
        for location in locations:
            try:
                extracted_details = self.extract_pharmacy_details(location)
                all_details.append(extracted_details)
            except Exception as e:
                print(f"Error processing Community Care Chemist location {location.get('id')}: {e}")
                
        print(f"Completed processing details for {len(all_details)} Community Care Chemist locations.")
        return all_details
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract specific fields from Community Care Chemist location data
        
        Args:
            pharmacy_data: The raw pharmacy data from the API
            
        Returns:
            Dictionary containing standardized pharmacy details
        """
        # Extract key details
        address = pharmacy_data.get('address', '')
        
        # Try to extract state and postcode from address if not already parsed
        state = pharmacy_data.get('state')
        postcode = pharmacy_data.get('postcode')
        suburb = None
        
        # Try to find suburb in address if not already extracted
        if address and state:
            # Simplistic extraction of suburb - assumes format like "X Street, Suburb STATE POSTCODE"
            parts = address.split(',')
            if len(parts) > 1:
                suburb_part = parts[-1].strip() if len(parts) == 2 else parts[-2].strip()
                # Remove the state and postcode from the suburb part
                suburb_part = re.sub(r'\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b', '', suburb_part)
                suburb_part = re.sub(r'\b\d{4}\b', '', suburb_part)
                suburb = suburb_part.strip()
        
        # Using fixed column order
        result = {
            'name': pharmacy_data.get('name'),
            'address': address,
            'email': pharmacy_data.get('email'),
            'fax': pharmacy_data.get('fax'),
            'latitude': None,  # Community Care Chemist doesn't provide coordinates
            'longitude': None, # Community Care Chemist doesn't provide coordinates
            'phone': pharmacy_data.get('phone'),
            'postcode': postcode,
            'state': state,
            'street_address': address,
            'suburb': suburb,
            'trading_hours': pharmacy_data.get('trading_hours', {}),
            'website': "https://www.communitycarechemist.com.au/"  # Default website
        }
        
        # Remove any None values to keep the data clean
        cleaned_result = {}
        for key, value in result.items():
            if value is not None:
                cleaned_result[key] = value
                
        return cleaned_result