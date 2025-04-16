import asyncio
import sys
import os
import csv
import json
from pathlib import Path

# append parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from services.session_manager import SessionManager
except ImportError:
    # Direct import if the related module is not found
    from session_manager import SessionManager

class PharmacyLocations:
    """
    Generic class to fetch pharmacy locations from different brands
    """
    BASE_URL = "https://app.medmate.com.au/connect/api/get_locations"
    DETAIL_URL = "https://app.medmate.com.au/connect/api/get_pharmacy"
    
    BRAND_CONFIGS = {
        "dds": {
            "businessid": "2",
            "session_id": "",
            "source": "DDSPharmacyWebsite"
        },
        "amcal": {
            "businessid": "4",
            "session_id": "",
            "source": "AmcalPharmacyWebsite"
        }
    }
    
    COMMON_HEADERS = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'no-cache',
        'Host': 'app.medmate.com.au',
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
        'X-Requested-With': 'XMLHttpRequest',
    }
    
    def __init__(self):
        self.session_manager = SessionManager()
        
    async def fetch_locations(self, brand):
        """
        Fetch locations for a specific pharmacy brand.
        
        Args:
            brand: String identifier for the brand (e.g., "dds", "amcal")
            
        Returns:
            Processed location data
        """
        if brand not in self.BRAND_CONFIGS:
            raise ValueError(f"Unknown pharmacy brand: {brand}")
            
        payload = self.BRAND_CONFIGS[brand]
        
        response = await self.session_manager.post(
            url=self.BASE_URL,
            json=payload,
            headers=self.COMMON_HEADERS
        )
        
        # Process the response
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            raise Exception(f"Failed to fetch {brand.upper()} locations: {response.status_code}")
    
    async def fetch_pharmacy_details(self, brand, location_id):
        """
        Fetch detailed information for a specific pharmacy.
        
        Args:
            brand: String identifier for the brand (e.g., "dds", "amcal")
            location_id: The ID of the location to get details for
            
        Returns:
            Detailed pharmacy data
        """
        if brand not in self.BRAND_CONFIGS:
            raise ValueError(f"Unknown pharmacy brand: {brand}")
            
        brand_config = self.BRAND_CONFIGS[brand]
        
        payload = {
            "session_id": brand_config["session_id"],
            "businessid": brand_config["businessid"],
            "locationid": location_id,
            "include_services": True,
            "source": brand_config["source"]
        }
        
        response = await self.session_manager.post(
            url=self.DETAIL_URL,
            json=payload,
            headers=self.COMMON_HEADERS
        )
        
        # Process the response
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            raise Exception(f"Failed to fetch pharmacy details: {response.status_code}")
            
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract specific fields from pharmacy location details response.
        
        Args:
            pharmacy_data: The raw pharmacy data from the API
            
        Returns:
            Dictionary containing only the requested fields
        """
        location_details = pharmacy_data.get('location_details', {})
        
        # Extract trading hours directly from the top-level field
        trading_hours = pharmacy_data.get('trading_hours', {})
        
        
        return {
            'name': location_details.get('locationname'),
            'phone': location_details.get('phone'),
            'email': location_details.get('email'),
            'fax': location_details.get('fax_number'),
            'state': location_details.get('state'),
            'address': location_details.get('address'),
            'suburb': location_details.get('suburb'),
            'postcode': location_details.get('postcode'),
            'street_address': location_details.get('streetaddress'),
            'latitude': location_details.get('latitude'),
            'longitude': location_details.get('longitude'),
            'website': location_details.get('website'),
            'trading_hours': trading_hours,
        }
    
    async def fetch_dds_locations(self):
        """Fetch Discount Drug Stores locations"""
        return await self.fetch_locations("dds")
        
    async def fetch_amcal_locations(self):
        """Fetch Amcal Pharmacy locations"""
        return await self.fetch_locations("amcal")
        
    async def fetch_dds_pharmacy_details(self, location_id):
        """Fetch details for a specific Discount Drug Store"""
        data = await self.fetch_pharmacy_details("dds", location_id)
        return self.extract_pharmacy_details(data)
        
    async def fetch_amcal_pharmacy_details(self, location_id):
        """Fetch details for a specific Amcal Pharmacy"""
        data = await self.fetch_pharmacy_details("amcal", location_id)
        return self.extract_pharmacy_details(data)
        
    async def fetch_all_locations_details(self, brand):
        """
        Fetch details for all locations of a specific brand and return as a list.
        Uses concurrent requests for better performance.
        
        Args:
            brand: String identifier for the brand (e.g., "dds", "amcal")
            
        Returns:
            List of dictionaries containing pharmacy details
        """
        if brand not in self.BRAND_CONFIGS:
            raise ValueError(f"Unknown pharmacy brand: {brand}")
            
        # First get all locations
        print(f"Fetching all {brand.upper()} locations...")
        locations = await self.fetch_locations(brand)
        if not locations:
            print(f"No {brand.upper()} locations found.")
            return []
            
        print(f"Found {len(locations)} {brand.upper()} locations. Fetching details concurrently...")
        
        # Create tasks for all locations
        tasks = []
        for location in locations:
            location_id = location.get('locationid')
            if location_id:
                task = self.fetch_pharmacy_details(brand, location_id)
                tasks.append((location_id, task))
        
        # Process each location to get details
        all_details = []
        batch_size = 10  # Process in batches to avoid overwhelming the server
        total_batches = (len(tasks) + batch_size - 1) // batch_size
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(tasks))
            batch_tasks = tasks[start_idx:end_idx]
            
            print(f"Processing batch {batch_idx + 1}/{total_batches} ({start_idx+1}-{end_idx} of {len(tasks)})...")
            
            # Run the batch concurrently
            results = await asyncio.gather(
                *[task for _, task in batch_tasks],
                return_exceptions=True
            )
            
            # Process results
            for i, result in enumerate(results):
                location_id, _ = batch_tasks[i]
                if isinstance(result, Exception):
                    print(f"Error fetching details for location {location_id}: {result}")
                else:
                    try:
                        extracted_details = self.extract_pharmacy_details(result)
                        all_details.append(extracted_details)
                    except Exception as e:
                        print(f"Error extracting details for location {location_id}: {e}")
                
        print(f"Completed fetching details for {len(all_details)} {brand.upper()} locations.")
        return all_details
        
    def save_to_csv(self, data, filename):
        """
        Save a list of dictionaries to a CSV file.
        
        Args:
            data: List of dictionaries with pharmacy details
            filename: Name of the CSV file to create
        """
        if not data:
            print(f"No data to save to {filename}")
            return
            
        # Ensure the output directory exists
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        filepath = output_dir / filename
        
        # Get all possible field names from all dictionaries
        fieldnames = set()
        for item in data:
            fieldnames.update(item.keys())
        fieldnames = sorted(list(fieldnames))
        
        print(f"Saving {len(data)} records to {filepath}...")
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
            
        print(f"Data successfully saved to {filepath}")
        
    async def fetch_and_save_all(self):
        """
        Fetch all locations for all brands concurrently and save to CSV files.
        """
        # Create task for each brand
        tasks = {brand: self.fetch_all_locations_details(brand) 
                for brand in self.BRAND_CONFIGS.keys()}
        
        # Execute all brand tasks concurrently
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        
        # Process results and save to CSV
        for brand, result in zip(tasks.keys(), results):
            try:
                if isinstance(result, Exception):
                    print(f"Error processing {brand} pharmacies: {result}")
                elif result:
                    self.save_to_csv(result, f"{brand}_pharmacies.csv")
                else:
                    print(f"No data found for {brand} pharmacies")
            except Exception as e:
                print(f"Error saving {brand} pharmacies data: {e}")

# Example usage
async def main():
    pharmacy_api = PharmacyLocations()
    
    # Fetch DDS locations
    dds_locations = await pharmacy_api.fetch_dds_locations()
    print(f"Found {len(dds_locations)} Discount Drug Store locations")
    
    # Fetch Amcal locations
    amcal_locations = await pharmacy_api.fetch_amcal_locations()
    print(f"Found {len(amcal_locations)} Amcal locations")
    
    # Fetch DDS pharmacy details
    if dds_locations:
        dds_details = await pharmacy_api.fetch_dds_pharmacy_details(dds_locations[0]['locationid'])
        print(f"DDS Pharmacy Details: {dds_details}")
    
    # Fetch Amcal pharmacy details
    if amcal_locations:
        amcal_details = await pharmacy_api.fetch_amcal_pharmacy_details(amcal_locations[0]['locationid'])
        print(f"Amcal Pharmacy Details: {amcal_details}")
    
    # Fetch and save all pharmacy details
    print("\nFetching and saving all pharmacy details...")
    await pharmacy_api.fetch_and_save_all()

if __name__ == "__main__":
    asyncio.run(main())