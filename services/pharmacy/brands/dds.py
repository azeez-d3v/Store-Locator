import asyncio
from rich import print
from ..base_handler import BasePharmacyHandler

class DDSHandler(BasePharmacyHandler):
    """Handler for Discount Drug Stores (DDS)"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.brand_name = "dds"
        self.brand_config = pharmacy_locations.BRAND_CONFIGS["dds"]
        
    async def fetch_locations(self):
        """Fetch all DDS locations"""
        payload = self.brand_config.copy()
        
        response = await self.session_manager.post(
            url=self.pharmacy_locations.BASE_URL,
            json=payload,
            headers=self.pharmacy_locations.COMMON_HEADERS
        )
        
        # Process the response
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            raise Exception(f"Failed to fetch DDS locations: {response.status_code}")
    
    async def fetch_pharmacy_details(self, location_id):
        """
        Fetch detailed information for a specific DDS pharmacy
        
        Args:
            location_id: The ID of the location to get details for
            
        Returns:
            Detailed pharmacy data
        """
        payload = {
            "session_id": self.brand_config["session_id"],
            "businessid": self.brand_config["businessid"],
            "locationid": location_id,
            "include_services": True,
            "source": self.brand_config["source"]
        }
        
        response = await self.session_manager.post(
            url=self.pharmacy_locations.DETAIL_URL,
            json=payload,
            headers=self.pharmacy_locations.COMMON_HEADERS
        )
        
        # Process the response
        if response.status_code == 200:
            data = response.json()
            return self.extract_pharmacy_details(data)
        else:
            raise Exception(f"Failed to fetch pharmacy details: {response.status_code}")
    
    async def fetch_all_locations_details(self):
        """
        Fetch details for all DDS locations and return as a list
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        # First get all locations
        print(f"Fetching all DDS locations...")
        locations = await self.fetch_locations()
        if not locations:
            print(f"No DDS locations found.")
            return []
            
        print(f"Found {len(locations)} DDS locations. Fetching details concurrently...")
        
        # Create tasks for all locations
        tasks = []
        for location in locations:
            location_id = location.get('locationid')
            if location_id:
                task = self.fetch_pharmacy_details(location_id)
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
                    all_details.append(result)
                
        print(f"Completed fetching details for {len(all_details)} DDS locations.")
        return all_details
    
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract specific fields from pharmacy location details response
        
        Args:
            pharmacy_data: The raw pharmacy data from the API
            
        Returns:
            Dictionary containing only the requested fields in a standardized order
        """
        location_details = pharmacy_data.get('location_details', {})
        
        # Extract trading hours directly from the top-level field
        trading_hours = pharmacy_data.get('trading_hours', {})
        
        # Using fixed column order
        result = {
            'name': location_details.get('locationname'),
            'address': location_details.get('address'),
            'email': location_details.get('email'),
            'fax': location_details.get('fax_number'),
            'latitude': location_details.get('latitude'),
            'longitude': location_details.get('longitude'),
            'phone': location_details.get('phone'),
            'postcode': location_details.get('postcode'),
            'state': location_details.get('state'),
            'street_address': location_details.get('streetaddress'),
            'suburb': location_details.get('suburb'),
            'trading_hours': trading_hours,
            'website': location_details.get('website')
        }
        
        # Remove any None values to keep the data clean
        cleaned_result = {}
        for key, value in result.items():
            if value is not None:
                cleaned_result[key] = value
                
        return cleaned_result