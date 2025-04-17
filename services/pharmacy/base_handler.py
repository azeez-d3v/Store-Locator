import asyncio
from abc import ABC, abstractmethod

class BasePharmacyHandler(ABC):
    """
    Base handler class for all pharmacy brand handlers
    """
    
    def __init__(self, pharmacy_locations):
        """
        Initialize the handler with a reference to the main PharmacyLocations instance
        
        Args:
            pharmacy_locations: The parent PharmacyLocations instance
        """
        self.pharmacy_locations = pharmacy_locations
        self.session_manager = pharmacy_locations.session_manager
        
    @abstractmethod
    async def fetch_locations(self):
        """
        Fetch all locations for this pharmacy brand
        
        Returns:
            List of locations
        """
        pass
        
    @abstractmethod
    async def fetch_pharmacy_details(self, location_id):
        """
        Fetch detailed information for a specific pharmacy
        
        Args:
            location_id: The ID of the location to get details for
            
        Returns:
            Detailed pharmacy data
        """
        pass
        
    @abstractmethod
    async def fetch_all_locations_details(self):
        """
        Fetch details for all locations and return as a list
        Uses concurrent requests for better performance
        
        Returns:
            List of dictionaries containing pharmacy details
        """
        pass
        
    @abstractmethod
    def extract_pharmacy_details(self, pharmacy_data):
        """
        Extract specific fields from pharmacy location details response
        
        Args:
            pharmacy_data: The raw pharmacy data from the API
            
        Returns:
            Dictionary containing only the requested fields in a standardized order
        """
        pass