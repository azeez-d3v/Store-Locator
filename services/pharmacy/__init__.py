from .core import PharmacyLocations

# Create a singleton instance for convenience
pharmacy_locations = PharmacyLocations()

__all__ = ['PharmacyLocations', 'pharmacy_locations']