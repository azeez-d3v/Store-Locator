"""
Pharmacy Location Services - Main Entry Point
This module provides functionality to fetch pharmacy locations from different brands.
"""

from services.pharmacy.core import PharmacyLocations
from services.pharmacy import pharmacy_locations

# Re-export the class and singleton instance for backward compatibility
__all__ = ['PharmacyLocations', 'pharmacy_locations']