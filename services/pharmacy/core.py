import asyncio
import sys
import os
import csv
from rich import print
from pathlib import Path

# Append parent directory to path for imports
parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(parent_dir)

try:
    from services.session_manager import SessionManager
except ImportError:
    # Direct import if the related module is not found
    from session_manager import SessionManager

# Import for progress reporting - if not available, provide fallback
try:
    import streamlit as st
except ImportError:
    # Create dummy streamlit functions for non-streamlit environments
    class DummySt:
        def progress(self, *args, **kwargs):
            return DummyProgress()
        
        def expander(self, *args, **kwargs):
            return DummyExpander()
            
    class DummyProgress:
        def __enter__(self):
            return self
            
        def __exit__(self, *args, **kwargs):
            pass
            
        def update(self, *args, **kwargs):
            pass
    
    class DummyExpander:
        def __enter__(self):
            return self
            
        def __exit__(self, *args, **kwargs):
            pass
            
        def write(self, *args, **kwargs):
            pass
    
    st = DummySt()

class PharmacyLocations:
    """
    Generic class to fetch pharmacy locations from different brands
    """
    # Base URLs
    BASE_URL = "https://app.medmate.com.au/connect/api/get_locations"
    DETAIL_URL = "https://app.medmate.com.au/connect/api/get_pharmacy"
    BLOOMS_URL = "https://api.storepoint.co/v2/15f056510a1d3a/locations"
    RAMSAY_URL = "https://ramsayportalapi-prod.azurewebsites.net/api/pharmacyclient/pharmacies"
    REVIVE_URL = "https://core.service.elfsight.com/p/boot/?page=https%3A%2F%2Frevivepharmacy.com.au%2Fstore-finder%2F&w=52ff3b25-4412-410c-bd3d-ea57b2814fac"
    OPTIMAL_URL = "https://core.service.elfsight.com/p/boot/?page=https%3A%2F%2Foptimalpharmacyplus.com.au%2Flocations%2F&w=d70b40db-e8b3-43bc-a63b-b3cce68941bf"
    COMMUNITY_URL = "https://www.communitycarechemist.com.au/"
    FOOTES_URL = "https://footespharmacies.com/stores/"
    FOOTES_SITEMAP_URL = "https://footespharmacies.com/stores-sitemap.xml"
    ALIVE_URL = "https://stockist.co/api/v1/u6442/locations/all"
    YDC_URL = "https://bc-wh.myintegrator.com.au/api/store/d75m9rit2s/location-list"
    CHEMIST_WAREHOUSE_URL = "https://www.chemistwarehouse.com.au/webapi/store/store-locator?BusinessGroupId=2&SearchByState=&SortByDistance=false"
    CHEMIST_WAREHOUSE_NZ_URL = "https://www.chemistwarehouse.co.nz/webapi/store/store-locator?BusinessGroupId=4&SearchByState=&SortByDistance=false"
    ANTIDOTE_NZ_URL = "https://www.antidotepharmacy.co.nz/"
    UNICHEM_NZ_LOCATIONS_URL = "https://www.closeby.co/embed/60e75b93df98a16d97499b8b8512e14f/locations?bounding_box&cachable=true&isInitialLoad=true"
    BARGAIN_CHEMIST_NZ_URL = "https://www.bargainchemist.co.nz/pages/find-a-store"
    PHARMASAVE_URL = "https://www.pharmasave.com.au/wp-admin/admin-ajax.php?action=store_search&lat=&lng=&max_results=100&search_radius=100&autoload=1"
    NOVA_URL = "https://www.novapharmacy.com.au/wp-admin/admin-ajax.php?action=store_search&lat=-&lng=&max_results=100&search_radius=100&autoload=1"
    CHOICE_URL = "https://www.choicepharmacy.com.au/wp-admin/admin-ajax.php?action=store_search&lat=&lng=&max_results=100&search_radius=100&autoload=1"
    BENDIGO_UFS_SITEMAP_URL = "https://www.bendigoufs.com.au/page-sitemap.xml"
    CHEMIST_KING_URLS = [
            "https://www.chemistking.com.au/colonellightgardens",
            "https://www.chemistking.com.au/frewville",
            "https://www.chemistking.com.au/hectorville",
            "https://www.chemistking.com.au/klemzig",
            "https://www.chemistking.com.au/morphettvale",
            "https://www.chemistking.com.au/mountgambier",
            "https://www.chemistking.com.au/murraybridge",
            "https://www.chemistking.com.au/springbank",
            "https://www.chemistking.com.au/welland"
        ]
    HEALTHY_PHARMACY_SITEMAP_URL = "https://www.healthylife.com.au/sitemap/stores.xml"
    PENNAS_URLS = [
            "https://www.pennaspharmacy.com.au/locations/pennas-discount-pharmacy-edensor-park",
            "https://www.pennaspharmacy.com.au/locations/pennas-discount-pharmacy-prestons",
            "https://www.pennaspharmacy.com.au/locations/pennas-discount-pharmacy-cecil-hills",
            "https://www.pennaspharmacy.com.au/locations/pennas-discount-pharmacy-green-valley",
            "https://www.pennaspharmacy.com.au/locations/pennas-discount-pharmacy-liverpool"
        ]
    FRIENDLY_CARE_URLS = [
            "https://www.friendlycare.com.au/headoffice",
            "https://www.friendlycare.com.au/ayr",
            "https://www.friendlycare.com.au/booval",
            "https://www.friendlycare.com.au/burleigh",
            "https://www.friendlycare.com.au/ipswichcbd",
            "https://www.friendlycare.com.au/jacobswell",
            "https://www.friendlycare.com.au/nundah",
            "https://www.friendlycare.com.au/sandgate"
        ]
    
    FULLIFE_URL = "https://www.fullife.com.au/locations"
    GOOD_PRICE_URL = "https://www.goodpricepharmacy.com.au/amlocator/index/ajax/"
    HEALTHY_LIFE_SITEMAP_URL = "https://www.healthylife.com.au/sitemap/stores.xml"
    HEALTHY_LIFE_BASE_URL = "https://www.healthylife.com.au"
    HEALTHY_WORLD_URL = "https://healthyworldpharmacy.com.au/pages/locations"
    WIZARD_URL = "https://www.wizardpharmacy.com.au/store-finder"
    SUPERCHEM_API_URL = "https://www.superchem.com.au/_system/action/group-store-finder/all-locations"
    CHEMIST_HUB_URLS =  [
            "https://www.chemisthub.au/store-locator/chemist-hub-rockdale",
            "https://www.chemisthub.au/store-locator/chemist-hub-sanctuary-point",
            "https://www.chemisthub.au/store-locator/chemist-hub-valentine",
            "https://www.chemisthub.au/store-locator/panania-pharmacy",
            "https://www.chemisthub.au/store-locator/chemist-hub-ingleburn-medical-centre-pharmacy",
            "https://www.chemisthub.au/store-locator/chemist-hub-panania",
            "https://www.chemisthub.au/store-locator/chemist-hub-kareela-community-pharmacy",
            "https://www.chemisthub.au/store-locator/chemist-hub-wallsend"
        ]
    COMPLETE_CARE_URLS = [
        "https://completecarepharmacies.com.au/locations/bairnsdale/",
        "https://completecarepharmacies.com.au/locations/bellambi/",
        "https://completecarepharmacies.com.au/locations/kurri-kurri/",
        "https://completecarepharmacies.com.au/locations/landsborough/",
        "https://completecarepharmacies.com.au/locations/penguin/",
        "https://completecarepharmacies.com.au/locations/rosny/",
        "https://completecarepharmacies.com.au/locations/south-hobart/"
    ]
    # Brand configurations
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
    
    # Common headers used across API calls
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
        # Import brand-specific handlers dynamically to avoid circular imports
        from services.pharmacy.brands import amcal, dds, blooms, ramsay, revive, optimal, community, footes, alive, ydc, chemist_warehouse, pharmasave, nova, choice, bendigo_ufs, chemist_king, friendly_care, fullife, good_price, healthy_pharmacy, healthy_world, pennas, wizard, chemist_hub, superchem, complete_care
        # Import NZ handlers
        from services.pharmacy.brands.nz import chemist_warehouse_nz, antidote, unichem, bargain_chemist, woolworths
        
        self.brand_handlers = {
            "amcal": amcal.AmcalHandler(self),
            "dds": dds.DDSHandler(self),
            "blooms": blooms.BloomsHandler(self),
            "ramsay": ramsay.RamsayHandler(self),
            "revive": revive.ReviveHandler(self),
            "optimal": optimal.OptimalHandler(self),
            "community": community.CommunityHandler(self),
            "footes": footes.FootesHandler(self),
            "alive": alive.AliveHandler(self),
            "ydc": ydc.YdcHandler(self),
            "chemist_warehouse": chemist_warehouse.ChemistWarehouseHandler(self),
            "pharmasave": pharmasave.PharmasaveHandler(self),
            "nova": nova.NovaHandler(self),
            "choice": choice.ChoiceHandler(self),
            "bendigo_ufs": bendigo_ufs.BendigoUfsHandler(self),
            "chemist_king": chemist_king.ChemistKingHandler(self),
            "friendly_care": friendly_care.FriendlyCareHandler(self),
            "fullife": fullife.FullifeHandler(self),
            "good_price": good_price.GoodPriceHandler(self),
            "healthy_pharmacy": healthy_pharmacy.HealthyPharmacyHandler(self),
            "healthy_world": healthy_world.HealthyWorldPharmacyHandler(self),
            "pennas": pennas.PennasPharmacyHandler(self),
            "wizard": wizard.WizardPharmacyHandler(self),
            "chemist_hub": chemist_hub.ChemistHubHandler(self),
            "superchem": superchem.SuperChemHandler(self),
            "complete_care": complete_care.CompleteCareHandler(self),
            # New Zealand handlers
            "chemist_warehouse_nz": chemist_warehouse_nz.ChemistWarehouseNZHandler(self),
            "antidote_nz": antidote.AntidotePharmacyNZHandler(self),
            "unichem_nz": unichem.UnichemNZHandler(self),
            "bargain_chemist_nz": bargain_chemist.BargainChemistNZHandler(self),
            "woolworths_nz": woolworths.WoolworthsPharmacyNZHandler(self)
        }

    async def fetch_locations(self, brand):
        """
        Fetch locations for a specific pharmacy brand.
        
        Args:
            brand: String identifier for the brand (e.g., "dds", "amcal", "blooms", "ramsay", "revive", "optimal", "community")
            
        Returns:
            Processed location data
        """
        if brand in self.brand_handlers:
            return await self.brand_handlers[brand].fetch_locations()
            
        raise ValueError(f"Unknown pharmacy brand: {brand}")
    
    async def fetch_pharmacy_details(self, brand, location_id):
        """
        Fetch detailed information for a specific pharmacy.
        
        Args:
            brand: String identifier for the brand (e.g., "dds", "amcal", "blooms", "ramsay", "revive", "optimal", "community")
            location_id: The ID of the location to get details for
            
        Returns:
            Detailed pharmacy data
        """
        if brand in self.brand_handlers:
            return await self.brand_handlers[brand].fetch_pharmacy_details(location_id)
            
        raise ValueError(f"Unknown pharmacy brand: {brand}")
    
    async def fetch_all_locations_details(self, brand):
        """
        Fetch details for all locations of a specific brand and return as a list.
        Uses concurrent requests for better performance.
        
        Args:
            brand: String identifier for the brand (e.g., "dds", "amcal", "blooms", "ramsay", "revive", "optimal", "community")
            
        Returns:
            List of dictionaries containing pharmacy details
        """
        if brand in self.brand_handlers:
            return await self.brand_handlers[brand].fetch_all_locations_details()
            
        raise ValueError(f"Unknown pharmacy brand: {brand}")
        
    def save_to_csv(self, data, filename):
        """
        Save a list of dictionaries to a CSV file.
        
        Args:
            data: List of dictionaries with pharmacy details
            filename: Name of the CSV file to create (ignored, will use "pharmacy_details.csv")
            
        Returns:
            bool: True if save successful, False otherwise
        """
        if not data:
            print("No data to save")
            return False
            
        # Ensure the output directory exists
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        # Use fixed filename "pharmacy_details.csv"
        filepath = output_dir / filename
        
        # Define the exact field order we want with the specified field names
        fixed_fieldnames = [
            'EntityName', 'OutletAddress', 'Phone', 'Fax', 'Email', 
            'Working hours', 'latitude', 'longitude'
        ]
        
        # Map original field names to the new field names
        field_mapping = {
            'EntityName': 'name',
            'OutletAddress': 'address',
            'Phone': 'phone',
            'Fax': 'fax',
            'Email': 'email',
            'Working hours': 'trading_hours',
            'latitude': 'latitude',
            'longitude': 'longitude'
        }
        
        try:
            # Filter and map data to include only the specified fields with the new names
            filtered_data = []
            for item in data:
                mapped_item = {}
                for new_field, original_field in field_mapping.items():
                    mapped_item[new_field] = item.get(original_field, None)
                filtered_data.append(mapped_item)
            
            print(f"Saving {len(filtered_data)} records to {filepath}...")
            
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fixed_fieldnames)
                writer.writeheader()
                writer.writerows(filtered_data)
                
            print(f"Data successfully saved to {filepath}")
            return True
        except Exception as e:
            print(f"Error saving data to {filepath}: {e}")
            return False
        
    async def fetch_and_save_all(self, selected_brands=None):
        """
        Fetch all locations for all brands concurrently and save to CSV files.
        
        Args:
            selected_brands: List of brands to fetch. If None, fetch all brands.
            
        Returns:
            dict: A summary of results with counts of successful and failed operations
        """
        # Get list of brands to process
        if selected_brands is None:
            # If no brands specified, use all brands
            brands = list(self.brand_handlers.keys())
        else:
            # Only use the brands that were selected
            brands = [brand for brand in selected_brands if brand in self.brand_handlers]
        
        # Track results for reporting
        results_summary = {
            "total_brands": len(brands),
            "successful_brands": 0,
            "failed_brands": 0,
            "total_locations": 0,
            "details": {}
        }
        
        # Create tasks for each brand
        tasks = {brand: self.fetch_all_locations_details(brand) for brand in brands}
        
        # Execute all brand tasks concurrently
        fetch_results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        
        # Process results and save to CSV
        for brand, result in zip(tasks.keys(), fetch_results):
            brand_summary = {
                "status": "success",
                "locations": 0,
                "error": None
            }
            
            try:
                if isinstance(result, Exception):
                    print(f"Error processing {brand.upper()} pharmacies: {result}")
                    brand_summary["status"] = "failed"
                    brand_summary["error"] = str(result)
                    results_summary["failed_brands"] += 1
                elif result:
                    # Save data to CSV and track success
                    save_success = self.save_to_csv(result, f"{brand}_pharmacies.csv")
                    if save_success:
                        brand_summary["locations"] = len(result)
                        results_summary["total_locations"] += len(result)
                        results_summary["successful_brands"] += 1
                    else:
                        brand_summary["status"] = "failed"
                        brand_summary["error"] = "Failed to save CSV file"
                        results_summary["failed_brands"] += 1
                else:
                    print(f"No data found for {brand.upper()} pharmacies")
                    brand_summary["status"] = "empty"
                    brand_summary["error"] = "No data found"
                    # Not counting as failure, just zero locations
                    results_summary["successful_brands"] += 1
            except Exception as e:
                error_msg = f"Error saving {brand.upper()} pharmacies data: {e}"
                print(error_msg)
                brand_summary["status"] = "failed"
                brand_summary["error"] = error_msg
                results_summary["failed_brands"] += 1
            
            # Add brand details to summary
            results_summary["details"][brand] = brand_summary
        
        # Print summary report
        print(f"\nFetch and Save Summary:")
        print(f"- Brands processed: {results_summary['total_brands']}")
        print(f"- Successful: {results_summary['successful_brands']}")
        print(f"- Failed: {results_summary['failed_brands']}")
        print(f"- Total locations: {results_summary['total_locations']}")
        
        return results_summary

    # Convenience methods for backward compatibility
    async def fetch_dds_locations(self):
        """Fetch Discount Drug Stores locations"""
        return await self.fetch_locations("dds")
        
    async def fetch_amcal_locations(self):
        """Fetch Amcal Pharmacy locations"""
        return await self.fetch_locations("amcal")
        
    async def fetch_blooms_locations_list(self):
        """Fetch Blooms The Chemist locations"""
        return await self.fetch_locations("blooms")
        
    async def fetch_ramsay_locations_list(self):
        """Fetch Ramsay Pharmacy locations"""
        return await self.fetch_locations("ramsay")
        
    async def fetch_revive_locations_list(self):
        """Fetch Revive Pharmacy locations"""
        return await self.fetch_locations("revive")
    
    async def fetch_optimal_locations_list(self):
        """Fetch Optimal Pharmacy Plus locations"""
        return await self.fetch_locations("optimal")
    
    async def fetch_community_locations_list(self):
        """Fetch Community Care Chemist locations"""
        return await self.fetch_locations("community")
    
    async def fetch_footes_locations_list(self):
        """Fetch Footes Pharmacy locations"""
        return await self.fetch_locations("footes")
        
    async def fetch_dds_pharmacy_details(self, location_id):
        """Fetch details for a specific Discount Drug Store"""
        return await self.fetch_pharmacy_details("dds", location_id)
        
    async def fetch_amcal_pharmacy_details(self, location_id):
        """Fetch details for a specific Amcal Pharmacy"""
        return await self.fetch_pharmacy_details("amcal", location_id)