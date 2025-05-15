# 🏥 Pharmacy Store Locator Analytics Dashboard

A comprehensive tool for fetching, analyzing, and visualizing pharmacy location data across multiple pharmacy banners in Australia and New Zealand.

## 📋 Overview

The Pharmacy Store Locator is a Streamlit-based web application that allows users to:

1. Fetch pharmacy location data from multiple pharmacy banners
2. Analyze geographic distribution, trading hours, and service offerings
3. Compare data across different pharmacy chains
4. Visualize pharmacy data through interactive maps, charts, and statistics

## 📑 Table of Contents

- [🏥 Pharmacy Store Locator Analytics Dashboard](#-pharmacy-store-locator-analytics-dashboard)
  - [📋 Overview](#-overview)
  - [📑 Table of Contents](#-table-of-contents)
  - [🚀 Features](#-features)
    - [Data Collection](#data-collection)
    - [Data Analysis](#data-analysis)
    - [Advanced Features](#advanced-features)
  - [💻 Tech Stack](#-tech-stack)
    - [Core Technologies](#core-technologies)
    - [Scraping Approach Comparison](#scraping-approach-comparison)
      - [Current Approach (curl\_cffi + BeautifulSoup)](#current-approach-curl_cffi--beautifulsoup)
      - [Implementation Details](#implementation-details)
      - [Comparison with Selenium](#comparison-with-selenium)
    - [Package Management](#package-management)
  - [🔧 Installation](#-installation)
    - [Prerequisites](#prerequisites)
    - [Setup Instructions](#setup-instructions)
      - [Easy Setup (Windows)](#easy-setup-windows)
      - [Setup with UV (Recommended)](#setup-with-uv-recommended)
      - [Manual Setup (Alternative)](#manual-setup-alternative)
  - [📊 Usage Guide](#-usage-guide)
    - [Fetching Pharmacy Data](#fetching-pharmacy-data)
    - [Analyzing Data](#analyzing-data)
    - [Viewing Fetch History](#viewing-fetch-history)
  - [📁 Project Structure](#-project-structure)
  - [📝 Technical Details](#-technical-details)
    - [Components](#components)
    - [Dependencies](#dependencies)
    - [Data Model](#data-model)
  - [🧩 Adding a New Pharmacy Banner](#-adding-a-new-pharmacy-banner)
    - [Overview of Pharmacy Handler Architecture](#overview-of-pharmacy-handler-architecture)
    - [Step 1: Create a New Handler Class](#step-1-create-a-new-handler-class)
    - [Step 2: Implement Required Methods](#step-2-implement-required-methods)
      - [2.1 `fetch_locations()`](#21-fetch_locations)
      - [2.2 `fetch_pharmacy_details(self, location)`](#22-fetch_pharmacy_detailsself-location)
      - [2.3 `fetch_all_locations_details()`](#23-fetch_all_locations_details)
      - [2.4 `extract_pharmacy_details(self, pharmacy_data)`](#24-extract_pharmacy_detailsself-pharmacy_data)
    - [Step 3: Helper Methods](#step-3-helper-methods)
    - [Step 4: Register Your Handler in the System](#step-4-register-your-handler-in-the-system)
    - [Step 5: Test Your Implementation](#step-5-test-your-implementation)
    - [Tips for Different Pharmacy Website Types](#tips-for-different-pharmacy-website-types)
      - [1. API-Based Websites](#1-api-based-websites)
      - [2. HTML-Based Websites](#2-html-based-websites)
      - [3. JavaScript-Heavy Websites](#3-javascript-heavy-websites)
    - [Common Challenges and Solutions](#common-challenges-and-solutions)
  - [📄 License](#-license)

## 🚀 Features

### Data Collection

- **Multi-banner support**: Fetches data from 25+ pharmacy banners:
  - **Australia**:
    - Discount Drug Stores (DDS)
    - Amcal
    - Blooms The Chemist
    - Ramsay Pharmacy
    - Revive Pharmacy
    - Optimal Pharmacy Plus
    - Community Care Chemist
    - Footes Pharmacy
    - Alive Pharmacy
    - Your Discount Chemist (YDC)
    - Chemist Warehouse
    - Pharmasave
    - Nova Pharmacy
    - Choice Pharmacy
    - Bendigo UFS
    - Chemist King
    - Friendly Care Pharmacy
    - Fullife Pharmacy
    - Good Price Pharmacy
    - Healthy Life Pharmacy
    - Healthy World Pharmacy
    - Pennas Pharmacy
    - Wizard Pharmacy
    - Chemist Hub
    - SuperChem
    - Complete Care Pharmacy
    - TerryWhite Chemmart
    - MyChemist
    - Direct Chemist Outlet
  - **New Zealand**:
    - Antidote Pharmacy
    - Bargain Chemist
    - Chemist Warehouse NZ
    - Complete Care NZ
    - Unichem
    - Woolworths Pharmacy
- **Asynchronous data fetching**: Efficiently retrieves data using modern async/await patterns
- **Structured storage**: Saves all fetched data as CSV files in the output directory
- **Fetch history tracking**: Logs all data retrieval operations with timestamps and success status

### Data Analysis

- **Interactive data exploration**: Browse and search pharmacy location data
- **Geographic visualization**:
  - View pharmacy distribution by state
  - Interactive map display showing exact pharmacy locations
  - Hover tooltips with store information
- **Trading hours analysis**:
  - Visual representation of opening hours
  - Detailed weekly schedule view
  - Opening hours comparison across days
- **Data completeness analysis**: Visualize data quality and completeness across fields
- **Banner comparison**: Compare metrics across different pharmacy chains

### Advanced Features

- **Robust error handling**: Gracefully handles various data formats and missing information
- **Responsive UI**: Well-organized tabbed interface with meaningful visualizations

## 💻 Tech Stack

### Core Technologies

- **Python 3.11+**: Leverages modern Python features for improved performance and code organization
- **Streamlit**: Powers the interactive web interface and data visualizations
- **Pandas**: Handles data manipulation, transformation, and analysis
- **Plotly**: Creates interactive visualizations, charts, and maps
- **BeautifulSoup4**: Parses HTML content from pharmacy websites
- **curl_cffi**: Provides high-performance HTTP request capabilities with browser emulation
- **lxml**: XML/HTML parsing library for efficient data extraction
- **openpyxl**: A Python library to read/write Excel files

### Scraping Approach Comparison

#### Current Approach (curl_cffi + BeautifulSoup)

The project uses a combination of `curl_cffi` for HTTP requests and `BeautifulSoup` for HTML parsing, offering several advantages:

- **Performance**: curl_cffi provides significantly faster request times (3-10x) compared to traditional methods
- **Browser Impersonation**: Simulates real browsers using modern fingerprinting techniques to bypass basic anti-bot measures
- **Resource Efficiency**: Uses minimal system resources (10-50MB per session) compared to full browser automation
- **Concurrency**: Native AsyncIO integration enables true parallel requests with minimal overhead
- **Maintainability**: Clean, modular code structure with clear separation of concerns
- **Error Resilience**: Built-in retries and error handling for more reliable data collection
- **Specialized Handlers**: Each pharmacy banner has a dedicated handler class inheriting from `BasePharmacyHandler`

#### Implementation Details

The current implementation consists of several key components:

1. **SessionManager**: Wraps curl_cffi's AsyncSession to provide browser impersonation and concurrent requests

   ```python
   async with AsyncSession(impersonate="chrome131") as session:
       return await session.get(url, headers=combined_headers)
   ```

2. **BasePharmacyHandler**: Abstract base class that defines the interface for all pharmacy handlers

   ```python
   class BasePharmacyHandler(ABC):
       @abstractmethod
       async def fetch_locations(self):
           """Fetch all locations for this pharmacy brand"""
           pass
       
       @abstractmethod
       async def fetch_pharmacy_details(self, location_id):
           """Fetch detailed information for a specific pharmacy"""
           pass
   ```

3. **Brand-specific Handlers**: Specialized classes for each pharmacy banner that implement the scraping logic

#### Comparison with Selenium

| Feature | Current Approach (curl_cffi) | Selenium |
|---------|------------------------------|----------|
| **Speed** | 3-10x faster for most scenarios | Slower due to browser overhead and rendering time |
| **Resource Usage** | Lightweight (10-50MB memory per session) | Heavy (200-300MB+ per browser instance) |
| **Concurrent Requests** | Simple AsyncIO implementation (can easily handle 50+ concurrent requests) | Requires complex thread/process pools with higher overhead |
| **JavaScript Support** | Limited (static content and basic JS-rendered content) | Full JavaScript execution engine |
| **Bot Detection Evasion** | Browser fingerprint impersonation with customizable headers | Full browser environment (harder to detect but more resource-intensive) |
| **Setup Complexity** | Minimal dependencies (pip install curl_cffi beautifulsoup4) | Requires browser drivers, ChromeDriver/GeckoDriver configuration, and regular updates |
| **Maintenance** | Lower maintenance overhead with fewer dependencies | Higher maintenance (browser/driver version compatibility issues) |
| **Error Handling** | Clean async/await patterns with exception handling | More complex error states due to browser behavior |
| **Headless Operation** | Native headless operation with minimal footprint | Requires explicit headless configuration |
| **Development Time** | Faster implementation with clear patterns | More boilerplate code for browser setup and management |
| **Best For** | Static websites, JSON APIs, moderate anti-bot sites | SPAs, heavy JavaScript apps, complex interactions, user simulation |

The current approach is ideal for this project because:

1. **Scalability**: Efficiently handles 25+ pharmacy banners with minimal resource usage
   - A single server can process hundreds of pharmacies in parallel with current approach
   - Selenium would require significantly more server resources for the same throughput

2. **Performance**: Most pharmacy websites use relatively simple HTML structures or JSON APIs
   - Example performance for complete data collection:
     - curl_cffi: ~2-3 minutes for 300+ pharmacy locations
     - Selenium equivalent: ~15-20 minutes for same workload

3. **Architecture Benefits**: The modular design makes adding new pharmacy handlers straightforward
   - Each handler is isolated and can be customized for specific site behaviors
   - Common patterns are abstracted in the base class

4. **Resource Efficiency**: Memory and CPU demands remain low even when scaling to many requests
   - Multiple instances can run on standard hardware without performance degradation
   - Enables deployment in resource-constrained environments

5. **Resilience**: AsyncIO error handling provides better recovery from temporary failures
   - Built-in retry mechanisms for transient network issues
   - Faster failure detection without browser timeout overhead

For highly interactive sites with complex JavaScript rendering, the codebase can still incorporate Selenium selectively while maintaining the existing architecture, providing the best of both approaches when needed.
  
### Package Management

- **UV**: Modern, high-performance Python package manager and resolver
  - Faster installation speeds compared to traditional pip
  - Precise dependency resolution
  - Lockfile generation for reproducible environments

## 🔧 Installation

### Prerequisites

- Python 3.11+
- Windows Operating System
- UV package manager (optional but recommended)

### Setup Instructions

#### Easy Setup (Windows)

1. Clone the repository:

   ```bash
   git clone https://github.com/azeez-d3v/Store-Locator.git
   cd Store-Locator
   ```

2. Run the setup batch file:

   ```bash
   setup.bat
   ```

   This will:
   - Create a virtual environment (.venv folder)
   - Upgrade pip to the latest version
   - Install all required dependencies from requirements.txt

3. Run the application:

   ```bash
   run.bat
   ```

   This will:
   - Activate the virtual environment
   - Verify all required packages are installed
   - Launch the Streamlit application

> **Note**: This setup process uses Python's built-in `venv` module and is not configured for Conda environments. If you're using Conda, you'll need to manually create a Conda environment and install the required packages using `conda install` or `pip install -r requirements.txt` within your Conda environment.

#### Setup with UV (Recommended)

1. Clone the repository:

   ```bash
   git clone https://github.com/azeez-d3v/Store-Locator.git
   cd Store-Locator
   ```

2. Install UV if you don't have it already:

   ```bash
   pip install uv
   ```

3. Create a virtual environment and install dependencies:

   ```bash
   uv venv
   uv pip install -r requirements.txt
   ```

   Or install directly from pyproject.toml:

   ```bash
   uv venv
   uv pip sync
   ```

4. Activate the environment and run the application:

   ```bash
   .venv\Scripts\activate
   streamlit run app.py
   ```

#### Manual Setup (Alternative)

1. Clone the repository:

   ```bash
   git clone https://github.com/azeez-d3v/Store-Locator.git
   cd Store-Locator
   ```

2. Create and activate a virtual environment:

   ```bash
   python -m venv .venv
   .venv\Scripts\activate
   ```

3. Install the required packages:

   ```bash
   pip install -r requirements.txt
   ```

4. Run the application:

   ```bash
   streamlit run app.py
   ```

## 📊 Usage Guide

### Fetching Pharmacy Data

1. Navigate to the "Data Fetching" tab
2. Select one or more pharmacy banners using the checkboxes
3. Click "Fetch Data" and wait for the process to complete
4. The system will display a success message with the number of locations fetched

### Analyzing Data

1. Go to the "Data Analysis" tab
2. Select a pharmacy banner dataset from the dropdown menu
3. Explore the various analysis tabs:
   - **Data Overview**: View basic statistics and a complete dataset table
   - **Trading Hours**: Analyze opening hours and weekly schedule patterns
   - **Geographic Distribution**: View state distribution and map visualization
   - **Data Completeness**: Check data quality across different fields

### Viewing Fetch History

1. Navigate to the "Fetch History" tab
2. Review past fetch operations with timestamps, banner names, record counts, and success status

## 📁 Project Structure

```python
app.py                  # Main Streamlit application
requirements.txt        # Python dependencies
run.bat                 # Script to run the application
setup.bat               # Script to set up the environment
logs/
  app_logs.json         # Log file tracking fetch operations
output/
  *_pharmacies.csv      # Fetched pharmacy data files (AU and NZ)
services/
  pharmacy.py           # Main pharmacy handler
  session_manager.py    # HTTP session management utilities
  pharmacy/
    __init__.py         
    base_handler.py     # Base class for pharmacy handlers
    core.py             # Core functionality for pharmacy data fetching
    utils.py            # Utility functions for data processing
    banners/
      __init__.py
      alive.py          # Individual banner implementations
      amcal.py
      # ... other AU banner implementations
      nz/
        # New Zealand banner implementations
```

## 📝 Technical Details

### Components

- **Streamlit**: Powers the web interface and visualizations
- **Pandas**: Handles data manipulation and analysis
- **Plotly**: Creates interactive visualizations and maps
- **BeautifulSoup**: Parses HTML content from pharmacy websites
- **curl_cffi**: Performs HTTP requests with browser fingerprinting capabilities
- **Asyncio**: Enables asynchronous data fetching
- **Python 3.11+**: Modern Python features for better code organization

### Dependencies

All dependencies are specified in both `requirements.txt` and `pyproject.toml` files:

- `streamlit`: Web application framework
- `pandas`: Data analysis and manipulation
- `plotly`: Interactive visualizations
- `beautifulsoup4`: HTML parsing
- `curl_cffi`: HTTP client library
- `lxml`: XML/HTML parsing
- `openpyxl`: Read/Write Excel Files

### Data Model

Each pharmacy record typically contains:

- Name and contact information
- Geographic coordinates and address
- Working hours (when available)
- Email (when available)

## 🧩 Adding a New Pharmacy Banner

This section provides a comprehensive guide on how to add support for a new pharmacy banner to the system.

### Overview of Pharmacy Handler Architecture

Every pharmacy banner has its own handler class that inherits from `BasePharmacyHandler`. These handlers are responsible for:

1. Fetching basic location data for all pharmacies in the banner
2. Retrieving detailed information for each pharmacy location
3. Standardizing the data into a consistent format for storage and analysis

### Step 1: Create a New Handler Class

Create a new Python file in the appropriate directory:

- For Australian pharmacies: `services/pharmacy/brands/your_banner_name.py`
- For New Zealand pharmacies: `services/pharmacy/brands/nz/your_banner_name.py`

Example structure:

```python
from datetime import datetime
import re
import logging
from bs4 import BeautifulSoup

from ..base_handler import BasePharmacyHandler

class YourBannerHandler(BasePharmacyHandler):
    """Handler for Your Banner Pharmacy stores"""
    
    def __init__(self, pharmacy_locations):
        super().__init__(pharmacy_locations)
        self.banner_name = "your_banner"
        self.base_url = "https://www.yourbanner.com.au/stores"  # Main URL for store locations
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
        }
        self.logger = logging.getLogger(__name__)
```

### Step 2: Implement Required Methods

Every handler must implement these four essential methods:

#### 2.1 `fetch_locations()`

This method retrieves a list of basic pharmacy locations. Depending on the source website, this might use an API or scrape HTML.

**API Example:**

```python
async def fetch_locations(self):
    """
    Fetch all locations for this pharmacy banner
    
    Returns:
        List of locations with basic information
    """
    try:
        # Make request to the API endpoint
        response = await self.session_manager.get(
            url="https://api.yourbanner.com.au/stores",
            headers=self.headers
        )
        
        if response.status_code != 200:
            self.logger.error(f"Failed to fetch locations: HTTP {response.status_code}")
            return []
        
        # Parse the JSON response
        json_data = response.json()
        stores = json_data.get('stores', [])
        
        # Process each store into our standard format
        all_locations = []
        for i, store in enumerate(stores):
            try:
                location = {
                    'id': store.get('id', str(i)),
                    'name': store.get('name', ''),
                    'url': store.get('url', ''),
                    'banner': 'Your Banner'
                }
                all_locations.append(location)
            except Exception as e:
                self.logger.warning(f"Error creating location item {i}: {str(e)}")
        
        self.logger.info(f"Found {len(all_locations)} locations")
        return all_locations
    except Exception as e:
        self.logger.error(f"Exception when fetching locations: {str(e)}")
        return []
```

**HTML Scraping Example:**

```python
async def fetch_locations(self):
    """
    Fetch all locations by scraping the store locator page
    
    Returns:
        List of locations
    """
    try:
        # Make request to the store locator page
        response = await self.session_manager.get(
            url=self.base_url,
            headers=self.headers
        )
        
        if response.status_code != 200:
            self.logger.error(f"Failed to fetch locations: HTTP {response.status_code}")
            return []
            
        # Parse HTML content
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find store elements (adjust selectors based on the website's structure)
        store_elements = soup.select('div.store-card')
        
        all_locations = []
        for i, element in enumerate(store_elements):
            try:
                # Extract store information from the HTML
                store_name = element.select_one('h3.store-name').text.strip()
                store_url = element.select_one('a.store-link')['href']
                store_id = str(i + 1)
                
                location = {
                    'id': store_id,
                    'name': store_name,
                    'url': store_url if store_url.startswith('http') else f"https://www.yourbanner.com.au{store_url}",
                    'banner': 'Your Banner'
                }
                all_locations.append(location)
            except Exception as e:
                self.logger.warning(f"Error creating location item {i}: {str(e)}")
        
        self.logger.info(f"Found {len(all_locations)} locations")
        return all_locations
    except Exception as e:
        self.logger.error(f"Exception when fetching locations: {str(e)}")
        return []
```

#### 2.2 `fetch_pharmacy_details(self, location)`

This method fetches detailed information for a specific pharmacy location.

```python
async def fetch_pharmacy_details(self, location):
    """
    Get details for a specific pharmacy location
    
    Args:
        location: Dict containing basic pharmacy location info
        
    Returns:
        Complete pharmacy details
    """
    try:
        # Get the store URL from the location data
        store_url = location.get('url', '')
        if not store_url:
            self.logger.error(f"No URL found for location {location.get('id', '')}")
            return {}
        
        # Make request to the store page
        response = await self.session_manager.get(
            url=store_url,
            headers=self.headers
        )
        
        if response.status_code != 200:
            self.logger.error(f"Failed to fetch details: HTTP {response.status_code}")
            return {}
        
        # Parse the HTML content using BeautifulSoup
        try:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract detailed store information (customize based on website structure)
            store_details = self._extract_store_details(soup, location)
            
            self.logger.info(f"Extracted details for {location.get('name', '')}")
            
            return store_details
        except Exception as e:
            self.logger.error(f"HTML parsing error: {str(e)}")
            return {}
    except Exception as e:
        self.logger.error(f"Exception when fetching details: {str(e)}")
        return {}
        
def _extract_store_details(self, soup, location):
    """
    Extract all store details from the pharmacy page
    
    Args:
        soup: BeautifulSoup object of the store page
        location: Basic location information
        
    Returns:
        Dictionary with complete pharmacy details
    """
    try:
        # Extract store information from HTML
        store_id = location.get('id', '')
        store_name = location.get('name', '')
        store_url = location.get('url', '')
        
        # Initialize variables
        address = ""
        phone = ""
        email = ""
        trading_hours = {}
        latitude = None
        longitude = None
        
        # Look for contact information section (customize selectors)
        contact_section = soup.select_one('div.contact-info')
        if contact_section:
            # Extract address
            address_element = contact_section.select_one('p.address')
            if address_element:
                address = address_element.text.strip()
                
            # Extract phone
            phone_element = contact_section.select_one('p.phone')
            if phone_element:
                phone = phone_element.text.strip()
                
            # Extract email
            email_element = contact_section.select_one('a[href^="mailto:"]')
            if email_element:
                email = email_element.text.strip()
        
        # Look for trading hours section
        hours_section = soup.select_one('div.trading-hours')
        if hours_section:
            # Extract day and hours information
            hour_items = hours_section.select('li.hours-item')
            for item in hour_items:
                day_hours_text = item.text.strip()
                # Parse day and hours (format: "Monday: 8am to 6pm")
                day_hours_match = re.match(r'([^:]+):\s*(.*)', day_hours_text)
                if day_hours_match:
                    day = day_hours_match.group(1).strip()
                    hours_value = day_hours_match.group(2).strip()
                    
                    # Handle closed days
                    if hours_value.lower() == 'closed':
                        trading_hours[day] = {'open': 'Closed', 'close': 'Closed'}
                    else:
                        # Parse time ranges like "8am to 6pm"
                        time_parts = hours_value.split(' to ')
                        if len(time_parts) == 2:
                            trading_hours[day] = {
                                'open': time_parts[0].strip(),
                                'close': time_parts[1].strip()
                            }
        
        # Look for map coordinates (often in a script tag or data attributes)
        map_element = soup.select_one('div[data-lat][data-lng]')
        if map_element:
            latitude = map_element.get('data-lat')
            longitude = map_element.get('data-lng')
        
        # Parse address into components
        address_components = self._parse_address(address)
        
        # Create the final pharmacy details object
        result = {
            'banner': 'Your Banner',
            'name': store_name,
            'store_id': store_id,
            'address': address,
            'street_address': address_components.get('street', ''),
            'suburb': address_components.get('suburb', ''),
            'state': address_components.get('state', ''),
            'postcode': address_components.get('postcode', ''),
            'phone': phone,
            'email': email,
            'website': store_url,
            'trading_hours': trading_hours,
            'latitude': latitude,
            'longitude': longitude,
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Remove any None values
        return {k: v for k, v in result.items() if v is not None}
    except Exception as e:
        self.logger.error(f"Error extracting store details: {str(e)}")
        return {
            'banner': 'Your Banner',
            'name': store_name,
            'store_id': store_id,
            'website': store_url,
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
```

#### 2.3 `fetch_all_locations_details()`

This method fetches details for all pharmacy locations, typically by calling `fetch_pharmacy_details()` for each location.

```python
async def fetch_all_locations_details(self):
    """
    Fetch details for all pharmacy locations
    
    Returns:
        List of dictionaries containing pharmacy details
    """
    self.logger.info("Fetching all pharmacy locations...")
    
    try:
        # First get all basic location data
        locations = await self.fetch_locations()
        if not locations:
            return []
        
        # Initialize the list for storing complete pharmacy details
        all_details = []
        
        # Option 1: Sequential processing (simpler but slower)
        for i, location in enumerate(locations):
            try:
                self.logger.info(f"Processing details for location {i+1}/{len(locations)}: {location.get('name', '')}")
                store_details = await self.fetch_pharmacy_details(location)
                if store_details:
                    all_details.append(store_details)
            except Exception as e:
                self.logger.warning(f"Error processing location {i}: {str(e)}")
        
        # Option 2: Concurrent processing (faster)
        # Uncomment this code and comment out Option 1 for concurrent processing
        '''
        import asyncio
        
        # Create a semaphore to limit concurrent connections
        semaphore = asyncio.Semaphore(5)  # Adjust based on website limitations
        
        async def fetch_with_semaphore(location):
            """Helper function to fetch details with semaphore control"""
            async with semaphore:
                try:
                    return await self.fetch_pharmacy_details(location)
                except Exception as e:
                    self.logger.warning(f"Error fetching details for {location.get('name')}: {e}")
                    return None
        
        # Create tasks for all locations
        tasks = [fetch_with_semaphore(location) for location in locations]
        
        # Process results as they complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out any None results or exceptions
        all_details = [
            result for result in results 
            if result and not isinstance(result, Exception)
        ]
        '''
        
        self.logger.info(f"Successfully processed {len(all_details)} locations")
        return all_details
    except Exception as e:
        self.logger.error(f"Exception when fetching all locations: {str(e)}")
        return []
```

#### 2.4 `extract_pharmacy_details(self, pharmacy_data)`

This method standardizes pharmacy data into a consistent format.

```python
def extract_pharmacy_details(self, pharmacy_data):
    """
    Extract specific fields from pharmacy data
    
    Args:
        pharmacy_data: Dictionary containing raw pharmacy data
        
    Returns:
        Standardized pharmacy details dictionary
    """
    if not pharmacy_data:
        return {}
        
    # For some pharmacies, data is already in the right format from _extract_store_details
    # Just return it as is, or perform any additional standardization
    return pharmacy_data
```

### Step 3: Helper Methods

Add helper methods for parsing addresses, trading hours, etc.:

```python
def _parse_address(self, address):
    """
    Parse an address string into components
    
    Args:
        address: The full address string
        
    Returns:
        Dictionary with address components (street, suburb, state, postcode)
    """
    if not address:
        return {'street': '', 'suburb': '', 'state': '', 'postcode': ''}
    
    # Normalize the address
    normalized_address = address.strip().replace('\n', ', ')
    
    # Default result
    result = {'street': '', 'suburb': '', 'state': '', 'postcode': ''}
    
    # State mapping
    state_mapping = {
        'NEW SOUTH WALES': 'NSW',
        'VICTORIA': 'VIC',
        'QUEENSLAND': 'QLD',
        'SOUTH AUSTRALIA': 'SA',
        'WESTERN AUSTRALIA': 'WA',
        'TASMANIA': 'TAS',
        'NORTHERN TERRITORY': 'NT',
        'AUSTRALIAN CAPITAL TERRITORY': 'ACT',
        'NSW': 'NSW',
        'VIC': 'VIC',
        'QLD': 'QLD',
        'SA': 'SA',
        'WA': 'WA',
        'TAS': 'TAS',
        'NT': 'NT',
        'ACT': 'ACT'
    }
    
    # Pattern to match addresses in format: street, suburb, state, postcode
    pattern = r'(.*?),\s*([^,]+?),\s*([^,]+?),\s*(\d{4})$'
    match = re.search(pattern, normalized_address)
    
    if match:
        result['street'] = match.group(1).strip()
        result['suburb'] = match.group(2).strip()
        result['state'] = match.group(3).strip()
        result['postcode'] = match.group(4).strip()
        
        # Normalize state
        for state_name, abbr in state_mapping.items():
            if result['state'].upper() == state_name:
                result['state'] = abbr
                break
    else:
        # Try to extract postcode (4 digits at the end of the string)
        postcode_match = re.search(r'(\d{4})$', normalized_address)
        if postcode_match:
            result['postcode'] = postcode_match.group(1)
            
            # Try to infer state from postcode
            try:
                postcode_num = int(result['postcode'])
                if 1000 <= postcode_num <= 2999:
                    result['state'] = 'NSW'
                elif 3000 <= postcode_num <= 3999:
                    result['state'] = 'VIC'
                elif 4000 <= postcode_num <= 4999:
                    result['state'] = 'QLD'
                elif 5000 <= postcode_num <= 5999:
                    result['state'] = 'SA'
                elif 6000 <= postcode_num <= 6999:
                    result['state'] = 'WA'
                elif 7000 <= postcode_num <= 7999:
                    result['state'] = 'TAS'
                elif 800 <= postcode_num <= 999:
                    result['state'] = 'NT'
                elif 2600 <= postcode_num <= 2618 or 2900 <= postcode_num <= 2920:
                    result['state'] = 'ACT'
            except (ValueError, TypeError):
                pass
    
    return result

def _format_phone(self, phone):
    """Format phone number consistently"""
    if not phone:
        return ""
    
    # Remove non-numeric characters except + for country code
    digits = ''.join(c for c in phone if c.isdigit() or c == '+')
    
    # Format Australian phone numbers
    if digits.startswith('61') or digits.startswith('+61'):
        # Format as +61 X XXXX XXXX
        if digits.startswith('+'):
            digits = digits[1:]
        
        if len(digits) == 11 and digits.startswith('61'):
            formatted = f"+{digits[0:2]} {digits[2]} {digits[3:7]} {digits[7:]}"
            return formatted
    
    # Return original if not matching standard format
    return phone
```

### Step 4: Register Your Handler in the System

Add your handler to the core.py file:

1. Add the import at the top of `services/pharmacy/core.py`:

```python
from services.pharmacy.banners import your_banner
```

2. Add your handler to the banner_handlers dictionary in the `__init__` method:

```python
self.banner_handlers = {
    # existing handlers...
    "your_banner": your_banner.YourBannerHandler(self),
}
```

3. Add the URL to the constants section if needed:

```python
YOUR_BRAND_URL = "https://www.yourbanner.com.au/stores"
```

### Step 5: Test Your Implementation

1. Run the application:

```bash
streamlit run app.py
```

2. Navigate to the "Data Fetching" tab
3. Select your new pharmacy banner
4. Click "Fetch Data" and verify the data is correctly retrieved and processed

### Tips for Different Pharmacy Website Types

#### 1. API-Based Websites

If the pharmacy website uses an API:

- Use browser developer tools (F12) to identify API endpoints
- Look for XHR/Fetch requests when navigating the store locator
- Examine request headers and parameters
- Use the `session_manager.get()` or `session_manager.post()` methods to make API calls

#### 2. HTML-Based Websites

If the pharmacy website requires HTML scraping:

- Use BeautifulSoup to parse HTML content
- Identify key HTML elements using browser developer tools
- Create helper methods for parsing complex structures
- Be defensive with your selectors (use try/except blocks)

#### 3. JavaScript-Heavy Websites

For websites that load data via JavaScript:

- Look for data embedded in the HTML (often in script tags or data attributes)
- Check for JSON data in the page source
- If data is only loaded via AJAX, find and use those endpoints

### Common Challenges and Solutions

1. **Rate Limiting**: Use semaphores to limit concurrent requests
2. **Dynamic Content**: Look for API endpoints or embedded data
3. **Varied Address Formats**: Create robust address parsing logic
4. **Inconsistent Hours Format**: Add custom parsing for each format
5. **CAPTCHA/Bot Protection**: Add appropriate headers and adjust request patterns

By following this guide, you should be able to add support for most pharmacy banners. Remember to respect each website's terms of service and avoid excessive requests that might impact their servers.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

© 2025 Pharmacy Store Locator Analytics Dashboard
