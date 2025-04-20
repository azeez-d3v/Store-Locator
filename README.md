# 🏥 Pharmacy Store Locator Analytics Dashboard

A comprehensive tool for fetching, analyzing, and visualizing pharmacy location data across multiple pharmacy brands in Australia and New Zealand.

## 📋 Overview

The Pharmacy Store Locator is a Streamlit-based web application that allows users to:

1. Fetch pharmacy location data from multiple pharmacy brands
2. Analyze geographic distribution, trading hours, and service offerings
3. Compare data across different pharmacy chains
4. Visualize pharmacy data through interactive maps, charts, and statistics

## 🚀 Features

### Data Collection

- **Multi-brand support**: Fetches data from 25+ pharmacy brands:
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
    - Healthy Pharmacy
    - Healthy World Pharmacy
    - Pennas Pharmacy
    - Wizard Pharmacy
    - Chemist Hub
    - SuperChem
    - Complete Care Pharmacy
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
- **Brand comparison**: Compare metrics across different pharmacy chains

### Advanced Features

- **Service offering analysis**: Compare available services across pharmacy brands
- **Robust error handling**: Gracefully handles various data formats and missing information
- **Responsive UI**: Well-organized tabbed interface with meaningful visualizations

## 🔧 Installation

### Prerequisites

- Python 3.9+
- Windows Operating System

### Setup Instructions

#### Easy Setup (Windows)

1. Clone the repository:

   ```bash
   git clone https://github.com/azeez-d3v/Store-Locator.git
   cd Store-Locator
   ```

2. Run the setup batch file:

   ```
   setup.bat
   ```

   This will:
   - Create a virtual environment (.venv folder)
   - Upgrade pip to the latest version
   - Install all required dependencies from requirements.txt

3. Run the application:

   ```
   run.bat
   ```

   This will:
   - Activate the virtual environment
   - Verify all required packages are installed
   - Launch the Streamlit application

> **Note**: This setup process uses Python's built-in `venv` module and is not configured for Conda environments. If you're using Conda, you'll need to manually create a Conda environment and install the required packages using `conda install` or `pip install -r requirements.txt` within your Conda environment.

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
2. Select one or more pharmacy brands using the checkboxes
3. Click "Fetch Data" and wait for the process to complete
4. The system will display a success message with the number of locations fetched

### Analyzing Data

1. Go to the "Data Analysis" tab
2. Select a pharmacy brand dataset from the dropdown menu
3. Explore the various analysis tabs:
   - **Data Overview**: View basic statistics and a complete dataset table
   - **Trading Hours**: Analyze opening hours and weekly schedule patterns
   - **Geographic Distribution**: View state distribution and map visualization
   - **Data Completeness**: Check data quality across different fields
   - **Advanced Analysis**: Compare data across multiple pharmacy brands

### Viewing Fetch History

1. Navigate to the "Fetch History" tab
2. Review past fetch operations with timestamps, brand names, record counts, and success status

## 📁 Project Structure

```
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
    brands/
      __init__.py
      alive.py          # Individual brand implementations
      amcal.py
      # ... other AU brand implementations
      nz/
        # New Zealand brand implementations
```

## 📝 Technical Details

### Components

- **Streamlit**: Powers the web interface and visualizations
- **Pandas**: Handles data manipulation and analysis
- **Plotly**: Creates interactive visualizations and maps
- **BeautifulSoup**: Parses HTML content from pharmacy websites
- **Asyncio**: Enables asynchronous data fetching
- **Python 3.9+**: Modern Python features for better code organization

### Data Model

Each pharmacy record typically contains:

- Name and contact information
- Geographic coordinates and address
- Trading hours (when available)
- Available services
- Website and email (when available)

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

© 2025 Pharmacy Store Locator Analytics Dashboard
