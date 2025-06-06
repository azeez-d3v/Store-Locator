from pathlib import Path
from datetime import datetime
import os
import sys
import json
import asyncio
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.pharmacy import PharmacyLocations

# Set page configuration
st.set_page_config(
    page_title="Pharmacy Store Locator Analytics",
    page_icon="💊",
    layout="centered",
)

# Initialize directories
OUTPUT_DIR = Path("output")
LOGS_DIR = Path("logs")
OUTPUT_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# Initialize log file
LOG_FILE = LOGS_DIR / "app_logs.json"
if not LOG_FILE.exists():
    with open(LOG_FILE, "w") as f:
        json.dump({"fetch_history": []}, f)

def load_logs():
    """Load the logs from the JSON file"""
    try:
        with open(LOG_FILE, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return {"fetch_history": []}

def save_logs(logs):
    """Save the logs to the JSON file"""
    with open(LOG_FILE, "w") as f:
        json.dump(logs, f, indent=2)

def add_fetch_log(brand, count, success):
    """Add a fetch log entry"""
    logs = load_logs()
    logs["fetch_history"].append({
        "timestamp": datetime.now().isoformat(),
        "brand": brand,
        "count": count,
        "success": success
    })
    save_logs(logs)

def get_data_files():
    """Get a list of all data files (Excel and CSV) in the output directory"""
    excel_files = [f for f in OUTPUT_DIR.glob("*.xlsx") if f.is_file()]
    csv_files = [f for f in OUTPUT_DIR.glob("*.csv") if f.is_file()]
    return excel_files + csv_files

def load_data(file_path):
    """Load data from a file (Excel or CSV)"""
    if str(file_path).endswith('.xlsx'):
        return pd.read_excel(file_path, sheet_name="pharmacy_details")
    else:
        return pd.read_csv(file_path)

# Helper function to safely check if a column exists
def safe_column_check(df, column_name):
    return column_name in df.columns

# Main content
st.title("🏥 Pharmacy Analytics Dashboard")
# Create main tabs
tab_fetch, tab_analyze, tab_history = st.tabs(["Data Fetching", "Data Analysis", "Fetch History"])

# Tab 1: Data Fetching
with tab_fetch:
    st.header("Pharmacy Store Locator")
    st.image("https://img.icons8.com/color/96/000000/pharmacy-shop.png", width=100)
    
    # Define all available pharmacy brands by country
    au_pharmacy_brands = {
        "dds": "Discount Drug Stores",
        "amcal": "Amcal",
        "blooms": "Blooms The Chemist",
        "ramsay": "Ramsay Pharmacy",
        "revive": "Revive Pharmacy",
        "optimal": "Optimal Pharmacy Plus",
        "community": "Community Care Chemist",
        "footes": "Footes Pharmacy",
        "alive": "Alive Pharmacy",
        "ydc": "Your Discount Chemist",
        "chemist_warehouse": "Chemist Warehouse",
        "pharmasave": "Pharmasave",
        "nova": "Nova Pharmacy",
        "choice": "Choice Pharmacy",
        "bendigo_ufs": "Bendigo UFS",
        "chemist_king": "Chemist King",
        "friendly_care": "FriendlyCare Pharmacy",
        "fullife": "Fullife Pharmacy",
        "good_price": "Good Price Pharmacy",
        "healthy_life_pharmacy": "Healthy Life Pharmacy",
        "healthy_world": "Healthy World Pharmacy",
        "pennas": "Pennas Pharmacy",
        "wizard": "Wizard Pharmacy",
        "chemist_hub": "Chemist Hub",
        "superchem": "SuperChem Pharmacy",
        "complete_care": "Complete Care Pharmacy",
        "terry_white": "TerryWhite Chemmart",
        "my_chemist": "My Chemist",
        "direct_chemist": "Direct Chemist Outlet",
        "priceline": "Priceline Pharmacy",
        "advantage": "Advantage Pharmacy",
        "alliance": "Alliance Pharmacy",
        "capital_chemist": "Capital Chemist",
        "caremore": "Caremore Pharmacy",
        "chemist_discount_centre": "Chemist Discount Centre",
        "chemist_works": "Chemist Works",
        "chemsave": "Chemsave Pharmacy",
        "greenleaf": "Greenleaf Pharmacy",
        "healthsave": "Healthsave Pharmacy",
        "jadin": "Jadin Chemist Group",
        "livelife": "Livelife Pharmacy",
        "pharmacist_advice": "Pharmacist Advice Pharmacy",
        "pharmacy4less": "Pharmacy 4 Less",
        "pharmacy_co": "Pharmacy & Co",
        "soul_pattinson": "Soul Pattinson Chemist",
        "star_discount": "Star Discount Chemist",
        "ufs_dispensaries": "UFS Dispensaries",
        "united_chemist": "United Chemist",
        "pharmacy777": "Pharmacy 777",
        "pharmacy_select": "Pharmacy Select",
        "quality_pharmacy": "Quality Pharmacy",
        "vitality_pharmacy": "Vitality Pharmacy",
        "wholelife_pharmacy": "Wholelife Pharmacy & Healthfoods",
    }

    nz_pharmacy_brands = {
        "chemist_warehouse_nz": "Chemist Warehouse NZ",
        "antidote_nz": "Antidote Pharmacy NZ",
        "unichem_nz": "Unichem NZ",
        "bargain_chemist_nz": "Bargain Chemist NZ",
        "woolworths_nz": "Woolworths Pharmacy NZ",
        "life_pharmacy_nz": "Life Pharmacy NZ"
    }

    # Combine all brands for backward compatibility
    all_pharmacy_brands = {**au_pharmacy_brands, **nz_pharmacy_brands}
    
    # Initialize session state variables
    if "selected_pharmacies" not in st.session_state:
        st.session_state["selected_pharmacies"] = []
    if "selected_au_pharmacies" not in st.session_state:
        st.session_state["selected_au_pharmacies"] = []
    if "selected_nz_pharmacies" not in st.session_state:
        st.session_state["selected_nz_pharmacies"] = []
    
    # Create callback functions to update session state
    def on_au_selection_change():
        # Update main selection list with both AU and NZ selections
        nz_selections = st.session_state.get("selected_nz_pharmacies", [])
        st.session_state["selected_pharmacies"] = st.session_state["selected_au_pharmacies"] + nz_selections
    
    def on_nz_selection_change():
        # Update main selection list with both AU and NZ selections
        au_selections = st.session_state.get("selected_au_pharmacies", [])
        st.session_state["selected_pharmacies"] = au_selections + st.session_state["selected_nz_pharmacies"]
    
    # Create AU and NZ tabs for pharmacy selection
    au_tab, nz_tab = st.tabs(["Australia (AU)", "New Zealand (NZ)"])
    
    # Tab for Australian pharmacies
    with au_tab:
        st.subheader("Australian Pharmacies")
        
        # Add Select All and Clear Selection buttons in a row
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Select All AU", key="select_all_au"):
                # Update session state to include all AU brands
                au_brands = list(au_pharmacy_brands.keys())
                # Update both the main selected_pharmacies and the specific AU selection
                st.session_state["selected_au_pharmacies"] = au_brands
                # Preserve any NZ selections
                nz_selections = [b for b in st.session_state["selected_pharmacies"] if b in nz_pharmacy_brands]
                st.session_state["selected_pharmacies"] = au_brands + nz_selections
                st.rerun()
        
        with col2:
            if st.button("Clear AU Selection", key="clear_au_selection"):
                # Remove only AU brands from session state
                st.session_state["selected_au_pharmacies"] = []
                nz_selections = [b for b in st.session_state["selected_pharmacies"] if b in nz_pharmacy_brands]
                st.session_state["selected_pharmacies"] = nz_selections
                st.rerun()
        
        # Get current AU selections
        current_au_selections = [b for b in st.session_state["selected_pharmacies"] if b in au_pharmacy_brands]
        
        # Use multiselect for AU pharmacy selection
        selected_au_pharmacies = st.multiselect(
            "Choose Australian pharmacy banners",
            options=list(au_pharmacy_brands.keys()),
            format_func=lambda x: au_pharmacy_brands[x],
            default=current_au_selections,
            key="selected_au_pharmacies",
            on_change=on_au_selection_change
        )
        
        # Update session state with combined AU and NZ selections
        nz_selections = [b for b in st.session_state["selected_pharmacies"] if b in nz_pharmacy_brands]
        st.session_state["selected_pharmacies"] = selected_au_pharmacies + nz_selections
    
    # Tab for New Zealand pharmacies
    with nz_tab:
        st.subheader("New Zealand Pharmacies")
        
        # Add Select All and Clear Selection buttons in a row
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Select All NZ", key="select_all_nz"):
                # Update session state to include all NZ brands
                nz_brands = list(nz_pharmacy_brands.keys())
                # Update both the main selected_pharmacies and the specific NZ selection
                st.session_state["selected_nz_pharmacies"] = nz_brands
                # Preserve any AU selections
                au_selections = [b for b in st.session_state["selected_pharmacies"] if b in au_pharmacy_brands]
                st.session_state["selected_pharmacies"] = au_selections + nz_brands
                st.rerun()
        
        with col2:
            if st.button("Clear NZ Selection", key="clear_nz_selection"):
                # Remove only NZ brands from session state
                st.session_state["selected_nz_pharmacies"] = []
                au_selections = [b for b in st.session_state["selected_pharmacies"] if b in au_pharmacy_brands]
                st.session_state["selected_pharmacies"] = au_selections
                st.rerun()
        
        # Get current NZ selections
        current_nz_selections = [b for b in st.session_state["selected_pharmacies"] if b in nz_pharmacy_brands]
        
        # Use multiselect for NZ pharmacy selection
        selected_nz_pharmacies = st.multiselect(
            "Choose New Zealand pharmacy banners",
            options=list(nz_pharmacy_brands.keys()),
            format_func=lambda x: nz_pharmacy_brands[x],
            default=current_nz_selections,
            key="selected_nz_pharmacies",
            on_change=on_nz_selection_change
        )
        
        # Update session state with combined AU and NZ selections
        au_selections = [b for b in st.session_state["selected_pharmacies"] if b in au_pharmacy_brands]
        st.session_state["selected_pharmacies"] = au_selections + selected_nz_pharmacies
    
    # Display current selection summary
    if st.session_state["selected_pharmacies"]:
        au_count = len([b for b in st.session_state["selected_pharmacies"] if b in au_pharmacy_brands])
        nz_count = len([b for b in st.session_state["selected_pharmacies"] if b in nz_pharmacy_brands])
        
        st.info(f"Current selection: {len(st.session_state['selected_pharmacies'])} total pharmacy brands " +
               f"({au_count} from Australia, {nz_count} from New Zealand)")
    
    # Fetch data button
    if st.button("Fetch Data", key="fetch_button"):
        with st.spinner("Fetching data..."):
            try:
                # Run the fetch operation asynchronously
                pharmacy_api = PharmacyLocations()
                
                async def fetch_data():
                    # Always use session_state directly to ensure latest selection
                    selected_brands = st.session_state["selected_pharmacies"]
                      # Get current month and year for filename
                    current_date = datetime.now()
                    month_name = current_date.strftime('%b').capitalize()
                    month_year = f"{month_name}_{current_date.year}"
                    
                    if len(selected_brands) > 1:
                        # Fetch multiple brands
                        results = await pharmacy_api.fetch_and_save_all(selected_brands, month_year)
                        
                        # Add fetch log entries for each brand
                        for brand, result in results["details"].items():
                            # Add log entry for this brand
                            add_fetch_log(
                                brand, 
                                result.get("locations", 0), 
                                result.get("status") == "success"
                            )
                        
                        # st.success(f"Successfully fetched data for {', '.join(b.upper() for b in selected_brands)}!")
                        brand_badges = " ".join([f":blue-badge[:material/home_app_logo: {brand.upper()}]" for brand in selected_brands])
                        st.markdown(brand_badges)
                        st.badge("fetch success", icon=":material/check:", color="green")
                    elif len(selected_brands) == 1:
                        # Fetch a single brand
                        brand = selected_brands[0]
                        details = await pharmacy_api.fetch_all_locations_details(brand)
                        if details:                            # Use month_year in the filename
                            brand_capitalized = brand.title()
                            pharmacy_api.save_to_excel(details, f"{brand_capitalized}_{month_year}.xlsx")
                            add_fetch_log(brand, len(details), True)
                            st.badge(f"{brand.upper()}", icon=":material/home_app_logo:", color="blue")
                            st.badge(f"{len(details)} location fetched", icon=":material/trail_length:", color="green")
                        else:
                            add_fetch_log(brand, 0, False)
                            st.error(f"No data found for {brand.upper()}")
                    else:
                        st.warning("Please select at least one brand to fetch")
                        
                # Run the async function
                if st.session_state["selected_pharmacies"]:
                    asyncio.run(fetch_data())
                else:
                    st.warning("Please select at least one brand to fetch")
            except Exception as e:
                for brand in st.session_state["selected_pharmacies"]:
                    add_fetch_log(brand, 0, False)
                st.error(f"Error fetching data: {str(e)}")

# Tab 3: Fetch History
with tab_history:
    st.header("Fetch History")
    logs = load_logs()
    if logs["fetch_history"]:
        history_df = pd.DataFrame(logs["fetch_history"])
        history_df["timestamp"] = pd.to_datetime(history_df["timestamp"])
        history_df["timestamp"] = history_df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
        history_df.sort_values(by="timestamp", ascending=False, inplace=True)
        st.dataframe(
            history_df[["timestamp", "brand", "count", "success"]],
            hide_index=True,
            use_container_width=True
        )
    else:
        st.info("No fetch history available")

# Tab 2: Data Analysis
with tab_analyze:
    # Data selection
    csv_files = get_data_files()
    if not csv_files:
        st.warning("No data files found. Please fetch data first.")
    else:
        # Create AU/NZ tabs for file organization
        au_tab, nz_tab = st.tabs(["Australia (AU) Files", "New Zealand (NZ) Files"])
        
        # Organize files by country
        au_files = {}
        nz_files = {}
        
        # Classify files as AU or NZ based on filename or content
        for f in csv_files:
            # NZ files should have "nz" in the name
            if "_nz_" in f.stem.lower():
                nz_files[f.stem] = str(f)
            else:
                # Default to AU if not explicitly labeled as NZ
                au_files[f.stem] = str(f)
        
        # Tab for Australian pharmacy files
        with au_tab:
            if au_files:
                selected_au_file = st.selectbox("Select Australian pharmacy data file", 
                                                list(au_files.keys()),
                                                key="au_file_selector")
                
                if selected_au_file:
                    # Load the selected data
                    df = load_data(au_files[selected_au_file])
                    
                    # Display record count
                    st.header(f"📊 {selected_au_file} - {len(df)} locations")
                    
                    # Create tabs for different analyses
                    overview_tab, trading_tab, geo_tab, completeness_tab = st.tabs([
                        "Data Overview", 
                        "Trading Hours", 
                        "Geographic Distribution", 
                        "Data Completeness"
                    ])
                    
                    # Tab: Data Overview
                    with overview_tab:
                        # Reorder the dataframe to show 'Entity Name' column first
                        if safe_column_check(df, "Entity Name"):
                            cols = ["Entity Name"] + [col for col in df.columns if col != "Entity Name"]
                            display_df = df[cols]
                        else:
                            display_df = df
                        
                        st.dataframe(display_df, use_container_width=True)
                        
                        # Key metrics
                        st.subheader("Key Metrics")
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Number of Pharmacies", len(df))
                        with col2:
                            # Check for both "phone" and "Phone" columns
                            phone_column = None
                            if safe_column_check(df, "phone"):
                                phone_column = "phone"
                            elif safe_column_check(df, "Phone"):
                                phone_column = "Phone"
                                
                            if phone_column:
                                has_phone = df[phone_column].notna().sum()
                                phone_percentage = int((has_phone / len(df)) * 100 if len(df) > 0 else 0)
                                st.metric("Locations with Phone", f"{has_phone} ({phone_percentage}%)")
                            else:
                                st.metric("Locations with Phone", "N/A")
                        
                        with col3:
                            # Check for both "email" and "Email" columns
                            email_column = None
                            if safe_column_check(df, "email"):
                                email_column = "email"
                            elif safe_column_check(df, "Email"):
                                email_column = "Email"
                                
                            if email_column:
                                populated_email = df[email_column].notna().sum()
                                email_percentage = int((populated_email / len(df)) * 100 if len(df) > 0 else 0)
                                st.metric("Locations with Email", f"{populated_email} ({email_percentage}%)")
                            else:
                                st.metric("Locations with Email", "N/A")
                    
                    # Tab: Trading Hours Analysis 
                    with trading_tab:
                        if safe_column_check(df, "Working hours"):
                            st.header("Trading Hours Analysis")
                            
                            # Check if any pharmacy has trading hours data
                            has_hours = False
                            for _, row in df.iterrows():
                                try:
                                    trading_hours_data = row["Working hours"]
                                    # Handle different formats of trading_hours data
                                    if isinstance(trading_hours_data, str) and trading_hours_data and trading_hours_data.lower() not in ["nan", "{}", "null", ""]:
                                        # Try to parse the string as JSON
                                        try:
                                            parsed_data = json.loads(trading_hours_data.replace("'", "\""))
                                            if parsed_data and isinstance(parsed_data, dict):
                                                has_hours = True
                                                break
                                        except json.JSONDecodeError:
                                            # If standard JSON parsing fails, try eval for Python dict string format
                                            try:
                                                parsed_data = eval(trading_hours_data)
                                                if parsed_data and isinstance(parsed_data, dict):
                                                    has_hours = True
                                                    break
                                            except:
                                                continue
                                    elif isinstance(trading_hours_data, dict) and trading_hours_data:
                                        has_hours = True
                                        break
                                except (TypeError, AttributeError):
                                    continue
                                    
                            if has_hours:
                                # Select a pharmacy to view trading hours
                                pharmacies_with_hours = []
                                for i, row in df.iterrows():
                                    try:
                                        trading_hours_str = row["Working hours"]
                                        trading_hours_data = None
                                        
                                        # Try to parse trading_hours if it's a string
                                        if isinstance(trading_hours_str, str) and trading_hours_str and trading_hours_str.lower() not in ["nan", "{}", "null", ""]:
                                            try:
                                                # Try standard JSON parsing first
                                                trading_hours_data = json.loads(trading_hours_str.replace("'", "\""))
                                            except json.JSONDecodeError:
                                                # If that fails, try Python's eval for dict-like strings
                                                try:
                                                    trading_hours_data = eval(trading_hours_str)
                                                except:
                                                    trading_hours_data = None
                                        elif isinstance(trading_hours_str, dict):
                                            trading_hours_data = trading_hours_str
                                            
                                        if trading_hours_data and isinstance(trading_hours_data, dict):
                                            name = row["Entity Name"] if safe_column_check(df, "Entity Name") and pd.notna(row["Entity Name"]) else f"Pharmacy #{i}"
                                            pharmacies_with_hours.append((i, name))
                                    except (TypeError, AttributeError):
                                        continue
                                
                                if pharmacies_with_hours:
                                    # Create dropdown for pharmacy selection instead of checkboxes
                                    st.subheader("Select a pharmacy to view trading hours")
                                    
                                    # Create a list of pharmacy names
                                    pharmacy_names = [name for _, name in pharmacies_with_hours]
                                    
                                    # Use dropdown for pharmacy selection
                                    selected_pharmacy = st.selectbox(
                                        "Select pharmacy", 
                                        pharmacy_names,
                                        key="pharmacy_selector_au"
                                    )
                                    
                                    # Find the actual index of the selected pharmacy
                                    selected_idx = next((idx for idx, name in pharmacies_with_hours if name == selected_pharmacy), None)
                                    
                                    if selected_idx is not None:
                                        # Parse and display trading hours
                                        trading_hours_str = df.iloc[selected_idx]["Working hours"]
                                        try:
                                            trading_hours = None
                                            if isinstance(trading_hours_str, str):
                                                try:
                                                    # Try standard JSON parsing first with quote replacement
                                                    trading_hours = json.loads(trading_hours_str.replace("'", "\""))
                                                except json.JSONDecodeError:
                                                    # If that fails, try Python's eval for dict-like strings
                                                    try:
                                                        trading_hours = eval(trading_hours_str)
                                                    except:
                                                        trading_hours = None
                                            elif isinstance(trading_hours_str, dict):
                                                trading_hours = trading_hours_str
                                            
                                            if trading_hours:
                                                # Create a table for trading hours
                                                hours_data = []
                                                day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday", "Public Holiday"]
                                                
                                                # Sort days according to conventional order
                                                for day in day_order:
                                                    if day in trading_hours:
                                                        hours = trading_hours[day]
                                                        if isinstance(hours, dict):
                                                            hours_data.append({
                                                                "Day": day,
                                                                "Opens": hours.get("open", "N/A"),
                                                                "Closes": hours.get("close", hours.get("closed", "N/A"))
                                                            })
                                                
                                                # Add any days not in the standard order
                                                for day, hours in trading_hours.items():
                                                    if day not in day_order and isinstance(hours, dict):
                                                        hours_data.append({
                                                            "Day": day,
                                                            "Opens": hours.get("open", "N/A"),
                                                            "Closes": hours.get("close", hours.get("closed", "N/A"))
                                                        })
                                                        
                                                if hours_data:
                                                    hours_df = pd.DataFrame(hours_data)
                                                    st.table(hours_df)
                                                    
                                                    # Create visualization of opening hours
                                                    st.subheader("Weekly Hours Visualization")
                                                    
                                                    # Convert times to numeric format for visualization
                                                    chart_data = []
                                                    for entry in hours_data:
                                                        day = entry["Day"]
                                                        if day != "Public Holiday":  # Skip public holiday for the chart
                                                            opens = entry["Opens"]
                                                            closes = entry["Closes"]
                                                            
                                                            # Convert time strings to hours (decimal)
                                                            try:
                                                                if opens != "N/A" and closes != "N/A":
                                                                    # Try to parse AM/PM format
                                                                    open_time = pd.to_datetime(opens, format="%I:%M %p", errors="coerce")
                                                                    close_time = pd.to_datetime(closes, format="%I:%M %p", errors="coerce")
                                                                    
                                                                    if pd.isna(open_time) or pd.isna(close_time):
                                                                        # Try 24-hour format
                                                                        open_time = pd.to_datetime(opens, format="%H:%M", errors="coerce")
                                                                        close_time = pd.to_datetime(closes, format="%H:%M", errors="coerce")
                                                                    
                                                                    if not pd.isna(open_time) and not pd.isna(close_time):
                                                                        open_hour = open_time.hour + open_time.minute/60
                                                                        close_hour = close_time.hour + close_time.minute/60
                                                                        
                                                                        chart_data.append({
                                                                            "Day": day,
                                                                            "Open Hour": open_hour,
                                                                            "Close Hour": close_hour,
                                                                            "Hours Open": close_hour - open_hour
                                                                        })
                                                            except Exception:
                                                                pass
                                                        
                                                    if chart_data:
                                                        chart_df = pd.DataFrame(chart_data)
                                                        
                                                        # Create a horizontal bar chart of opening hours
                                                        fig = go.Figure()
                                                        
                                                        # Add bars representing opening hours
                                                        for i, row in chart_df.iterrows():
                                                            fig.add_trace(go.Bar(
                                                                y=[row["Day"]],
                                                                x=[row["Hours Open"]],
                                                                orientation='h',
                                                                base=[row["Open Hour"]],
                                                                name=row["Day"],
                                                                text=f"{row['Open Hour']:.2f} - {row['Close Hour']:.2f}",
                                                                hoverinfo="text"
                                                            ))
                                                        
                                                        fig.update_layout(
                                                            title="Weekly Opening Hours",
                                                            xaxis_title="Hour of Day",
                                                            yaxis_title="Day of Week",
                                                            xaxis=dict(tickmode='linear', tick0=0, dtick=2, range=[6, 24]),
                                                            height=400,
                                                            barmode='stack',
                                                            showlegend=False
                                                        )
                                                        
                                                        st.plotly_chart(fig, use_container_width=True, key="au_trading_hours_chart")
                                                else:
                                                    st.info("No structured trading hours data available for this pharmacy.")
                                            else:
                                                st.info("No trading hours data available for this pharmacy.")
                                        except (json.JSONDecodeError, TypeError):
                                            st.error("Could not parse trading hours data.")
                                else:
                                    st.info("No pharmacies with trading hours data found.")
                            else:
                                st.info("No trading hours data available in this dataset.")
                      # Tab: Geographic Distribution
                    with geo_tab:
                        # Display pharmacies on a map
                        if safe_column_check(df, "latitude") and safe_column_check(df, "longitude"):
                            # Remove rows with missing lat/long values
                            map_df = df.dropna(subset=["latitude", "longitude"])
                            
                            # Convert latitude and longitude to numeric
                            map_df["latitude"] = pd.to_numeric(map_df["latitude"], errors="coerce")
                            map_df["longitude"] = pd.to_numeric(map_df["longitude"], errors="coerce")
                            
                            # Drop rows with invalid lat/long values
                            map_df = map_df.dropna(subset=["latitude", "longitude"])
                            
                            if not map_df.empty:
                                hover_data = ["OutletAddress", "Phone"]
                                if safe_column_check(map_df, "suburb"):
                                    hover_data.append("suburb")
                                if safe_column_check(map_df, "state"):
                                    hover_data.append("state")
                                    
                                hover_name = "Entity Name" if safe_column_check(map_df, "Entity Name") else None
                                
                                fig_map = px.scatter_map(
                                    map_df,
                                    lat="latitude",
                                    lon="longitude",
                                    hover_name=hover_name,
                                    hover_data=hover_data,
                                    zoom=3,
                                    map_style="open-street-map",
                                    title=f"Pharmacy Locations - {selected_au_file}"
                                )
                                fig_map.update_layout(height=600)
                                st.plotly_chart(fig_map, use_container_width=True, key="au_geo_map")
                            else:
                                st.warning("No valid geographic data available for mapping")
                        else:
                            st.warning("Geographic data (latitude/longitude) not available")
                    
                    # Tab: Data Completeness Analysis
                    with completeness_tab:
                        completeness_data = {}
                        for col in df.columns:
                            completeness_data[col] = df[col].notna().sum() / len(df) * 100 if len(df) > 0 else 0
                        
                        completeness_df = pd.DataFrame({
                            "Field": completeness_data.keys(),
                            "Completeness (%)": completeness_data.values()
                        })
                        
                        fig_completeness = px.bar(
                            completeness_df.sort_values("Completeness (%)"), 
                            x="Completeness (%)", 
                            y="Field",
                            orientation="h",
                            text="Completeness (%)",
                            title="Data Completeness by Field"
                        )
                        # Fix the percentage display format
                        fig_completeness.update_layout(height=500)
                        fig_completeness.update_traces(
                            texttemplate="%{text:.1f}%", 
                            textposition='outside'
                        )
                        st.plotly_chart(fig_completeness, use_container_width=True, key="au_completeness_chart")
            else:
                st.warning("No Australian pharmacy data files found. Please fetch data first.")
        
        # Tab for New Zealand pharmacy files
        with nz_tab:
            if nz_files:
                selected_nz_file = st.selectbox("Select New Zealand pharmacy data file", 
                                               list(nz_files.keys()),
                                               key="nz_file_selector")
                
                if selected_nz_file:
                    # Load the selected data
                    df = load_data(nz_files[selected_nz_file])
                    
                    # Display record count
                    st.header(f"📊 {selected_nz_file} - {len(df)} locations")
                    
                    # Create tabs for different analyses
                    overview_tab, trading_tab, geo_tab, completeness_tab = st.tabs([
                        "Data Overview", 
                        "Trading Hours", 
                        "Geographic Distribution", 
                        "Data Completeness"
                    ])
                    
                    # Tab: Data Overview
                    with overview_tab:
                        # Reorder the dataframe to show 'Entity Name' column first
                        if safe_column_check(df, "Entity Name"):
                            cols = ["Entity Name"] + [col for col in df.columns if col != "Entity Name"]
                            display_df = df[cols]
                        else:
                            display_df = df
                        
                        st.dataframe(display_df, use_container_width=True)
                        
                        # Key metrics
                        st.subheader("Key Metrics")
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            st.metric("Number of Pharmacies", len(df))

                        with col2:
                            # Check for both "phone" and "Phone" columns
                            phone_column = None
                            if safe_column_check(df, "phone"):
                                phone_column = "phone"
                            elif safe_column_check(df, "Phone"):
                                phone_column = "Phone"
                                
                            if phone_column:
                                has_phone = df[phone_column].notna().sum()
                                phone_percentage = int((has_phone / len(df)) * 100 if len(df) > 0 else 0)
                                st.metric("Locations with Phone", f"{has_phone} ({phone_percentage}%)")
                            else:
                                st.metric("Locations with Phone", "N/A")
                                
                        with col3:
                            # Check for both "email" and "Email" columns
                            email_column = None
                            if safe_column_check(df, "email"):
                                email_column = "email"
                            elif safe_column_check(df, "Email"):
                                email_column = "Email"
                                
                            if email_column:
                                populated_email = df[email_column].notna().sum()
                                email_percentage = int((populated_email / len(df)) * 100 if len(df) > 0 else 0)
                                st.metric("Locations with Email", f"{populated_email} ({email_percentage}%)")
                            else:
                                st.metric("Locations with Email", "N/A")
                        
                    
                    # Tab: Trading Hours Analysis
                    with trading_tab:
                        if safe_column_check(df, "Working hours"):
                            st.header("Trading Hours Analysis")
                            
                            # Check if any pharmacy has trading hours data
                            has_hours = False
                            for _, row in df.iterrows():
                                try:
                                    trading_hours_data = row["Working hours"]
                                    # Handle different formats of trading_hours data
                                    if isinstance(trading_hours_data, str) and trading_hours_data and trading_hours_data.lower() not in ["nan", "{}", "null", ""]:
                                        # Try to parse the string as JSON
                                        try:
                                            parsed_data = json.loads(trading_hours_data.replace("'", "\""))
                                            if parsed_data and isinstance(parsed_data, dict):
                                                has_hours = True
                                                break
                                        except json.JSONDecodeError:
                                            # If standard JSON parsing fails, try eval for Python dict string format
                                            try:
                                                parsed_data = eval(trading_hours_data)
                                                if parsed_data and isinstance(parsed_data, dict):
                                                    has_hours = True
                                                    break
                                            except:
                                                continue
                                    elif isinstance(trading_hours_data, dict) and trading_hours_data:
                                        has_hours = True
                                        break
                                except (TypeError, AttributeError):
                                    continue
                                    
                            if has_hours:
                                # Select a pharmacy to view trading hours
                                pharmacies_with_hours = []
                                for i, row in df.iterrows():
                                    try:
                                        trading_hours_str = row["Working hours"]
                                        trading_hours_data = None
                                        
                                        # Try to parse trading_hours if it's a string
                                        if isinstance(trading_hours_str, str) and trading_hours_str and trading_hours_str.lower() not in ["nan", "{}", "null", ""]:
                                            try:
                                                # Try standard JSON parsing first
                                                trading_hours_data = json.loads(trading_hours_str.replace("'", "\""))
                                            except json.JSONDecodeError:
                                                # If that fails, try Python's eval for dict-like strings
                                                try:
                                                    trading_hours_data = eval(trading_hours_str)
                                                except:
                                                    trading_hours_data = None
                                        elif isinstance(trading_hours_str, dict):
                                            trading_hours_data = trading_hours_str
                                            
                                        if trading_hours_data and isinstance(trading_hours_data, dict):
                                            name = row["Entity Name"] if safe_column_check(df, "Entity Name") and pd.notna(row["Entity Name"]) else f"Pharmacy #{i}"
                                            pharmacies_with_hours.append((i, name))
                                    except (TypeError, AttributeError):
                                        continue
                                
                                if pharmacies_with_hours:
                                    # Create dropdown for pharmacy selection instead of checkboxes
                                    st.subheader("Select a pharmacy to view trading hours")
                                    
                                    # Create a list of pharmacy names
                                    pharmacy_names = [name for _, name in pharmacies_with_hours]
                                    
                                    # Use dropdown for pharmacy selection
                                    selected_pharmacy = st.selectbox(
                                        "Select pharmacy", 
                                        pharmacy_names,
                                        key="pharmacy_selector_nz"
                                    )
                                    
                                    # Find the actual index of the selected pharmacy
                                    selected_idx = next((idx for idx, name in pharmacies_with_hours if name == selected_pharmacy), None)
                                    
                                    if selected_idx is not None:
                                        # Parse and display trading hours
                                        trading_hours_str = df.iloc[selected_idx]["Working hours"]
                                        try:
                                            trading_hours = None
                                            if isinstance(trading_hours_str, str):
                                                try:
                                                    # Try standard JSON parsing first with quote replacement
                                                    trading_hours = json.loads(trading_hours_str.replace("'", "\""))
                                                except json.JSONDecodeError:
                                                    # If that fails, try Python's eval for dict-like strings
                                                    try:
                                                        trading_hours = eval(trading_hours_str)
                                                    except:
                                                        trading_hours = None
                                            elif isinstance(trading_hours_str, dict):
                                                trading_hours = trading_hours_str
                                            
                                            if trading_hours:
                                                # Create a table for trading hours
                                                hours_data = []
                                                day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday", "Public Holiday"]
                                                
                                                # Sort days according to conventional order
                                                for day in day_order:
                                                    if day in trading_hours:
                                                        hours = trading_hours[day]
                                                        if isinstance(hours, dict):
                                                            hours_data.append({
                                                                "Day": day,
                                                                "Opens": hours.get("open", "N/A"),
                                                                "Closes": hours.get("close", hours.get("closed", "N/A"))
                                                            })
                                                
                                                # Add any days not in the standard order
                                                for day, hours in trading_hours.items():
                                                    if day not in day_order and isinstance(hours, dict):
                                                        hours_data.append({
                                                            "Day": day,
                                                            "Opens": hours.get("open", "N/A"),
                                                            "Closes": hours.get("close", hours.get("closed", "N/A"))
                                                        })
                                                        
                                                if hours_data:
                                                    hours_df = pd.DataFrame(hours_data)
                                                    st.table(hours_df)
                                                    
                                                    # Create visualization of opening hours
                                                    st.subheader("Weekly Hours Visualization")
                                                    
                                                    # Convert times to numeric format for visualization
                                                    chart_data = []
                                                    for entry in hours_data:
                                                        day = entry["Day"]
                                                        if day != "Public Holiday":  # Skip public holiday for the chart
                                                            opens = entry["Opens"]
                                                            closes = entry["Closes"]
                                                            
                                                            # Convert time strings to hours (decimal)
                                                            try:
                                                                if opens != "N/A" and closes != "N/A":
                                                                    # Try to parse AM/PM format
                                                                    open_time = pd.to_datetime(opens, format="%I:%M %p", errors="coerce")
                                                                    close_time = pd.to_datetime(closes, format="%I:%M %p", errors="coerce")
                                                                    
                                                                    if pd.isna(open_time) or pd.isna(close_time):
                                                                        # Try 24-hour format
                                                                        open_time = pd.to_datetime(opens, format="%H:%M", errors="coerce")
                                                                        close_time = pd.to_datetime(closes, format="%H:%M", errors="coerce")
                                                                    
                                                                    if not pd.isna(open_time) and not pd.isna(close_time):
                                                                        open_hour = open_time.hour + open_time.minute/60
                                                                        close_hour = close_time.hour + close_time.minute/60
                                                                        
                                                                        chart_data.append({
                                                                            "Day": day,
                                                                            "Open Hour": open_hour,
                                                                            "Close Hour": close_hour,
                                                                            "Hours Open": close_hour - open_hour
                                                                        })
                                                            except Exception:
                                                                pass
                                                        
                                                    if chart_data:
                                                        chart_df = pd.DataFrame(chart_data)
                                                        
                                                        # Create a horizontal bar chart of opening hours
                                                        fig = go.Figure()
                                                        
                                                        # Add bars representing opening hours
                                                        for i, row in chart_df.iterrows():
                                                            fig.add_trace(go.Bar(
                                                                y=[row["Day"]],
                                                                x=[row["Hours Open"]],
                                                                orientation='h',
                                                                base=[row["Open Hour"]],
                                                                name=row["Day"],
                                                                text=f"{row['Open Hour']:.2f} - {row['Close Hour']:.2f}",
                                                                hoverinfo="text"
                                                            ))
                                                        
                                                        fig.update_layout(
                                                            title="Weekly Opening Hours",
                                                            xaxis_title="Hour of Day",
                                                            yaxis_title="Day of Week",
                                                            xaxis=dict(tickmode='linear', tick0=0, dtick=2, range=[6, 24]),
                                                            height=400,
                                                            barmode='stack',
                                                            showlegend=False
                                                        )
                                                        
                                                        st.plotly_chart(fig, use_container_width=True, key="nz_trading_hours_chart")
                                                else:
                                                    st.info("No structured trading hours data available for this pharmacy.")
                                            else:
                                                st.info("No trading hours data available for this pharmacy.")
                                        except (json.JSONDecodeError, TypeError):
                                            st.error("Could not parse trading hours data.")
                                else:
                                    st.info("No pharmacies with trading hours data found.")
                            else:
                                st.info("No trading hours data available in this dataset.")
                    
                    # Tab: Geographic Distribution
                    with geo_tab:
                        # Display pharmacies on a map
                        if safe_column_check(df, "latitude") and safe_column_check(df, "longitude"):
                            # Remove rows with missing lat/long values
                            map_df = df.dropna(subset=["latitude", "longitude"])
                            
                            # Convert latitude and longitude to numeric
                            map_df["latitude"] = pd.to_numeric(map_df["latitude"], errors="coerce")
                            map_df["longitude"] = pd.to_numeric(map_df["longitude"], errors="coerce")
                            
                            # Drop rows with invalid lat/long values
                            map_df = map_df.dropna(subset=["latitude", "longitude"])
                            
                            if not map_df.empty:
                                hover_data = ["OutletAddress", "Phone"]
                                if safe_column_check(map_df, "suburb"):
                                    hover_data.append("suburb")
                                if safe_column_check(map_df, "postcode"):
                                    hover_data.append("postcode")
                                    
                                hover_name = "Entity Name" if safe_column_check(map_df, "Entity Name") else None
                                
                                fig_map = px.scatter_map(
                                    map_df,
                                    lat="latitude",
                                    lon="longitude",
                                    hover_name=hover_name,
                                    hover_data=hover_data,
                                    zoom=5,
                                    map_style="open-street-map",
                                    title=f"Pharmacy Locations - {selected_nz_file}"
                                )
                                fig_map.update_layout(height=600)
                                st.plotly_chart(fig_map, use_container_width=True, key="nz_geo_map")
                            else:
                                st.warning("No valid geographic data available for mapping")
                        else:
                            st.warning("Geographic data (latitude/longitude) not available")
                    
                    # Tab: Data Completeness Analysis
                    with completeness_tab:
                        completeness_data = {}
                        for col in df.columns:
                            completeness_data[col] = df[col].notna().sum() / len(df) * 100 if len(df) > 0 else 0
                        
                        completeness_df = pd.DataFrame({
                            "Field": completeness_data.keys(),
                            "Completeness (%)": completeness_data.values()
                        })
                        
                        fig_completeness = px.bar(
                            completeness_df.sort_values("Completeness (%)"), 
                            x="Completeness (%)", 
                            y="Field",
                            orientation="h",
                            text="Completeness (%)",
                            title="Data Completeness by Field"
                        )
                        # Fix the percentage display format
                        fig_completeness.update_layout(height=500)
                        fig_completeness.update_traces(
                            texttemplate="%{text:.1f}%", 
                            textposition='outside'
                        )
                        st.plotly_chart(fig_completeness, use_container_width=True, key="nz_completeness_chart")
            else:
                st.warning("No New Zealand pharmacy data files found. Please fetch data first.")
        
        # Create a section for comparing AU and NZ data 
        st.header("Cross-Country Comparison")
        st.info("Select one pharmacy brand from each country to compare data between Australia and New Zealand")
        
        col1, col2 = st.columns(2)
        compare_au_file = None
        compare_nz_file = None
        
        with col1:
            if au_files:
                compare_au_file = st.selectbox("Select Australian pharmacy banner", 
                                              list(au_files.keys()),
                                              key="compare_au_selector")
            else:
                st.warning("No Australian pharmacy data files available")
        
        with col2:
            if nz_files:
                compare_nz_file = st.selectbox("Select New Zealand pharmacy banner", 
                                              list(nz_files.keys()),
                                              key="compare_nz_selector")
            else:
                st.warning("No New Zealand pharmacy data files available")
        
        if compare_au_file and compare_nz_file:
            st.subheader(f"Comparing {compare_au_file} (AU) with {compare_nz_file} (NZ)")
            
            # Load both datasets
            au_df = load_data(au_files[compare_au_file])
            nz_df = load_data(nz_files[compare_nz_file])
            
            # Display basic counts
            col1, col2 = st.columns(2)
            with col1:
                st.metric(f"{compare_au_file} Locations", len(au_df))
            with col2:
                st.metric(f"{compare_nz_file} Locations", len(nz_df))
            
            # Basic feature comparison
            st.subheader("Data Availability Comparison")
            
            # Compare common fields
            common_fields = ["Email", "Phone", "Working hours"]
            
            # Prepare comparison data
            comparison_data = []
            for field in common_fields:
                au_pct = au_df[field].notna().mean() * 100 if field in au_df.columns else 0
                nz_pct = nz_df[field].notna().mean() * 100 if field in nz_df.columns else 0
                
                comparison_data.append({
                    "Field": field.capitalize(), 
                    "AU Percentage": au_pct,
                    "NZ Percentage": nz_pct
                })
            
            # Create comparison DataFrame
            if comparison_data:
                comp_df = pd.DataFrame(comparison_data)
                
                # Display comparison chart
                fig_comp = px.bar(
                    comp_df,
                    x="Field",
                    y=["AU Percentage", "NZ Percentage"],
                    barmode="group",
                    title="Data Field Availability Comparison"
                )
                fig_comp.update_layout(height=400)
                fig_comp.update_traces(texttemplate='%{y:.1f}%', textposition='outside')
                st.plotly_chart(fig_comp, use_container_width=True, key="comparison_chart")
                
                # Show data table
                st.dataframe(comp_df, use_container_width=True)
                
                # Trading hours comparison - if both have trading hours
                if all(safe_column_check(df, "Working hours") for df in [au_df, nz_df]):
                    st.subheader("Trading Hours Comparison")
                    
                    # Function to get average opening and closing time for given day
                    def get_avg_hours(df, day):
                        opens = []
                        closes = []
                        
                        for _, row in df.iterrows():
                            try:
                                # Parse trading hours
                                if isinstance(row["Working hours"], str):
                                    hours = json.loads(row["Working hours"].replace("'", "\""))
                                elif isinstance(row["Working hours"], dict):
                                    hours = row["Working hours"]
                                else:
                                    continue
                                    
                                if day in hours and isinstance(hours[day], dict):
                                    day_hours = hours[day]
                                    if "open" in day_hours and "closed" in day_hours:
                                        opens.append(day_hours["open"])
                                        closes.append(day_hours["closed"])
                            except:
                                continue
                                
                        return {"opens": opens, "closes": closes}
                    
                    # Compare Monday as an example
                    au_hours = get_avg_hours(au_df, "Monday")
                    nz_hours = get_avg_hours(nz_df, "Monday")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write("#### Australia Monday Hours")
                        if au_hours["opens"]:
                            st.write(f"Most common opening time: {max(set(au_hours['opens']), key=au_hours['opens'].count)}")
                            st.write(f"Most common closing time: {max(set(au_hours['closes']), key=au_hours['closes'].count)}")
                        else:
                            st.write("No Monday hours data available")
                            
                    with col2:
                        st.write("#### New Zealand Monday Hours")
                        if nz_hours["opens"]:
                            st.write(f"Most common opening time: {max(set(nz_hours['opens']), key=nz_hours['opens'].count)}")
                            st.write(f"Most common closing time: {max(set(nz_hours['closes']), key=nz_hours['closes'].count)}")
                        else:
                            st.write("No Monday hours data available")
        elif st.button("Skip Comparison"):
            st.info("Comparison skipped")          

# Footer
st.markdown("---")
st.caption("Pharmacy Store Locator Analytics Dashboard © 2025")