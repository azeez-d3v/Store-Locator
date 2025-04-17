import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import os
import asyncio
from pathlib import Path
from datetime import datetime
import sys
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

def get_csv_files():
    """Get a list of all CSV files in the output directory"""
    return [f for f in OUTPUT_DIR.glob("*.csv") if f.is_file()]

def load_data(file_path):
    """Load data from a CSV file"""
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
    
    # Use checkboxes for brand selection
    st.subheader("Select pharmacy to fetch")
    
    # Add Select All and Clear Selection buttons in a row
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Select All"):
            st.session_state["fetch_dds"] = True
            st.session_state["fetch_amcal"] = True
            st.session_state["fetch_blooms"] = True
            st.session_state["fetch_ramsay"] = True
            st.session_state["fetch_revive"] = True
            st.session_state["fetch_optimal"] = True
            st.session_state["fetch_community"] = True
            st.session_state["fetch_footes"] = True
            st.session_state["fetch_alive"] = True
            st.session_state["fetch_ydc"] = True
            st.session_state["fetch_chemist_warehouse"] = True
            st.session_state["fetch_pharmasave"] = True
            st.session_state["fetch_nova"] = True
            st.session_state["fetch_choice"] = True
            st.rerun()
    
    with col2:
        if st.button("Clear Selection"):
            st.session_state["fetch_dds"] = False
            st.session_state["fetch_amcal"] = False
            st.session_state["fetch_blooms"] = False
            st.session_state["fetch_ramsay"] = False
            st.session_state["fetch_revive"] = False
            st.session_state["fetch_optimal"] = False
            st.session_state["fetch_community"] = False
            st.session_state["fetch_footes"] = False
            st.session_state["fetch_alive"] = False
            st.session_state["fetch_ydc"] = False
            st.session_state["fetch_chemist_warehouse"] = False
            st.session_state["fetch_pharmasave"] = False
            st.session_state["fetch_nova"] = False
            st.session_state["fetch_choice"] = False
            st.rerun()
    
    # Initialize checkbox states in session state if they don't exist
    if "fetch_dds" not in st.session_state:
        st.session_state["fetch_dds"] = False
    if "fetch_amcal" not in st.session_state:
        st.session_state["fetch_amcal"] = False
    if "fetch_blooms" not in st.session_state:
        st.session_state["fetch_blooms"] = False
    if "fetch_ramsay" not in st.session_state:
        st.session_state["fetch_ramsay"] = False
    if "fetch_revive" not in st.session_state:
        st.session_state["fetch_revive"] = False
    if "fetch_optimal" not in st.session_state:
        st.session_state["fetch_optimal"] = False
    if "fetch_community" not in st.session_state:
        st.session_state["fetch_community"] = False
    if "fetch_footes" not in st.session_state:
        st.session_state["fetch_footes"] = False
    if "fetch_alive" not in st.session_state:
        st.session_state["fetch_alive"] = False
    if "fetch_ydc" not in st.session_state:
        st.session_state["fetch_ydc"] = False
    if "fetch_chemist_warehouse" not in st.session_state:
        st.session_state["fetch_chemist_warehouse"] = False
    if "fetch_pharmasave" not in st.session_state:
        st.session_state["fetch_pharmasave"] = False
    if "fetch_nova" not in st.session_state:
        st.session_state["fetch_nova"] = False
    if "fetch_choice" not in st.session_state:
        st.session_state["fetch_choice"] = False
    
    fetch_dds = st.checkbox("Discount Drug Stores", value=st.session_state["fetch_dds"], key="fetch_dds")
    fetch_amcal = st.checkbox("Amcal", value=st.session_state["fetch_amcal"], key="fetch_amcal")
    fetch_blooms = st.checkbox("Blooms The Chemist", value=st.session_state["fetch_blooms"], key="fetch_blooms")
    fetch_ramsay = st.checkbox("Ramsay Pharmacy", value=st.session_state["fetch_ramsay"], key="fetch_ramsay")
    fetch_revive = st.checkbox("Revive Pharmacy", value=st.session_state["fetch_revive"], key="fetch_revive")
    fetch_optimal = st.checkbox("Optimal Pharmacy Plus", value=st.session_state["fetch_optimal"], key="fetch_optimal")
    fetch_community = st.checkbox("Community Care Chemist", value=st.session_state["fetch_community"], key="fetch_community")
    fetch_footes = st.checkbox("Footes Pharmacy", value=st.session_state["fetch_footes"], key="fetch_footes")
    fetch_alive = st.checkbox("Alive Pharmacy", value=st.session_state["fetch_alive"], key="fetch_alive")
    fetch_ydc = st.checkbox("Your Discount Chemist", value=st.session_state["fetch_ydc"], key="fetch_ydc")
    fetch_chemist_warehouse = st.checkbox("Chemist Warehouse", value=st.session_state["fetch_chemist_warehouse"], key="fetch_chemist_warehouse")
    fetch_pharmasave = st.checkbox("Pharmasave", value=st.session_state["fetch_pharmasave"], key="fetch_pharmasave")
    fetch_nova = st.checkbox("Nova Pharmacy", value=st.session_state["fetch_nova"], key="fetch_nova")
    fetch_choice = st.checkbox("Choice Pharmacy", value=st.session_state["fetch_choice"], key="fetch_choice")
    
    if st.button("Fetch Data"):
        with st.spinner("Fetching data..."):
            try:
                # Run the fetch operation asynchronously
                pharmacy_api = PharmacyLocations()
                
                async def fetch_data():
                    selected_brands = []
                    if fetch_dds: selected_brands.append("dds")
                    if fetch_amcal: selected_brands.append("amcal")
                    if fetch_blooms: selected_brands.append("blooms")
                    if fetch_ramsay: selected_brands.append("ramsay") 
                    if fetch_revive: selected_brands.append("revive")
                    if fetch_optimal: selected_brands.append("optimal")
                    if fetch_community: selected_brands.append("community")
                    if fetch_footes: selected_brands.append("footes")
                    if fetch_alive: selected_brands.append("alive")
                    if fetch_ydc: selected_brands.append("ydc")
                    if fetch_chemist_warehouse: selected_brands.append("chemist_warehouse")
                    if fetch_pharmasave: selected_brands.append("pharmasave")
                    if fetch_nova: selected_brands.append("nova")
                    if fetch_choice: selected_brands.append("choice")
                    
                    if len(selected_brands) > 1:
                        # Fetch multiple brands
                        await pharmacy_api.fetch_and_save_all(selected_brands)
                        st.success(f"Successfully fetched data for {', '.join(b.upper() for b in selected_brands)}!")
                    elif len(selected_brands) == 1:
                        # Fetch a single brand
                        brand = selected_brands[0]
                        details = await pharmacy_api.fetch_all_locations_details(brand)
                        if details:
                            pharmacy_api.save_to_csv(details, f"{brand}_pharmacies.csv")
                            add_fetch_log(brand, len(details), True)
                            st.success(f"Successfully fetched {len(details)} {brand.upper()} locations!")
                        else:
                            add_fetch_log(brand, 0, False)
                            st.error(f"No data found for {brand.upper()}")
                    else:
                        st.warning("Please select at least one brand to fetch")
                        
                # Run the async function
                if fetch_dds or fetch_amcal or fetch_blooms or fetch_ramsay or fetch_revive or fetch_optimal or fetch_community or fetch_footes or fetch_alive or fetch_ydc or fetch_chemist_warehouse or fetch_pharmasave or fetch_nova or fetch_choice:
                    asyncio.run(fetch_data())
            except Exception as e:
                brand = []
                if fetch_dds: brand.append("dds")
                if fetch_amcal: brand.append("amcal")
                if fetch_blooms: brand.append("blooms")
                if fetch_ramsay: brand.append("ramsay")
                if fetch_revive: brand.append("revive")
                if fetch_optimal: brand.append("optimal")
                if fetch_community: brand.append("community")
                if fetch_footes: brand.append("footes")
                if fetch_alive: brand.append("alive")
                if fetch_ydc: brand.append("ydc")
                if fetch_chemist_warehouse: brand.append("chemist_warehouse")
                if fetch_pharmasave: brand.append("pharmasave")
                if fetch_nova: brand.append("nova")
                if fetch_choice: brand.append("choice")
                for b in brand:
                    add_fetch_log(b, 0, False)
                st.error(f"Error fetching data: {str(e)}")

# Tab 3: Fetch History
with tab_history:
    st.header("Fetch History")
    logs = load_logs()
    if logs["fetch_history"]:
        history_df = pd.DataFrame(logs["fetch_history"])
        history_df["timestamp"] = pd.to_datetime(history_df["timestamp"])
        history_df["timestamp"] = history_df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
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
    csv_files = get_csv_files()
    if not csv_files:
        st.warning("No data files found. Please fetch data first.")
    else:
        # Use dropdown for file selection as originally requested
        file_options = {f.stem: str(f) for f in csv_files}
        selected_file_name = st.selectbox("Select data file to analyze", list(file_options.keys()))
        
        if selected_file_name:  # Only proceed if a file is selected
            # Load the selected data
            df = load_data(file_options[selected_file_name])
            
            # Display record count
            st.header(f"📊 {selected_file_name} - {len(df)} locations")
            
            # Create tabs for different analyses
            overview_tab, trading_tab, geo_tab, completeness_tab, advanced_tab = st.tabs([
                "Data Overview", 
                "Trading Hours", 
                "Geographic Distribution", 
                "Data Completeness", 
                "Advanced Analysis"
            ])
            
            # Tab: Data Overview
            with overview_tab:
                # Reorder the dataframe to show 'name' column first
                if safe_column_check(df, "name"):
                    cols = ["name"] + [col for col in df.columns if col != "name"]
                    display_df = df[cols]
                else:
                    display_df = df
                
                st.dataframe(display_df, use_container_width=True)
                
                # Key metrics
                st.subheader("Key Metrics")
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    if safe_column_check(df, "state"):
                        states_count = df["state"].value_counts()
                        st.metric("Number of States", len(states_count))
                    else:
                        st.metric("Number of States", "N/A")
                
                with col2:
                    if safe_column_check(df, "email"):
                        populated_email = df["email"].notna().sum()
                        email_percentage = int((populated_email / len(df)) * 100) if len(df) > 0 else 0
                        st.metric("Locations with Email", f"{populated_email} ({email_percentage}%)")
                    else:
                        st.metric("Locations with Email", "N/A")
                
                with col3:
                    if safe_column_check(df, "website"):
                        has_website = df["website"].notna().sum()
                        website_percentage = int((has_website / len(df)) * 100) if len(df) > 0 else 0
                        st.metric("Locations with Website", f"{has_website} ({website_percentage}%)")
                    else:
                        st.metric("Locations with Website", "N/A")
            
            # Tab: Trading Hours Analysis
            with trading_tab:
                if safe_column_check(df, "trading_hours"):
                    st.header("Trading Hours Analysis")
                    
                    # Check if any pharmacy has trading hours data
                    has_hours = False
                    for _, row in df.iterrows():
                        try:
                            trading_hours_data = row["trading_hours"]
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
                                trading_hours_str = row["trading_hours"]
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
                                    name = row["name"] if safe_column_check(df, "name") and pd.notna(row["name"]) else f"Pharmacy #{i}"
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
                                key="pharmacy_selector"
                            )
                            
                            # Find the actual index of the selected pharmacy
                            selected_idx = next((idx for idx, name in pharmacies_with_hours if name == selected_pharmacy), None)
                            
                            if selected_idx is not None:
                                # Parse and display trading hours
                                trading_hours_str = df.iloc[selected_idx]["trading_hours"]
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
                                                        "Closes": hours.get("closed", "N/A")
                                                    })
                                        
                                        # Add any days not in the standard order
                                        for day, hours in trading_hours.items():
                                            if day not in day_order and isinstance(hours, dict):
                                                hours_data.append({
                                                    "Day": day,
                                                    "Opens": hours.get("open", "N/A"),
                                                    "Closes": hours.get("closed", "N/A")
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
                                                
                                                st.plotly_chart(fig, use_container_width=True)
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
                else:
                    st.info("No trading hours data available in this dataset.")
            
            # Tab: Geographic Distribution
            with geo_tab:
                state_tab, map_tab = st.tabs(["By State", "Map View"])
                
                with state_tab:
                    if safe_column_check(df, "state"):
                        # Define valid Australian states
                        valid_states = {"NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT"}
                        
                        # Clean state data to standardize formatting
                        df_state = df.copy()
                        if safe_column_check(df_state, "state"):
                            # Convert state column to proper format - handle different data types
                            # First convert all values to strings
                            df_state["state"] = df_state["state"].astype(str)
                            # Now we can safely use string methods
                            df_state["state"] = df_state["state"].str.upper().str.strip()
                            
                            # Map common variations to standard abbreviations
                            state_mapping = {
                                "NEW SOUTH WALES": "NSW",
                                "VICTORIA": "VIC",
                                "QUEENSLAND": "QLD",
                                "SOUTH AUSTRALIA": "SA",
                                "WESTERN AUSTRALIA": "WA",
                                "TASMANIA": "TAS",
                                "NORTHERN TERRITORY": "NT",
                                "AUSTRALIAN CAPITAL TERRITORY": "ACT",
                                # Additional variations that might appear
                                "NSW": "NSW",
                                "VIC": "VIC",
                                "QLD": "QLD",
                                "SA": "SA",
                                "WA": "WA", 
                                "TAS": "TAS",
                                "NT": "NT",
                                "ACT": "ACT"
                            }
                            
                            df_state["state"] = df_state["state"].replace(state_mapping)
                            
                            # Filter for valid Australian states
                            valid_state_data = df_state[df_state["state"].isin(valid_states)]
                            if len(valid_state_data) > 0:
                                state_counts = valid_state_data["state"].value_counts().reset_index()
                                state_counts.columns = ["State", "Count"]
                                
                                # Sort states in a logical order
                                state_order = ["NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT"]
                                state_counts["State_Order"] = state_counts["State"].apply(lambda x: state_order.index(x) if x in state_order else 999)
                                state_counts = state_counts.sort_values("State_Order").drop("State_Order", axis=1)
                                
                                fig_states = px.bar(
                                    state_counts,
                                    x="State", 
                                    y="Count",
                                    color="Count",
                                    text_auto=True,
                                    title=f"Pharmacy Distribution by State - {selected_file_name}"
                                )
                                fig_states.update_layout(height=500)
                                st.plotly_chart(fig_states, use_container_width=True)
                            else:
                                st.warning("No valid Australian state data found in this dataset")
                        else:
                            st.warning("State data not available in this dataset")
                    else:
                        st.warning("State data not available in this dataset")
                
                with map_tab:
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
                            hover_data = ["address", "phone"]
                            if safe_column_check(map_df, "suburb"):
                                hover_data.append("suburb")
                            if safe_column_check(map_df, "state"):
                                hover_data.append("state")
                                
                            hover_name = "name" if safe_column_check(map_df, "name") else None
                            
                            fig_map = px.scatter_map(
                                map_df,
                                lat="latitude",
                                lon="longitude",
                                hover_name=hover_name,
                                hover_data=hover_data,
                                zoom=3,
                                map_style="open-street-map",
                                title=f"Pharmacy Locations - {selected_file_name}"
                            )
                            fig_map.update_layout(height=600)
                            st.plotly_chart(fig_map, use_container_width=True)
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
                st.plotly_chart(fig_completeness, use_container_width=True)
            
            # Tab: Advanced Analysis
            with advanced_tab:
                # Get all available CSV files for comparison
                all_csv_files = get_csv_files()
                all_brands = {f.stem: str(f) for f in all_csv_files}
                
                st.subheader("Multi-Brand Comparison")
                
                # Allow user to select up to 4 datasets to compare
                st.write("Select up to 4 datasets to compare:")
                
                # Create a 2x2 grid for brand selection checkboxes
                col1, col2 = st.columns(2)
                selected_brands = {}
                
                # Use session state to keep track of selected brands
                if "selected_brands_for_comparison" not in st.session_state:
                    st.session_state.selected_brands_for_comparison = {}
                    
                # Create checkboxes for brand selection in the 2x2 grid
                brand_names = list(all_brands.keys())
                
                for i, brand in enumerate(brand_names):
                    # Place in appropriate column
                    with col1 if i % 2 == 0 else col2:
                        if brand in st.session_state.selected_brands_for_comparison:
                            default = st.session_state.selected_brands_for_comparison[brand]
                        else:
                            default = False
                            
                        is_selected = st.checkbox(brand, value=default, key=f"compare_{brand}")
                        if is_selected:
                            selected_brands[brand] = all_brands[brand]
                            st.session_state.selected_brands_for_comparison[brand] = True
                        else:
                            if brand in st.session_state.selected_brands_for_comparison:
                                st.session_state.selected_brands_for_comparison[brand] = False
                
                # Clear selection button
                if st.button("Clear Comparison Selection"):
                    st.session_state.selected_brands_for_comparison = {brand: False for brand in brand_names}
                    st.rerun()
                
                # Display error if more than 4 brands are selected
                if len(selected_brands) > 4:
                    st.error("Please select at most 4 brands for comparison.")
                    selected_brands = dict(list(selected_brands.items())[:4])
                
                if len(selected_brands) >= 2:
                    st.subheader(f"Comparing {len(selected_brands)} Brands")
                    
                    # Load selected brand dataframes
                    brand_dfs = {}
                    for brand_name, file_path in selected_brands.items():
                        brand_dfs[brand_name] = load_data(file_path)
                    
                    # Display metrics for each brand in a row
                    cols = st.columns(len(selected_brands))
                    
                    for i, (brand_name, _) in enumerate(selected_brands.items()):
                        with cols[i]:
                            brand_df = brand_dfs[brand_name]
                            st.metric(f"{brand_name} Locations", len(brand_df))
                            
                            # Add a small data quality score based on completeness
                            if not brand_df.empty:
                                completeness_pct = brand_df.notna().sum().sum() / (len(brand_df) * len(brand_df.columns)) * 100
                                st.metric("Data Quality", f"{completeness_pct:.1f}%")
                    
                    # Standardize state data for all brands
                    st.subheader("State Distribution Comparison")
                    valid_states = {"NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT"}
                    state_mapping = {
                        "NEW SOUTH WALES": "NSW",
                        "VICTORIA": "VIC", 
                        "QUEENSLAND": "QLD",
                        "SOUTH AUSTRALIA": "SA",
                        "WESTERN AUSTRALIA": "WA",
                        "TASMANIA": "TAS",
                        "NORTHERN TERRITORY": "NT",
                        "AUSTRALIAN CAPITAL TERRITORY": "ACT",
                        # Additional variations that might appear
                        "NSW": "NSW",
                        "VIC": "VIC",
                        "QLD": "QLD",
                        "SA": "SA",
                        "WA": "WA", 
                        "TAS": "TAS",
                        "NT": "NT",
                        "ACT": "ACT"
                    }
                    
                    brand_state_dfs = []
                    
                    for brand_name, brand_df in brand_dfs.items():
                        if safe_column_check(brand_df, "state"):
                            # Clean and standardize state data
                            brand_df_copy = brand_df.copy()
                            if isinstance(brand_df_copy["state"], pd.Series):
                                brand_df_copy["state"] = brand_df_copy["state"].str.upper().str.strip()
                                brand_df_copy["state"] = brand_df_copy["state"].replace(state_mapping)
                                
                                # Filter for valid Australian states
                                valid_state_data = brand_df_copy[brand_df_copy["state"].isin(valid_states)]
                                
                                if not valid_state_data.empty:
                                    states = valid_state_data["state"].value_counts().reset_index()
                                    states.columns = ["State", "Count"]
                                    states["Brand"] = brand_name
                                    brand_state_dfs.append(states)
                            else:
                                st.warning(f"State data in {brand_name} is not in the expected format.")
                        else:
                            st.warning(f"State data not available for {brand_name}")
                    
                    if brand_state_dfs:
                        combined_states = pd.concat(brand_state_dfs)
                        
                        # Sort states in standard order
                        state_order = ["NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT"]
                        combined_states["State_Order"] = combined_states["State"].apply(
                            lambda x: state_order.index(x) if x in state_order else 999
                        )
                        combined_states = combined_states.sort_values("State_Order")
                        
                        # Create the comparison chart
                        fig_comparison = px.bar(
                            combined_states, 
                            x="State", 
                            y="Count", 
                            color="Brand",
                            barmode="group",
                            category_orders={"State": state_order},
                            title="Pharmacy Distribution by State - Brand Comparison"
                        )
                        fig_comparison.update_layout(height=500)
                        st.plotly_chart(fig_comparison, use_container_width=True)
                        
                        # Add a percentage view option
                        if st.checkbox("Show as percentage of brand total"):
                            # Calculate percentages
                            percentage_data = []
                            for brand in combined_states["Brand"].unique():
                                brand_data = combined_states[combined_states["Brand"] == brand].copy()
                                total = brand_data["Count"].sum()
                                brand_data["Percentage"] = (brand_data["Count"] / total * 100) if total > 0 else 0
                                percentage_data.append(brand_data)
                            
                            percentage_df = pd.concat(percentage_data)
                            
                            # Create percentage chart
                            fig_percentage = px.bar(
                                percentage_df,
                                x="State",
                                y="Percentage",
                                color="Brand",
                                barmode="group",
                                category_orders={"State": state_order},
                                title="Pharmacy Distribution by State (%) - Brand Comparison"
                            )
                            fig_percentage.update_layout(height=500)
                            fig_percentage.update_traces(texttemplate='%{y:.1f}%', textposition='outside')
                            st.plotly_chart(fig_percentage, use_container_width=True)
                    
                    # Compare other interesting metrics if available
                    metrics_to_compare = []
                    
                    # Check if we have email data across brands
                    if all(safe_column_check(df, "email") for df in brand_dfs.values()):
                        metrics_to_compare.append("Email Availability")
                    
                    # Check if we have website data across brands
                    if all(safe_column_check(df, "website") for df in brand_dfs.values()):
                        metrics_to_compare.append("Website Availability")
                    
                    # Check if we have trading_hours data across brands
                    if all(safe_column_check(df, "trading_hours") for df in brand_dfs.values()):
                        metrics_to_compare.append("Trading Hours Availability")
                    
                    if metrics_to_compare:
                        st.subheader("Additional Metrics Comparison")
                        
                        comparison_data = []
                        for brand_name, df in brand_dfs.items():
                            brand_row = {"Brand": brand_name}
                            
                            # Calculate availability percentages for each metric
                            if "Email Availability" in metrics_to_compare:
                                email_pct = df["email"].notna().mean() * 100 if "email" in df else 0
                                brand_row["Email Availability"] = email_pct
                                
                            if "Website Availability" in metrics_to_compare:
                                website_pct = df["website"].notna().mean() * 100 if "website" in df else 0
                                brand_row["Website Availability"] = website_pct
                                
                            if "Trading Hours Availability" in metrics_to_compare:
                                # Check if trading_hours field has actual content
                                has_hours = 0
                                total = len(df)
                                
                                for _, row in df.iterrows():
                                    try:
                                        trading_hours_data = row.get("trading_hours", None)
                                        if pd.notna(trading_hours_data):
                                            # Handle different formats
                                            if isinstance(trading_hours_data, str):
                                                if trading_hours_data.strip() not in ["", "{}", "null", "nan"]:
                                                    has_hours += 1
                                            elif isinstance(trading_hours_data, dict) and trading_hours_data:
                                                has_hours += 1
                                    except:
                                        pass
                                        
                                hours_pct = (has_hours / total * 100) if total > 0 else 0
                                brand_row["Trading Hours Availability"] = hours_pct
                            
                            comparison_data.append(brand_row)
                        
                        # Create comparison dataframe
                        if comparison_data:
                            comp_df = pd.DataFrame(comparison_data)
                            
                            # Melt the dataframe for easier plotting
                            melted_df = pd.melt(
                                comp_df, 
                                id_vars=["Brand"], 
                                var_name="Metric", 
                                value_name="Percentage"
                            )
                            
                            # Create comparison chart
                            fig_metrics = px.bar(
                                melted_df,
                                x="Brand",
                                y="Percentage",
                                color="Metric",
                                barmode="group",
                                title="Data Availability Comparison (%)"
                            )
                            fig_metrics.update_layout(height=500)
                            fig_metrics.update_traces(texttemplate='%{y:.1f}%', textposition='outside')
                            st.plotly_chart(fig_metrics, use_container_width=True)
                            
                            # Display the raw data
                            with st.expander("View Comparison Data"):
                                st.dataframe(comp_df)
                else:
                    st.info("Please select at least 2 brands to compare (maximum 4).")

# Footer
st.markdown("---")
st.caption("Pharmacy Store Locator Analytics Dashboard © 2025")