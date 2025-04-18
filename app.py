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
        "healthy_pharmacy": "Healthy Pharmacy",
        "healthy_world": "Healthy World Pharmacy",
        "pennas": "Pennas Pharmacy",
        "wizard": "Wizard Pharmacy",
        "chemist_hub": "Chemist Hub",
        "superchem": "SuperChem Pharmacy"
    }

    nz_pharmacy_brands = {
        "chemist_warehouse_nz": "Chemist Warehouse NZ"
    }

    # Combine all brands for backward compatibility
    all_pharmacy_brands = {**au_pharmacy_brands, **nz_pharmacy_brands}
    
    # Initialize session state for selected pharmacies if not exists
    if "selected_pharmacies" not in st.session_state:
        st.session_state["selected_pharmacies"] = []
    
    # Create a callback function to update session state
    def update_selection():
        # This function updates session state immediately when selection changes
        pass # Placeholder function to avoid errors
    
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
                # Preserve any NZ selections
                nz_selections = [b for b in st.session_state["selected_pharmacies"] if b in nz_pharmacy_brands]
                st.session_state["selected_pharmacies"] = au_brands + nz_selections
                st.rerun()
        
        with col2:
            if st.button("Clear AU Selection", key="clear_au_selection"):
                # Remove only AU brands from session state
                st.session_state["selected_pharmacies"] = [b for b in st.session_state["selected_pharmacies"] 
                                                          if b not in au_pharmacy_brands]
                st.rerun()
        
        # Get current AU selections
        current_au_selections = [b for b in st.session_state["selected_pharmacies"] if b in au_pharmacy_brands]
        
        # Use multiselect for AU pharmacy selection
        selected_au_pharmacies = st.multiselect(
            "Choose Australian pharmacy brands",
            options=list(au_pharmacy_brands.keys()),
            format_func=lambda x: au_pharmacy_brands[x],
            default=current_au_selections,
            key="selected_au_pharmacies"
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
                # Preserve any AU selections
                au_selections = [b for b in st.session_state["selected_pharmacies"] if b in au_pharmacy_brands]
                st.session_state["selected_pharmacies"] = au_selections + nz_brands
                st.rerun()
        
        with col2:
            if st.button("Clear NZ Selection", key="clear_nz_selection"):
                # Remove only NZ brands from session state
                st.session_state["selected_pharmacies"] = [b for b in st.session_state["selected_pharmacies"] 
                                                          if b not in nz_pharmacy_brands]
                st.rerun()
        
        # Get current NZ selections
        current_nz_selections = [b for b in st.session_state["selected_pharmacies"] if b in nz_pharmacy_brands]
        
        # Use multiselect for NZ pharmacy selection
        selected_nz_pharmacies = st.multiselect(
            "Choose New Zealand pharmacy brands",
            options=list(nz_pharmacy_brands.keys()),
            format_func=lambda x: nz_pharmacy_brands[x],
            default=current_nz_selections,
            key="selected_nz_pharmacies"
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
                        # Rest of your existing data overview code
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
                                        key="pharmacy_selector_au"
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
                            # Australian state analysis
                            if safe_column_check(df, "state"):
                                # Define valid Australian states
                                valid_states = {"NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT"}
                                
                                # Clean state data to standardize formatting
                                df_state = df.copy()
                                # Convert state column to proper format
                                df_state["state"] = df_state["state"].astype(str)
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
                                    # Additional variations
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
                                        title=f"Pharmacy Distribution by State - {selected_au_file}"
                                    )
                                    fig_states.update_layout(height=500)
                                    st.plotly_chart(fig_states, use_container_width=True)
                                else:
                                    st.warning("No valid Australian state data found in this dataset")
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
                                        title=f"Pharmacy Locations - {selected_au_file}"
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
                            if safe_column_check(df, "suburb"):
                                suburbs_count = df["suburb"].value_counts()
                                st.metric("Number of Suburbs", len(suburbs_count))
                            else:
                                st.metric("Number of Suburbs", "N/A")
                        
                        with col2:
                            if safe_column_check(df, "email"):
                                populated_email = df["email"].notna().sum()
                                email_percentage = int((populated_email / len(df)) * 100) if len(df) > 0 else 0
                                st.metric("Locations with Email", f"{populated_email} ({email_percentage}%)")
                            else:
                                st.metric("Locations with Email", "N/A")
                        
                        with col3:
                            if safe_column_check(df, "phone"):
                                has_phone = df["phone"].notna().sum()
                                phone_percentage = int((has_phone / len(df)) * 100) if len(df) > 0 else 0
                                st.metric("Locations with Phone", f"{has_phone} ({phone_percentage}%)")
                            else:
                                st.metric("Locations with Phone", "N/A")
                    
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
                                        key="pharmacy_selector_nz"
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
                        region_tab, map_tab = st.tabs(["By Region", "Map View"])
                        
                        with region_tab:
                            # New Zealand region analysis
                            st.subheader("New Zealand Regional Distribution")
                            
                            # For NZ data, use suburb field for regional analysis
                            if safe_column_check(df, "suburb"):
                                suburb_counts = df["suburb"].value_counts().reset_index()
                                suburb_counts.columns = ["Suburb", "Count"]
                                
                                # Sort by count in descending order
                                suburb_counts = suburb_counts.sort_values("Count", ascending=False)
                                
                                if not suburb_counts.empty:
                                    # Show top 15 suburbs
                                    top_suburbs = suburb_counts.head(15)
                                    
                                    fig_suburbs = px.bar(
                                        top_suburbs,
                                        x="Suburb", 
                                        y="Count",
                                        color="Count",
                                        text_auto=True,
                                        title=f"Top Suburbs - {selected_nz_file}"
                                    )
                                    fig_suburbs.update_layout(height=500)
                                    st.plotly_chart(fig_suburbs, use_container_width=True)
                                else:
                                    st.warning("No suburb data available for analysis")
                            else:
                                st.warning("Suburb data not available in this dataset")
                        
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
                                    if safe_column_check(map_df, "postcode"):
                                        hover_data.append("postcode")
                                        
                                    hover_name = "name" if safe_column_check(map_df, "name") else None
                                    
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
                compare_au_file = st.selectbox("Select Australian pharmacy brand", 
                                              list(au_files.keys()),
                                              key="compare_au_selector")
            else:
                st.warning("No Australian pharmacy data files available")
        
        with col2:
            if nz_files:
                compare_nz_file = st.selectbox("Select New Zealand pharmacy brand", 
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
            common_fields = ["email", "phone", "trading_hours"]
            
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
                st.plotly_chart(fig_comp, use_container_width=True)
                
                # Show data table
                st.dataframe(comp_df, use_container_width=True)
                
                # Trading hours comparison - if both have trading hours
                if all(safe_column_check(df, "trading_hours") for df in [au_df, nz_df]):
                    st.subheader("Trading Hours Comparison")
                    
                    # Function to get average opening and closing time for given day
                    def get_avg_hours(df, day):
                        opens = []
                        closes = []
                        
                        for _, row in df.iterrows():
                            try:
                                # Parse trading hours
                                if isinstance(row["trading_hours"], str):
                                    hours = json.loads(row["trading_hours"].replace("'", "\""))
                                elif isinstance(row["trading_hours"], dict):
                                    hours = row["trading_hours"]
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
            
        # Advanced analysis tab
        st.header("Advanced Analysis")
        
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
            
            # Standardize state/region data for all brands
            st.subheader("Geographic Distribution Comparison")
            
            # Check if we're comparing AU and NZ data
            has_au = any("country" not in df.columns or (df["country"].astype(str).str.upper() == "AU").any() for df in brand_dfs.values())
            has_nz = any("country" in df.columns and (df["country"].astype(str).str.upper() == "NZ").any() for df in brand_dfs.values())
            
            if has_au and has_nz:
                st.info("Cross-country comparison detected. Comparing by country.")
                
                # Count locations by country
                country_data = []
                for brand, df in brand_dfs.items():
                    # Determine country
                    if "country" in df.columns:
                        au_count = len(df[df["country"].astype(str).str.upper() != "NZ"])
                        nz_count = len(df[df["country"].astype(str).str.upper() == "NZ"])
                    else:
                        # Default to AU if no country specified
                        au_count = len(df)
                        nz_count = 0
                        
                    country_data.append({
                        "Brand": brand,
                        "Country": "Australia",
                        "Count": au_count
                    })
                    country_data.append({
                        "Brand": brand,
                        "Country": "New Zealand",
                        "Count": nz_count
                    })
                
                # Create country comparison chart
                country_df = pd.DataFrame(country_data)
                fig_country = px.bar(
                    country_df,
                    x="Brand",
                    y="Count",
                    color="Country",
                    barmode="group",
                    title="Locations by Country"
                )
                st.plotly_chart(fig_country, use_container_width=True)
            else:
                # All AU or all NZ - perform standard state analysis
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
                    # Additional variations
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
                    elif safe_column_check(brand_df, "suburb") and ("country" in brand_df.columns and (brand_df["country"].astype(str).str.upper() == "NZ").any()):
                        # For NZ data, use suburbs instead
                        suburb_counts = brand_df["suburb"].value_counts().reset_index()
                        suburb_counts.columns = ["Region", "Count"]
                        suburb_counts["Brand"] = brand_name
                        
                        # Only keep top 5 suburbs to avoid cluttering
                        brand_state_dfs.append(suburb_counts.head(5))
                
                if brand_state_dfs:
                    combined_data = pd.concat(brand_state_dfs)
                    
                    # Determine if we're comparing states or suburbs
                    if "State" in combined_data.columns:
                        # Working with states - use sorted order
                        state_order = ["NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT"]
                        combined_data["State_Order"] = combined_data["State"].apply(
                            lambda x: state_order.index(x) if x in state_order else 999
                        )
                        combined_data = combined_data.sort_values("State_Order")
                        
                        # Create the comparison chart
                        fig_comparison = px.bar(
                            combined_data, 
                            x="State", 
                            y="Count", 
                            color="Brand",
                            barmode="group",
                            category_orders={"State": state_order},
                            title="Pharmacy Distribution by State - Brand Comparison"
                        )
                        fig_comparison.update_layout(height=500)
                        st.plotly_chart(fig_comparison, use_container_width=True)
                    else:
                        # Working with NZ regions/suburbs
                        fig_regions = px.bar(
                            combined_data,
                            x="Region",
                            y="Count",
                            color="Brand",
                            barmode="group",
                            title="Pharmacy Distribution by Region - Brand Comparison"
                        )
                        fig_regions.update_layout(height=500)
                        st.plotly_chart(fig_regions, use_container_width=True)
                    
                    # Add a percentage view option
                    if st.checkbox("Show as percentage of brand total"):
                        # Calculate percentages
                        percentage_data = []
                        for brand in combined_data["Brand"].unique():
                            brand_data = combined_data[combined_data["Brand"] == brand].copy()
                            total = brand_data["Count"].sum()
                            if "State" in brand_data.columns:
                                brand_data["Percentage"] = (brand_data["Count"] / total * 100) if total > 0 else 0
                            else:
                                brand_data["Percentage"] = (brand_data["Count"] / total * 100) if total > 0 else 0
                            percentage_data.append(brand_data)
                        
                        percentage_df = pd.concat(percentage_data)
                        
                        # Create percentage chart
                        if "State" in percentage_df.columns:
                            fig_percentage = px.bar(
                                percentage_df,
                                x="State",
                                y="Percentage",
                                color="Brand",
                                barmode="group",
                                category_orders={"State": state_order},
                                title="Pharmacy Distribution by State (%) - Brand Comparison"
                            )
                        else:
                            fig_percentage = px.bar(
                                percentage_df,
                                x="Region",
                                y="Percentage",
                                color="Brand",
                                barmode="group",
                                title="Pharmacy Distribution by Region (%) - Brand Comparison"
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
                        email_pct = df["email"].notna().mean() * 100 if "email" in df.columns else 0
                        brand_row["Email Availability"] = email_pct
                        
                    if "Website Availability" in metrics_to_compare:
                        website_pct = df["website"].notna().mean() * 100 if "website" in df.columns else 0
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