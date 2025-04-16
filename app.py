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
    st.subheader("Select pharmacy brand to fetch")
    fetch_dds = st.checkbox("Discount Drug Stores")
    fetch_amcal = st.checkbox("Amcal")
    fetch_blooms = st.checkbox("Blooms The Chemist")
    fetch_ramsay = st.checkbox("Ramsay Pharmacy")
    fetch_revive = st.checkbox("Revive Pharmacy")
    fetch_optimal = st.checkbox("Optimal Pharmacy Plus")
    fetch_community = st.checkbox("Community Care Chemist")
    fetch_footes = st.checkbox("Footes Pharmacy")
    
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
                    
                    if len(selected_brands) > 1:
                        # Fetch multiple brands
                        await pharmacy_api.fetch_and_save_all()
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
                if fetch_dds or fetch_amcal or fetch_blooms or fetch_ramsay or fetch_revive or fetch_optimal or fetch_community or fetch_footes:
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
                        state_counts = df["state"].value_counts().reset_index()
                        state_counts.columns = ["State", "Count"]
                        
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
                            
                            fig_map = px.scatter_mapbox(
                                map_df,
                                lat="latitude",
                                lon="longitude",
                                hover_name=hover_name,
                                hover_data=hover_data,
                                zoom=3,
                                mapbox_style="open-street-map",
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
                # Check if multiple pharmacy files are available for comparison
                dds_file = OUTPUT_DIR / "dds_pharmacies.csv"
                amcal_file = OUTPUT_DIR / "amcal_pharmacies.csv"
                blooms_file = OUTPUT_DIR / "blooms_pharmacies.csv"
                
                # Create a list of available brand files
                available_brands = []
                brand_dfs = {}
                
                if dds_file.exists():
                    available_brands.append(("DDS", dds_file))
                    brand_dfs["DDS"] = load_data(dds_file)
                
                if amcal_file.exists():
                    available_brands.append(("Amcal", amcal_file))
                    brand_dfs["Amcal"] = load_data(amcal_file)
                
                if blooms_file.exists():
                    available_brands.append(("Blooms", blooms_file))
                    brand_dfs["Blooms"] = load_data(blooms_file)
                
                if len(available_brands) >= 2:
                    st.subheader("Brand Comparison")
                    
                    # Display metrics for each brand
                    cols = st.columns(len(available_brands))
                    
                    for i, (brand_name, _) in enumerate(available_brands):
                        with cols[i]:
                            brand_df = brand_dfs[brand_name]
                            st.metric(f"{brand_name} Locations", len(brand_df))
                    
                    # State distribution comparison
                    brand_state_dfs = []
                    all_brands_have_state = True
                    
                    for brand_name, brand_df in brand_dfs.items():
                        if safe_column_check(brand_df, "state"):
                            states = brand_df["state"].value_counts().reset_index()
                            states.columns = ["State", "Count"]
                            states["Brand"] = brand_name
                            brand_state_dfs.append(states)
                        else:
                            all_brands_have_state = False
                            st.warning(f"State data not available for {brand_name}")
                    
                    if all_brands_have_state and brand_state_dfs:
                        combined_states = pd.concat(brand_state_dfs)
                        
                        fig_comparison = px.bar(
                            combined_states, 
                            x="State", 
                            y="Count", 
                            color="Brand",
                            barmode="group",
                            title="State Distribution Comparison"
                        )
                        fig_comparison.update_layout(height=500)
                        st.plotly_chart(fig_comparison, use_container_width=True)
                        
                    # Services comparison (if available)
                    services_available = all(
                        safe_column_check(brand_df, "services") for brand_df in brand_dfs.values()
                    )
                    
                    if services_available:
                        st.subheader("Services Comparison")
                        
                        all_services = {}
                        for brand_name, brand_df in brand_dfs.items():
                            if safe_column_check(brand_df, "services"):
                                service_counts = {}
                                # Count services across all locations
                                for _, row in brand_df.iterrows():
                                    services = row.get("services", [])
                                    if isinstance(services, str):
                                        try:
                                            # Try to convert string representation of list to actual list
                                            services = eval(services)
                                        except:
                                            services = [s.strip() for s in services.split(',') if s.strip()]
                                        
                                    if isinstance(services, list):
                                        for service in services:
                                            service = service.strip().lower()
                                            if service:
                                                service_counts[service] = service_counts.get(service, 0) + 1
                                
                                all_services[brand_name] = service_counts
                        
                        # Create a unified dataframe of services
                        if all_services:
                            service_df_data = []
                            
                            # Get top 10 services for each brand
                            for brand, services in all_services.items():
                                top_services = sorted(services.items(), key=lambda x: x[1], reverse=True)[:10]
                                for service, count in top_services:
                                    service_df_data.append({
                                        "Brand": brand,
                                        "Service": service.title(),
                                        "Count": count,
                                        "Percentage": (count / len(brand_dfs[brand])) * 100
                                    })
                            
                            if service_df_data:
                                service_df = pd.DataFrame(service_df_data)
                                
                                fig_services = px.bar(
                                    service_df,
                                    x="Service",
                                    y="Percentage",
                                    color="Brand",
                                    barmode="group",
                                    title="Top Services by Brand (% of Locations)",
                                    labels={"Percentage": "% of Brand Locations"}
                                )
                                fig_services.update_layout(height=500, xaxis={'categoryorder': 'total descending'})
                                st.plotly_chart(fig_services, use_container_width=True)
                                
                                # Display the raw data table
                                with st.expander("View Services Data"):
                                    st.dataframe(service_df)
                else:
                    st.info("To compare brands, please fetch data for at least two pharmacy brands.")

# Footer
st.markdown("---")
st.caption("Pharmacy Store Locator Analytics Dashboard © 2025")