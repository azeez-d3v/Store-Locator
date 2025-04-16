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
    layout="wide",
    initial_sidebar_state="expanded"
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

# Sidebar
st.sidebar.title("Pharmacy Store Locator")
st.sidebar.image("https://img.icons8.com/color/96/000000/pharmacy-shop.png", width=100)

# Data fetching section
st.sidebar.header("Data Fetching")
fetch_brand = st.sidebar.selectbox(
    "Select pharmacy brand to fetch",
    ["dds", "amcal", "both"]
)

if st.sidebar.button("Fetch Data"):
    with st.spinner(f"Fetching data for {fetch_brand}..."):
        try:
            # Run the fetch operation asynchronously
            pharmacy_api = PharmacyLocations()
            
            async def fetch_data():
                if fetch_brand == "both":
                    await pharmacy_api.fetch_and_save_all()
                    st.sidebar.success("Successfully fetched data for both brands!")
                else:
                    # Create task for selected brand
                    details = await pharmacy_api.fetch_all_locations_details(fetch_brand)
                    if details:
                        pharmacy_api.save_to_csv(details, f"{fetch_brand}_pharmacies.csv")
                        add_fetch_log(fetch_brand, len(details), True)
                        st.sidebar.success(f"Successfully fetched {len(details)} {fetch_brand.upper()} locations!")
                    else:
                        add_fetch_log(fetch_brand, 0, False)
                        st.sidebar.error(f"No data found for {fetch_brand.upper()}")
                        
            # Run the async function
            asyncio.run(fetch_data())
        except Exception as e:
            add_fetch_log(fetch_brand, 0, False)
            st.sidebar.error(f"Error fetching data: {str(e)}")

# Fetch history
st.sidebar.header("Fetch History")
logs = load_logs()
if logs["fetch_history"]:
    history_df = pd.DataFrame(logs["fetch_history"])
    history_df["timestamp"] = pd.to_datetime(history_df["timestamp"])
    history_df["timestamp"] = history_df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    st.sidebar.dataframe(
        history_df[["timestamp", "brand", "count", "success"]],
        hide_index=True,
        use_container_width=True
    )
else:
    st.sidebar.info("No fetch history available")

# Main content
st.title("🏥 Pharmacy Analytics Dashboard")

# Data selection
csv_files = get_csv_files()
if not csv_files:
    st.warning("No data files found. Please fetch data first.")
else:
    file_options = {f.stem: str(f) for f in csv_files}
    selected_file = st.selectbox("Select data file to analyze", list(file_options.keys()))
    
    # Load the selected data
    df = load_data(file_options[selected_file])
    
    # Display record count
    st.header(f"📊 {selected_file} - {len(df)} locations")
    
    # Data overview
    with st.expander("Raw Data Preview"):
        st.dataframe(df, use_container_width=True)
    
    # Trading Hours section
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
                pharmacy_options = {name: idx for idx, name in pharmacies_with_hours}
                selected_pharmacy = st.selectbox("Select a pharmacy to view trading hours", list(pharmacy_options.keys()))
                selected_idx = pharmacy_options[selected_pharmacy]
                
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
            st.info("No trading hours data available in this dataset.")
                    
    # Key metrics
    st.header("Key Metrics")
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
    
    # Geographic distribution
    st.header("Geographic Distribution")
    tab1, tab2 = st.tabs(["By State", "Map View"])
    
    with tab1:
        if safe_column_check(df, "state"):
            state_counts = df["state"].value_counts().reset_index()
            state_counts.columns = ["State", "Count"]
            
            fig_states = px.bar(
                state_counts, 
                x="State", 
                y="Count",
                color="Count",
                text_auto=True,
                title=f"Pharmacy Distribution by State - {selected_file}"
            )
            fig_states.update_layout(height=500)
            st.plotly_chart(fig_states, use_container_width=True)
        else:
            st.warning("State data not available in this dataset")
    
    with tab2:
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
                    title=f"Pharmacy Locations - {selected_file}"
                )
                fig_map.update_layout(height=600)
                st.plotly_chart(fig_map, use_container_width=True)
            else:
                st.warning("No valid geographic data available for mapping")
        else:
            st.warning("Geographic data (latitude/longitude) not available")
    
    # Data Completeness Analysis
    st.header("Data Completeness Analysis")
    
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
    
    # Advanced Analysis
    st.header("Advanced Analysis")
    
    # Check if both files are available for comparison
    dds_file = OUTPUT_DIR / "dds_pharmacies.csv"
    amcal_file = OUTPUT_DIR / "amcal_pharmacies.csv"
    
    if dds_file.exists() and amcal_file.exists():
        st.subheader("Brand Comparison")
        
        dds_df = load_data(dds_file)
        amcal_df = load_data(amcal_file)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("DDS Locations", len(dds_df))
        with col2:
            st.metric("Amcal Locations", len(amcal_df))
        
        # State distribution comparison
        if safe_column_check(dds_df, "state") and safe_column_check(amcal_df, "state"):
            dds_states = dds_df["state"].value_counts().reset_index()
            dds_states.columns = ["State", "Count"]
            dds_states["Brand"] = "DDS"
            
            amcal_states = amcal_df["state"].value_counts().reset_index()
            amcal_states.columns = ["State", "Count"]
            amcal_states["Brand"] = "Amcal"
            
            combined_states = pd.concat([dds_states, amcal_states])
            
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
        else:
            st.warning("State data not available for comparison")
        
# Footer
st.markdown("---")
st.caption("Pharmacy Store Locator Analytics Dashboard © 2025")