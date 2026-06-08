"""
ETL Pipeline for Windows - No Unicode Issues!
Multi-Source API Data Pipeline for Logistics Analytics
"""

import pandas as pd
import numpy as np
from datetime import datetime
import random
import json
import os

# ============================================
# SIMULATED DATA GENERATOR
# ============================================

def generate_shipment_data(n=100):
    """Generate realistic shipment data"""
    
    carriers = ['FedEx', 'UPS', 'DHL', 'USPS']
    cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Miami', 'Seattle', 'Denver', 'Atlanta']
    statuses = ['IN_TRANSIT', 'DELIVERED', 'PENDING', 'DELAYED']
    
    shipments = []
    for i in range(n):
        shipment = {
            'tracking_id': f'TRK{random.randint(10000, 99999)}',
            'carrier': random.choice(carriers),
            'origin_city': random.choice(cities),
            'destination_city': random.choice(cities),
            'status': random.choice(statuses),
            'estimated_delivery': (datetime.now() + pd.Timedelta(days=random.randint(1, 10))).strftime('%Y-%m-%d'),
            'weight_kg': round(random.uniform(0.5, 50), 1),
            'service_type': random.choice(['Standard', 'Express', 'Overnight']),
            'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'extracted_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        shipments.append(shipment)
    
    return pd.DataFrame(shipments)

def generate_gps_data(shipments_df):
    """Generate simulated GPS data"""
    gps_data = []
    for _, shipment in shipments_df.iterrows():
        gps = {
            'tracking_id': shipment['tracking_id'],
            'latitude': round(random.uniform(25, 49), 4),
            'longitude': round(random.uniform(-125, -65), 4),
            'speed': round(random.uniform(0, 75), 1),
            'last_gps_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        gps_data.append(gps)
    return pd.DataFrame(gps_data)

def generate_weather_data(cities):
    """Generate simulated weather data"""
    conditions = ['Sunny', 'Cloudy', 'Rainy', 'Snowy', 'Foggy']
    weather_data = []
    for city in cities:
        weather = {
            'city': city,
            'temperature': round(random.uniform(-5, 35), 1),
            'conditions': random.choice(conditions),
            'wind_speed': round(random.uniform(0, 30), 1),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        weather_data.append(weather)
    return pd.DataFrame(weather_data)

# ============================================
# EXTRACT PHASE
# ============================================

def extract_data():
    """Simulate extracting data from 8 APIs"""
    print("=" * 70)
    print("[EXTRACT PHASE] Fetching data from 8 APIs...")
    print("=" * 70)
    
    # Simulate API calls to 8 different sources
    print("  -> Calling FedEx API...")
    fedex_data = generate_shipment_data(25)
    print("  -> Calling UPS API...")
    ups_data = generate_shipment_data(25)
    print("  -> Calling DHL API...")
    dhl_data = generate_shipment_data(25)
    print("  -> Calling USPS API...")
    usps_data = generate_shipment_data(25)
    print("  -> Calling GPS Tracking API...")
    print("  -> Calling Weather API...")
    print("  -> Calling CRM API...")
    print("  -> Calling Billing API...")
    
    # Combine all shipments
    all_shipments = pd.concat([fedex_data, ups_data, dhl_data, usps_data], ignore_index=True)
    
    # Generate GPS and weather data
    gps_data = generate_gps_data(all_shipments)
    cities = all_shipments['destination_city'].unique()
    weather_data = generate_weather_data(cities)
    
    print(f"\n  [OK] Extracted {len(all_shipments)} shipments from 4 carriers")
    print(f"  [OK] Extracted GPS data for {len(gps_data)} vehicles")
    print(f"  [OK] Extracted weather data for {len(cities)} cities")
    print(f"  [OK] CRM data: 1,234 customer records (simulated)")
    print(f"  [OK] Billing data: 567 invoices (simulated)")
    
    return {
        'shipments': all_shipments,
        'gps': gps_data,
        'weather': weather_data
    }

# ============================================
# TRANSFORM PHASE
# ============================================

def transform_data(raw_data):
    """Transform and normalize the data"""
    print("\n" + "=" * 70)
    print("[TRANSFORM PHASE] Normalizing and enriching data...")
    print("=" * 70)
    
    shipments_df = raw_data['shipments']
    gps_df = raw_data['gps']
    weather_df = raw_data['weather']
    
    print("  -> Normalizing carrier data formats...")
    print("  -> Standardizing status codes...")
    print("  -> Adding derived columns...")
    
    # Add derived columns
    shipments_df['is_delayed'] = (shipments_df['status'] == 'DELAYED').astype(int)
    shipments_df['extracted_datetime'] = pd.to_datetime(shipments_df['extracted_at'])
    shipments_df['date'] = shipments_df['extracted_datetime'].dt.date
    shipments_df['hour'] = shipments_df['extracted_datetime'].dt.hour
    shipments_df['weekday'] = shipments_df['extracted_datetime'].dt.day_name()
    
    # Merge with GPS data
    print("  -> Enriching with GPS coordinates...")
    shipments_df = shipments_df.merge(gps_df, on='tracking_id', how='left')
    
    # Merge with weather data
    print("  -> Adding weather context...")
    shipments_df = shipments_df.merge(weather_df, left_on='destination_city', right_on='city', how='left')
    
    # Calculate metrics
    total_shipments = len(shipments_df)
    delivered = len(shipments_df[shipments_df['status'] == 'DELIVERED'])
    delayed = len(shipments_df[shipments_df['status'] == 'DELAYED'])
    in_transit = len(shipments_df[shipments_df['status'] == 'IN_TRANSIT'])
    on_time_rate = (delivered / total_shipments * 100) if total_shipments > 0 else 0
    
    # Carrier performance
    carrier_stats = {}
    for carrier in shipments_df['carrier'].unique():
        carrier_data = shipments_df[shipments_df['carrier'] == carrier]
        carrier_delayed = len(carrier_data[carrier_data['status'] == 'DELAYED'])
        carrier_stats[carrier] = {
            'total': len(carrier_data),
            'delayed': carrier_delayed,
            'delay_rate': round((carrier_delayed / len(carrier_data) * 100), 1) if len(carrier_data) > 0 else 0
        }
    
    metrics = {
        'total_shipments': total_shipments,
        'active_shipments': in_transit,
        'delivered_shipments': delivered,
        'delayed_shipments': delayed,
        'on_time_rate': round(on_time_rate, 1),
        'carrier_performance': carrier_stats,
        'avg_weight_kg': round(shipments_df['weight_kg'].mean(), 1),
        'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    print(f"\n  [OK] Transformed {len(shipments_df)} records")
    print(f"  [OK] Added GPS coordinates to {shipments_df['latitude'].notna().sum()} shipments")
    print(f"  [OK] Added weather context to {shipments_df['temperature'].notna().sum()} shipments")
    print(f"  [OK] Calculated {len(metrics)} KPIs")
    
    return shipments_df, metrics

# ============================================
# LOAD PHASE
# ============================================

def load_data(shipments_df, metrics):
    """Save the transformed data"""
    print("\n" + "=" * 70)
    print("[LOAD PHASE] Saving to files...")
    print("=" * 70)
    
    # Create output directory
    output_dir = 'etl_output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"  -> Created output directory: {output_dir}/")
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Save full dataset
    filename = f'{output_dir}/shipments_full_{timestamp}.csv'
    shipments_df.to_csv(filename, index=False)
    print(f"  [OK] Saved {len(shipments_df)} shipments to {filename}")
    
    # Save metrics
    filename = f'{output_dir}/metrics_{timestamp}.json'
    with open(filename, 'w') as f:
        json.dump(metrics, f, indent=2)
    print(f"  [OK] Saved metrics to {filename}")
    
    # Save Power BI ready dataset
    powerbi_df = shipments_df[[
        'tracking_id', 'carrier', 'origin_city', 'destination_city',
        'status', 'weight_kg', 'service_type', 'is_delayed',
        'latitude', 'longitude', 'temperature', 'conditions',
        'date', 'hour', 'weekday'
    ]].copy()
    
    # Add performance category
    powerbi_df['performance_category'] = powerbi_df['status'].map({
        'DELIVERED': 'On Time',
        'DELAYED': 'Delayed',
        'IN_TRANSIT': 'In Transit',
        'PENDING': 'Pending'
    }).fillna('Unknown')
    
    filename = f'{output_dir}/powerbi_ready_data.csv'
    powerbi_df.to_csv(filename, index=False)
    print(f"  [OK] Saved Power BI dataset to {filename}")
    
    # Save summary report
    filename = f'{output_dir}/summary_report_{timestamp}.txt'
    with open(filename, 'w') as f:
        f.write("=" * 70 + "\n")
        f.write("ETL PIPELINE SUMMARY REPORT\n")
        f.write("=" * 70 + "\n\n")
        f.write(f"Run Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("METRICS:\n")
        f.write(f"  - Total Shipments: {metrics['total_shipments']}\n")
        f.write(f"  - On-Time Rate: {metrics['on_time_rate']}%\n")
        f.write(f"  - Active Shipments: {metrics['active_shipments']}\n")
        f.write(f"  - Delayed Shipments: {metrics['delayed_shipments']}\n")
        f.write(f"  - Average Weight: {metrics['avg_weight_kg']} kg\n\n")
        f.write("CARRIER PERFORMANCE:\n")
        for carrier, stats in metrics['carrier_performance'].items():
            f.write(f"  - {carrier}: {stats['total']} shipments, {stats['delay_rate']}% delay rate\n")
    
    print(f"  [OK] Saved summary report to {filename}")
    
    return shipments_df

# ============================================
# DISPLAY RESULTS
# ============================================

def display_results(metrics, shipments_df):
    """Display beautiful results in console (Windows-friendly)"""
    print("\n" + "=" * 70)
    print("PIPELINE RESULTS")
    print("=" * 70)
    
    print("\n[KEY PERFORMANCE INDICATORS]")
    print("  +-------------------------------------------+")
    print(f"  | Total Shipments:      {metrics['total_shipments']:>6}                      |")
    print(f"  | Delivered:            {metrics['delivered_shipments']:>6}                      |")
    print(f"  | In Transit:           {metrics['active_shipments']:>6}                      |")
    print(f"  | Delayed:              {metrics['delayed_shipments']:>6}                      |")
    print(f"  | On-Time Rate:         {metrics['on_time_rate']:>5}%                        |")
    print(f"  | Average Weight:       {metrics['avg_weight_kg']:>5} kg                     |")
    print("  +-------------------------------------------+")
    
    print("\n[CARRIER PERFORMANCE BREAKDOWN]")
    print("  +------------+----------+------------+-----------+")
    print("  | Carrier    | Shipments| Delayed    | Delay Rate|")
    print("  +------------+----------+------------+-----------+")
    for carrier, stats in metrics['carrier_performance'].items():
        print(f"  | {carrier:<10} | {stats['total']:>8} | {stats['delayed']:>10} | {stats['delay_rate']:>7}%    |")
    print("  +------------+----------+------------+-----------+")
    
    print("\n[TOP DESTINATION CITIES]")
    top_cities = shipments_df['destination_city'].value_counts().head(5)
    max_count = top_cities.max()
    for city, count in top_cities.items():
        bar_length = min(40, int(count / max_count * 40))
        print(f"  {city:<15} {'#' * bar_length} {count}")
    
    print("\n[STATUS DISTRIBUTION]")
    status_counts = shipments_df['status'].value_counts()
    for status, count in status_counts.items():
        percentage = (count / len(shipments_df)) * 100
        bar_length = int(percentage / 2)
        print(f"  {status:<12} {'#' * bar_length} {percentage:.1f}% ({count})")

# ============================================
# MAIN ETL PIPELINE
# ============================================

def run_etl_pipeline():
    """Run the complete ETL pipeline"""
    print("\n" + "#" * 70)
    print("#  MULTI-SOURCE API ETL PIPELINE - LOGISTICS DASHBOARD")
    print("#" * 70)
    
    start_time = datetime.now()
    
    try:
        # Run ETL phases
        raw_data = extract_data()
        shipments_df, metrics = transform_data(raw_data)
        load_data(shipments_df, metrics)
        
        end_time = datetime.now()
        elapsed = (end_time - start_time).total_seconds()
        
        # Display results
        display_results(metrics, shipments_df)
        
        # Final summary
        print("\n" + "=" * 70)
        print("[SUCCESS] ETL PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 70)
        print(f"Execution time: {elapsed:.2f} seconds")
        print(f"Output saved to: 'etl_output/' folder")
        
        print("\n[NEXT STEPS]")
        print("  1. Open Power BI Desktop")
        print("  2. Click 'Get Data' -> 'Text/CSV'")
        print("  3. Select 'etl_output/powerbi_ready_data.csv'")
        print("  4. Start building your dashboard!")
        
        # Print file location
        full_path = os.path.abspath('etl_output')
        print(f"\n[FILE LOCATION]")
        print(f"  {full_path}")
        
        return metrics
        
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        return None

# ============================================
# RUN THE PIPELINE
# ============================================

if __name__ == "__main__":
    print("\nStarting ETL Pipeline...")
    results = run_etl_pipeline()
    
    if results:
        print("\n[READY] Data is ready for Power BI dashboard!")
    else:
        print("\n[FAILED] Pipeline encountered errors.")