import pandas as pd
import numpy as np
import json
import glob
import os
from datetime import timedelta

# 1. SETUP
ATTRIBUTION_WINDOW_DAYS = 7
INACTIVITY_TIMEOUT_MINS = 30

def run_transformation():
    # --- STEP 1: LOAD & CLEAN (Bronze) ---
    files = sorted(glob.glob("./data/events_*.csv"))
    print(f"Loading {len(files)} files...")
    
    all_events = []
    for file in files:
        try:
            df = pd.read_csv(file)
            # Fix Schema Drift (Feb 27)
            if 'clientId' in df.columns: df.rename(columns={'clientId': 'client_id'}, inplace=True)
            # Fix Missing Referrer (Mar 04)
            if 'referrer' not in df.columns: df['referrer'] = np.nan
            all_events.append(df)
        except Exception as e:
            print(f"Skipping {file}: {e}")
            
    if not all_events: return
    
    master_df = pd.concat(all_events, ignore_index=True)
    master_df['timestamp'] = pd.to_datetime(master_df['timestamp'])
    master_df.drop_duplicates(inplace=True)
    master_df = master_df[master_df['client_id'].notnull()] # Filter Ghost Users

    # --- STEP 2: SESSIONIZE (Silver) ---
    def classify_channel(row):
        url = str(row.get('page_url', '')).lower()
        ref = str(row.get('referrer', '')).lower()
        
        # Channel Logic
        if 'utm_medium=cpc' in url or 'utm_medium=paid' in url: return 'Paid Search'
        if 'utm_source=facebook' in url or 'utm_source=instagram' in url: return 'Paid Social'
        if 'utm_medium=email' in url: return 'Email'
        if 'google.' in ref: return 'Organic Search'
        if 'facebook.' in ref or 't.co' in ref: return 'Organic Social'
        if ref == 'nan' or ref == '': return 'Direct'
        return 'Referral'

    master_df['derived_channel'] = master_df.apply(classify_channel, axis=1)
    
    # Session Logic
    master_df.sort_values(['client_id', 'timestamp'], inplace=True)
    master_df['time_diff'] = master_df.groupby('client_id')['timestamp'].diff().dt.total_seconds() / 60
    master_df['is_new_session'] = (master_df['time_diff'] > INACTIVITY_TIMEOUT_MINS) | (master_df['time_diff'].isnull())
    master_df['session_id'] = master_df['client_id'].astype(str) + '_S' + master_df.groupby('client_id')['is_new_session'].cumsum().astype(str)
    
    # Create Silver Table
    silver_sessions = master_df.groupby('session_id').agg({
        'client_id': 'first',
        'timestamp': ['min', 'max'],
        'event_name': 'count',
        'derived_channel': 'first' # First Touch Attribution
    }).reset_index()
    silver_sessions.columns = ['session_id', 'client_id', 'session_start', 'session_end', 'events', 'channel']
    
    # --- STEP 3: ATTRIBUTE (Gold) ---
    conversions = master_df[master_df['event_name'].isin(['purchase', 'checkout_completed'])].copy()
    
    # Extract Revenue
    def parse_rev(x):
        try:
            d = json.loads(x)
            return pd.Series([float(d.get('value') or d.get('revenue') or 0), d.get('transaction_id')])
        except: return pd.Series([0.0, None])
        
    conversions[['revenue', 'transaction_id']] = conversions['event_data'].apply(parse_rev)
    conversions = conversions[conversions['revenue'] > 0].sort_values('timestamp').drop_duplicates('transaction_id')
    
    attribution_rows = []
    for _, conv in conversions.iterrows():
        # 7-Day Window Lookback
        lookback = conv['timestamp'] - timedelta(days=ATTRIBUTION_WINDOW_DAYS)
        mask = (silver_sessions['client_id'] == conv['client_id']) & \
               (silver_sessions['session_start'] < conv['timestamp']) & \
               (silver_sessions['session_start'] >= lookback)
        
        user_sessions = silver_sessions[mask].sort_values('session_start')
        
        if not user_sessions.empty:
            attribution_rows.append({
                'transaction_id': conv['transaction_id'],
                'revenue': conv['revenue'],
                'first_click': user_sessions.iloc[0]['channel'],
                'last_click': user_sessions.iloc[-1]['channel']
            })
        else:
            attribution_rows.append({
                'transaction_id': conv['transaction_id'],
                'revenue': conv['revenue'],
                'first_click': 'Unattributed',
                'last_click': 'Unattributed'
            })
            
    gold_attribution = pd.DataFrame(attribution_rows)
    
    # Save Files
    silver_sessions.to_csv('puffy_transformed_sessions.csv', index=False)
    gold_attribution.to_csv('puffy_transformed_attribution.csv', index=False)
    print("Success! Saved 'puffy_transformed_sessions.csv' and 'puffy_transformed_attribution.csv'")

if __name__ == "__main__":
    run_transformation()