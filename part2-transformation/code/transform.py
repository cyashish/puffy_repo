import pandas as pd
import os
from datetime import timedelta
import re
import json
import argparse
import numpy as np

def load_data(data_dir):
    dfs = []
    for file in sorted(os.listdir(data_dir)):
        if file.endswith('.csv'):
            df = pd.read_csv(os.path.join(data_dir, file))
            df['date'] = file.split('_')[1].replace('.csv', '')
            if 'client_id' in df.columns:
                df = df.rename(columns={'client_id': 'clientId'})
            if 'referrer' not in df.columns:
                df['referrer'] = np.nan
            dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

def parse_source(row):
    referrer = str(row['referrer']) if pd.notnull(row['referrer']) else ''
    page_url = str(row['page_url'])
    if 'source-' in referrer:
        match = re.search(r'source-([a-f0-9]+)', referrer)
        return match.group(1) if match else 'unknown'
    if 'utm_source' in page_url:
        utm = re.search(r'utm_source=([^&]+)', page_url)
        return utm.group(1) if utm else 'direct'
    return 'direct'

def extract_revenue(event_name, event_data):
    if event_name == 'checkout_completed' and pd.notnull(event_data):
        try:
            data = json.loads(event_data)
            return data.get('revenue', 0)
        except:
            return 0
    return 0

def sessionize(df):
    df = df.dropna(subset=['clientId'])
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values(['clientId', 'timestamp'])
    df['source'] = df.apply(parse_source, axis=1)
    df['revenue'] = df.apply(lambda row: extract_revenue(row['event_name'], row['event_data']), axis=1)
    df['time_diff'] = df.groupby('clientId')['timestamp'].diff()
    df['session_start'] = (df['time_diff'] > timedelta(minutes=30)) | df['time_diff'].isnull()
    df['session_id'] = df.groupby('clientId')['session_start'].cumsum()
    sessions = df.groupby(['clientId', 'session_id']).agg(
        start_time=('timestamp', 'min'),
        end_time=('timestamp', 'max'),
        source=('source', 'first'),
        events_count=('event_name', 'count'),
        revenue=('revenue', 'sum')
    ).reset_index()
    sessions['duration_min'] = (sessions['end_time'] - sessions['start_time']).dt.total_seconds() / 60
    sessions['converted'] = sessions['revenue'] > 0
    return sessions

def attribute(sessions, df):
    purchases = df[df['event_name'] == 'checkout_completed' and df['revenue'] > 0]
    attribs = []
    for _, p in purchases.iterrows():
        user_sessions = sessions[(sessions['clientId'] == p['clientId']) & (sessions['start_time'] < p['timestamp']) & (sessions['start_time'] > p['timestamp'] - timedelta(days=7))]
        if not user_sessions.empty:
            source = user_sessions['source'].iloc[-1]  # last-click
            attribs.append({'trans_id': p.name, 'source': source, 'revenue': p['revenue']})
    return pd.DataFrame(attribs)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_dir', default='data/')
    parser.add_argument('--output_dir', default='transformed/')
    args = parser.parse_args()
    os.makedirs(args.output_dir, exist_ok=True)
    df = load_data(args.data_dir)
    sessions = sessionize(df)
    sessions.to_parquet(os.path.join(args.output_dir, 'sessions.parquet'))
    attrib = attribute(sessions, df)
    attrib.to_parquet(os.path.join(args.output_dir, 'attributions.parquet'))
    print('Transformation complete.')
