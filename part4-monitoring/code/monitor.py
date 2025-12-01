import pandas as pd
import numpy as np
from scipy import stats
import argparse

def load_transformed(transformed_dir):
    sessions = pd.read_parquet(f'{transformed_dir}/sessions.parquet')
    sessions['date'] = sessions['start_time'].dt.date
    daily = sessions.groupby('date').agg(revenue=('revenue', 'sum'), events=('events_count', 'sum'))
    return daily

def detect_anomalies(df, metric, thresh=2.5):
    z = np.abs(stats.zscore(df[metric].dropna()))
    anomalies = df.iloc[z[z > thresh].index]
    return anomalies

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--transformed_dir', default='transformed/')
    args = parser.parse_args()
    daily = load_transformed(args.transformed_dir)
    anom_rev = detect_anomalies(daily, 'revenue')
    if not anom_rev.empty:
        print(f'Alert: Revenue anomalies {anom_rev}')
    print('Monitoring complete.')
