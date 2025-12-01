import pandas as pd
import matplotlib.pyplot as plt
import os
import argparse

def generate_charts(transformed_dir, output_dir='charts/'):
    os.makedirs(output_dir, exist_ok=True)
    sessions = pd.read_parquet(os.path.join(transformed_dir, 'sessions.parquet'))
    sessions['date'] = sessions['start_time'].dt.date
    daily_rev = sessions.groupby('date')['revenue'].sum()
    plt.figure(figsize=(10,5))
    daily_rev.plot(kind='line')
    plt.title('Daily Revenue')
    plt.savefig(os.path.join(output_dir, 'daily_revenue.png'))
    plt.close()
    source_rev = sessions.groupby('source')['revenue'].sum()
    plt.figure(figsize=(10,5))
    source_rev.plot(kind='bar')
    plt.title('Revenue by Source')
    plt.savefig(os.path.join(output_dir, 'source_revenue.png'))
    plt.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--transformed_dir', default='transformed/')
    args = parser.parse_args()
    generate_charts(args.transformed_dir)
