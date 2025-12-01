"""
Puffy E-Commerce Production Monitor
-----------------------------------
This script validates the integrity of the daily ETL pipeline.
It checks Schema, Data Quality (Nulls), and Business Logic (Revenue/Attribution).

Usage:
    python production_monitor.py --raw_dir ./data --gold_file puffy_transformed_attribution.csv --check_date 2025-03-04

Exit Codes:
    0: Success (Green/Yellow)
    2: Configuration Error (Missing files)
    3: Critical Data Failure (Red) -> Blocks downstream tasks
"""

import argparse
import glob
import os
import json
import sys
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ================= CONFIGURATION =================
# Columns that MUST exist
REQUIRED_RAW_COLS = ['client_id', 'timestamp', 'page_url', 'referrer']
REQUIRED_GOLD_COLS = ['transaction_id', 'revenue', 'first_click_channel', 'last_click_channel', 'conversion_time']

# Business Logic Thresholds
PAID_CHANNELS = ['Paid Search', 'Paid Social']
PAID_REV_THRESHOLD = 1000.0   # If Revenue > $1000, we expect at least $1 of Paid Media attribution
ROW_VOLUME_DELTA = 0.50       # Alert if row count varies by +/- 50% vs 7-day avg
NULL_CLIENTID_THRESHOLD = 0.05 # Alert if > 5% of users are null (Ghost Users)
REFERRER_BLACKOUT_THRESHOLD = 0.90 # Alert if > 90% of referrers are missing
UNATTRIBUTED_SHARE_THRESHOLD = 0.40 # Alert if > 40% of revenue is Unattributed

# Slack Alerting
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK") # Reads from environment variable
# =================================================

class ProductionMonitor:
    def __init__(self, raw_df, gold_df, check_date=None):
        self.raw = raw_df.copy()
        self.gold = gold_df.copy()
        self.alerts = [] 
        self.status = "GREEN"
        
        # Date Handling (Crucial for Backfills)
        if check_date:
            self.check_date = pd.to_datetime(check_date).date()
        else:
            # Default to the latest date found in the Gold data
            if 'conversion_time' in self.gold.columns:
                self.gold['conversion_time'] = pd.to_datetime(self.gold['conversion_time'])
                self.check_date = self.gold['conversion_time'].max().date()
            else:
                self.check_date = datetime.now().date()

    def _add_alert(self, severity, code, message, details=None):
        """Log an alert and upgrade system status if needed."""
        alert = {
            'timestamp': datetime.now().isoformat(),
            'check_date': str(self.check_date),
            'severity': severity,
            'code': code,
            'message': message,
            'details': details or {}
        }
        self.alerts.append(alert)
        
        # Logic: P0 = Critical (Red), P1 = Warning (Yellow)
        if severity == "P0":
            self.status = "RED"
        elif severity == "P1" and self.status != "RED":
            self.status = "YELLOW"

    def check_schema_integrity(self):
        """Layer 1: Do the tables have the right shape?"""
        # Check Raw
        for c in REQUIRED_RAW_COLS:
            if c not in self.raw.columns:
                self._add_alert("P0", "MISSING_RAW_COL", f"Missing Raw Column: {c}")
        
        # Check Gold
        for c in REQUIRED_GOLD_COLS:
            if c not in self.gold.columns:
                self._add_alert("P0", "MISSING_GOLD_COL", f"Missing Gold Column: {c}")

    def check_row_volume(self):
        """Layer 1: Did we get a normal amount of data?"""
        if 'timestamp' not in self.raw.columns: return

        # Pre-process raw dates
        self.raw['event_date'] = pd.to_datetime(self.raw['timestamp'], errors='coerce', utc=True).dt.date
        
        # Calculate daily counts
        daily_counts = self.raw.groupby('event_date').size().reset_index(name='count')
        daily_counts = daily_counts.sort_values('event_date')
        
        # Calculate 7-Day Rolling Average (Lagged by 1 day)
        daily_counts['rolling_avg'] = daily_counts['count'].rolling(window=7, min_periods=1).mean().shift(1)
        
        # Get stats for the check_date
        today_stats = daily_counts[daily_counts['event_date'] == self.check_date]
        
        if today_stats.empty:
            self._add_alert("P0", "NO_DATA", f"No raw rows found for date {self.check_date}")
            return

        count = today_stats.iloc[0]['count']
        avg = today_stats.iloc[0]['rolling_avg']
        
        # Anomaly Detection
        if pd.notna(avg) and avg > 0:
            upper_bound = avg * (1 + ROW_VOLUME_DELTA)
            lower_bound = avg * (1 - ROW_VOLUME_DELTA)
            
            if count < lower_bound or count > upper_bound:
                self._add_alert("P0", "VOLUME_ANOMALY", 
                                f"Row volume ({count}) deviated >50% from 7-day avg ({int(avg)})",
                                {'count': int(count), 'avg': int(avg)})
        
        self.daily_counts = daily_counts # Save for artifacts

    def check_data_quality(self):
        """Layer 2: Nulls and Ghost Users"""
        # Filter raw data to just the date we are checking
        daily_raw = self.raw[self.raw['event_date'] == self.check_date].copy()
        
        if daily_raw.empty: return

        # 1. Null Client IDs (Ghost Users)
        if 'client_id' in daily_raw.columns:
            null_rate = daily_raw['client_id'].isnull().mean()
            if null_rate > NULL_CLIENTID_THRESHOLD:
                self._add_alert("P1", "GHOST_USERS", f"Null Client ID Rate: {null_rate:.1%}", {'rate': null_rate})

        # 2. Referrer Blackout (The Mar 04 Detection)
        if 'referrer' in daily_raw.columns:
            # Count empty strings or NaNs as missing
            missing = daily_raw['referrer'].replace('', np.nan).isnull().mean()
            if missing > REFERRER_BLACKOUT_THRESHOLD:
                self._add_alert("P0", "REFERRER_BLACKOUT", f"Missing Referrer Rate: {missing:.1%}", {'rate': missing})

    def check_business_logic(self):
        """Layer 3: The 'Puffy Rules' (Revenue & Attribution)"""
        if 'conversion_time' not in self.gold.columns: return

        # Filter Gold to check_date
        self.gold['conversion_date'] = pd.to_datetime(self.gold['conversion_time']).dt.date
        daily_gold = self.gold[self.gold['conversion_date'] == self.check_date].copy()
        
        total_rev = daily_gold['revenue'].sum()

        # 1. Zero Revenue Panic
        if total_rev == 0:
            self._add_alert("P0", "ZERO_REVENUE", f"Total Revenue is $0.00 for {self.check_date}")
            return # Stop further checks if rev is 0

        # 2. Paid Media Blindness (The UTM Strip Check)
        paid_rev = daily_gold[daily_gold['last_click_channel'].isin(PAID_CHANNELS)]['revenue'].sum()
        
        if total_rev > PAID_REV_THRESHOLD and paid_rev == 0:
            self._add_alert("P0", "PAID_BLINDNESS", 
                            f"Total Rev is ${total_rev:,.0f} but Paid Channel Rev is $0. Check UTM parsing.",
                            {'total_rev': total_rev, 'paid_rev': paid_rev})

        # 3. High Unattributed Rate
        unattributed_rev = daily_gold[daily_gold['last_click_channel'] == 'Unattributed']['revenue'].sum()
        unattr_share = unattributed_rev / total_rev
        
        if unattr_share > UNATTRIBUTED_SHARE_THRESHOLD:
            self._add_alert("P1", "HIGH_UNATTRIBUTED", f"Unattributed Share is {unattr_share:.1%}", {'share': unattr_share})

    def run(self):
        print(f"--- STARTING MONITOR FOR {self.check_date} ---")
        self.check_schema_integrity()
        self.check_row_volume()
        self.check_data_quality()
        self.check_business_logic()
        return self.status, self.alerts

# ================= UTILITIES =================
def send_slack_alert(summary, alerts):
    if not SLACK_WEBHOOK: return
    
    color = "#FF0000" if "RED" in summary else "#FFFF00"
    payload = {
        "text": f"*{summary}*",
        "attachments": [{"color": color, "text": json.dumps(a['message']) for a in alerts}]
    }
    try:
        requests.post(SLACK_WEBHOOK, json=payload, timeout=5)
        print("Slack alert sent.")
    except Exception as e:
        print(f"Failed to send Slack alert: {e}")

def save_artifacts(alerts, counts_df, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    pd.DataFrame(alerts).to_csv(os.path.join(output_dir, "monitor_alerts.csv"), index=False)
    if counts_df is not None:
        counts_df.to_csv(os.path.join(output_dir, "monitor_volume_stats.csv"), index=False)

# ================= MAIN EXECUTION =================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw_dir", required=True, help="Directory containing raw events_*.csv files")
    parser.add_argument("--gold_file", required=True, help="Path to attribution csv")
    parser.add_argument("--check_date", help="Specific date to check (YYYY-MM-DD). Defaults to latest gold date.")
    parser.add_argument("--output_dir", default="monitor_artifacts", help="Folder to save CSV logs")
    
    args = parser.parse_args()

    # 1. Load Data
    try:
        # Load Gold
        gold_df = pd.read_csv(args.gold_file)
        
        # Load Raw (Find all files in directory)
        # Fix: use os.path.join for OS independence
        search_path = os.path.join(args.raw_dir, "events_*.csv")
        raw_files = sorted(glob.glob(search_path))
        
        if not raw_files:
            print(f"[FATAL] No raw files found in {search_path}")
            sys.exit(2)
            
        # In production, we might only load the file for the specific date, 
        # but for this logic we load them all to calculate rolling averages.
        raw_dfs = []
        for f in raw_files:
            df = pd.read_csv(f)
            # Normalize schema drift immediately upon load to prevent false schema alerts
            if 'clientId' in df.columns: df.rename(columns={'clientId': 'client_id'}, inplace=True)
            raw_dfs.append(df)
        
        raw_df = pd.concat(raw_dfs, ignore_index=True)

    except Exception as e:
        print(f"[FATAL] Failed to load data: {e}")
        sys.exit(2)

    # 2. Run Monitor
    monitor = ProductionMonitor(raw_df, gold_df, check_date=args.check_date)
    status, alerts = monitor.run()

    # 3. Reporting
    print(f"\nFINAL STATUS: {status}")
    for a in alerts:
        print(f"[{a['severity']}] {a['message']}")

    # 4. Save Artifacts
    save_artifacts(alerts, getattr(monitor, 'daily_counts', None), args.output_dir)

    # 5. Alerting & Exit
    if alerts:
        send_slack_alert(f"Data Pipeline Status: {status}", alerts)

    if status == "RED":
        print("🚨 CRITICAL FAILURE. Blocking Pipeline.")
        sys.exit(3) # This signal tells Airflow/dbt to FAIL the job
    else:
        print("✅ Pipeline Healthy (or Warnings only).")
        sys.exit(0)