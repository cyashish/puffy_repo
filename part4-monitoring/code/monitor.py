# production_monitor.py
"""
Production monitor implementing:
- Layer1: pipeline health (row presence / volume)
- Layer2: schema integrity, null-rate checks
- Layer3: business logic checks (revenue, paid attribution, unattributed share)

Usage:
    python production_monitor.py --raw_dir /path/to/raw --gold_file /path/to/puffy_transformed_attribution.csv
"""

import argparse
import glob
import os
import json
import sys
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

# ------------ CONFIG ------------
REQUIRED_RAW_COLS = ['client_id', 'timestamp', 'page_url', 'referrer']
REQUIRED_GOLD_COLS = ['transaction_id', 'revenue', 'first_click_channel', 'last_click_channel', 'conversion_time']

PAID_CHANNELS = ['Paid Search', 'Paid Social']
PAID_REV_THRESHOLD = 1000.0  # Puffy rule threshold
ROW_VOLUME_DELTA = 0.5  # 50%
NULL_CLIENTID_THRESHOLD = 0.05  # 5%
REFERRER_BLACKOUT_THRESHOLD = 0.90  # 90%
UNATTRIBUTED_SHARE_THRESHOLD = 0.40  # 40%

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK")  # optional
# ---------------------------------

class ProductionMonitor:
    def __init__(self, raw_df, gold_df, check_date=None):
        self.raw = raw_df.copy()
        self.gold = gold_df.copy()
        if check_date is None:
            # choose latest conversion date from gold
            if 'conversion_time' in self.gold.columns:
                self.check_date = pd.to_datetime(self.gold['conversion_time']).dt.date.max()
            else:
                self.check_date = pd.to_datetime('today').date()
        else:
            self.check_date = pd.to_datetime(check_date).date()
        self.alerts = []  # list of dicts {severity, code, message, details}
        self.status = "GREEN"  # GREEN, YELLOW, RED

    def _add_alert(self, severity, code, message, details=None):
        self.alerts.append({'severity': severity, 'code': code, 'message': message, 'details': details or {}})
        if severity == "P0":
            self.status = "RED"
        elif severity == "P1" and self.status != "RED":
            self.status = "YELLOW"

    def check_schema_integrity(self):
        # Raw
        for c in REQUIRED_RAW_COLS:
            if c not in self.raw.columns:
                self._add_alert("P0", "MISSING_RAW_COL", f"Missing Raw Column: {c}", {'column': c})
        # Gold
        for c in REQUIRED_GOLD_COLS:
            if c not in self.gold.columns:
                self._add_alert("P0", "MISSING_GOLD_COL", f"Missing Gold Column: {c}", {'column': c})

    def check_row_volume(self):
        # compute row counts per date for raw data and compare with 7-day avg (if available)
        if 'timestamp' not in self.raw.columns:
            return
        self.raw['timestamp_parsed'] = pd.to_datetime(self.raw['timestamp'], errors='coerce', utc=True)
        self.raw['date'] = self.raw['timestamp_parsed'].dt.date
        counts = self.raw.groupby('date').size().rename('rows').reset_index()
        if counts.empty:
            self._add_alert("P0", "NO_ROWS", "No rows found in raw data for any date", {})
            return
        counts = counts.sort_values('date')
        target_date = self.check_date
        # compute 7-day rolling avg up to day before target_date
        counts['rows_7d_avg'] = counts['rows'].rolling(7, min_periods=1).mean().shift(1)
        row_today = counts[counts['date'] == target_date]
        if row_today.empty:
            self._add_alert("P0", "NO_ROWS_TODAY", f"No rows for target date {target_date}", {})
            return
        row_val = int(row_today['rows'].iloc[0])
        row_avg = row_today['rows_7d_avg'].iloc[0]
        if pd.notna(row_avg) and row_avg > 0:
            if row_val < (1 - ROW_VOLUME_DELTA) * row_avg or row_val > (1 + ROW_VOLUME_DELTA) * row_avg:
                self._add_alert("P0", "ROW_VOLUME_DELTA", f"Row volume for {target_date} changed by >=50% vs 7d avg",
                                {'rows_today': row_val, 'rows_7d_avg': float(row_avg)})
        # persist rows metric for metrics output
        self.row_counts = counts

    def check_null_client_ids(self):
        if 'client_id' not in self.raw.columns:
            return
        target_mask = self.raw['date'] == self.check_date if 'date' in self.raw.columns else pd.Series([True] * len(self.raw))
        null_rate = self.raw.loc[target_mask, 'client_id'].isnull().mean()
        if null_rate > NULL_CLIENTID_THRESHOLD:
            self._add_alert("P1", "NULL_CLIENT_ID_RATE", f"High Null client_id rate: {null_rate:.1%}", {'null_rate': float(null_rate)})

    def check_referrer_blackout(self):
        if 'referrer' not in self.raw.columns or 'date' not in self.raw.columns:
            return
        mask = self.raw['date'] == self.check_date
        total = mask.sum()
        if total == 0:
            return
        missing = self.raw.loc[mask, 'referrer'].isnull() | (self.raw.loc[mask, 'referrer'].astype(str).str.strip() == '')
        missing_rate = missing.mean()
        if missing_rate >= REFERRER_BLACKOUT_THRESHOLD:
            self._add_alert("P0", "REFERRER_BLACKOUT", f"Referrer blackout detected: {missing_rate:.1%} missing for {self.check_date}",
                            {'missing_rate': float(missing_rate)})

    def check_business_logic(self):
        # Ensure conversion_time parsed
        if 'conversion_time' not in self.gold.columns or 'revenue' not in self.gold.columns:
            return
        self.gold['conversion_time'] = pd.to_datetime(self.gold['conversion_time'], errors='coerce', utc=True)
        target_mask = self.gold['conversion_time'].dt.date == self.check_date
        daily_rev_df = self.gold.loc[target_mask].copy()
        total_rev = daily_rev_df['revenue'].astype(float).sum() if not daily_rev_df.empty else 0.0

        # 1. Zero revenue
        if total_rev == 0.0:
            self._add_alert("P0", "ZERO_REVENUE", f"Total revenue is $0 for {self.check_date}", {'total_rev': total_rev})
            # blocking condition; we still continue to gather additional hints

        # 2. Puffy Rule - Paid channels = 0 but total revenue > threshold
        if total_rev > PAID_REV_THRESHOLD:
            paid_rev = daily_rev_df.loc[daily_rev_df['last_click_channel'].isin(PAID_CHANNELS), 'revenue'].astype(float).sum()
            if paid_rev == 0:
                self._add_alert("P0", "PAID_BLINDNESS", f"Paid channels revenue is $0 while total revenue is ${total_rev:.2f}", {'total_rev': total_rev})

        # 3. Unattributed share
        unattributed_rev = daily_rev_df.loc[daily_rev_df['last_click_channel'].fillna('') == 'Unattributed', 'revenue'].astype(float).sum()
        if total_rev > 0:
            unattr_share = unattributed_rev / total_rev
            if unattr_share > UNATTRIBUTED_SHARE_THRESHOLD:
                self._add_alert("P2", "HIGH_UNATTRIBUTED_SHARE", f"High unattributed revenue share: {unattr_share:.1%}", {'unattr_share': float(unattr_share)})

    def run_all(self):
        self.check_schema_integrity()
        self.check_row_volume()
        self.check_null_client_ids()
        self.check_referrer_blackout()
        self.check_business_logic()
        return self.status, self.alerts

# ---------------- Utility functions ----------------
def send_slack_message(msg):
    if not SLACK_WEBHOOK:
        return False
    import requests
    try:
        requests.post(SLACK_WEBHOOK, json={"text": msg}, timeout=5)
        return True
    except Exception:
        return False

def save_artifacts(alerts, row_counts, out_dir='.'):
    alerts_df = pd.DataFrame(alerts)
    alerts_df.to_csv(os.path.join(out_dir, 'monitoring_alerts.csv'), index=False)
    if hasattr(row_counts, 'to_csv'):
        row_counts.to_csv(os.path.join(out_dir, 'monitoring_metrics.csv'), index=False)

# --------------- CLI / Execution -------------------
def main(raw_dir, gold_file):
    # Load gold
    if not os.path.exists(gold_file):
        print(f"[FATAL] gold file not found: {gold_file}")
        sys.exit(2)
    gold_df = pd.read_csv(gold_file)
    # load latest raw events file
    raw_files = sorted(glob.glob(os.path.join(raw_dir, 'events_*.csv')))
    if not raw_files:
        print(f"[FATAL] no raw event files in {raw_dir}")
        sys.exit(2)
    raw_file = raw_files[-1]
    raw_df = pd.read_csv(raw_file)
    # normalize common column names
    if 'clientId' in raw_df.columns and 'client_id' not in raw_df.columns:
        raw_df = raw_df.rename(columns={'clientId':'client_id'})
    monitor = ProductionMonitor(raw_df, gold_df)
    status, alerts = monitor.run_all()
    # write artifacts
    try:
        save_artifacts(alerts, getattr(monitor, 'row_counts', pd.DataFrame()), out_dir='.')
    except Exception as e:
        print("Error saving artifacts:", e)
    # decide exit code and optional notifications
    # P0 -> exit non-zero so orchestrator can block downstream publish
    p0_present = any(a['severity']=='P0' for a in alerts)
    summary = f"MONITOR STATUS: {status} | Alerts: {len(alerts)}"
    print(summary)
    # optionally send slack
    if alerts and SLACK_WEBHOOK:
        send_slack_message(summary + "\n" + json.dumps(alerts, default=str, indent=2))
    if p0_present:
        # exit non-zero to block pipeline publish
        sys.exit(3)
    else:
        sys.exit(0)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw_dir", default=".", help="directory with events_*.csv raw files")
    parser.add_argument("--gold_file", default="puffy_transformed_attribution.csv", help="gold attribution CSV")
    args = parser.parse_args()
    main(args.raw_dir, args.gold_file)
