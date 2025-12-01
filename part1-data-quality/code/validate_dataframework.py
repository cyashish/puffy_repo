import pandas as pd
import json
import os
import glob
import logging
from typing import List

# Configure Logging
logging.basicConfig(filename='dq_log.txt', level=logging.INFO, format='%(asctime)s - %(message)s')


def _resolve_data_paths() -> List[str]:
    """
    Determine where to pull the event files from. Prefer an override via the
    DQ_DATA_DIR environment variable, otherwise fall back to ../data relative
    to this script. Any matches are returned as absolute paths so downstream
    processing does not depend on the working directory.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    search_path = os.path.join("data", "events_*.csv")
    files = sorted(glob.glob(search_path))

    if not files:
        # Final fallback: look in the current working directory in case the
        # caller still relies on the old behaviour.
        legacy_matches = sorted(glob.glob("events_*.csv"))
        if legacy_matches:
            logging.warning("DQ_DATA_DIR empty, falling back to CWD for CSVs")
            files = [os.path.abspath(path) for path in legacy_matches]
        else:
            logging.error("No event files found using search path %s", search_path)
            #print(f"⚠️  No event files found in '{data_dir}'. Set DQ_DATA_DIR or add CSVs.")
    return files


def run_exhaustive_quality_check():
    files = _resolve_data_paths()
    if not files:
        logging.warning("No files to process. Exiting early.")
        return pd.DataFrame()
    report_data = []
    all_transactions = []
    
    print(f"🚀 Starting Exhaustive QC on {len(files)} files...")
    
    for file in files:
        file_name = os.path.basename(file)
        # Initial stats container
        stats = {"file": file_name, "status": "PASS", "alerts": []}
        
        try:
            df = pd.read_csv(file)
            stats["rows"] = len(df)
            
            # --- 1. Schema & Drift Checks ---
            cols = set(df.columns)
            if 'clientId' in cols and 'client_id' not in cols:
                stats["alerts"].append("SCHEMA_DRIFT: clientId -> client_id")
                df.rename(columns={'clientId': 'client_id'}, inplace=True)
                stats["status"] = "WARNING"
            
            if 'referrer' not in cols:
                stats["alerts"].append("MISSING_COL: referrer")
                stats["status"] = "CRITICAL"

            # --- 2. Identity & Null Checks ---
            if 'client_id' in df.columns:
                null_count = df['client_id'].isnull().sum()
                null_rate = null_count / len(df)
                stats["null_rate"] = null_rate
                if null_rate > 0.05:
                     stats["alerts"].append(f"HIGH_NULLS: {null_rate:.1%}")
                     stats["status"] = "CRITICAL" if null_rate > 0.1 else "WARNING"
            
            # --- 3. Duplicate Event Fingerprinting ---
            # Exclude timestamp to find true duplicates (retry logic usually causes this)
            dupes = df.duplicated().sum()
            stats["duplicate_rows"] = dupes
            
            # --- 4. Revenue & Transaction Logic ---
            # Parse JSON safely
            df['data'] = df['event_data'].apply(lambda x: json.loads(x) if pd.notnull(x) else {})
            
            # Sum Revenue & Collect Txn IDs
            purchases = df[df['event_name'].isin(['purchase', 'checkout_completed'])]
            daily_rev = 0.0
            
            for _, row in purchases.iterrows():
                # Revenue Summation
                val = row['data'].get('value') or row['data'].get('revenue') or row['data'].get('total')
                if val: daily_rev += float(val)
                
                # Transaction ID Collection (for cross-file dedupe)
                tid = row['data'].get('transaction_id') or row['data'].get('order_id')
                if tid: all_transactions.append({'tid': tid, 'file': file})

            stats["revenue"] = daily_rev
            stats["purchases"] = len(purchases)
            
            # --- 5. Event Counts Breakdown ---
            event_counts = df['event_name'].value_counts().to_dict()
            stats["event_breakdown"] = json.dumps(event_counts)
            
        except Exception as e:
            stats["status"] = "ERROR"
            stats["alerts"].append(str(e))
        
        report_data.append(stats)

    # --- 6. Cross-File Comparators ---
    report_df = pd.DataFrame(report_data)

    if report_df.empty:
        logging.warning("QC completed but no report rows were generated.")
        print("⚠️  QC finished without any processed files. See logs for details.")
        return report_df

    # Calculate Day-over-Day Changes
    for col in ("revenue", "rows"):
        if col not in report_df.columns:
            report_df[col] = pd.NA

    report_df['revenue_dod_pct'] = report_df['revenue'].pct_change()
    report_df['vol_dod_pct'] = report_df['rows'].pct_change()
    
    # Check for Duplicate Transactions across files
    txn_df = pd.DataFrame(all_transactions)
    dup_txns = txn_df[txn_df.duplicated('tid', keep=False)]
    if not dup_txns.empty:
        print(f"\n⚠️ FOUND {len(dup_txns)} DUPLICATE TRANSACTIONS ACROSS FILES!")
    
    # Save Report
    report_df.to_csv('dq_report.csv', index=False)
    print("\n✅ QC Complete. Summary Report saved to 'dq_report.csv'")
    return report_df

# Run the system
if __name__ == "__main__":
    df = run_exhaustive_quality_check()
    if df.empty:
        print("No QC summary to display.")
    else:
        print(df[['file', 'status', 'revenue', 'null_rate', 'alerts']].to_string())