import pandas as pd
from production_monitor import ProductionMonitor

def make_raw(df_rows, date_str):
    df = pd.DataFrame(df_rows)
    df['timestamp'] = pd.to_datetime(date_str)
    return df

def make_gold(rows, conv_date):
    df = pd.DataFrame(rows)
    df['conversion_time'] = pd.to_datetime(conv_date)
    return df

def test_zero_revenue_blocks():
    raw = make_raw([{'client_id': 'u1', 'referrer':'a','page_url':'/'}], "2025-03-04T00:00:00Z")
    gold = make_gold([{'transaction_id': 't1', 'revenue': 0.0, 'last_click_channel': 'Organic'}], "2025-03-04T01:00:00Z")
    mon = ProductionMonitor(raw, gold, check_date="2025-03-04")
    status, alerts = mon.run()
    assert status == "RED"
    assert any(a['code']=="ZERO_REVENUE" for a in alerts)
