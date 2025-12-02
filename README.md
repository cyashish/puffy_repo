# Puffy – Head of Data Infrastructure & Analytics  
**Skills Test Submission** | Senior Director level

---

## Business Conclusion (TL;DR)

Puffy is a **high-converting, structurally attractive DTC mattress business** (0.64 % CVR, 35.2 % checkout completion — both top-decile).  
The company is currently **flying blind and burning $650–700k every 14 days** because **77 % of revenue is unattributable** due to tracking collapse on 2025-02-27.

**Root cause**: schema drift + missing client_id propagation + no UTM persistence.  
**Fixing analytics reliability is the single highest-ROI initiative** available — worth **$2.5–4.0M annual EBITDA**.

---

## Repository Overview

This repository delivers a **complete, production-grade analytics platform** that would have:
- Detected the Feb 27 attribution collapse **on day 1**
- Prevented $6.7k revenue overcounting
- Restored full channel visibility within hours

## Project Structure
puffy_repo/
├── part1-data-quality/
│   ├── validate_dataframework.py          # Streaming DQ + fail-fast + quarantine
│   ├── validation-dataframework.docx      # Full findings & methodology
│   └── schema.yaml
├── part2-transformation/
│   ├── transform.sql                      # Bronze → Silver → Gold ELT
│   ├── validation_tr.sql                  # 9/9 passing test suite
│   ├── documentation_transformation.docx
│   └── architecture_diagram.png
├── part3-analysis/
│   ├── Analysis_Recommendation.docx       # Executive summary + charts + $ impact
│   └── supporting_analysis/
├── part4-monitoring/
│   ├── monitor.py                         # 3-layer monitoring (health/quality/business)
│   ├── test_monitor.py
│   ├── documentation_monitor.docx
│   └── grafana_dashboard.json
├── data/                                   # Raw 14-day event files (not committed)
├── requirements.txt
└── README.md                               # You are here

----

## Key Deliverables & Results

| Part | What Was Delivered                                  | Outcome |
|------|-----------------------------------------------------|--------|
| 1    | Production-grade streaming data quality framework   | Detected schema drift, duplicate transactions ($6.7k inflation), 78.6 % journey loss |
| 2    | Scalable ELT pipeline (pure SQL) + full test suite  | 100 % revenue reconciliation, 9/9 tests passed |
| 3    | Executive analysis + financial impact modeling      | Identified measurement (not conversion) as constraint → $2.5–4.0M EBITDA unlock |
| 4    | 3-layer monitoring system with statistical alerts  | Would have fired P0 alert on Feb 27 within minutes |


---

## Quick Start (Local Validation)

```bash
# 1. Install
pip install -r requirements.txt

# 2. Run data quality framework
python part1-data-quality/validate_dataframework.py
# → dq_report.csv + console summary of all issues found

#3. Run transformations
# SQL transformation + tests designed for Snowflake/BigQuery (see part2-transformation/validation_tr.sql)
# 4. Run monitoring
python part4-monitoring/monitor.py --check_date 2025-03-04
# → GREEN/YELLOW/RED status + Slack alert simulation

