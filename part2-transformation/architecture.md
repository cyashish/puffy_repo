## Transformation

%% Production Architecture

flowchart TD

&nbsp;   A\[S3 Raw Landing<br/>date-partitioned CSVs<br/>events\_YYYYMMDD.csv] 

&nbsp;   --> B{Data Quality Validator<br/>Python + Schema Contract}



&nbsp;   B -->|CRITICAL / Duplicates / Drift| C\[S3 Quarantine Bucket<br/>+ Slack #data-alerts<br/>+ CloudWatch Alarm]

&nbsp;   B -->|PASS| D\[S3 Clean / Trusted Zone]



&nbsp;   D --> E\[Airflow DAG<br/>hourly schedule]

&nbsp;   E --> F\[dbt Cloud / Core<br/>sessions | attribution | funnel]

&nbsp;   F --> G\[Snowflake Warehouse<br/>partitioned by event\_date]



&nbsp;   G --> H\[Looker Dashboards<br/>Executive + Marketing<br/>5-min refresh]

&nbsp;   G --> I\[Grafana Monitoring<br/>Freshness | Volume<br/>Revenue Z-score | Duplicate Rate]



&nbsp;   B -->|fail-fast| J\[Airflow Task Failure<br/>→ PagerDuty On-Call]

&nbsp;   I --> K\[Slack + PagerDuty<br/>Critical alerts only]



&nbsp;   style A fill:#e3f2fd,stroke:#1976d2

&nbsp;   style C fill:#ffebee,stroke:#c62828,stroke-width:3px

&nbsp;   style J fill:#fff3e0,stroke:#ef6c00

&nbsp;   style H fill:#e8f5e8,stroke:#2e7d32

&nbsp;   style I fill:#f3e5f5,stroke:#7b1fa2



