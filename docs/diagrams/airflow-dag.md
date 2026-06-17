
```mermaid
flowchart TD
    A[check_source_file] --> B[extract_available_months]
    B --> C[load_monthly_partition]
    C --> D[run_data_quality_checks]
    D --> E[publish_ingestion_summary]
```
