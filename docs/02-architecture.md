# Architecture

## High-Level Architecture

```
flowchart LR
    A[BAF Dataset] --> B[Batch Ingestion - Airflow]
    A --> C[Kafka Producer]
    C --> D[Kafka Topic: account-applications]
    D --> E[Spark Structured Streaming]
    E --> F[Streaming Features]
    F --> G[Fraud Scoring]
    G --> H[Flagged Applications]

    B --> I[DuckDB Raw Layer]
    I --> J[dbt Bronze]
    J --> K[dbt Silver]
    K --> L[dbt Gold]
    L --> M[Analytics / Monitoring / ML Training]