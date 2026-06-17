# Data Modeling with dbt

## Goal

Use dbt to model the analytical layers of the fraud detection platform.

## Layers

### Bronze

- rename columns;
- cast data types;
- add ingestion metadata;
- keep data close to source.

### Silver

- handle missing values;
- create derived features;
- standardize categorical values;
- remove duplicates.

### Gold

- create model-ready dataset;
- create monitoring marts;
- create fairness marts;
- create fraud analytics marts.

## Suggested Models

```text
models/
├── bronze/
│   └── br_account_applications.sql
├── silver/
│   └── sv_account_applications.sql
├── gold/
│   ├── gd_model_features.sql
│   ├── gd_fraud_monitoring.sql
│   ├── gd_drift_metrics.sql
│   └── gd_fairness_metrics.sql
````

## Tests

- not_null;
    
- accepted_values;
    
- unique;
    
- relationships;
    
- custom tests for fraud rate thresholds.
    
