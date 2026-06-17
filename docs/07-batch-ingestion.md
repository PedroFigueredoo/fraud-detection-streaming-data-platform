# Batch Ingestion

## Goal

Load BAF data incrementally by month into DuckDB.

## Partition Strategy

The dataset contains a `month` column. This column is used to simulate monthly incremental ingestion.

## Airflow DAG

```text
check_source_file
  -> extract_months
  -> load_monthly_partition
  -> run_quality_checks
  -> publish_ingestion_summary
````

## Quality Checks

- row count by month;
    
- fraud count by month;
    
- null checks;
    
- protected attribute distribution;
    
- duplicate check;
    
- schema validation.
    

## Output

The ingestion process writes monthly partitions to the local analytical warehouse.

