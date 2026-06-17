# Data Pipeline

## Pipeline Layers

```text
raw
  -> bronze
  -> silver
  -> gold
  -> monitoring / scoring / analytics
````

## Raw Layer

Stores data as received from the source.

## Bronze Layer

Applies basic typing, renaming and metadata columns.

## Silver Layer

Applies cleaning, null handling, feature derivation and deduplication.

## Gold Layer

Creates analytical tables for ML, monitoring and business analysis.

## Main Pipeline Outputs

- account applications table;
    
- fraud monitoring table;
    
- model scoring table;
    
- flagged applications table;
    
- fairness metrics table;
    
- drift metrics table.
    
