# Streaming Design

## Goal

Simulate real-time bank account applications using Kafka and Spark Structured Streaming.

## Kafka Topic

| Topic | Description |
|---|---|
| `account-applications` | Raw account application events |

## Event Schema

```json
{
  "application_id": "string",
  "event_timestamp": "string",
  "month": 0,
  "customer_age": "string",
  "employment_status": "string",
  "income": 0,
  "fraud_bool": 0
}
````

## Spark Streaming Responsibilities

- consume events from Kafka;
    
- parse JSON schema;
    
- validate required fields;
    
- calculate streaming features;
    
- send invalid records to dead-letter queue;
    
- write scored applications to DuckDB or local parquet.
    

## Streaming Features

- application volume by time window;
    
- volume by customer age group;
    
- suspicious application rate by income range;
    
- application velocity by device/session features;
    
- score distribution by window.
    
