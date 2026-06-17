# Project Overview

## Problem

Fraud detection in digital bank account opening requires fast ingestion, reliable data processing, real-time scoring, monitoring and continuous model evaluation.

This project simulates a real-time fraud detection data platform using the Bank Account Fraud dataset.

## Goal

Build an end-to-end data engineering platform that supports:

- batch ingestion;
- streaming ingestion;
- feature engineering;
- fraud scoring;
- drift analysis;
- fairness analysis;
- monitoring;
- analytical SQL views.

## Main Technologies

- Docker
- Apache Kafka
- Apache Spark
- Spark Structured Streaming
- Apache Airflow
- DuckDB
- dbt
- Python
- scikit-learn
- SQL

## Business Questions

- How many account applications are potentially fraudulent?
- Does fraud behavior change over time?
- Does model performance degrade across months?
- Are some demographic groups disproportionately affected by false positives?
- Can we detect high-risk applications in near real time?


## Project Scope

This project is not intended to maximize fraud model performance.  
The main goal is to demonstrate data engineering skills applied to a realistic fraud detection scenario.

The focus is on:

- reliable ingestion;
- batch and streaming processing;
- data quality;
- analytical modeling;
- temporal evaluation;
- monitoring;
- drift and fairness analysis.

Out of scope for the first version:

- production-grade cloud deployment;
- advanced model tuning;
- real customer data;
- real-time API serving;
- Kubernetes deployment.

## Known Limitations

- Kafka events are simulated from a static dataset.
- DuckDB is used as a local analytical warehouse, not as a production distributed database.
- The initial ML model prioritizes simplicity and integration over maximum predictive performance.
- Real-time scoring is implemented for educational purposes and does not represent a regulated production fraud system.