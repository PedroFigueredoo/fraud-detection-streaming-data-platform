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
  
  ## Repository Structure

```text
.
├── dags/                  # Airflow DAGs
├── data/                  # Raw and processed local data
├── docker/                # Docker images and service configs
├── docs/                  # Project documentation
├── ingestion/             # Batch ingestion scripts
├── kafka/                 # Kafka producer and consumer
├── ml/                    # Model training and scoring
├── modeling/              # dbt project and analytical models
├── monitoring/            # Drift, fairness and alerting scripts
├── processing/            # Spark batch/streaming jobs
├── spark/                 # Spark jobs and checkpoints
├── warehouse/             # DuckDB database file
├── docker-compose.yml
├── Makefile
└── README.md
```
## `How to Run`

### Initial Setup

```bash
git clone <repository-url>
cd fraud-detection-streaming-data-platform
```
### Start Environment

```bash
docker compose up -d --build
```

### Services

|Service|URL|
|---|---|
|Airflow|[http://localhost:8080](http://localhost:8080/)|
|Spark Master|[http://localhost:8081](http://localhost:8081/)|
|Kafka|localhost:9092|

### Airflow Credentials

```text
username: admin
password: admin
```

### Create Kafka Topic

```bash
make create-topic
```

### List Kafka Topics

```bash
make list-topics
```

### Check Running Containers

```bash
docker compose ps
```
