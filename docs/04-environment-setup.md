## Requirements

- Docker
- Docker Compose
- Git
- Python 3.11+
- Java 20+
- Make

## Initial Setup

```bash
git clone <repository-url>
cd fraud-detection-streaming-data-platform
```
## Start Environment

```bash
docker compose up -d --build
```

## Services

|Service|URL|
|---|---|
|Airflow|[http://localhost:8080](http://localhost:8080/)|
|Spark Master|[http://localhost:8081](http://localhost:8081/)|
|Kafka|localhost:9092|

## Airflow Credentials

```text
username: admin
password: admin
```

## Create Kafka Topic

```bash
make create-topic
```

## Check Running Containers

```bash
docker compose ps
```
