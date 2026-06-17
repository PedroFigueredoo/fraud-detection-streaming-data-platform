````markdown
# Troubleshooting

## Docker

### Problem

Container does not start.

### Possible Causes

- port already in use;
- insufficient memory;
- invalid environment variable;
- volume permission issue.

### Useful Commands

```bash
docker compose ps
docker compose logs -f
docker compose down -v
````

---

## Kafka

### Problem

Producer cannot connect to Kafka.

### Possible Causes

- using wrong bootstrap server;
    
- topic does not exist;
    
- Kafka still starting.
    

### Bootstrap Servers

From host machine:

```text
localhost:9092
```

From Docker network:

```text
kafka:29092
```

---

## Airflow

### Problem

DAG does not appear in UI.

### Possible Causes

- Python syntax error;
    
- file outside `dags/`;
    
- scheduler not running.
    

### Useful Commands

```bash
docker compose logs -f airflow-scheduler
```
