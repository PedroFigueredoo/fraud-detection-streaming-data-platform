Engineering Decisions

## Decision 001 — Use BAF Dataset

### Context

The project needs a public fraud dataset that supports temporal analysis, class imbalance, and fairness evaluation.

### Decision

Use the BAF - Bank Account Fraud dataset.

### Reasoning

The dataset includes realistic fraud characteristics, a temporal column and protected attributes, making it useful for drift and fairness analysis.

### Trade-offs

The data is not a live production stream. Kafka streaming is simulated by replaying dataset rows as events.

---

## Decision 002 — Use DuckDB as Local Warehouse

### Context

The project needs a lightweight analytical database for local development.

### Decision

Use DuckDB.

### Reasoning

DuckDB is simple, file-based, fast for analytical workloads and works well with Python and dbt.

### Trade-offs

DuckDB does not represent a distributed production warehouse, but it is appropriate for a local portfolio project.

---

## Decision 003 — Use Kafka for Event Simulation

### Context

Fraud detection requires near-real-time processing.

### Decision

Use Kafka to simulate account application events.

### Reasoning

Kafka makes the architecture closer to real event-driven systems used in financial platforms.

### Trade-offs

Events are replayed from a static dataset, not received from a real application API.
## Known Limitations

- Kafka events are simulated from a static dataset.
- DuckDB is used as a local analytical warehouse, not as a production distributed database.
- The initial ML model prioritizes simplicity and integration over maximum predictive performance.
- Real-time scoring is implemented for educational purposes and does not represent a regulated production fraud system.