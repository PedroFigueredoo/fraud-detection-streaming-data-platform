# Monitoring and Alerts

## Goal

Monitor the fraud detection pipeline from both data and business perspectives.

## Operational Metrics

- number of processed events;
- number of invalid events;
- streaming latency;
- failed batches;
- Kafka consumer lag;
- model scoring errors.

## Business Metrics

- fraud rate;
- high-risk application rate;
- average risk score;
- false positive rate;
- false negative rate;
- score drift.

## Alert Rules

Examples:

- fraud rate exceeds threshold;
- score distribution changes more than expected;
- false positive rate increases for a protected group;
- data volume drops unexpectedly;
- malformed event rate increases.

## Alert Output

Initial implementation uses Python logs. Future versions may integrate Slack, email or dashboards.
```
