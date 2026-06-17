# ML Scoring

## Goal

Train a lightweight fraud detection model and integrate it into the streaming pipeline.

## Model

Initial model:

- Random Forest;
- class_weight = balanced;
- temporal split;
- evaluated using fraud-specific metrics.

## Temporal Split

| Period | Usage |
|---|---|
| Months 0-5 | Training |
| Months 6-11 | Testing |

## Metrics

The model is not evaluated using accuracy due to class imbalance.

Main metrics:

- AUC-ROC;
- Precision;
- Recall;
- F1-score;
- Precision-Recall AUC;
- false positive rate by group.

## Model Artifact

The trained model is serialized using `joblib`.

```text
ml/artifacts/fraud_model.joblib
````

## Scoring Output

High-risk applications are written to:

```text
flagged_applications
```
