## BAF - Bank Account Fraud Dataset

## Source

This project uses the Bank Account Fraud Dataset (BAF), a public and privacy-preserving dataset designed for fraud detection experiments.

The dataset is used only for educational and portfolio purposes. No real customer, banking, internal company or sensitive production data is used in this project.

## Local Files

The raw dataset files are stored locally under:

```text
data/raw/
```

Current local files:

`data/raw/` is the canonical pipeline input. Kaggle caches are acquisition
sources only; place downloaded files here before running batch or replay.

```text
Base.csv
Variant I.csv
Variant II.csv
Variant III.csv
Variant IV.csv
Variant V.csv
```

These files are intentionally ignored by Git and are not pushed to GitHub.

## Initial Dataset Used

The first batch ingestion step uses:

```text
data/raw/Base.csv
```

Initial validation results:

| Metric       |     Value |
| ------------ | --------: |
| Total rows   | 1,000,000 |
| Total months |         8 |
| Month range  |    0 to 7 |
| Fraud rows   |    11,029 |
| Fraud rate   |   1.1029% |

## Fraud Rate by Month

| Month | Total Rows | Fraud Rows | Fraud Rate |
| ----: | ---------: | ---------: | ---------: |
|     0 |    132,440 |      1,500 |    1.1326% |
|     1 |    127,620 |      1,198 |    0.9387% |
|     2 |    136,979 |      1,198 |    0.8746% |
|     3 |    150,936 |      1,392 |    0.9222% |
|     4 |    127,691 |      1,452 |    1.1371% |
|     5 |    119,323 |      1,411 |    1.1825% |
|     6 |    108,168 |      1,450 |    1.3405% |
|     7 |     96,843 |      1,428 |    1.4746% |

## Temporal Split Note

The original project plan considered a temporal split using months 0–5 for training and 6–11 for testing. However, the local `Base.csv` file currently available contains months 0–7.

For the Base dataset, the initial temporal split will therefore be:

```text
Train: months 0–5
Test: months 6–7
```

This decision avoids random shuffling and reduces temporal leakage.

## Privacy Note

The dataset files are not versioned in Git because they are large raw data files. Only ingestion code, documentation and reproducible pipeline logic are versioned.
