## Dataset Name

BAF - Bank Account Fraud Dataset

## Source

Feedzai / NeurIPS 2022.

## Why this dataset?

The BAF dataset is useful for this project because it includes:

- realistic bank account opening fraud patterns;
- temporal behavior through the `month` column;
- strong class imbalance;
- protected attributes for fairness analysis;
- multiple dataset variants for bias and drift evaluation.

## Main Target Column

| Column | Description |
|---|---|
| `fraud_bool` | Indicates whether the application is fraudulent |

## Temporal Column

| Column | Description |
|---|---|
| `month` | Used for temporal split and drift analysis |

## Protected / Sensitive Attributes

| Column | Usage |
|---|---|
| `customer_age` | Fairness and bias analysis |
| `employment_status` | Distribution and monitoring |
| `income` | Distribution and fairness monitoring |

## Important Notes

The model should not be evaluated using a random split only. Since the dataset contains temporal behavior, the project uses a temporal split:

- months 0-5 for training;
- months 6-11 for testing.

## Data Privacy Note

This project uses only public and privacy-preserving datasets.  
No real customer data, internal company data or sensitive banking information is used.

The streaming flow is simulated by replaying public dataset rows as events.