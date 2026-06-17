# Drift and Fairness Analysis

## Goal

Evaluate model behavior over time and across protected groups.

## Drift Analysis

The dataset includes a `month` column, which allows temporal performance monitoring.

Questions:

- Does fraud rate change over time?
- Does score distribution change between months?
- Does model performance degrade in later months?
- Are there changes in feature distributions?

## Fairness Analysis

Protected attributes used for monitoring:

- customer_age;
- employment_status;
- income.

## Fairness Metrics

- false positive rate by group;
- false negative rate by group;
- approval/rejection rate by group;
- score distribution by group.

## Base vs Variant I

The project compares the BAF Base dataset with Variant I to analyze how controlled bias affects model behavior.

## Expected Outcome

The goal is not only to train a model, but to understand whether the data platform can support responsible fraud monitoring.
````
