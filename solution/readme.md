# DWH Coding Challenge Solution by Raymond Samuel

## Summary

This solution processes event logs for `accounts`, `cards`, and `saving_accounts` tables, reconstructs their historical states including all changes, joins them into a denormalized table, and identifies transactions based on changes in `balance` or `credit_used`.

## Implementation

- **Data Loading**: JSON event logs are loaded from the `data` directory.
- **Event Application**: Create and update operations are applied to reconstruct the historical tables.
- **Denormalization**: The tables are joined based on Resources definition.
- **Transaction Identification**: Transactions are identified as changes in `balance` or `credit_used`.

## How to Run

1. **Build Docker Image**:

    ```sh
    docker build -t dwh-coding-challenge .
    ```

2. **Run Docker Container (Windows)**:

    ```sh
    docker run --rm -v %cd%/data:/app/data dwh-coding-challenge
    ```

The script will print the historical tables, the joined table, and the identified transactions.

## Directory Structure
solution/
|-- data/
|   |-- accounts/
|   |-- cards/
|   `-- savings_accounts/
|-- Dockerfile
|-- main.py
|-- requirements.txt
`-- README.md

## Result Output
![stdout ](result.png)
