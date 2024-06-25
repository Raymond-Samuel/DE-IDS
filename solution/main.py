import os
import json
import pandas as pd
from glob import glob

def load_events(directory):
    events = []
    for filepath in glob(os.path.join(directory, '*.json')):
        with open(filepath, 'r') as file:
            events.append(json.load(file))
    return events

def process_events(events):
    records = {}
    for event in sorted(events, key=lambda x: x['ts']):
        record_id = event['id']
        if event['op'] == 'c':
            records[record_id] = event['data']
        elif event['op'] == 'u':
            if record_id in records:
                records[record_id].update(event['set'])
    return records

def load_and_process_table(directory):
    events = load_events(directory)
    records = process_events(events)
    return pd.DataFrame.from_dict(records, orient='index')

def main():
    # Load and process each table
    accounts_df = load_and_process_table('data/accounts')
    cards_df = load_and_process_table('data/cards')
    savings_accounts_df = load_and_process_table('data/savings_accounts')

    # Display the historical view of each table
    print("Accounts Table:")
    print(accounts_df)

    print("\nCards Table:")
    print(cards_df)

    print("\nSaving Accounts Table:")
    print(savings_accounts_df)

    # Join the tables to get a denormalized view
    joined_df = accounts_df.merge(cards_df, left_on='card_id', right_on='card_id', how='left', suffixes=('_account', '_card'))
    joined_df = joined_df.merge(savings_accounts_df, left_on='savings_account_id', right_on='savings_account_id', how='left', suffixes=('', '_saving_account'))

    print("\nJoined Table:")
    print(joined_df)

    # Identify transactions (changes in balance or credit used)
    transactions = []
    for event in load_events('data/cards'):
        if event['op'] == 'u' and 'credit_used' in event['set']:
            transactions.append({'id': event['id'], 'ts': event['ts'], 'type': 'credit_used', 'value': event['set']['credit_used']})

    for event in load_events('data/savings_accounts'):
        if event['op'] == 'u' and 'balance' in event['set']:
            transactions.append({'id': event['id'], 'ts': event['ts'], 'type': 'balance', 'value': event['set']['balance']})

    transactions_df = pd.DataFrame(transactions)

    print("\nTransactions:")
    print(transactions_df)

if __name__ == "__main__":
    main()
