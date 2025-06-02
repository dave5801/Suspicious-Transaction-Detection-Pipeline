def transform(df):
    # Example: Flag high-value transactions
    df['flagged'] = df['amount'] > 10000
    return df
