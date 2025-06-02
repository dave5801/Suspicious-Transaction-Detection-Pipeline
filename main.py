from extract import extract_transactions
from transform import transform
from load import load_to_sqlite

def main():
    df = extract_transactions("sample_data/transactions.csv")
    df_cleaned = transform(df)
    load_to_sqlite(df_cleaned)

if __name__ == "__main__":
    main()
