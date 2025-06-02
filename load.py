import sqlite3

def load_to_sqlite(df, db_name="transactions.db"):
    conn = sqlite3.connect(db_name)
    df.to_sql("transactions_cleaned", conn, if_exists="replace", index=False)
    conn.close()
