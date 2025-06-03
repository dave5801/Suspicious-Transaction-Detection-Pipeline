import sqlite3
import logging
from typing import Optional
from pathlib import Path

logger = logging.getLogger(__name__)

def load_to_sqlite(df, db_name: str = "transactions.db") -> Optional[bool]:
    """
    Load processed transaction data into SQLite database.
    
    Args:
        df: Processed DataFrame containing transaction data
        db_name (str): Name of the SQLite database file
        
    Returns:
        Optional[bool]: True if successful, None if failed
        
    Raises:
        sqlite3.Error: If database operations fail
    """
    try:
        # Ensure the database directory exists
        db_path = Path(db_name)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Connecting to database: {db_name}")
        conn = sqlite3.connect(db_name)
        
        # Create indices for better query performance
        logger.info("Creating database indices")
        df.to_sql("transactions_cleaned", conn, if_exists="replace", index=False)
        
        # Create indices on commonly queried columns
        cursor = conn.cursor()
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_flagged ON transactions_cleaned(flagged)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_risk_score ON transactions_cleaned(risk_score)")
        
        # Add metadata table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_metadata (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                total_transactions INTEGER,
                flagged_transactions INTEGER,
                avg_risk_score FLOAT
            )
        """)
        
        # Insert metadata
        cursor.execute("""
            INSERT INTO pipeline_metadata (total_transactions, flagged_transactions, avg_risk_score)
            VALUES (?, ?, ?)
        """, (
            len(df),
            df['flagged'].sum(),
            df['risk_score'].mean() if 'risk_score' in df.columns else 0
        ))
        
        conn.commit()
        logger.info("Successfully loaded data into database")
        return True
        
    except sqlite3.Error as e:
        logger.error(f"Database error: {str(e)}")
        raise
        
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise
        
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Database connection closed")
