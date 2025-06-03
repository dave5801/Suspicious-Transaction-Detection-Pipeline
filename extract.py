import pandas as pd
import logging
from pathlib import Path
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def extract_transactions(file_path: str) -> Optional[pd.DataFrame]:
    """
    Extract transaction data from a CSV file.
    
    Args:
        file_path (str): Path to the CSV file containing transaction data
        
    Returns:
        Optional[pd.DataFrame]: DataFrame containing transaction data or None if extraction fails
        
    Raises:
        FileNotFoundError: If the specified file doesn't exist
        ValueError: If the file doesn't contain required columns
    """
    try:
        # Validate file exists
        if not Path(file_path).exists():
            raise FileNotFoundError(f"Transaction file not found: {file_path}")
            
        # Read the CSV file
        logger.info(f"Reading transaction data from {file_path}")
        df = pd.read_csv(file_path)
        
        # Validate required columns
        required_columns = ['amount', 'timestamp', 'transaction_id']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
            
        # Validate data types
        if not pd.api.types.is_numeric_dtype(df['amount']):
            raise ValueError("Amount column must be numeric")
            
        logger.info(f"Successfully loaded {len(df)} transactions")
        return df
        
    except Exception as e:
        logger.error(f"Error extracting transactions: {str(e)}")
        raise
