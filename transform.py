import pandas as pd
import numpy as np
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

def calculate_risk_score(row: pd.Series) -> float:
    """
    Calculate a risk score for a transaction based on multiple factors.
    
    Args:
        row (pd.Series): Transaction data
        
    Returns:
        float: Risk score between 0 and 1
    """
    score = 0.0
    
    # Amount-based risk
    if row['amount'] > 10000:
        score += 0.4
    elif row['amount'] > 5000:
        score += 0.2
        
    # Time-based risk (if timestamp is available)
    if 'timestamp' in row:
        hour = pd.to_datetime(row['timestamp']).hour
        if hour < 6 or hour > 22:  # Unusual hours
            score += 0.2
            
    # Location-based risk (if location data is available)
    if 'location' in row and row['location'] == 'high_risk_country':
        score += 0.3
        
    return min(score, 1.0)

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform transaction data and identify suspicious patterns.
    
    Args:
        df (pd.DataFrame): Raw transaction data
        
    Returns:
        pd.DataFrame: Processed data with risk scores and flags
    """
    logger.info("Starting transaction transformation")
    
    # Create a copy to avoid modifying the original
    df_processed = df.copy()
    
    # Calculate risk scores
    df_processed['risk_score'] = df_processed.apply(calculate_risk_score, axis=1)
    
    # Flag suspicious transactions
    df_processed['flagged'] = df_processed['risk_score'] > 0.5
    
    # Add transaction velocity (if timestamp is available)
    if 'timestamp' in df_processed.columns:
        df_processed['timestamp'] = pd.to_datetime(df_processed['timestamp'])
        df_processed['hour'] = df_processed['timestamp'].dt.hour
        
        # Calculate transactions per hour per account
        if 'account_id' in df_processed.columns:
            hourly_counts = df_processed.groupby(['account_id', 'hour']).size()
            df_processed['hourly_transaction_count'] = df_processed.apply(
                lambda row: hourly_counts.get((row['account_id'], row['hour']), 0),
                axis=1
            )
            
            # Flag high-velocity transactions
            df_processed['high_velocity'] = df_processed['hourly_transaction_count'] > 5
    
    logger.info(f"Transformation complete. Flagged {df_processed['flagged'].sum()} suspicious transactions")
    return df_processed
