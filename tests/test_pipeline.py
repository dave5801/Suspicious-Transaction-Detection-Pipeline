import unittest
import pandas as pd
import numpy as np
from pathlib import Path
import sys
import os

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extract import extract_transactions
from transform import transform, calculate_risk_score
from load import load_to_sqlite

class TestTransactionPipeline(unittest.TestCase):
    def setUp(self):
        """Set up test data"""
        self.test_data = pd.DataFrame({
            'amount': [1000, 15000, 5000, 2000],
            'timestamp': [
                '2024-01-01 10:00:00',
                '2024-01-01 23:00:00',
                '2024-01-01 15:00:00',
                '2024-01-01 03:00:00'
            ],
            'transaction_id': ['T1', 'T2', 'T3', 'T4'],
            'account_id': ['A1', 'A1', 'A2', 'A2']
        })
        
        # Create test CSV
        self.test_csv = 'tests/test_data.csv'
        self.test_data.to_csv(self.test_csv, index=False)
        
        # Test database
        self.test_db = 'tests/test_transactions.db'

    def tearDown(self):
        """Clean up test files"""
        if Path(self.test_csv).exists():
            Path(self.test_csv).unlink()
        if Path(self.test_db).exists():
            Path(self.test_db).unlink()

    def test_extract_transactions(self):
        """Test transaction extraction"""
        df = extract_transactions(self.test_csv)
        self.assertEqual(len(df), 4)
        self.assertTrue(all(col in df.columns for col in ['amount', 'timestamp', 'transaction_id']))

    def test_calculate_risk_score(self):
        """Test risk score calculation"""
        # Test high-value transaction
        high_value_row = pd.Series({
            'amount': 15000,
            'timestamp': '2024-01-01 23:00:00'
        })
        self.assertGreater(calculate_risk_score(high_value_row), 0.5)

        # Test normal transaction
        normal_row = pd.Series({
            'amount': 1000,
            'timestamp': '2024-01-01 10:00:00'
        })
        self.assertLess(calculate_risk_score(normal_row), 0.5)

    def test_transform(self):
        """Test data transformation"""
        df_transformed = transform(self.test_data)
        self.assertTrue('risk_score' in df_transformed.columns)
        self.assertTrue('flagged' in df_transformed.columns)
        self.assertTrue(df_transformed['flagged'].dtype == bool)

    def test_load_to_sqlite(self):
        """Test database loading"""
        df_transformed = transform(self.test_data)
        result = load_to_sqlite(df_transformed, self.test_db)
        self.assertTrue(result)
        self.assertTrue(Path(self.test_db).exists())

if __name__ == '__main__':
    unittest.main() 