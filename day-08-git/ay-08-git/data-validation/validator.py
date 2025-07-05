#!/usr/bin/env python3
"""
Data Validation Module for ETL Pipelines
Ensures data quality before processing
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Any
import logging
class DataValidator:
"""Comprehensive data validation for ETL pipelines."""
def __init__(self):
self.validation_results = {}
self.logger = logging.getLogger(__name__)
def validate_completeness(self, df: pd.DataFrame, required_columns: List[str]) -> D
"""Check for missing required columns and null values."""
results = {
'test': 'completeness',
'passed': True,
'issues': []
}
# Check required columns exist
missing_columns = set(required_columns) - set(df.columns)
if missing_columns:
results['passed'] = False
results['issues'].append(f"Missing required columns: {missing_columns}")
# Check for null values in required columns
for col in required_columns:
if col in df.columns:
null_count = df[col].isnull().sum()
if null_count > 0:
results['passed'] = False
results['issues'].append(f"Column '{col}' has {null_count} null val
return results
def validate_data_types(self, df: pd.DataFrame, expected_types: Dict[str, str]) ->
"""Validate column data types match expectations."""
results = {
'test': 'data_types',
'passed': True,
'issues': []
}
for col, expected_type in expected_types.items():
if col in df.columns:
actual_type = str(df[col].dtype)
if expected_type not in actual_type:
results['passed'] = False
results['issues'].append(f"Column '{col}' expected {expected_type}
return results
def validate_ranges(self, df: pd.DataFrame, range_checks: Dict[str, Dict]) -> Dict
"""Validate numeric columns are within expected ranges."""
results = {
'test': 'ranges',
'passed': True,
'issues': []
}
for col, checks in range_checks.items():
if col in df.columns:
if 'min' in checks:
below_min = (df[col] < checks['min']).sum()
if below_min > 0:
results['passed'] = False
results['issues'].append(f"Column '{col}' has {below_min} value
if 'max' in checks:
above_max = (df[col] > checks['max']).sum()
if above_max > 0:
results['passed'] = False
results['issues'].append(f"Column '{col}' has {above_max} value
return results
def run_full_validation(self, df: pd.DataFrame, validation_config: Dict) -> Dict[st
"""Run complete validation suite."""
all_results = {
'timestamp': pd.Timestamp.now(),
'total_records': len(df),
'validation_passed': True,
'tests': []
}
# Run completeness checks
if 'required_columns' in validation_config:
completeness_result = self.validate_completeness(df, validation_config['req
all_results['tests'].append(completeness_result)
if not completeness_result['passed']:
all_results['validation_passed'] = False
# Run data type checks
if 'expected_types' in validation_config:
types_result = self.validate_data_types(df, validation_config['expected_typ
all_results['tests'].append(types_result)
if not types_result['passed']:
all_results['validation_passed'] = False
# Run range checks
if 'range_checks' in validation_config:
ranges_result = self.validate_ranges(df, validation_config['range_checks']
all_results['tests'].append(ranges_result)
if not ranges_result['passed']:
all_results['validation_passed'] = False
return all_results
# Example usage and testing
if __name__ == "__main__":
# Create sample data for testing
sample_data = pd.DataFrame({
'customer_id': [1, 2, 3, None, 5],
'sales': [100.50, 250.75, -10.00, 150.25, 500.00],
'quantity': [1, 2, 3, 1, 5],
'order_date': pd.date_range('2025-01-01', periods=5)
})
# Define validation configuration
config = {
'required_columns': ['customer_id', 'sales', 'quantity'],
'expected_types': {
'customer_id': 'int',
'sales': 'float',
'quantity': 'int'
},
'range_checks': {
'sales': {'min': 0, 'max': 10000},
'quantity': {'min': 1, 'max': 100}
}
}
# Run validation
validator = DataValidator()
results = validator.run_full_validation(sample_data, config)
print("=== Data Validation Results ===")
print(f"Validation passed: {results['validation_passed']}")
print(f"Total records: {results['total_records']}")
print("\nTest Results:")
for test in results['tests']:
print(f"- {test['test']}: {'PASS' if test['passed'] else 'FAIL'}")
if test['issues']:
for issue in test['issues']:
print(f" ❌ {issue}")
EOF
