#!/usr/bin/env python3
"""
Day 2: Python Fundamentals for Data Engineering
E-commerce transaction data processor
"""
import pandas as pd
import json
from datetime import datetime
import os
class EcommerceDataProcessor:
"""Process e-commerce transaction data with error handling."""
def __init__(self, data_file):
self.data_file = data_file
self.df = None
def load_data(self):
"""Load data with comprehensive error handling."""
try:
if self.data_file.endswith('.csv'):
self.df = pd.read_csv(self.data_file)
elif self.data_file.endswith('.json'):
self.df = pd.read_json(self.data_file)
else:
raise ValueError("Unsupported file format")
print(f"✅ Successfully loaded {len(self.df)} records")
return True
except FileNotFoundError:
print(f"❌ Error: File {self.data_file} not found")
return False
except Exception as e:
print(f"❌ Error loading data: {e}")
return False
def analyze_data(self):
"""Perform basic data analysis."""
if self.df is None:
print("No data loaded")
return
print("\n=== Data Analysis Summary ===")
print(f"Dataset shape: {self.df.shape}")
print(f"Memory usage: {self.df.memory_usage(deep=True).sum() / 1024:.2f} KB")
print("\nColumn information:")
print(self.df.info())
if __name__ == "__main__":
# Example usage
processor = EcommerceDataProcessor("sample_data.csv")
if processor.load_data():
processor.analyze_data()
EOF                                                          
