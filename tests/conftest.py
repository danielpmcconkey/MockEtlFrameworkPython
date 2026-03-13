"""Shared fixtures for ETL framework tests."""

import os
import sys

# Ensure src is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
