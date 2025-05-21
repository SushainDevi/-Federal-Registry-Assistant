import os
import datetime
from pathlib import Path

# Federal Registry API configuration
FR_API_BASE_URL = "https://www.federalregister.gov/api/v1"

# Data storage paths
DATA_DIR = Path("data")
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# Create directories if they don't exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Default date range (past 2 months as suggested in requirements)
DEFAULT_START_DATE = (datetime.datetime.now() - datetime.timedelta(days=60)).strftime("%Y-%m-%d")
DEFAULT_END_DATE = datetime.datetime.now().strftime("%Y-%m-%d")

# Define document types of interest
DOCUMENT_TYPES = [
    "RULE", 
    "PRORULE", 
    "NOTICE", 
    "PRESDOCU"
]

# Batch size for processing and uploading
BATCH_SIZE = 100