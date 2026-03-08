"""
Sentinel Agent: Monitors 10 high-volatility Base tokens, calculates signals.
Runs every 30 seconds via CloudWatch trigger.
"""
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
from dataclasses import asdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import project modules
try:
    from firebase_config import firebase_manager, Signal, TOKENS_COLLECTION, SYSTEM_COLLECTION
    import ta  # Technical Analysis library
    import requests
    from sklearn.ensemble import RandomForestClassifier
    import joblib
    import boto3
    from botocore.exceptions import ClientError
except ImportError as e:
    logger.error(f"Import error: {e}")
    raise

class SentinelAgent:
    """Monitors tokens and generates trading signals"""
    
    def __init__(self):
        self.firebase = firebase_manager
        self.db = self.firebase.db
        self.ml_model = None
        self.ml_features = ['rsi_value', 'volume_ratio', 'ma_gap', 'volatility_24h', 'hour_of_day']
        self._load_ml_model()
        
        # Initialize Coingecko API
        self.coingecko_base_url = "https://api.coingecko.com/api/v3"
        self.api_key = os.getenv('COINGECKO_API_KEY', '')
        
        # Cache for token data to reduce API calls
        self.data_cache = {}
        self.cache_ttl = 60  # seconds
        
    def _load_ml_model(self