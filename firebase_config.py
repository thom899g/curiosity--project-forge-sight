"""
Firebase Firestore configuration and state management.
CRITICAL: This is the single source of truth for all distributed agents.
"""
import os
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import logging
from dataclasses import dataclass, asdict
import json

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    import firebase_admin
    from firebase_admin import credentials, firestore
    from google.cloud.firestore_v1 import SERVER_TIMESTAMP
    
    # Initialize Firebase only once
    if not firebase_admin._apps:
        # Check for service account key in multiple locations
        key_paths = [
            os.getenv('FIREBASE_SERVICE_ACCOUNT_KEY'),
            'firebase_service_key.json',
            '/tmp/firebase_service_key.json',
            os.path.expanduser('~/.config/firebase_key.json')
        ]
        
        cred = None
        for path in key_paths:
            if path and os.path.exists(path):
                try:
                    cred = credentials.Certificate(path)
                    logger.info(f"Loaded Firebase credentials from {path}")
                    break
                except Exception as e:
                    logger.warning(f"Failed to load credentials from {path}: {e}")
        
        if not cred:
            # For Lambda, credentials might be in environment variable
            key_json = os.getenv('FIREBASE_SERVICE_ACCOUNT_JSON')
            if key_json:
                try:
                    key_dict = json.loads(key_json)
                    cred = credentials.Certificate(key_dict)
                    logger.info("Loaded Firebase credentials from environment")
                except (json.JSONDecodeError, ValueError) as e:
                    logger.error(f"Invalid Firebase JSON in environment: {e}")
                    raise
        
        if not cred:
            raise FileNotFoundError("Firebase service account key not found in any location")
        
        firebase_admin.initialize_app(cred)
    
    db = firestore.client()
    logger.info("Firebase Firestore initialized successfully")
    
except ImportError as e:
    logger.error(f"Firebase Admin SDK not installed: {e}")
    raise
except Exception as e:
    logger.error(f"Failed to initialize Firebase: {e}")
    raise

# Data models for type safety
@dataclass
class TokenConfig:
    """Configuration for a monitored token"""
    symbol: str
    contract_address: str
    dex: str = "uniswap_v3"
    base_asset: str = "ETH"
    volatility_threshold: float = 0.15
    enabled: bool = True
    last_updated: datetime = None
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        if self.last_updated:
            data['last_updated'] = self.last_updated
        else:
            data['last_updated'] = SERVER_TIMESTAMP
        return data

@dataclass
class Position:
    """Active trading position"""
    id: str
    token_symbol: str
    entry_price: float
    entry_time: datetime
    position_size_usd: float
    stop_loss: float  # -2%
    take_profit: float  # +3%
    trailing_stop: Optional[float] = None
    status: str = "open"  # open, closed, liquidated
    exit_price: Optional[float] = None
    exit_time: Optional[datetime] = None
    pnl_percentage: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        # Convert datetime objects
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = value.isoformat()
        return data

@dataclass 
class Signal:
    """Trading signal with confidence"""
    token_symbol: str
    timestamp: datetime
    rsi_value: float
    rsi_signal: bool
    volume_ratio: float
    volume_signal: bool
    ma_fast: float
    ma_slow: float
    ma_signal: bool
    ml_confidence: float
    composite_score: float
    action: str  # buy, sell, hold
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['timestamp'] = data['timestamp'].isoformat()
        return data

# Firestore collections
TOKENS_COLLECTION = "tokens"
POSITIONS_COLLECTION = "positions"
SIGNALS_COLLECTION = "signals"
EXECUTIONS_COLLECTION = "executions"
PNL_COLLECTION = "pnl"
SYSTEM_COLLECTION = "system"

class FirebaseManager:
    """Manages all Firebase operations with error handling"""
    
    def __init__(self):
        self.db = db
        self._setup_collections()
    
    def _setup_collections(self):
        """Ensure collections exist with initial documents"""
        try:
            # System configuration
            system_ref = self.db.collection(SYSTEM_COLLECTION).document("config")
            if not system_ref.get().exists:
                system_ref.set({
                    "circuit_breakers": {},
                    "adaptive_params": {
                        "rsi_overbought": 70,
                        "rsi_oversold": 30,
                        "volume_spike_threshold": 2.5,
                        "max_position_size_percent": 0.1,
                        "gas_price_threshold_gwei": 50
                    },
                    "ml_model_version": "v1.0",
                    "system_status": "active",
                    "last_health_check": SERVER_TIMESTAMP
                })
                logger.info("Initialized system configuration")
            
            # Initialize token configurations (10 high-volatility Base tokens)
            tokens = [
                TokenConfig("USDC", "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913", "uniswap_v3", "ETH"),
                TokenConfig("DAI", "0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb", "uniswap_v3", "ETH"),
                TokenConfig("CBETH", "0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22", "uniswap_v3", "ETH"),
                TokenConfig("WSTETH", "0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452", "uniswap_v3", "ETH"),
                TokenConfig("AXL", "0x23ee2343B892b1BB63503a4FAbc840E0e2C6810f", "uniswap_v3", "ETH"),
                TokenConfig("AERO", "0x940181a94A35A4569E4529A3CDfB74e38FD98631", "velodrome", "ETH"),
                TokenConfig("MAV", "0x64b5dfdC5F6a3b0578645B548c5F9C6C6ed356A6", "uniswap_v3", "ETH"),
                TokenConfig("SONNE", "0x22a2488fE295047Ba13BD8cCCdBC8361DBD8cf7c", "uniswap_v3", "ETH"),
                TokenConfig("BSWAP", "0x78a087d713Be963Bf307b18F2Ff8122EF9A63ae9", "baseswap", "ETH"),
                TokenConfig("ODOS", "0x0efB5aA390C4d35F1cD791F2a26E80b3C1a0d71F", "uniswap_v3", "ETH"),
            ]
            
            for token in tokens:
                doc_ref = self.db.collection(TOKENS_COLLECTION).document(token.symbol)
                if not doc_ref.get().exists:
                    doc_ref.set(token.to_dict())
            
            logger.info(f"Initialized {len(tokens)} token configurations")
            
        except Exception as e:
            logger.error(f"Failed to setup collections: {e}")
            raise
    
    def update_token_signal(self, token_symbol: str, signal_data: Dict[str, Any]) -> bool:
        """Update signal data for a token"""
        try:
            doc_ref = self.db.collection(TOKENS_COLLECTION).document(token_symbol)
            doc_ref.update({
                **signal_data,
                "last_signal_update": SERVER_TIMESTAMP
            })
            return True
        except Exception as e:
            logger.error(f"Failed to update signal for {token_symbol}: {e}")
            return False
    
    def create_position(self, position: Position) -> str:
        """Create a new position document"""
        try:
            doc_ref = self.db.collection(POSITIONS_COLLECTION).document()
            position_dict = position.to_dict()
            position_dict['created_at'] = SERVER_TIMESTAMP
            doc_ref.set(position_dict)
            logger.info(f"Created position {doc_ref.id} for {position.token_symbol}")
            return doc_ref.id
        except Exception as e:
            logger.error(f"Failed to create position: {e}")
            raise
    
    def log_execution(self, execution_data: Dict[str, Any]) -> None:
        """Log trade execution details"""
        try:
            doc_ref = self.db.collection(EXECUTIONS_COLLECTION).document()
            execution_data['timestamp'] = SERVER_TIMESTAMP
            doc_ref.set(execution_data)
        except Exception as e:
            logger.error(f"Failed to log execution: {e}")
    
    def record_pnl(self, hourly_pnl: float, daily_pnl: float) -> None:
        """Record hourly PnL"""
        try:
            hour_key = datetime.utcnow().strftime("%Y-%m-%d-%H")
            doc_ref = self.db.collection(PNL_COLLECTION).document(hour_key)
            doc_ref.set({
                "hourly_pnl": hourly_pnl,
                "daily_pnl": daily_pnl,
                "timestamp": SERVER_TIMESTAMP,
                "open_positions": self.get_open_positions_count()
            }, merge=True)
            logger.info(f"Recorded PnL: hourly=${hourly_pnl:.2f}, daily=${daily_pnl:.2f}")
        except Exception as e:
            logger.error(f"Failed to record PnL: {e}")
    
    def get_open_positions_count(self) -> int:
        """Count open positions"""
        try:
            query = self.db.collection(POSITIONS_COLLECTION).where("status", "==", "open")
            return len(list(query.stream()))
        except Exception as e:
            logger.error(f"Failed to count open positions: {e}")
            return 0
    
    def check_circuit_breaker(self, token_symbol: str) -> bool:
        """Check if circuit breaker is active for token"""
        try:
            doc_ref = self.db.collection(SYSTEM_COLLECTION).document("circuit_breakers")
            doc = doc_ref.get()
            if doc.exists:
                breakers = doc.to_dict()
                token_key = f"{token_symbol}_paused"
                if token_key in breakers and breakers[token_key]:
                    paused_until = breakers.get(f"{token_symbol}_paused_until")
                    if paused_until and paused_until > datetime.utcnow():
                        return False
            return True
        except Exception as e:
            logger.error(f"Failed to check circuit breaker: {e}")
            return True
    
    def trigger_circuit_breaker(self, token_symbol: str, reason: str, duration_minutes: int = 15) -> None:
        """Trigger circuit breaker for a token"""
        try:
            doc_ref = self.db.collection(SYSTEM_COLLECTION).document("circuit_breakers")
            paused_until = datetime.utcnow() + timedelta(minutes=duration_minutes)
            
            doc_ref.set({
                f"{token_symbol}_paused": True,
                f"{token_symbol}_paused_until": paused_until,
                f"{token_symbol}_paused_reason": reason,
                "last_triggered": SERVER_TIMESTAMP
            }, merge=True)
            
            logger.warning(f"Circuit breaker triggered for {token_symbol}: {reason}")
            
        except Exception as e:
            logger.error(f"Failed to trigger circuit breaker: {e}")

# Global instance
firebase_manager = FirebaseManager()