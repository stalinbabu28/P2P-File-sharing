import sqlite3
import logging
import os
import threading # <-- Added
from typing import List, Tuple, Optional

# --- Configuration ---
logger = logging.getLogger(__name__)

REPUTATION_RULES = {
    "SUCCESSFUL_UPLOAD": 3,
    "SUCCESSFUL_DOWNLOAD": 3,
    "VERIFIED_INTEGRITY": 2,
    "CONNECTION_TIMEOUT": -1,
    "REFUSED_UPLOAD": -3,
    "CORRUPTED_DATA": -5
}

ALPHA = 0.8
BETA = 0.2
DEFAULT_REPUTATION = 10

class ReputationManager:
    def __init__(self, peer_id: str):
        self.peer_id = peer_id
        self.lock = threading.Lock() # <-- Mutex for thread safety
        self.db_path = f"peer_storage_{self.peer_id}/reputation.db"
        
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        self.conn = self._init_db()
        if self.conn is None:
            logger.error("Failed to initialize reputation database.")
    
    def _init_db(self) -> Optional[sqlite3.Connection]:
        try:
            # check_same_thread=False is required, but we use a Lock anyway for safety
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS reputation (
                    peer_id TEXT PRIMARY KEY,
                    score REAL NOT NULL,
                    interactions INTEGER NOT NULL
                )
            ''')
            conn.commit()
            logger.info(f"Reputation database initialized at {self.db_path}")
            return conn
        except sqlite3.Error as e:
            logger.error(f"Error initializing reputation database: {e}")
            return None

    def get_reputation(self, peer_id: str) -> float:
        if self.conn is None: return DEFAULT_REPUTATION
        
        with self.lock: # Protected read
            try:
                cursor = self.conn.cursor()
                cursor.execute("SELECT score FROM reputation WHERE peer_id = ?", (peer_id,))
                result = cursor.fetchone()
                if result:
                    return float(result[0])
                return DEFAULT_REPUTATION
            except sqlite3.Error as e:
                logger.error(f"Error getting reputation for {peer_id}: {e}")
                return DEFAULT_REPUTATION

    def update_reputation(self, peer_id: str, event_type: str):
        if self.conn is None: return

        delta_r = REPUTATION_RULES.get(event_type)
        if delta_r is None:
            logger.warning(f"Unknown reputation event type: {event_type}")
            return

        with self.lock: # Protected write transaction
            try:
                cursor = self.conn.cursor()
                
                cursor.execute("SELECT score, interactions FROM reputation WHERE peer_id = ?", (peer_id,))
                result = cursor.fetchone()
                
                if result:
                    old_score, interactions = result
                    old_score = float(old_score)
                    interactions = int(interactions)
                else:
                    old_score = DEFAULT_REPUTATION
                    interactions = 0
                
                new_score = (ALPHA * old_score) + (BETA * delta_r)
                
                cursor.execute('''
                    INSERT INTO reputation (peer_id, score, interactions)
                    VALUES (?, ?, ?)
                    ON CONFLICT(peer_id) DO UPDATE SET
                        score = excluded.score,
                        interactions = interactions + 1
                ''', (peer_id, new_score, interactions + 1))
                
                self.conn.commit()
                logger.debug(f"Updated reputation for {peer_id}: {old_score:.2f} -> {new_score:.2f} (Event: {event_type})")
                
            except sqlite3.Error as e:
                logger.error(f"Error updating reputation for {peer_id}: {e}")

    def get_peers_sorted_by_reputation(self, peer_ids: List[str]) -> List[Tuple[str, float]]:
        if self.conn is None:
            return [(peer_id, DEFAULT_REPUTATION) for peer_id in peer_ids]

        with self.lock:
            try:
                peer_scores = []
                for peer_id in peer_ids:
                    # Internal logic replicated to stay within one lock transaction
                    cursor = self.conn.cursor()
                    cursor.execute("SELECT score FROM reputation WHERE peer_id = ?", (peer_id,))
                    result = cursor.fetchone()
                    score = float(result[0]) if result else DEFAULT_REPUTATION
                    peer_scores.append((peer_id, score))
                
                peer_scores.sort(key=lambda x: x[1], reverse=True)
                return peer_scores
            except Exception as e:
                logger.error(f"Error sorting peers: {e}")
                return [(peer_id, DEFAULT_REPUTATION) for peer_id in peer_ids]
    
    def close(self):
        with self.lock:
            if self.conn:
                self.conn.close()
                logger.info("Reputation database connection closed.")