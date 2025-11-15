import sqlite3
import logging
import os
from typing import List, Tuple, Optional

# --- Configuration ---
# Get a logger for this specific module
logger = logging.getLogger(__name__)

# --- Reputation Update Rules (from PDF, Page 6) ---
REPUTATION_RULES = {
    "SUCCESSFUL_UPLOAD": 3,
    "SUCCESSFUL_DOWNLOAD": 3, # My addition, seems logical
    "VERIFIED_INTEGRITY": 2,
    "CONNECTION_TIMEOUT": -1,
    "REFUSED_UPLOAD": -3,
    "CORRUPTED_DATA": -5
}

# --- Reputation Formula (from PDF, Page 6) ---
ALPHA = 0.8 # Weight for old reputation
BETA = 0.2  # Weight for new interaction
DEFAULT_REPUTATION = 10 # Starting reputation for unknown peers

class ReputationManager:
    """
    Manages the local peer reputation database (db.sqlite).
    Implements the reputation update formula and rules.
    """
    def __init__(self, peer_id: str):
        self.peer_id = peer_id
        # Each peer gets its own private database file
        self.db_path = f"peer_storage_{self.peer_id}/reputation.db"
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        self.conn = self._init_db()
        if self.conn is None:
            logger.error("Failed to initialize reputation database. Reputation system will be disabled.")
    
    def _init_db(self) -> Optional[sqlite3.Connection]:
        """Initializes the SQLite database and 'reputation' table."""
        try:
            conn = sqlite3.connect(self.db_path, check_same_thread=False) # Allow access from different threads
            cursor = conn.cursor()
            
            # Table: reputation
            # - peer_id: The ID of the peer we are scoring
            # - score: The reputation score (R_ij in the formula)
            # - interactions: Total number of interactions
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
        """Gets the current reputation score for a given peer."""
        if self.conn is None:
            return DEFAULT_REPUTATION

        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT score FROM reputation WHERE peer_id = ?", (peer_id,))
            result = cursor.fetchone()
            
            if result:
                return float(result[0])
            else:
                # Peer not in DB, return default score
                return DEFAULT_REPUTATION
        except sqlite3.Error as e:
            logger.error(f"Error getting reputation for {peer_id}: {e}")
            return DEFAULT_REPUTATION # Return default on error

    def update_reputation(self, peer_id: str, event_type: str):
        """
        Updates a peer's reputation based on an interaction event.
        Applies the formula: R_new = alpha * R_old + beta * DeltaR
        """
        if self.conn is None:
            return

        delta_r = REPUTATION_RULES.get(event_type)
        if delta_r is None:
            logger.warning(f"Unknown reputation event type: {event_type}")
            return

        try:
            cursor = self.conn.cursor()
            
            # Get current score and interactions
            cursor.execute("SELECT score, interactions FROM reputation WHERE peer_id = ?", (peer_id,))
            result = cursor.fetchone()
            
            if result:
                old_score, interactions = result
                old_score = float(old_score)
                interactions = int(interactions)
            else:
                # First interaction
                old_score = DEFAULT_REPUTATION
                interactions = 0
            
            # Apply the formula from the PDF
            new_score = (ALPHA * old_score) + (BETA * delta_r)
            
            # Update the database
            cursor.execute('''
                INSERT INTO reputation (peer_id, score, interactions)
                VALUES (?, ?, ?)
                ON CONFLICT(peer_id) DO UPDATE SET
                    score = excluded.score,
                    interactions = interactions + 1
            ''', (peer_id, new_score, interactions + 1))
            
            self.conn.commit()
            
            # Changed from logging.INFO to logging.DEBUG for clean tqdm
            logger.debug(f"Updated reputation for {peer_id}: {old_score:.2f} -> {new_score:.2f} (Event: {event_type})")
            
        except sqlite3.Error as e:
            logger.error(f"Error updating reputation for {peer_id}: {e}")

    def get_peers_sorted_by_reputation(self, peer_ids: List[str]) -> List[Tuple[str, float]]:
        """
        Given a list of peer IDs, returns a new list sorted
        from highest reputation to lowest.
        """
        if self.conn is None:
            # Return list with default scores if DB fails
            return [(peer_id, DEFAULT_REPUTATION) for peer_id in peer_ids]

        try:
            peer_scores = []
            for peer_id in peer_ids:
                peer_scores.append((peer_id, self.get_reputation(peer_id)))
            
            # Sort the list by score (index 1), descending
            peer_scores.sort(key=lambda x: x[1], reverse=True)
            
            return peer_scores
            
        except Exception as e:
            logger.error(f"Error sorting peers by reputation: {e}")
            # Fallback
            return [(peer_id, DEFAULT_REPUTATION) for peer_id in peer_ids]
    
    def close(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Reputation database connection closed.")