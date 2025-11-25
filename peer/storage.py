import os
import json
import logging
import threading
from typing import Dict, Any, Set, Optional, List
from peer import file_utils

logger = logging.getLogger(__name__)

class StorageManager:
    def __init__(self, peer_id: str, instance_dir: str = None):
        self.peer_id = peer_id
        self.lock = threading.Lock()
        
        # Allow overriding the base directory for multi-peer testing
        if instance_dir:
            self.base_dir = instance_dir
        else:
            self.base_dir = f"peer_storage_{self.peer_id}"
            
        self.downloads_dir = os.path.join(self.base_dir, "downloads")
        self.completed_dir = os.path.join(self.base_dir, "completed")
        self.metadata_file = os.path.join(self.base_dir, "storage_meta.json")
        
        os.makedirs(self.downloads_dir, exist_ok=True)
        os.makedirs(self.completed_dir, exist_ok=True)
        
        self.file_metadata: Dict[str, Dict[str, Any]] = {}
        self.chunk_tracker: Dict[str, Set[int]] = {}
        self.file_locations: Dict[str, str] = {}
        self._load_metadata()

    def _load_metadata(self):
        with self.lock:
            try:
                if os.path.exists(self.metadata_file):
                    with open(self.metadata_file, 'r') as f:
                        data = json.load(f)
                        self.file_metadata = data.get('file_metadata', {})
                        self.file_locations = data.get('file_locations', {})
                        chunk_data = data.get('chunk_tracker', {})
                        self.chunk_tracker = {h: set(c) for h, c in chunk_data.items()}
                    logger.info(f"Loaded storage metadata from {self.metadata_file}")
                else:
                    self._save_metadata_internal()
            except Exception as e:
                logger.error(f"Error loading metadata: {e}")
                self._reset_metadata()

    def _reset_metadata(self):
        self.file_metadata = {}
        self.chunk_tracker = {}
        self.file_locations = {}

    def _save_metadata(self):
        with self.lock:
            self._save_metadata_internal()

    def _save_metadata_internal(self):
        try:
            with open(self.metadata_file, 'w') as f:
                chunk_data = {h: list(c) for h, c in self.chunk_tracker.items()}
                data = {
                    'file_metadata': self.file_metadata,
                    'chunk_tracker': chunk_data,
                    'file_locations': self.file_locations
                }
                json.dump(data, f, indent=4)
        except IOError as e:
            logger.error(f"Could not save metadata to {self.metadata_file}: {e}")

    def add_file_to_share(self, file_path: str, chunk_size: int) -> Optional[Dict[str, Any]]:
        abs_file_path = os.path.abspath(file_path)
        if not os.path.exists(abs_file_path):
            logger.error(f"File not found: {file_path}")
            return None
            
        file_meta = file_utils.get_file_metadata(abs_file_path, chunk_size)
        
        if file_meta:
            with self.lock:
                file_hash = file_meta['hash']
                self.file_metadata[file_hash] = file_meta
                self.file_locations[file_hash] = abs_file_path
                self.chunk_tracker[file_hash] = set(range(file_meta['chunk_count']))
                self._save_metadata_internal()
            return file_meta
        return None

    def get_shared_files_info(self) -> List[Dict[str, Any]]:
        with self.lock:
            return list(self.file_metadata.values())

    def get_original_file_path(self, file_hash: str) -> Optional[str]:
        with self.lock:
            return self.file_locations.get(file_hash)

    # --- NEW: Check if chunks actually exist on disk ---
    def has_physical_chunks(self, file_hash: str, chunk_count: int) -> bool:
        """Checks if the actual .0, .1 files exist in the downloads folder."""
        # Check the first and last chunk as a heuristic, or strictly check all
        first_chunk = os.path.join(self.downloads_dir, f"{file_hash}.0")
        last_chunk = os.path.join(self.downloads_dir, f"{file_hash}.{chunk_count-1}")
        return os.path.exists(first_chunk) and os.path.exists(last_chunk)

    def add_downloading_file(self, file_meta: Dict[str, Any]):
        with self.lock:
            file_hash = file_meta['hash']
            if file_hash not in self.file_metadata:
                self.file_metadata[file_hash] = file_meta
                self.chunk_tracker[file_hash] = set()
                self._save_metadata_internal()

    def store_chunk(self, file_hash: str, chunk_index: int, chunk_data: bytes) -> bool:
        with self.lock:
            if file_hash not in self.file_metadata:
                return False
        
        chunk_filename = f"{file_hash}.{chunk_index}"
        chunk_path = os.path.join(self.downloads_dir, chunk_filename)
        
        try:
            with open(chunk_path, 'wb') as f:
                f.write(chunk_data)
            
            with self.lock:
                self.chunk_tracker[file_hash].add(chunk_index)
                self._save_metadata_internal()
            return True
        except IOError as e:
            logger.error(f"Failed to write chunk {chunk_path}: {e}")
            return False

    def get_chunk_data_for_upload(self, file_hash: str, chunk_index: int, chunk_size: int) -> Optional[bytes]:
        original_path = None
        with self.lock:
            if not self._has_chunk_internal(file_hash, chunk_index):
                return None
            if file_hash in self.file_locations:
                original_path = self.file_locations[file_hash]

        if original_path:
            return file_utils.read_chunk_from_file(original_path, chunk_index, chunk_size)
        
        chunk_filename = f"{file_hash}.{chunk_index}"
        chunk_path = os.path.join(self.downloads_dir, chunk_filename)
        
        if os.path.exists(chunk_path):
            try:
                with open(chunk_path, 'rb') as f:
                    return f.read()
            except: return None
        return None

    def has_chunk(self, file_hash: str, chunk_index: int) -> bool:
        with self.lock:
            return self._has_chunk_internal(file_hash, chunk_index)

    def _has_chunk_internal(self, file_hash: str, chunk_index: int) -> bool:
        return file_hash in self.chunk_tracker and chunk_index in self.chunk_tracker[file_hash]

    def get_missing_chunks(self, file_hash: str) -> Set[int]:
        with self.lock:
            if file_hash not in self.file_metadata:
                return set()
            total = self.file_metadata[file_hash]['chunk_count']
            owned = self.chunk_tracker.get(file_hash, set())
            return set(range(total)) - owned

    def is_download_complete(self, file_hash: str) -> bool:
        with self.lock:
            if file_hash not in self.file_metadata: return False
            total = self.file_metadata[file_hash]['chunk_count']
            owned = self.chunk_tracker.get(file_hash, set())
            return len(owned) == total