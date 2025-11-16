import os
import json
import logging
from typing import Dict, Any, Set, Optional, List
from peer import file_utils

logger = logging.getLogger(__name__)

class StorageManager:
    """
    Manages the peer's local file storage.
    """
    def __init__(self, peer_id: str):
        self.peer_id = peer_id
        
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
        """Loads file metadata and chunk tracker from disk."""
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
                logger.info("No existing metadata file found. Starting fresh and creating one.")
                self._save_metadata()
        except json.JSONDecodeError:
            logger.error(f"Error decoding metadata file. Starting fresh.")
            self._reset_metadata()
        except Exception as e:
            logger.error(f"Error loading metadata: {e}")
            self._reset_metadata()

    def _reset_metadata(self):
        """Resets metadata to a clean state."""
        self.file_metadata = {}
        self.chunk_tracker = {}
        self.file_locations = {}

    def _save_metadata(self):
        """Saves the current file metadata and chunk tracker to disk."""
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
        """
        Analyzes a file for sharing (On-Demand).
        Does NOT copy or split the file, just calculates metadata.
        """
        abs_file_path = os.path.abspath(file_path)
        if not os.path.exists(abs_file_path):
            logger.error(f"File not found: {file_path}")
            return None
            
        file_meta = file_utils.get_file_metadata(abs_file_path, chunk_size)
        
        if file_meta:
            file_hash = file_meta['hash']
            self.file_metadata[file_hash] = file_meta
            
            self.file_locations[file_hash] = abs_file_path
            
            self.chunk_tracker[file_hash] = set(range(file_meta['chunk_count']))
            
            self._save_metadata()
            logger.info(f"Now sharing '{file_meta['name']}' from {abs_file_path}")
            return file_meta
        else:
            logger.error(f"Failed to get metadata for file: {file_path}")
            return None

    def get_shared_files_info(self) -> List[Dict[str, Any]]:
        """Returns a list of metadata for all fully owned (shared) files."""
        return list(self.file_metadata.values())

    def add_downloading_file(self, file_meta: Dict[str, Any]):
        """
        Adds metadata for a file we want to start downloading.
        """
        file_hash = file_meta['hash']
        if file_hash not in self.file_metadata:
            self.file_metadata[file_hash] = file_meta
            self.chunk_tracker[file_hash] = set() 
            self._save_metadata()
            logger.info(f"Added new download target: {file_meta['name']} (hash: {file_hash[:10]}...)")
        else:
            logger.info(f"Already tracking file: {file_meta['name']}")

    def store_chunk(self, file_hash: str, chunk_index: int, chunk_data: bytes) -> bool:
        """Saves a received chunk to the downloads directory."""
        if file_hash not in self.file_metadata:
            logger.warning(f"Received chunk for untracked file hash: {file_hash}")
            return False
        
        chunk_filename = f"{file_hash}.{chunk_index}"
        chunk_path = os.path.join(self.downloads_dir, chunk_filename)
        
        try:
            with open(chunk_path, 'wb') as f:
                f.write(chunk_data)
            
            self.chunk_tracker[file_hash].add(chunk_index)
            self._save_metadata()
            logger.debug(f"Stored chunk {chunk_index} for file {file_hash[:10]}...")
            return True
        except IOError as e:
            logger.error(f"Failed to write chunk {chunk_path}: {e}")
            return False

    def get_chunk_data_for_upload(self, file_hash: str, chunk_index: int, chunk_size: int) -> Optional[bytes]:
        """
        Gets the raw data for a chunk, either from the original file
        (if we're the seeder) or from our downloaded chunks.
        """
        if not self.has_chunk(file_hash, chunk_index):
            logger.warning(f"Upload requested for chunk {chunk_index} of {file_hash[:10]}, but we don't have it.")
            return None

        if file_hash in self.file_locations:
            original_path = self.file_locations[file_hash]
            return file_utils.read_chunk_from_file(original_path, chunk_index, chunk_size)
        
        chunk_filename = f"{file_hash}.{chunk_index}"
        chunk_path = os.path.join(self.downloads_dir, chunk_filename)
        
        if os.path.exists(chunk_path):
            try:
                with open(chunk_path, 'rb') as f:
                    return f.read()
            except IOError as e:
                logger.error(f"Error reading owned chunk {chunk_path}: {e}")
                return None
        else:
            logger.error(f"Chunk {chunk_index} in tracker but not found at {chunk_path}.")
            return None

    def has_chunk(self, file_hash: str, chunk_index: int) -> bool:
        """Checks if we have a specific chunk."""
        return file_hash in self.chunk_tracker and chunk_index in self.chunk_tracker[file_hash]

    def get_missing_chunks(self, file_hash: str) -> Set[int]:
        """Returns a set of chunk indexes we still need for a file."""
        if file_hash not in self.file_metadata:
            return set()
        
        total_chunks = self.file_metadata[file_hash]['chunk_count']
        all_chunks = set(range(total_chunks))
        owned_chunks = self.chunk_tracker.get(file_hash, set())
        
        return all_chunks - owned_chunks

    def is_download_complete(self, file_hash: str) -> bool:
        """Checks if we have all chunks for a file."""
        missing = self.get_missing_chunks(file_hash)
        return file_hash in self.file_metadata and not missing