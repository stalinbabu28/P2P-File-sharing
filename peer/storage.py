import os
import json
import logging
from typing import Dict, Any, Set, Optional, List

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [Storage] - %(levelname)s - %(message)s')

class StorageManager:
    """
    Manages the peer's local file storage.
    
    Handles:
    - Shared directory (files to upload)
    - Downloads directory (incomplete chunks)
    - Completed directory (reassembled files)
    - Metadata about file chunks
    """
    def __init__(self, peer_id: str):
        self.peer_id = peer_id
        
        # Define base storage paths
        self.base_dir = f"peer_storage_{self.peer_id}"
        self.shared_dir = os.path.join(self.base_dir, "shared")
        self.downloads_dir = os.path.join(self.base_dir, "downloads") # For chunks
        self.completed_dir = os.path.join(self.base_dir, "completed")
        self.metadata_file = os.path.join(self.base_dir, "storage_meta.json")
        
        # Create directories
        os.makedirs(self.shared_dir, exist_ok=True)
        os.makedirs(self.downloads_dir, exist_ok=True)
        os.makedirs(self.completed_dir, exist_ok=True)
        
        # Load or initialize metadata
        # self.file_metadata -> { "file_hash": { "name": ..., "size": ..., "chunk_count": ... } }
        # self.chunk_tracker -> { "file_hash": set(chunk_index, ...) }
        self.file_metadata: Dict[str, Dict[str, Any]] = {}
        self.chunk_tracker: Dict[str, Set[int]] = {}
        self._load_metadata()

    def _load_metadata(self):
        """Loads file metadata and chunk tracker from disk."""
        try:
            if os.path.exists(self.metadata_file):
                with open(self.metadata_file, 'r') as f:
                    data = json.load(f)
                    self.file_metadata = data.get('file_metadata', {})
                    # Convert lists back to sets
                    chunk_data = data.get('chunk_tracker', {})
                    self.chunk_tracker = {h: set(c) for h, c in chunk_data.items()}
                logging.info(f"Loaded storage metadata from {self.metadata_file}")
            else:
                logging.info("No existing metadata file found. Starting fresh and creating one.")
                self._save_metadata() # <-- This line is new
        except json.JSONDecodeError:
            logging.error(f"Error decoding metadata file. Starting fresh.")
            self.file_metadata = {}
            self.chunk_tracker = {}
        except Exception as e:
            logging.error(f"Error loading metadata: {e}")
            self.file_metadata = {}
            self.chunk_tracker = {}

    def _save_metadata(self):
        """Saves the current file metadata and chunk tracker to disk."""
        try:
            with open(self.metadata_file, 'w') as f:
                # Convert sets to lists for JSON serialization
                chunk_data = {h: list(c) for h, c in self.chunk_tracker.items()}
                data = {
                    'file_metadata': self.file_metadata,
                    'chunk_tracker': chunk_data
                }
                json.dump(data, f, indent=4)
        except IOError as e:
            logging.error(f"Could not save metadata to {self.metadata_file}: {e}")

    def add_file_to_share(self, file_path: str, chunk_size: int) -> Optional[Dict[str, Any]]:
        """
        Adds a new file to the shared directory, splits it into chunks,
        and tracks its metadata.
        """
        # We need file_utils for this
        from peer.file_utils import split_file, get_file_hash
        
        # 1. Copy file to shared directory
        file_name = os.path.basename(file_path)
        shared_file_path = os.path.join(self.shared_dir, file_name)
        try:
            if not os.path.exists(shared_file_path):
                 # Simple copy, replace with move or symbolic link for efficiency
                import shutil
                shutil.copy(file_path, shared_file_path)
        except IOError as e:
            logging.error(f"Failed to copy file to shared dir: {e}")
            return None
            
        # 2. Split file into chunks in the downloads dir (so we can serve them)
        file_meta = split_file(shared_file_path, chunk_size, self.downloads_dir)
        
        if file_meta:
            file_hash = file_meta['hash']
            self.file_metadata[file_hash] = file_meta
            # We have all chunks
            self.chunk_tracker[file_hash] = set(range(file_meta['chunk_count']))
            self._save_metadata()
            logging.info(f"Added file '{file_name}' (hash: {file_hash[:10]}...) to storage.")
            return file_meta
        else:
            logging.error(f"Failed to split and process file: {file_name}")
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
            self.chunk_tracker[file_hash] = set() # We have no chunks yet
            self._save_metadata()
            logging.info(f"Added new download target: {file_meta['name']} (hash: {file_hash[:10]}...)")
        else:
            logging.info(f"Already tracking file: {file_meta['name']}")

    def store_chunk(self, file_hash: str, chunk_index: int, chunk_data: bytes) -> bool:
        """Saves a received chunk to the downloads directory."""
        if file_hash not in self.file_metadata:
            logging.warning(f"Received chunk for untracked file hash: {file_hash}")
            return False
        
        chunk_filename = f"{file_hash}.{chunk_index}"
        chunk_path = os.path.join(self.downloads_dir, chunk_filename)
        
        try:
            with open(chunk_path, 'wb') as f:
                f.write(chunk_data)
            
            # Update tracker
            self.chunk_tracker[file_hash].add(chunk_index)
            self._save_metadata()
            logging.debug(f"Stored chunk {chunk_index} for file {file_hash[:10]}...")
            return True
        except IOError as e:
            logging.error(f"Failed to write chunk {chunk_path}: {e}")
            return False

    def get_chunk_path(self, file_hash: str, chunk_index: int) -> Optional[str]:
        """Gets the file path for a requested chunk, if we have it."""
        if self.has_chunk(file_hash, chunk_index):
            chunk_filename = f"{file_hash}.{chunk_index}"
            return os.path.join(self.downloads_dir, chunk_filename)
        logging.debug(f"Chunk {chunk_index} for file {file_hash[:10]}... not found.")
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