import os
import hashlib
import logging
from typing import List, Dict, Optional

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [FileUtils] - %(levelname)s - %(message)s')

def get_file_hash(file_path: str) -> Optional[str]:
    """Calculates the SHA-256 hash of an entire file."""
    if not os.path.exists(file_path):
        logging.warning(f"File not found, cannot calculate hash: {file_path}")
        return None
        
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            # Read and update hash in chunks
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except IOError as e:
        logging.error(f"Error reading file {file_path} for hashing: {e}")
        return None

def get_chunk_hashes(file_path: str, chunk_size: int) -> List[str]:
    """
    Splits a file into chunks and returns a list of SHA-256 hashes for each chunk.
    """
    if not os.path.exists(file_path):
        logging.warning(f"File not found, cannot calculate chunk hashes: {file_path}")
        return []
    
    chunk_hashes = []
    try:
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                sha256_hash = hashlib.sha256(chunk).hexdigest()
                chunk_hashes.append(sha256_hash)
        logging.info(f"Calculated {len(chunk_hashes)} chunk hashes for {file_path}")
        return chunk_hashes
    except IOError as e:
        logging.error(f"Error reading file {file_path} for chunk hashing: {e}")
        return []

def split_file(file_path: str, chunk_size: int, output_dir: str) -> Optional[Dict[str, any]]:
    """
    Splits a file into chunks and saves them to a directory.
    Returns a metadata dictionary about the file.
    """
    if not os.path.exists(file_path):
        logging.warning(f"File not found, cannot split: {file_path}")
        return None

    try:
        file_size = os.path.getsize(file_path)
        file_name = os.path.basename(file_path)
        file_hash = get_file_hash(file_path)
        
        if file_hash is None:
            return None # Hashing failed

        os.makedirs(output_dir, exist_ok=True)
        
        chunk_hashes = []
        chunk_count = 0
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                
                chunk_hash = hashlib.sha256(chunk).hexdigest()
                chunk_hashes.append(chunk_hash)
                
                chunk_filename = f"{file_hash}.{chunk_count}"
                chunk_path = os.path.join(output_dir, chunk_filename)
                
                with open(chunk_path, 'wb') as chunk_file:
                    chunk_file.write(chunk)
                
                chunk_count += 1
        
        logging.info(f"Split {file_name} into {chunk_count} chunks in {output_dir}")
        
        return {
            "name": file_name,
            "size": file_size,
            "hash": file_hash,
            "chunk_count": chunk_count,
            "chunk_hashes": chunk_hashes # <-- RETURN CHUNK HASHES
        }
    except IOError as e:
        logging.error(f"Error splitting file {file_path}: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred in split_file: {e}")
        return None

def reassemble_file(file_hash: str, chunk_count: int, chunks_dir: str, output_path: str) -> bool:
    """
    Reassembles a file from its chunks.
    """
    try:
        with open(output_path, 'wb') as output_file:
            for i in range(chunk_count):
                chunk_filename = f"{file_hash}.{i}"
                chunk_path = os.path.join(chunks_dir, chunk_filename)
                
                if not os.path.exists(chunk_path):
                    logging.error(f"Missing chunk {i} ({chunk_path}) for file {file_hash}")
                    return False
                
                with open(chunk_path, 'rb') as chunk_file:
                    output_file.write(chunk_file.read())
        
        logging.info(f"Successfully reassembled file {file_hash} to {output_path}")
        return True
    except IOError as e:
        logging.error(f"Error reassembling file {file_hash}: {e}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred in reassemble_file: {e}")
        return False

# --- NEW FUNCTION ---
def verify_chunk_data(chunk_data: bytes, expected_hash: str) -> bool:
    """Verifies the integrity of a single chunk's raw data."""
    try:
        actual_hash = hashlib.sha256(chunk_data).hexdigest()
        is_valid = actual_hash == expected_hash
        if not is_valid:
            logging.warning(f"Chunk integrity check FAILED. Expected {expected_hash}, got {actual_hash}")
        return is_valid
    except Exception as e:
        logging.error(f"Error verifying chunk data: {e}")
        return False

def verify_chunk(chunk_path: str, expected_hash: str) -> bool:
    """Verifies the integrity of a single file chunk."""
    try:
        with open(chunk_path, 'rb') as f:
            chunk_data = f.read()
            return verify_chunk_data(chunk_data, expected_hash)
    except IOError as e:
        logging.error(f"Could not read chunk {chunk_path} for verification: {e}")
        return False

def verify_file_integrity(file_path: str, expected_hash: str) -> bool:
    """Verifies the integrity of a fully reassembled file."""
    actual_hash = get_file_hash(file_path)
    if actual_hash is None:
        return False # File doesn't exist or read error
        
    is_valid = actual_hash == expected_hash
    if not is_valid:
        logging.warning(f"File integrity check FAILED for {file_path}. Expected {expected_hash}, got {actual_hash}")
    else:
        logging.info(f"File integrity check SUCCESS for {file_path}")
    return is_valid