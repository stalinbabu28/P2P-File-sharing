import os
import hashlib
import logging
from typing import List, Dict, Optional
from tqdm import tqdm

# --- Configuration ---
# Get a logger for this specific module
logger = logging.getLogger(__name__)

def get_file_hash(file_path: str) -> Optional[str]:
    """Calculates the SHA-256 hash of an entire file."""
    if not os.path.exists(file_path):
        logger.warning(f"File not found, cannot calculate hash: {file_path}")
        return None
        
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            # Read and update hash in chunks
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except IOError as e:
        logger.error(f"Error reading file {file_path} for hashing: {e}")
        return None

def get_file_metadata(file_path: str, chunk_size: int) -> Optional[Dict[str, any]]:
    """
    Reads a file and generates all metadata (main hash, chunk hashes)
    without writing any new files.
    """
    if not os.path.exists(file_path):
        logger.warning(f"File not found, cannot get metadata: {file_path}")
        return None

    try:
        file_size = os.path.getsize(file_path)
        file_name = os.path.basename(file_path)
        file_hash = get_file_hash(file_path) # Main file hash
        
        if file_hash is None:
            return None # Hashing failed

        chunk_hashes = []
        chunk_count = 0
        
        # Read the file again, this time for chunk hashes
        with open(file_path, 'rb') as f:
            with tqdm(
                total=file_size, 
                desc=f"Analyzing {file_name}", 
                unit='B', 
                unit_scale=True, 
                unit_divisor=1024
            ) as pbar:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    
                    chunk_hash = hashlib.sha256(chunk).hexdigest()
                    chunk_hashes.append(chunk_hash)
                    chunk_count += 1
                    pbar.update(len(chunk))
        
        logger.info(f"Analyzed {file_name}. {chunk_count} chunks.")
        
        return {
            "name": file_name,
            "size": file_size,
            "hash": file_hash,
            "chunk_count": chunk_count,
            "chunk_hashes": chunk_hashes
        }
    except IOError as e:
        logger.error(f"Error reading file {file_path} for metadata: {e}")
        return None

def read_chunk_from_file(file_path: str, chunk_index: int, chunk_size: int) -> Optional[bytes]:
    """
    Reads a specific chunk from a file using seek.
    """
    offset = chunk_index * chunk_size
    try:
        with open(file_path, 'rb') as f:
            f.seek(offset)
            return f.read(chunk_size)
    except IOError as e:
        logger.error(f"Error reading chunk {chunk_index} from {file_path}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error in read_chunk_from_file: {e}")
        return None

def reassemble_file(file_hash: str, chunk_count: int, chunks_dir: str, output_path: str) -> bool:
    """
    Reassembles a file from its chunks.
    """
    try:
        with open(output_path, 'wb') as output_file:
            # Use tqdm for reassembly progress
            for i in tqdm(range(chunk_count), desc=f"Reassembling {os.path.basename(output_path)}", unit="chunk"):
                chunk_filename = f"{file_hash}.{i}"
                chunk_path = os.path.join(chunks_dir, chunk_filename)
                
                if not os.path.exists(chunk_path):
                    logger.error(f"Missing chunk {i} ({chunk_path}) for file {file_hash}")
                    return False
                
                with open(chunk_path, 'rb') as chunk_file:
                    output_file.write(chunk_file.read())
        
        logger.info(f"Successfully reassembled file {file_hash} to {output_path}")
        return True
    except IOError as e:
        logger.error(f"Error reassembling file {file_hash}: {e}")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred in reassemble_file: {e}")
        return False

def verify_chunk_data(chunk_data: bytes, expected_hash: str) -> bool:
    """Verifies the integrity of a single chunk's raw data."""
    try:
        actual_hash = hashlib.sha256(chunk_data).hexdigest()
        is_valid = actual_hash == expected_hash
        if not is_valid:
            logger.warning(f"Chunk integrity check FAILED. Expected {expected_hash}, got {actual_hash}")
        return is_valid
    except Exception as e:
        logger.error(f"Error verifying chunk data: {e}")
        return False

def verify_file_integrity(file_path: str, expected_hash: str) -> bool:
    """Verifies the integrity of a fully reassembled file."""
    actual_hash = get_file_hash(file_path)
    if actual_hash is None:
        return False # File doesn't exist or read error
        
    is_valid = actual_hash == expected_hash
    if not is_valid:
        logger.warning(f"File integrity check FAILED for {file_path}. Expected {expected_hash}, got {actual_hash}")
    else:
        logger.info(f"File integrity check SUCCESS for {file_path}")
    return is_valid

# --- This function is deprecated but kept for completeness ---
def split_file(file_path: str, chunk_size: int, output_dir: str) -> Optional[Dict[str, any]]:
    """
    Splits a file into chunks and saves them to a directory.
    (This is the old, deprecated function. Use get_file_metadata for sharing)
    """
    logger.warning("split_file is a deprecated function. Use get_file_metadata for sharing.")
    if not os.path.exists(file_path):
        logger.warning(f"File not found, cannot split: {file_path}")
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
        
        logger.info(f"Split {file_name} into {chunk_count} chunks in {output_dir}")
        
        return {
            "name": file_name,
            "size": file_size,
            "hash": file_hash,
            "chunk_count": chunk_count,
            "chunk_hashes": chunk_hashes
        }
    except IOError as e:
        logger.error(f"Error splitting file {file_path}: {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred in split_file: {e}")
        return None