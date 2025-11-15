import socket
import json
import logging
import yaml
import os
from typing import Dict, Any, Optional, Tuple

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [Network] - %(levelname)s - %(message)s')
CONFIG_FILE = 'config.yaml'

def load_config():
    """Loads configuration from config.yaml"""
    with open(CONFIG_FILE, 'r') as f:
        return yaml.safe_load(f)

# --- General Socket Functions ---

def send_message(sock: socket.socket, message: Dict[str, Any]):
    """Serializes and sends a JSON message."""
    try:
        sock.sendall(json.dumps(message).encode('utf-8'))
    except socket.error as e:
        logging.error(f"Failed to send message: {e}")
        raise

def receive_message(sock: socket.socket, buffer_size: int) -> Optional[Dict[str, Any]]:
    """
    Receives and deserializes a JSON message.
    WARNING: This is a simple implementation. It assumes one recv()
    call will get one complete JSON message and nothing more.
    It's fine for simple, single-response tracker comms, but
    not for streaming JSON + raw data.
    """
    try:
        data = sock.recv(buffer_size)
        if not data:
            return None
        return json.loads(data.decode('utf-8'))
    except json.JSONDecodeError:
        logging.warning("Received invalid JSON message.")
        return None
    except socket.error as e:
        logging.error(f"Failed to receive message: {e}")
        return None
    except Exception as e:
        logging.error(f"Error in receive_message: {e}")
        return None

# --- Tracker Communication ---

def connect_to_tracker() -> Optional[socket.socket]:
    """Establishes a connection with the tracker server."""
    try:
        config = load_config()
        tracker_host = config['tracker']['host']
        tracker_port = config['tracker']['port']
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((tracker_host, tracker_port))
        sock.settimeout(10.0) # 10 second timeout
        logging.info(f"Connected to tracker at {tracker_host}:{tracker_port}")
        return sock
    except socket.error as e:
        logging.error(f"Could not connect to tracker: {e}")
        return None

def register_with_tracker(sock: socket.socket, peer_id: str, peer_port: int, files: list) -> Dict[str, Any]:
    """Sends a 'register' command to the tracker."""
    message = {
        "command": "register",
        "payload": {
            "peer_id": peer_id,
            "port": peer_port,
            "files": files
        }
    }
    send_message(sock, message)
    response = receive_message(sock, load_config()['tracker']['buffer_size'])
    return response or {"status": "error", "message": "No response from tracker"}

def query_tracker_for_file(sock: socket.socket, file_hash: str) -> Dict[str, Any]:
    """Sends a 'query_file' command to the tracker."""
    message = {
        "command": "query_file",
        "payload": {
            "file_hash": file_hash
        }
    }
    send_message(sock, message)
    response = receive_message(sock, load_config()['tracker']['buffer_size'])
    return response or {"status": "error", "message": "No response from tracker"}

# --- Peer-to-Peer Communication ---

def request_chunk_from_peer(peer_addr: Tuple[str, int], file_hash: str, chunk_index: int) -> Optional[bytes]:
    """
    Connects to another peer and requests a specific file chunk.
    
    Returns:
        The raw chunk data (bytes) if successful, None otherwise.
    """
    logging.info(f"Requesting chunk {chunk_index} of {file_hash[:10]}... from {peer_addr}")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(15.0) # 15 second timeout for P2P
            sock.connect(peer_addr)
            
            # 1. Send request message (JSON)
            request_message = {
                "command": "request_chunk",
                "payload": {
                    "file_hash": file_hash,
                    "chunk_index": chunk_index
                }
            }
            send_message(sock, request_message)
            
            # -----------------------------------------------------------------
            # --- START: MODIFIED CHUNK RECEIVE LOGIC ---
            # -----------------------------------------------------------------
            
            # 2. Receive response header (JSON)
            # We read from the stream until we can parse a complete JSON object.
            # This is robust against TCP bundling the JSON and raw data.
            buffer = b""
            raw_data_buffer = b"" # To store any over-read data
            json_decoder = json.JSONDecoder()

            while True:
                try:
                    # Try to decode the buffer
                    response_header, index = json_decoder.raw_decode(buffer.decode('utf-8'))
                    # If successful, store the remainder (start of raw chunk)
                    raw_data_buffer = buffer[index:]
                    break # We got our header
                except json.JSONDecodeError:
                    # Not enough data yet, read more
                    data = sock.recv(128) # Read a small chunk
                    if not data:
                        logging.warning(f"Peer {peer_addr} disconnected while sending header.")
                        return None
                    buffer += data
                except UnicodeDecodeError:
                    # This can happen if buffer is mid-character
                    data = sock.recv(1)
                    if not data:
                        logging.warning(f"Peer {peer_addr} disconnected on unicode error.")
                        return None
                    buffer += data
                
            if response_header.get('status') != 'success':
                logging.warning(f"Peer {peer_addr} returned error: {response_header.get('message')}")
                return None

            chunk_size = response_header.get('chunk_size')
            if chunk_size is None:
                logging.error(f"Peer {peer_addr} sent invalid success response (missing chunk_size)")
                return None

            # 3. Receive the raw chunk data
            # We start with whatever we over-read from the header
            chunk_data = raw_data_buffer
            bytes_received = len(raw_data_buffer)
            
            buffer_size = load_config()['tracker']['buffer_size'] # Get buffer size
            
            while bytes_received < chunk_size:
                bytes_to_read = min(buffer_size, chunk_size - bytes_received)
                data = sock.recv(bytes_to_read)
                if not data:
                    logging.error(f"Peer {peer_addr} disconnected before sending full chunk.")
                    return None
                chunk_data += data
                bytes_received += len(data)
            
            # -----------------------------------------------------------------
            # --- END: MODIFIED CHUNK RECEIVE LOGIC ---
            # -----------------------------------------------------------------
            
            logging.info(f"Successfully received chunk {chunk_index} ({bytes_received} bytes) from {peer_addr}")
            return chunk_data

    except socket.timeout:
        logging.warning(f"Connection to peer {peer_addr} timed out.")
        return None
    except socket.error as e:
        logging.error(f"Socket error with peer {peer_addr}: {e}")
        return None
    except Exception as e:
        logging.error(f"Error in request_chunk_from_peer: {e}")
        return None

def handle_peer_request(conn: socket.socket, addr: Tuple[str, int], storage_manager):
    """
    Handles an incoming connection from another peer.
    This function is run in a thread by the peer's server.
    """
    logging.info(f"Handling incoming peer connection from {addr}")
    config = load_config()
    buffer_size = config['tracker']['buffer_size']
    
    try:
        # 1. Receive the request message (JSON)
        message = receive_message(conn, buffer_size)
        if not message:
            logging.warning(f"No message received from peer {addr}. Closing.")
            return

        if message.get('command') != 'request_chunk':
            response = {"status": "error", "message": "Unknown command"}
            send_message(conn, response)
            return
            
        # 2. Process the request
        payload = message.get('payload', {})
        file_hash = payload.get('file_hash')
        chunk_index = payload.get('chunk_index')

        if file_hash is None or chunk_index is None:
            response = {"status": "error", "message": "Missing file_hash or chunk_index"}
            send_message(conn, response)
            return

        # 3. Get chunk from storage
        chunk_path = storage_manager.get_chunk_path(file_hash, chunk_index)
        
        if chunk_path and os.path.exists(chunk_path):
            try:
                with open(chunk_path, 'rb') as f:
                    chunk_data = f.read()
                
                # 4a. Send success header + raw chunk data
                response_header = {
                    "status": "success",
                    "chunk_size": len(chunk_data)
                }
                send_message(conn, response_header)
                conn.sendall(chunk_data)
                
                logging.info(f"Sent chunk {chunk_index} of {file_hash[:10]}... to {addr}")
                # TODO: Update reputation for successful upload
                # This is tricky because we don't know the peer_id, only IP/port
                # We would need to pass the reputation_manager here and
                # do a reverse lookup (ip:port -> peer_id)

            except IOError as e:
                logging.error(f"Could not read chunk {chunk_path}: {e}")
                response = {"status": "error", "message": "Internal file read error"}
                send_message(conn, response)
        else:
            # 4b. Send error message
            logging.warning(f"Peer {addr} requested chunk {chunk_index} we don't have.")
            response = {"status": "error", "message": "Chunk not found"}
            send_message(conn, response)

    except socket.error as e:
        logging.warning(f"Socket error with peer {addr}: {e}")
    except Exception as e:
        logging.error(f"Error handling peer {addr}: {e}")
    finally:
        conn.close()