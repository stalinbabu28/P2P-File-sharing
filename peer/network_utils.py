import socket
import json
import logging
import yaml
import os
from typing import Dict, Any, Optional, Tuple


logger = logging.getLogger(__name__)
CONFIG_FILE = 'config.yaml'

def load_config():
    """Loads configuration from config.yaml"""
    with open(CONFIG_FILE, 'r') as f:
        return yaml.safe_load(f)


def send_message(sock: socket.socket, message: Dict[str, Any]):
    """Serializes and sends a JSON message."""
    try:
        sock.sendall(json.dumps(message).encode('utf-8'))
    except socket.error as e:
        logger.error(f"Failed to send message: {e}")
        raise

def receive_json_message(sock: socket.socket, buffer_size: int) -> Optional[Dict[str, Any]]:
    """
    Robsutly receives a complete JSON message, handling large messages
    that exceed the buffer size.
    """
    buffer = b""
    json_decoder = json.JSONDecoder()
    
    while True:
        try:
            message, index = json_decoder.raw_decode(buffer.decode('utf-8'))
            return message
        except json.JSONDecodeError:
            try:
                data = sock.recv(buffer_size)
                if not data:
                    logger.warning("Connection closed while receiving message.")
                    return None
                buffer += data
            except socket.timeout:
                logger.warning("Socket timed out waiting for message.")
                return None
            except socket.error as e:
                logger.error(f"Socket error on recv: {e}")
                return None
        except UnicodeDecodeError:
            try:
                data = sock.recv(1)
                if not data:
                    return None
                buffer += data
            except socket.error as e:
                logger.error(f"Socket error on recv (unicode fix): {e}")
                return None
        except Exception as e:
            logger.error(f"Error in receive_json_message: {e}")
            return None


def connect_to_tracker() -> Optional[socket.socket]:
    """Establishes a connection with the tracker server."""
    try:
        config = load_config()
        tracker_host = config['tracker']['host']
        tracker_port = config['tracker']['port']
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((tracker_host, tracker_port))
        sock.settimeout(10.0)
        logger.info(f"Connected to tracker at {tracker_host}:{tracker_port}")
        return sock
    except socket.error as e:
        logger.error(f"Could not connect to tracker: {e}")
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
    response = receive_json_message(sock, load_config()['tracker']['buffer_size'])
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
    response = receive_json_message(sock, load_config()['tracker']['buffer_size'])
    return response or {"status": "error", "message": "No response from tracker"}


def request_chunk_from_peer(peer_addr: Tuple[str, int], file_hash: str, chunk_index: int) -> Optional[bytes]:
    """
    Connects to another peer and requests a specific file chunk.
    
    Returns:
        The raw chunk data (bytes) if successful, None otherwise.
    """
    logger.debug(f"Requesting chunk {chunk_index} of {file_hash[:10]}... from {peer_addr}")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(15.0)
            sock.connect(peer_addr)
            
            request_message = {
                "command": "request_chunk",
                "payload": {
                    "file_hash": file_hash,
                    "chunk_index": chunk_index
                }
            }
            send_message(sock, request_message)
            
            buffer = b""
            raw_data_buffer = b""

            while True:
                end_json_index = buffer.find(b'}')
                if end_json_index != -1:
                    header_bytes = buffer[:end_json_index + 1]
                    raw_data_buffer = buffer[end_json_index + 1:] 
                    try:
                        response_header = json.loads(header_bytes.decode('utf-8'))
                        break 
                    except json.JSONDecodeError:
                        pass 
                
                data = sock.recv(128)
                if not data:
                    logger.warning(f"Peer {peer_addr} disconnected while sending header.")
                    return None
                buffer += data
                
                if len(buffer) > 2048:
                    logger.error(f"Header from {peer_addr} is too large or invalid. Aborting.")
                    return None
            
            if response_header.get('status') != 'success':
                logger.warning(f"Peer {peer_addr} returned error: {response_header.get('message')}")
                return None

            chunk_size = response_header.get('chunk_size')
            if chunk_size is None:
                logger.error(f"Peer {peer_addr} sent invalid success response (missing chunk_size)")
                return None

            chunk_data = raw_data_buffer 
            bytes_received = len(raw_data_buffer)
            buffer_size = load_config()['tracker']['buffer_size']
            
            while bytes_received < chunk_size:
                bytes_to_read = min(buffer_size, chunk_size - bytes_received)
                data = sock.recv(bytes_to_read)
                if not data:
                    logger.error(f"Peer {peer_addr} disconnected before sending full chunk.")
                    return None
                chunk_data += data
                bytes_received += len(data)
            
            logger.debug(f"Successfully received chunk {chunk_index} ({bytes_received} bytes) from {peer_addr}")
            return chunk_data

    except socket.timeout:
        logger.warning(f"Connection to peer {peer_addr} timed out.")
        return None
    except socket.error as e:
        logger.error(f"Socket error with peer {peer_addr}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error in request_chunk_from_peer: {e}")
        return None

def handle_peer_request(conn: socket.socket, addr: Tuple[str, int], storage_manager):
    """
    Handles an incoming connection from another peer.
    This function is run in a thread by the peer's server.
    """
    logger.info(f"Handling incoming peer connection from {addr}")
    config = load_config()
    buffer_size = config['tracker']['buffer_size']
    chunk_size = config['peer']['chunk_size'] 
    
    try:
        message = receive_json_message(conn, buffer_size)
        if not message:
            logger.warning(f"No message received from peer {addr}. Closing.")
            return

        if message.get('command') != 'request_chunk':
            response = {"status": "error", "message": "Unknown command"}
            send_message(conn, response)
            return
            
        payload = message.get('payload', {})
        file_hash = payload.get('file_hash')
        chunk_index = payload.get('chunk_index')

        if file_hash is None or chunk_index is None:
            response = {"status": "error", "message": "Missing file_hash or chunk_index"}
            send_message(conn, response)
            return

        chunk_data = storage_manager.get_chunk_data_for_upload(file_hash, chunk_index, chunk_size)
        
        if chunk_data:
            try:
                response_header = {
                    "status": "success",
                    "chunk_size": len(chunk_data)
                }
                send_message(conn, response_header)
                conn.sendall(chunk_data)
                
                logger.info(f"Sent chunk {chunk_index} of {file_hash[:10]}... to {addr}")

            except IOError as e:
                logger.error(f"Could not read chunk data: {e}")
                response = {"status": "error", "message": "Internal file read error"}
                send_message(conn, response)
        else:
            logger.warning(f"Peer {addr} requested chunk {chunk_index} we don't have.")
            response = {"status": "error", "message": "Chunk not found"}
            send_message(conn, response)

    except socket.error as e:
        logger.warning(f"Socket error with peer {addr}: {e}")
    except Exception as e:
        logger.error(f"Error handling peer {addr}: {e}")
    finally:
        conn.close()