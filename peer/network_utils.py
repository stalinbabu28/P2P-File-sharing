import socket
import json
import logging
import yaml
import os
from typing import Dict, Any, Optional, Tuple

# --- Configuration ---
logger = logging.getLogger(__name__)
CONFIG_FILE = 'config.yaml'

def load_config():
    with open(CONFIG_FILE, 'r') as f:
        return yaml.safe_load(f)

def send_message(sock: socket.socket, message: Dict[str, Any]):
    try:
        sock.sendall(json.dumps(message).encode('utf-8'))
    except socket.error as e:
        logger.error(f"Failed to send: {e}")
        raise

def receive_json_message(sock: socket.socket, buffer_size: int) -> Optional[Dict[str, Any]]:
    buffer = b""
    json_decoder = json.JSONDecoder()
    while True:
        try:
            message, index = json_decoder.raw_decode(buffer.decode('utf-8'))
            return message
        except json.JSONDecodeError:
            try:
                data = sock.recv(buffer_size)
                if not data: return None
                buffer += data
            except Exception: return None
        except Exception: return None

# --- Tracker Communication ---

def connect_to_tracker() -> Optional[socket.socket]:
    try:
        config = load_config()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((config['tracker']['host'], config['tracker']['port']))
        sock.settimeout(10.0)
        return sock
    except Exception as e:
        logger.error(f"Tracker connection failed: {e}")
        return None

def register_with_tracker(sock: socket.socket, peer_id: str, peer_port: int, files: list) -> Dict[str, Any]:
    msg = {"command": "register", "payload": {"peer_id": peer_id, "port": peer_port, "files": files}}
    send_message(sock, msg)
    return receive_json_message(sock, load_config()['tracker']['buffer_size']) or {}

def query_tracker_for_file(sock: socket.socket, file_hash: str) -> Dict[str, Any]:
    msg = {"command": "query_file", "payload": {"file_hash": file_hash}}
    send_message(sock, msg)
    return receive_json_message(sock, load_config()['tracker']['buffer_size']) or {}

# --- NEW SEARCH FUNCTION ---
def search_tracker(sock: socket.socket, query: str) -> Dict[str, Any]:
    msg = {"command": "search", "payload": {"query": query}}
    send_message(sock, msg)
    return receive_json_message(sock, load_config()['tracker']['buffer_size']) or {}

# --- Peer Communication ---

def request_chunk_from_peer(peer_addr: Tuple[str, int], file_hash: str, chunk_index: int) -> Optional[bytes]:
    logger.debug(f"Requesting chunk {chunk_index} from {peer_addr}")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(15.0)
            sock.connect(peer_addr)
            
            send_message(sock, {
                "command": "request_chunk", 
                "payload": {"file_hash": file_hash, "chunk_index": chunk_index}
            })
            
            buffer = b""
            raw_data_buffer = b""
            while True:
                end_idx = buffer.find(b'}')
                if end_idx != -1:
                    header_bytes = buffer[:end_idx + 1]
                    raw_data_buffer = buffer[end_idx + 1:]
                    try:
                        header = json.loads(header_bytes.decode('utf-8'))
                        break
                    except: pass
                data = sock.recv(128)
                if not data: return None
                buffer += data
            
            if header.get('status') != 'success': return None
            
            chunk_data = raw_data_buffer
            received = len(chunk_data)
            total = header.get('chunk_size', 0)
            cfg_buf = load_config()['tracker']['buffer_size']
            
            while received < total:
                data = sock.recv(min(cfg_buf, total - received))
                if not data: return None
                chunk_data += data
                received += len(data)
            
            return chunk_data
    except Exception:
        return None

def handle_peer_request(conn: socket.socket, addr: Tuple[str, int], storage_manager):
    logger.info(f"Connection from {addr}")
    try:
        config = load_config()
        msg = receive_json_message(conn, config['tracker']['buffer_size'])
        if not msg: return

        if msg.get('command') == 'request_chunk':
            p = msg.get('payload', {})
            data = storage_manager.get_chunk_data_for_upload(
                p.get('file_hash'), p.get('chunk_index'), config['peer']['chunk_size']
            )
            if data:
                send_message(conn, {"status": "success", "chunk_size": len(data)})
                conn.sendall(data)
                logger.info(f"Sent chunk {p.get('chunk_index')} to {addr}")
            else:
                send_message(conn, {"status": "error", "message": "Not found"})
    except Exception as e:
        logger.error(f"Handler error: {e}")
    finally:
        conn.close()