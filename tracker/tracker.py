import socket
import threading
import json
import yaml
import logging
from typing import Dict, Any, List, Tuple, Optional

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [Tracker] - %(levelname)s - %(message)s')

CONFIG_FILE = 'config.yaml'

def load_config():
    try:
        with open(CONFIG_FILE, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logging.error(f"Error loading config: {e}")
        exit(1)

# --- Global State ---
# file_index -> {"file_hash": {"name": "...", "size": ..., "chunk_count": ..., "chunk_hashes": [...], "peers": set()}}
file_index: Dict[str, Dict[str, Any]] = {}
peer_registry: Dict[str, Tuple[str, int]] = {}

index_lock = threading.Lock()
peer_lock = threading.Lock()

# --- Receive Helper ---
def receive_json_message(conn: socket.socket, buffer_size: int) -> Optional[Dict[str, Any]]:
    buffer = b""
    json_decoder = json.JSONDecoder()
    while True:
        try:
            message, index = json_decoder.raw_decode(buffer.decode('utf-8'))
            return message
        except json.JSONDecodeError:
            data = conn.recv(buffer_size)
            if not data: return None
            buffer += data
        except Exception:
            return None

# --- Tracker Logic ---

def handle_register(payload: Dict[str, Any], client_ip: str) -> Dict[str, Any]:
    try:
        peer_id = payload['peer_id']
        peer_port = payload['port']
        files = payload['files']
        
        with peer_lock:
            peer_registry[peer_id] = (client_ip, peer_port)
            logging.info(f"Registered peer {peer_id} at {client_ip}:{peer_port}")

        with index_lock:
            for file_info in files:
                file_hash = file_info['hash']
                if file_hash not in file_index:
                    file_index[file_hash] = {
                        "name": file_info['name'],
                        "size": file_info['size'],
                        "chunk_count": file_info['chunk_count'],
                        "chunk_hashes": file_info['chunk_hashes'],
                        "peers": set()
                    }
                file_index[file_hash]["peers"].add(peer_id)
        return {"status": "success", "message": "Registered"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def handle_query_file(payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        file_hash = payload['file_hash']
        with index_lock:
            if file_hash not in file_index:
                return {"status": "error", "message": "File not found"}
            
            file_info = file_index[file_hash]
            peer_ids = file_info["peers"]
            
            found_peers = []
            with peer_lock:
                for peer_id in peer_ids:
                    if peer_id in peer_registry:
                        ip, port = peer_registry[peer_id]
                        found_peers.append({"id": peer_id, "ip": ip, "port": port})
            
            return {
                "status": "success",
                "file_name": file_info["name"],
                "file_size": file_info["size"],
                "chunk_count": file_info["chunk_count"],
                "chunk_hashes": file_info["chunk_hashes"],
                "peers": found_peers
            }
    except Exception as e:
        return {"status": "error", "message": str(e)}

# --- NEW SEARCH LOGIC ---
def handle_search(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Searches for files containing the query string."""
    query = payload.get('query', '').lower()
    results = []
    
    with index_lock:
        for file_hash, meta in file_index.items():
            if query in meta['name'].lower():
                results.append({
                    "hash": file_hash,
                    "name": meta['name'],
                    "size": meta['size'],
                    "seeders": len(meta['peers'])
                })
    
    return {"status": "success", "results": results}

def handle_deregister(peer_id: str):
    try:
        with peer_lock:
            if peer_id in peer_registry:
                del peer_registry[peer_id]
        with index_lock:
            to_prune = []
            for fh, data in file_index.items():
                if peer_id in data["peers"]:
                    data["peers"].remove(peer_id)
                if not data["peers"]:
                    to_prune.append(fh)
            for fh in to_prune:
                del file_index[fh]
        logging.info(f"Deregistered {peer_id}")
    except Exception:
        pass

def handle_client(conn: socket.socket, addr: Tuple[str, int]):
    logging.info(f"New connection from {addr}")
    config = load_config()
    peer_id = None
    try:
        while True:
            message = receive_json_message(conn, config['tracker']['buffer_size'])
            if not message: break
            
            command = message.get('command')
            payload = message.get('payload', {})
            if 'peer_id' in payload: peer_id = payload['peer_id']

            if command == 'register':
                resp = handle_register(payload, addr[0])
            elif command == 'query_file':
                resp = handle_query_file(payload)
            elif command == 'search':  # <-- Handle search command
                resp = handle_search(payload)
            else:
                resp = {"status": "error", "message": "Unknown command"}
            
            conn.sendall(json.dumps(resp).encode('utf-8'))
    except Exception as e:
        logging.warning(f"Client error: {e}")
    finally:
        if peer_id: handle_deregister(peer_id)
        conn.close()

def main():
    config = load_config()
    host = config['tracker']['host']
    port = config['tracker']['port']
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.listen(5)
        logging.info(f"Tracker listening on {host}:{port}")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    main()