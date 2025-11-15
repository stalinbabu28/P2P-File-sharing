import socket
import threading
import json
import yaml
import logging
from typing import Dict, Any, List, Tuple

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [Tracker] - %(levelname)s - %(message)s')

CONFIG_FILE = 'config.yaml'

def load_config():
    """Loads configuration from config.yaml"""
    try:
        with open(CONFIG_FILE, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logging.error(f"Configuration file {CONFIG_FILE} not found. Exiting.")
        exit(1)
    except Exception as e:
        logging.error(f"Error loading config: {e}. Exiting.")
        exit(1)

# --- Global State (Thread-Safe) ---
# We use dictionaries to store the state. For a larger application,
# this would be moved to tracker_db.py and use SQLite.

# file_index -> {"file_hash": {"name": "...", "size": ..., "chunk_count": ..., "peers": set()}}
file_index: Dict[str, Dict[str, Any]] = {}

# peer_registry -> {"peer_id": ("ip", port)}
peer_registry: Dict[str, Tuple[str, int]] = {}

# Locks to ensure thread-safety when modifying global state
index_lock = threading.Lock()
peer_lock = threading.Lock()

# --- Tracker Logic ---

def handle_register(payload: Dict[str, Any], client_ip: str) -> Dict[str, Any]:
    """
    Handles a 'register' command from a peer.
    Registers the peer and the files it announces.
    """
    try:
        peer_id = payload['peer_id']
        peer_port = payload['port']
        files = payload['files'] # List of {"hash": "...", "name": "...", "size": ..., "chunk_count": ...}
        
        peer_addr = (client_ip, peer_port)

        with peer_lock:
            peer_registry[peer_id] = peer_addr
            logging.info(f"Registered peer {peer_id} at {client_ip}:{peer_port}")

        with index_lock:
            for file_info in files:
                file_hash = file_info['hash']
                if file_hash not in file_index:
                    # First time we see this file
                    file_index[file_hash] = {
                        "name": file_info['name'],
                        "size": file_info['size'],
                        "chunk_count": file_info['chunk_count'],
                        "peers": set()
                    }
                # Add this peer to the set of peers for this file
                file_index[file_hash]["peers"].add(peer_id)
                logging.info(f"Indexed file {file_info['name']} (hash: {file_hash[:10]}...) for peer {peer_id}")
        
        return {"status": "success", "message": "Registered successfully"}

    except KeyError as e:
        logging.warning(f"Registration failed. Missing key: {e}")
        return {"status": "error", "message": f"Missing key: {e}"}
    except Exception as e:
        logging.error(f"Error in handle_register: {e}")
        return {"status": "error", "message": str(e)}

def handle_query_file(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handles a 'query_file' command.
    Returns a list of peers that have the requested file.
    """
    try:
        file_hash = payload['file_hash']
        logging.info(f"Received query for file hash: {file_hash[:10]}...")

        with index_lock:
            if file_hash not in file_index:
                logging.info(f"File hash {file_hash[:10]}... not found in index.")
                return {"status": "error", "message": "File not found"}
            
            file_info = file_index[file_hash]
            peer_ids = file_info["peers"]
            
            # --- Reputation Matching (Future Stub) ---
            # This is where we would implement the logic:
            # 1. Get the requesting peer's reputation (requester_rep = get_reputation(payload['peer_id']))
            # 2. Get reputations for all peers in `peer_ids`
            # 3. Sort/filter `peer_ids` based on reputation similarity
            # For now, we return all peers.
            
            found_peers = []
            with peer_lock:
                for peer_id in peer_ids:
                    if peer_id in peer_registry:
                        ip, port = peer_registry[peer_id]
                        found_peers.append({"id": peer_id, "ip": ip, "port": port})
            
            if not found_peers:
                logging.info(f"File hash {file_hash[:10]}... found, but no active peers have it.")
                return {"status": "error", "message": "File found, but no active peers available"}

            response = {
                "status": "success",
                "file_name": file_info["name"],
                "file_size": file_info["size"],
                "chunk_count": file_info["chunk_count"],
                "peers": found_peers
            }
            return response

    except KeyError as e:
        logging.warning(f"Query failed. Missing key: {e}")
        return {"status": "error", "message": f"Missing key: {e}"}
    except Exception as e:
        logging.error(f"Error in handle_query_file: {e}")
        return {"status": "error", "message": str(e)}

def handle_deregister(peer_id: str):
    """
    Handles a peer disconnecting.
    Removes the peer from the registry and all file indexes.
    """
    try:
        with peer_lock:
            if peer_id in peer_registry:
                del peer_registry[peer_id]
                logging.info(f"Deregistered peer {peer_id}")

        with index_lock:
            # This is slow, but simple. A proper DB would be faster.
            files_to_prune = []
            for file_hash, file_data in file_index.items():
                if peer_id in file_data["peers"]:
                    file_data["peers"].remove(peer_id)
                # If no peers have this file anymore, remove it from index
                if not file_data["peers"]:
                    files_to_prune.append(file_hash)
            
            for file_hash in files_to_prune:
                del file_index[file_hash]
                logging.info(f"Pruned file {file_hash[:10]}... from index (no peers).")
                
    except Exception as e:
        logging.error(f"Error in handle_deregister for peer {peer_id}: {e}")


def handle_client(conn: socket.socket, addr: Tuple[str, int]):
    """
    Main loop for handling a single connected client (peer).
    """
    client_ip, client_port = addr
    logging.info(f"New connection from {client_ip}:{client_port}")
    
    config = load_config()
    buffer_size = config['tracker']['buffer_size']
    
    peer_id = None # Track which peer this connection belongs to
    
    try:
        while True:
            data = conn.recv(buffer_size)
            if not data:
                logging.info(f"Connection from {client_ip}:{client_port} closed.")
                break
            
            # Assume messages are JSON strings, separated by newlines
            # This is a simple protocol; real-world might use length prefixing
            try:
                message = json.loads(data.decode('utf-8'))
                logging.debug(f"Received message: {message}")
                
                command = message.get('command')
                payload = message.get('payload', {})
                
                # Snag peer_id from payloads that have it
                if 'peer_id' in payload:
                    peer_id = payload['peer_id']

                response = {}
                if command == 'register':
                    response = handle_register(payload, client_ip)
                elif command == 'query_file':
                    response = handle_query_file(payload)
                # 'update_reputation' would be handled by peers, not the tracker,
                # based on the PDF (decentralized reputation).
                else:
                    response = {"status": "error", "message": "Unknown command"}
                
                # Send response back to peer
                conn.sendall(json.dumps(response).encode('utf-8'))

            except json.JSONDecodeError:
                logging.warning(f"Received invalid JSON from {client_ip}:{client_port}")
                conn.sendall(json.dumps({"status": "error", "message": "Invalid JSON"}).encode('utf-8'))
            except Exception as e:
                logging.error(f"Error handling message: {e}")
                conn.sendall(json.dumps({"status": "error", "message": "Internal server error"}).encode('utf-8'))
    
    except socket.error as e:
        logging.warning(f"Socket error with {client_ip}:{client_port}: {e}")
    finally:
        # Client disconnected
        if peer_id:
            # If the peer ever registered, deregister it
            handle_deregister(peer_id)
        conn.close()
        logging.info(f"Closed connection from {client_ip}:{client_port}")

def main():
    """
    Main function to start the tracker server.
    """
    config = load_config()
    host = config['tracker']['host']
    port = config['tracker']['port']
    
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((host, port))
            server_socket.listen(5)
            logging.info(f"Tracker server listening on {host}:{port}")
            
            while True:
                conn, addr = server_socket.accept()
                # Start a new thread for each client
                thread = threading.Thread(target=handle_client, args=(conn, addr))
                thread.daemon = True # Allows program to exit even if threads are running
                thread.start()
                
    except OSError as e:
        logging.error(f"Failed to bind to {host}:{port}. Error: {e}")
    except KeyboardInterrupt:
        logging.info("Tracker server shutting down.")

if __name__ == "__main__":
    main()