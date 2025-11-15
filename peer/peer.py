import socket
import threading
import uuid
import logging
import time
import os
import yaml
from typing import Dict, Any, Optional

from peer.storage import StorageManager
from peer.reputation import ReputationManager
from peer import file_utils
from peer import network_utils

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [Peer] - %(levelname)s - %(message)s')
CONFIG_FILE = 'config.yaml'

class Peer:
    def __init__(self):
        self.peer_id: str = f"peer_{uuid.uuid4().hex[:8]}"
        self.config = self._load_config()
        
        # Initialize helper modules
        self.storage: StorageManager = StorageManager(self.peer_id)
        self.reputation: ReputationManager = ReputationManager(self.peer_id)
        
        # Peer server setup
        self.server_port: int = self._get_free_port()
        self.server_host: str = "127.0.0.1" # Listen on localhost
        self.server_thread: Optional[threading.Thread] = None
        self.is_running: bool = True
        
        # --- FIX: Tracker connection is now a class member ---
        self.tracker_sock: Optional[socket.socket] = None
        
        logging.info(f"Peer {self.peer_id} initializing...")
        logging.info(f"Storage location: {self.storage.base_dir}")
        logging.info(f"Reputation DB: {self.reputation.db_path}")

    def _load_config(self) -> Dict[str, Any]:
        """Loads configuration from config.yaml"""
        try:
            with open(CONFIG_FILE, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logging.error(f"Failed to load config: {e}. Exiting.")
            exit(1)

    def _get_free_port(self) -> int:
        """Finds and returns an available port on the system."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0)) # Bind to port 0 to let OS pick
            return s.getsockname()[1] # Return the chosen port

    def start_server(self):
        """Starts the P2P server in a new thread to listen for other peers."""
        self.server_thread = threading.Thread(target=self._run_server, daemon=True)
        self.server_thread.start()
        logging.info(f"Peer server starting on {self.server_host}:{self.server_port}...")

    def _run_server(self):
        """The main loop for the peer's P2P server."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                server_socket.bind((self.server_host, self.server_port))
                server_socket.listen(5)
                
                while self.is_running:
                    try:
                        # Set a timeout so the loop can check `self.is_running`
                        server_socket.settimeout(1.0)
                        conn, addr = server_socket.accept()
                        
                        # Handle the peer connection in a new thread
                        handler_thread = threading.Thread(
                            target=network_utils.handle_peer_request,
                            args=(conn, addr, self.storage),
                            daemon=True
                        )
                        handler_thread.start()
                    except socket.timeout:
                        continue # Go back to check `self.is_running`
                    
        except OSError as e:
            if self.is_running:
                logging.error(f"Peer server socket error: {e}")
        finally:
            logging.info("Peer server shutting down.")

    # --- NEW METHOD ---
    def start_tracker_connection(self):
        """Establishes a persistent connection to the tracker."""
        if self.tracker_sock:
            logging.info("Tracker connection already established.")
            return
        
        logging.info("Connecting to tracker...")
        self.tracker_sock = network_utils.connect_to_tracker()
        if not self.tracker_sock:
            logging.error("Failed to connect to tracker. Peer will not be able to register or query.")

    def register_with_tracker(self):
        """Contacts the tracker to register this peer and its files."""
        # --- MODIFIED: Use self.tracker_sock ---
        if not self.tracker_sock:
            logging.warning("No tracker connection. Attempting to reconnect...")
            self.start_tracker_connection()
            if not self.tracker_sock:
                logging.error("Reconnect failed. Cannot register.")
                return

        logging.info("Registering with tracker...")
        try:
            files_info = self.storage.get_shared_files_info()
            response = network_utils.register_with_tracker(
                self.tracker_sock, self.peer_id, self.server_port, files_info
            )
            
            if response.get('status') == 'success':
                logging.info("Successfully registered with tracker.")
            else:
                logging.warning(f"Tracker registration failed: {response.get('message')}")
        except Exception as e:
            # If send fails, close socket and set to None so we can retry
            logging.error(f"Error communicating with tracker: {e}. Closing connection.")
            if self.tracker_sock:
                self.tracker_sock.close()
            self.tracker_sock = None
        # --- REMOVED: finally: tracker_sock.close() ---

    def share_file(self, file_path: str):
        """Adds a new file to the shared list and updates the tracker."""
        logging.info(f"Sharing new file: {file_path}")
        if not os.path.exists(file_path):
            logging.error(f"File not found: {file_path}")
            return

        chunk_size = self.config['peer']['chunk_size']
        file_meta = self.storage.add_file_to_share(file_path, chunk_size)
        
        if file_meta:
            logging.info(f"File '{file_meta['name']}' processed. Hash: {file_meta['hash']}")
            # Re-register with tracker to announce the new file
            self.register_with_tracker()
        else:
            logging.error(f"Failed to process and share file: {file_path}")

    def download_file(self, file_hash: str):
        """Main file download logic."""
        logging.info(f"Attempting to download file: {file_hash[:10]}...")
        
        # 1. Query Tracker
        # --- MODIFIED: Use self.tracker_sock ---
        if not self.tracker_sock:
            logging.warning("No tracker connection. Attempting to reconnect...")
            self.start_tracker_connection()
            if not self.tracker_sock:
                logging.error("Reconnect failed. Cannot query tracker.")
                return

        try:
            response = network_utils.query_tracker_for_file(self.tracker_sock, file_hash)
        except Exception as e:
            # If query fails, close socket and set to None
            logging.error(f"Error querying tracker: {e}. Closing connection.")
            if self.tracker_sock:
                self.tracker_sock.close()
            self.tracker_sock = None
            return # Abort download
        # --- REMOVED: finally: tracker_sock.close() ---
            
        if response.get('status') != 'success':
            logging.error(f"Tracker query failed: {response.get('message')}")
            return
            
        # 2. Process Tracker Response
        file_meta = {
            "name": response.get('file_name'),
            "size": response.get('file_size'),
            "hash": file_hash,
            "chunk_count": response.get('chunk_count'),
            "chunk_hashes": response.get('chunk_hashes', []) # TODO: Tracker should send this
        }
        self.storage.add_downloading_file(file_meta)
        
        peer_list = response.get('peers', [])
        if not peer_list:
            logging.warning("File found, but no peers are currently sharing it.")
            return

        # 3. Sort Peers by Reputation (THE CORE LOGIC)
        peer_ids = [p['id'] for p in peer_list]
        sorted_peers_with_score = self.reputation.get_peers_sorted_by_reputation(peer_ids)
        
        # Create a map of id -> (ip, port)
        peer_addr_map = {p['id']: (p['ip'], p['port']) for p in peer_list}
        
        # Create the final sorted list of addresses
        sorted_peer_addrs = []
        for peer_id, score in sorted_peers_with_score:
            if peer_id in peer_addr_map:
                sorted_peer_addrs.append(peer_addr_map[peer_id])
        
        logging.info(f"Found {len(sorted_peer_addrs)} peers. Sorted by reputation:")
        for i, (peer_id, score) in enumerate(sorted_peers_with_score):
            logging.info(f"  {i+1}. {peer_id} (Score: {score:.2f}) at {peer_addr_map.get(peer_id)}")

        # 4. Download Loop
        missing_chunks = list(self.storage.get_missing_chunks(file_hash))
        logging.info(f"Need to download {len(missing_chunks)} chunks...")
        
        for chunk_index in missing_chunks:
            chunk_data = None
            for peer_addr in sorted_peer_addrs:
                # TODO: We don't have peer_id here, only addr.
                # This is a flaw in the current design. We should use peer_id
                # and get the addr from the map.
                # For now, we just try in order.
                
                # Get peer_id from addr for reputation update
                peer_id_for_rep = next((pid for pid, addr in peer_addr_map.items() if addr == peer_addr), None)

                try:
                    chunk_data = network_utils.request_chunk_from_peer(peer_addr, file_hash, chunk_index)
                    
                    if chunk_data:
                        # 4a. Verify Chunk (TODO: use chunk_hashes)
                        # For now, we assume it's good. We'll verify the whole file at the end.
                        # A better way: `file_utils.verify_chunk(data, file_meta['chunk_hashes'][chunk_index])`
                        
                        # 4b. Store Chunk
                        self.storage.store_chunk(file_hash, chunk_index, chunk_data)
                        
                        # 4c. Update Reputation (GOOD)
                        if peer_id_for_rep:
                            self.reputation.update_reputation(peer_id_for_rep, "SUCCESSFUL_DOWNLOAD")
                        
                        logging.info(f"Successfully downloaded chunk {chunk_index} from {peer_addr}")
                        break # Move to the next chunk
                    else:
                        # 4d. Update Reputation (BAD - Refused/Error)
                        if peer_id_for_rep:
                            self.reputation.update_reputation(peer_id_for_rep, "REFUSED_UPLOAD")
                        logging.warning(f"Failed to get chunk {chunk_index} from {peer_addr}. Trying next peer.")
                
                except Exception as e:
                    # 4e. Update Reputation (BAD - Timeout)
                    if peer_id_for_rep:
                        self.reputation.update_reputation(peer_id_for_rep, "CONNECTION_TIMEOUT")
                    logging.warning(f"Error connecting to {peer_addr}: {e}. Trying next peer.")

            if not chunk_data:
                logging.error(f"Failed to download chunk {chunk_index} from any peer. Aborting download.")
                return # Abort download
        
        # 5. Finalize Download
        if self.storage.is_download_complete(file_hash):
            logging.info(f"Download complete for {file_hash[:10]}. Reassembling file...")
            
            output_path = os.path.join(self.storage.completed_dir, file_meta['name'])
            success = file_utils.reassemble_file(
                file_hash,
                file_meta['chunk_count'],
                self.storage.downloads_dir,
                output_path
            )
            
            if success:
                logging.info(f"File reassembled at {output_path}")
                # 6. Final Verification
                if file_utils.verify_file_integrity(output_path, file_hash):
                    logging.info("File integrity VERIFIED. Download successful!")
                    # Now that we have the file, re-register to become a seeder
                    self.register_with_tracker()
                else:
                    logging.error("File integrity check FAILED. Deleting corrupt file.")
                    os.remove(output_path)
            else:
                logging.error("Failed to reassemble file.")
        else:
            logging.error("Download finished, but some chunks are still missing.")

    def stop(self):
        """Stops the peer and closes connections."""
        self.is_running = False
        self.reputation.close()
        
        # --- MODIFIED: Close tracker socket ---
        if self.tracker_sock:
            logging.info("Closing connection to tracker.")
            self.tracker_sock.close()
            
        if self.server_thread:
            self.server_thread.join()
        logging.info(f"Peer {self.peer_id} has shut down.")