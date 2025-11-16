import socket
import threading
import uuid
import logging
import time
import os
import yaml
from typing import Dict, Any, Optional
from tqdm import tqdm

from peer.storage import StorageManager
from peer.reputation import ReputationManager
from peer import file_utils
from peer import network_utils

logger = logging.getLogger(__name__)
CONFIG_FILE = 'config.yaml'

class Peer:
    def __init__(self):
        self.peer_id: str = f"peer_{uuid.uuid4().hex[:8]}"
        self.config = self._load_config()
        
        self.storage: StorageManager = StorageManager(self.peer_id)
        self.reputation: ReputationManager = ReputationManager(self.peer_id)
        
        self.server_port: int = self._get_free_port()
        self.server_host: str = "127.0.0.1" 
        self.server_thread: Optional[threading.Thread] = None
        self.is_running: bool = True
        
        self.tracker_sock: Optional[socket.socket] = None
        
        logger.info(f"Peer {self.peer_id} initializing...")
        logger.info(f"Storage location: {self.storage.base_dir}")
        logger.info(f"Reputation DB: {self.reputation.db_path}")

    def _load_config(self) -> Dict[str, Any]:
        """Loads configuration from config.yaml"""
        try:
            with open(CONFIG_FILE, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {e}. Exiting.")
            exit(1)

    def _get_free_port(self) -> int:
        """Finds and returns an available port on the system."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0)) 
            return s.getsockname()[1]

    def start_server(self):
        """Starts the P2P server in a new thread to listen for other peers."""
        self.server_thread = threading.Thread(target=self._run_server, daemon=True)
        self.server_thread.start()
        logger.info(f"Peer server starting on {self.server_host}:{self.server_port}...")

    def _run_server(self):
        """The main loop for the peer's P2P server."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                server_socket.bind((self.server_host, self.server_port))
                server_socket.listen(5)
                
                while self.is_running:
                    try:
                        server_socket.settimeout(1.0)
                        conn, addr = server_socket.accept()
                        
                        handler_thread = threading.Thread(
                            target=network_utils.handle_peer_request,
                            args=(conn, addr, self.storage),
                            daemon=True
                        )
                        handler_thread.start()
                    except socket.timeout:
                        continue
                    
        except OSError as e:
            if self.is_running:
                logger.error(f"Peer server socket error: {e}")
        finally:
            logger.info("Peer server shutting down.")

    def start_tracker_connection(self):
        """Establishes a persistent connection to the tracker."""
        if self.tracker_sock:
            logger.info("Tracker connection already established.")
            return
        
        logger.info("Connecting to tracker...")
        self.tracker_sock = network_utils.connect_to_tracker()
        if not self.tracker_sock:
            logger.error("Failed to connect to tracker. Peer will not be able to register or query.")

    def register_with_tracker(self):
        """Contacts the tracker to register this peer and its files."""
        if not self.tracker_sock:
            logger.warning("No tracker connection. Attempting to reconnect...")
            self.start_tracker_connection()
            if not self.tracker_sock:
                logger.error("Reconnect failed. Cannot register.")
                return

        logger.info("Registering with tracker...")
        try:
            files_info = self.storage.get_shared_files_info()
            response = network_utils.register_with_tracker(
                self.tracker_sock, self.peer_id, self.server_port, files_info
            )
            
            if response.get('status') == 'success':
                logger.info("Successfully registered with tracker.")
            else:
                logger.warning(f"Tracker registration failed: {response.get('message')}")
        except Exception as e:
            logger.error(f"Error communicating with tracker: {e}. Closing connection.")
            if self.tracker_sock:
                self.tracker_sock.close()
            self.tracker_sock = None

    def share_file(self, file_path: str):
        """Adds a new file to the shared list and updates the tracker."""
        logger.info(f"Sharing new file: {file_path}")
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return

        chunk_size = self.config['peer']['chunk_size']
        file_meta = self.storage.add_file_to_share(file_path, chunk_size)
        
        if file_meta:
            logger.info(f"File '{file_meta['name']}' processed. Hash: {file_meta['hash']}")
            self.register_with_tracker()
        else:
            logger.error(f"Failed to process and share file: {file_path}")

    def download_file(self, file_hash: str):
        """Main file download logic."""
        logger.info(f"Attempting to download file: {file_hash[:10]}...")
        
        if not self.tracker_sock:
            logger.warning("No tracker connection. Attempting to reconnect...")
            self.start_tracker_connection()
            if not self.tracker_sock:
                logger.error("Reconnect failed. Cannot query tracker.")
                return

        try:
            response = network_utils.query_tracker_for_file(self.tracker_sock, file_hash)
        except Exception as e:
            logger.error(f"Error querying tracker: {e}. Closing connection.")
            if self.tracker_sock:
                self.tracker_sock.close()
            self.tracker_sock = None
            return 
            
        if response.get('status') != 'success':
            logger.error(f"Tracker query failed: {response.get('message')}")
            return
            
        file_meta = {
            "name": response.get('file_name'),
            "size": response.get('file_size'),
            "hash": file_hash,
            "chunk_count": response.get('chunk_count'),
            "chunk_hashes": response.get('chunk_hashes', [])
        }
        
        if not file_meta["chunk_hashes"] or len(file_meta["chunk_hashes"]) != file_meta["chunk_count"]:
            logger.error("Tracker did not return valid chunk hash information. Aborting.")
            return

        self.storage.add_downloading_file(file_meta)
        
        peer_list = response.get('peers', [])
        if not peer_list:
            logger.warning("File found, but no peers are currently sharing it.")
            return

        peer_ids = [p['id'] for p in peer_list]
        sorted_peers_with_score = self.reputation.get_peers_sorted_by_reputation(peer_ids)
        
        peer_addr_map = {p['id']: (p['ip'], p['port']) for p in peer_list}
        
        sorted_peer_addrs = []
        for peer_id, score in sorted_peers_with_score:
            if peer_id in peer_addr_map:
                sorted_peer_addrs.append( (peer_id, peer_addr_map[peer_id]) )
        
        logger.info(f"Found {len(sorted_peer_addrs)} peers. Sorted by reputation:")
        for i, (peer_id, score) in enumerate(sorted_peers_with_score):
            logger.info(f"  {i+1}. {peer_id} (Score: {score:.2f}) at {peer_addr_map.get(peer_id)}")

        missing_chunks = list(self.storage.get_missing_chunks(file_hash))
        owned_chunks_count = file_meta['chunk_count'] - len(missing_chunks)
        logger.info(f"Need to download {len(missing_chunks)} chunks...")
        
        for chunk_index in tqdm(
            missing_chunks, 
            desc=f"Downloading {file_meta['name']}",
            unit="chunk",
            total=file_meta['chunk_count'],
            initial=owned_chunks_count
        ):
            chunk_data = None
            for peer_id_for_rep, peer_addr in sorted_peer_addrs:
                
                if peer_id_for_rep == self.peer_id:
                    continue

                try:
                    chunk_data = network_utils.request_chunk_from_peer(peer_addr, file_hash, chunk_index)
                    
                    if chunk_data:
                        expected_hash = file_meta['chunk_hashes'][chunk_index]
                        
                        if file_utils.verify_chunk_data(chunk_data, expected_hash):
                            logger.debug(f"Chunk {chunk_index} integrity VERIFIED.")
                            self.storage.store_chunk(file_hash, chunk_index, chunk_data)
                            
                            self.reputation.update_reputation(peer_id_for_rep, "SUCCESSFUL_DOWNLOAD")
                            self.reputation.update_reputation(peer_id_for_rep, "VERIFIED_INTEGRITY")
                            
                            break
                        else:
                            logger.warning(f"Chunk {chunk_index} from {peer_addr} is CORRUPT. Discarding.")
                            self.reputation.update_reputation(peer_id_for_rep, "CORRUPTED_DATA")
                            chunk_data = None 
                            continue 
                    else:
                        self.reputation.update_reputation(peer_id_for_rep, "REFUSED_UPLOAD")
                        logger.warning(f"Failed to get chunk {chunk_index} from {peer_addr}. Trying next peer.")
                
                except Exception as e:
                    self.reputation.update_reputation(peer_id_for_rep, "CONNECTION_TIMEOUT")
                    logger.warning(f"Error connecting to {peer_addr}: {e}. Trying next peer.")

            if not chunk_data:
                logger.error(f"Failed to download chunk {chunk_index} from any peer. Aborting download.")
                return 
        
        if self.storage.is_download_complete(file_hash):
            logger.info(f"Download complete for {file_hash[:10]}. Reassembling file...")
            
            output_path = os.path.join(self.storage.completed_dir, file_meta['name'])
            success = file_utils.reassemble_file(
                file_hash,
                file_meta['chunk_count'],
                self.storage.downloads_dir,
                output_path
            )
            
            if success:
                logger.info(f"File reassembled at {output_path}")
                if file_utils.verify_file_integrity(output_path, file_hash):
                    logger.info("File integrity VERIFIED. Download successful!")
                    self.register_with_tracker()
                else:
                    logger.error("File integrity check FAILED. Deleting corrupt file.")
                    os.remove(output_path)
            else:
                logger.error("Failed to reassemble file.")
        else:
            logger.error("Download finished, but some chunks are still missing.")

    def stop(self):
        """Stops the peer and closes connections."""
        self.is_running = False
        self.reputation.close()
        
        if self.tracker_sock:
            logger.info("Closing connection to tracker.")
            self.tracker_sock.close()
            
        if self.server_thread:
            self.server_thread.join()
        logger.info(f"Peer {self.peer_id} has shut down.")