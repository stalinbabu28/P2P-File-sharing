import socket
import threading
import uuid
import logging
import time
import os
import yaml
import queue  # <-- Required for parallel logic
from typing import Dict, Any, Optional, List, Tuple
from tqdm import tqdm

from peer.storage import StorageManager
from peer.reputation import ReputationManager
from peer import file_utils
from peer import network_utils

# --- Configuration ---
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
        try:
            with open(CONFIG_FILE, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {e}. Exiting.")
            exit(1)

    def _get_free_port(self) -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            return s.getsockname()[1]

    def start_server(self):
        self.server_thread = threading.Thread(target=self._run_server, daemon=True)
        self.server_thread.start()
        logger.info(f"Peer server starting on {self.server_host}:{self.server_port}...")

    def _run_server(self):
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
                    except Exception as e:
                        logger.error(f"Server error accepting connection: {e}")
                        
        except OSError as e:
            if self.is_running:
                logger.error(f"Peer server socket error: {e}")
        finally:
            logger.info("Peer server shutting down.")

    def start_tracker_connection(self):
        if self.tracker_sock: return
        logger.info("Connecting to tracker...")
        self.tracker_sock = network_utils.connect_to_tracker()
        if not self.tracker_sock:
            logger.error("Failed to connect to tracker.")

    def register_with_tracker(self):
        if not self.tracker_sock:
            self.start_tracker_connection()
            if not self.tracker_sock: return

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
            logger.error(f"Error communicating with tracker: {e}")
            if self.tracker_sock: self.tracker_sock.close()
            self.tracker_sock = None

    def share_file(self, file_path: str):
        logger.info(f"Sharing new file: {file_path}")
        chunk_size = self.config['peer']['chunk_size']
        file_meta = self.storage.add_file_to_share(file_path, chunk_size)
        
        if file_meta:
            logger.info(f"File '{file_meta['name']}' processed. Hash: {file_meta['hash']}")
            self.register_with_tracker()
        else:
            logger.error(f"Failed to process and share file: {file_path}")

    # --- PARALLEL DOWNLOAD LOGIC ---
    def _download_worker(self, 
                         worker_id: int,
                         chunk_queue: queue.Queue, 
                         file_hash: str, 
                         file_meta: Dict[str, Any], 
                         sorted_peers: List[Tuple[str, Tuple[str, int]]],
                         progress_bar: tqdm):
        """
        Worker thread function to process download chunks from the queue.
        """
        while True:
            try:
                # Get a chunk index from the queue. Block for 1s to check stopping conditions.
                chunk_index = chunk_queue.get(timeout=1)
            except queue.Empty:
                # If queue is empty, this worker is done
                return

            success = False
            
            # --- LOAD BALANCING FIX: Round-Robin ---
            # Instead of always starting with the first peer (highest reputation),
            # we rotate the list based on the chunk index. This distributes
            # requests across all available peers evenly.
            num_peers = len(sorted_peers)
            if num_peers > 0:
                # E.g., Chunk 0 starts at peer 0, Chunk 1 starts at peer 1
                start_idx = chunk_index % num_peers
                # Create a rotated view of the peers list for this chunk
                # Peers are still tried in reputation order relative to the start index
                peers_to_try = sorted_peers[start_idx:] + sorted_peers[:start_idx]
            else:
                peers_to_try = sorted_peers

            for peer_id, peer_addr in peers_to_try:
                if peer_id == self.peer_id: continue

                try:
                    chunk_data = network_utils.request_chunk_from_peer(peer_addr, file_hash, chunk_index)
                    
                    if chunk_data:
                        expected_hash = file_meta['chunk_hashes'][chunk_index]
                        if file_utils.verify_chunk_data(chunk_data, expected_hash):
                            # Success!
                            logger.debug(f"[Worker-{worker_id}] Chunk {chunk_index} verified.")
                            self.storage.store_chunk(file_hash, chunk_index, chunk_data)
                            
                            self.reputation.update_reputation(peer_id, "SUCCESSFUL_DOWNLOAD")
                            self.reputation.update_reputation(peer_id, "VERIFIED_INTEGRITY")
                            
                            success = True
                            break # Stop trying peers for this chunk
                        else:
                            logger.warning(f"[Worker-{worker_id}] Chunk {chunk_index} from {peer_addr} CORRUPT.")
                            self.reputation.update_reputation(peer_id, "CORRUPTED_DATA")
                    else:
                        # Network failure or refusal
                        pass 
                
                except Exception as e:
                    logger.warning(f"[Worker-{worker_id}] Error downloading chunk {chunk_index}: {e}")

            if success:
                # Update the progress bar safely
                progress_bar.update(1)
                chunk_queue.task_done()
            else:
                logger.error(f"[Worker-{worker_id}] Failed to download chunk {chunk_index} from any peer.")
                chunk_queue.task_done()

    def download_file(self, file_hash: str):
        """
        Main download entry point. Sets up the queue and workers.
        """
        logger.info(f"Attempting to download file: {file_hash[:10]}...")
        
        # 1. Query Tracker
        if not self.tracker_sock:
            self.start_tracker_connection()
            if not self.tracker_sock: return

        try:
            response = network_utils.query_tracker_for_file(self.tracker_sock, file_hash)
        except Exception as e:
            logger.error(f"Error querying tracker: {e}")
            return
            
        if response.get('status') != 'success':
            logger.error(f"Tracker query failed: {response.get('message')}")
            return
            
        # 2. Prepare Metadata
        file_meta = {
            "name": response.get('file_name'),
            "size": response.get('file_size'),
            "hash": file_hash,
            "chunk_count": response.get('chunk_count'),
            "chunk_hashes": response.get('chunk_hashes', [])
        }
        self.storage.add_downloading_file(file_meta)
        
        # 3. Get Peers
        peer_list = response.get('peers', [])
        if not peer_list:
            logger.warning("No peers found for file.")
            return

        # Sort peers
        peer_ids = [p['id'] for p in peer_list]
        sorted_peers_with_score = self.reputation.get_peers_sorted_by_reputation(peer_ids)
        peer_addr_map = {p['id']: (p['ip'], p['port']) for p in peer_list}
        
        # List of (peer_id, addr) tuples
        sorted_peer_addrs = []
        for peer_id, score in sorted_peers_with_score:
            if peer_id in peer_addr_map:
                sorted_peer_addrs.append((peer_id, peer_addr_map[peer_id]))
        
        logger.info(f"Found {len(sorted_peer_addrs)} peers.")

        # 4. Queue Setup
        missing_chunks = list(self.storage.get_missing_chunks(file_hash))
        if not missing_chunks:
            logger.info("File already downloaded!")
            return

        chunk_queue = queue.Queue()
        for chunk_idx in missing_chunks:
            chunk_queue.put(chunk_idx)

        # 5. Start Workers
        NUM_WORKERS = 4
        threads = []
        
        # Create shared progress bar
        with tqdm(total=len(missing_chunks), desc=f"Downloading {file_meta['name']}", unit="chunk") as pbar:
            
            for i in range(NUM_WORKERS):
                t = threading.Thread(
                    target=self._download_worker,
                    args=(i, chunk_queue, file_hash, file_meta, sorted_peer_addrs, pbar),
                    daemon=True
                )
                t.start()
                threads.append(t)
            
            # Wait for queue to be empty
            chunk_queue.join()
            
            # Wait for threads to finish (they exit when queue is empty)
            for t in threads:
                t.join()

        # 6. Reassembly
        if self.storage.is_download_complete(file_hash):
            logger.info("Download complete. Reassembling...")
            output_path = os.path.join(self.storage.completed_dir, file_meta['name'])
            if file_utils.reassemble_file(file_hash, file_meta['chunk_count'], self.storage.downloads_dir, output_path):
                if file_utils.verify_file_integrity(output_path, file_hash):
                    logger.info("File verified successfully!")
                    self.register_with_tracker()
                else:
                    logger.error("File integrity failed after reassembly.")
        else:
            logger.error("Download finished but file is incomplete.")

    def stop(self):
        self.is_running = False
        self.reputation.close()
        if self.tracker_sock: self.tracker_sock.close()
        if self.server_thread: self.server_thread.join()
        logger.info(f"Peer {self.peer_id} has shut down.")