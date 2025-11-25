import socket
import threading
import uuid
import logging
import time
import os
import yaml
import queue
import json
import shutil
from typing import Dict, Any, Optional, List, Tuple

from peer.storage import StorageManager
from peer.reputation import ReputationManager
from peer.metrics import MetricsLogger
from peer import file_utils
from peer import network_utils

logger = logging.getLogger(__name__)
CONFIG_FILE = 'config.yaml'

class Peer:
    def __init__(self, instance_name: str = None, behavior: str = 'good'):
        self.behavior = behavior  # 'good', 'freeloader', 'malicious'
        
        self.instance_dir = f"peer_data_{instance_name}" if instance_name else None
        if self.instance_dir:
            os.makedirs(self.instance_dir, exist_ok=True)
            self.identity_file = os.path.join(self.instance_dir, "identity.json")
        else:
            self.identity_file = "identity.json"

        self.peer_id = self._load_or_create_identity()
        self.metrics = MetricsLogger(self.peer_id)
        self.metrics.log("startup", {"behavior": self.behavior, "instance": instance_name})

        self.config = self._load_config()
        self.storage = StorageManager(self.peer_id, instance_dir=self.instance_dir)
        self.reputation = ReputationManager(self.peer_id)
        
        self.server_port = self._get_free_port()
        self.server_host = "127.0.0.1"
        self.server_thread = None
        self.is_running = True
        self.tracker_sock = None
        
        self.active_downloads: Dict[str, Dict[str, Any]] = {} 
        self.download_history: List[Dict[str, Any]] = []

    def _load_or_create_identity(self) -> str:
        if os.path.exists(self.identity_file):
            try:
                with open(self.identity_file, 'r') as f:
                    data = json.load(f)
                    if 'peer_id' in data: return data['peer_id']
            except: pass
        new_id = f"peer_{uuid.uuid4().hex[:8]}"
        with open(self.identity_file, 'w') as f: json.dump({'peer_id': new_id}, f)
        return new_id

    def _load_config(self) -> Dict[str, Any]:
        try:
            with open(CONFIG_FILE, 'r') as f: return yaml.safe_load(f)
        except:
            return {'peer': {'chunk_size': 1048576}, 'tracker': {'host': '127.0.0.1', 'port': 9090, 'buffer_size': 4096}}

    def _get_free_port(self) -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            return s.getsockname()[1]

    def start_server(self):
        self.server_thread = threading.Thread(target=self._run_server, daemon=True)
        self.server_thread.start()
        logger.info(f"Peer server on port {self.server_port} [{self.behavior}]")

    def _run_server(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind((self.server_host, self.server_port))
                s.listen(5)
                while self.is_running:
                    try:
                        s.settimeout(1.0)
                        conn, addr = s.accept()
                        # Pass behavior to the handler
                        threading.Thread(target=network_utils.handle_peer_request, 
                                       args=(conn, addr, self.storage, self.behavior), daemon=True).start()
                    except socket.timeout: continue
        except Exception as e: logger.error(f"Server error: {e}")

    def start_tracker_connection(self):
        if self.tracker_sock: return
        self.tracker_sock = network_utils.connect_to_tracker()

    def register_with_tracker(self):
        if not self.tracker_sock: self.start_tracker_connection()
        if self.tracker_sock:
            try:
                files = self.storage.get_shared_files_info()
                network_utils.register_with_tracker(self.tracker_sock, self.peer_id, self.server_port, files)
            except:
                if self.tracker_sock: self.tracker_sock.close()
                self.tracker_sock = None

    def share_file(self, file_path: str):
        meta = self.storage.add_file_to_share(file_path, self.config['peer']['chunk_size'])
        if meta: self.register_with_tracker()
        return meta

    def search_files(self, query: str) -> List[Dict[str, Any]]:
        if not self.tracker_sock: self.start_tracker_connection()
        if not self.tracker_sock: return []
        try:
            resp = network_utils.search_tracker(self.tracker_sock, query)
            return resp.get('results', [])
        except: return []

    # --- DOWNLOAD LOGIC ---

    def start_download_thread(self, file_hash: str, destination_path: str = None):
        if file_hash in self.active_downloads and self.active_downloads[file_hash]['status'] == 'Downloading': return
        t = threading.Thread(target=self.download_file, args=(file_hash, destination_path), daemon=True)
        t.start()

    def _update_rep_and_log(self, peer_id, event):
        old, new = self.reputation.update_reputation(peer_id, event)
        self.metrics.log_reputation(peer_id, event, old, new)

    def _download_worker(self, q: queue.Queue, f_hash: str, f_meta: dict, peers: list):
        while True:
            try: idx = q.get(timeout=1)
            except queue.Empty: return

            success = False
            if not peers: 
                q.task_done()
                continue
            
            start = idx % len(peers)
            rotated_peers = peers[start:] + peers[:start]

            for pid, addr in rotated_peers:
                if pid == self.peer_id: continue
                
                start_time = time.time()
                try:
                    data = network_utils.request_chunk_from_peer(addr, f_hash, idx)
                    duration = time.time() - start_time
                    
                    if data:
                        size = len(data)
                        speed = (size * 8) / (duration * 1000000) if duration > 0 else 0 # Mbps
                        
                        if file_utils.verify_chunk_data(data, f_meta['chunk_hashes'][idx]):
                            self.storage.store_chunk(f_hash, idx, data)
                            
                            self._update_rep_and_log(pid, "SUCCESSFUL_DOWNLOAD")
                            self._update_rep_and_log(pid, "VERIFIED_INTEGRITY")
                            self.metrics.log_download(f_hash, idx, duration, size, pid, "success", speed)
                            success = True
                            break
                        else:
                            self._update_rep_and_log(pid, "CORRUPTED_DATA")
                            self.metrics.log_download(f_hash, idx, duration, size, pid, "corrupt", speed)
                    else:
                        # Failed/Refused
                        self._update_rep_and_log(pid, "REFUSED_UPLOAD")
                        self.metrics.log_download(f_hash, idx, duration, 0, pid, "failed", 0)
                except Exception: 
                    pass
            
            if success:
                if f_hash in self.active_downloads:
                    self.active_downloads[f_hash]['completed_chunks'] += 1
                    total = self.active_downloads[f_hash]['total_chunks']
                    if total > 0:
                        self.active_downloads[f_hash]['progress'] = (self.active_downloads[f_hash]['completed_chunks'] / total) * 100
                q.task_done()
            else:
                q.task_done()

    def download_file(self, file_hash: str, destination_path: str = None):
        if not self.tracker_sock: self.start_tracker_connection()
        try:
            resp = network_utils.query_tracker_for_file(self.tracker_sock, file_hash)
            if resp.get('status') != 'success': return

            f_meta = {
                'name': resp['file_name'], 'size': resp['file_size'],
                'chunk_hashes': resp['chunk_hashes'], 'chunk_count': resp['chunk_count']
            }
            self.storage.add_downloading_file({'hash': file_hash, **f_meta})
            self.metrics.log("download_start", {"file": f_meta['name'], "size": f_meta['size']})
            
            missing = list(self.storage.get_missing_chunks(file_hash))
            
            self.active_downloads[file_hash] = {
                'hash': file_hash, 'name': f_meta['name'], 'size': f_meta['size'],
                'total_chunks': f_meta['chunk_count'],
                'completed_chunks': f_meta['chunk_count'] - len(missing),
                'progress': 0, 'status': 'Downloading', 'final_path': ''
            }

            if not missing:
                original_path = self.storage.get_original_file_path(file_hash)
                if original_path and os.path.exists(original_path):
                    self._finalize_download(file_hash, original_path, destination_path, move=False)
                    return
                out = os.path.join(self.storage.completed_dir, f_meta['name'])
                if os.path.exists(out):
                    self._finalize_download(file_hash, out, destination_path)
                    return
                if self.storage.has_physical_chunks(file_hash, f_meta['chunk_count']):
                    if file_utils.reassemble_file(file_hash, f_meta['chunk_count'], self.storage.downloads_dir, out):
                        self._finalize_download(file_hash, out, destination_path)
                        return
                missing = list(range(f_meta['chunk_count']))

            peer_list = resp.get('peers', [])
            peer_ids = [p['id'] for p in peer_list]
            sorted_ids = self.reputation.get_peers_sorted_by_reputation(peer_ids)
            addr_map = {p['id']: (p['ip'], p['port']) for p in peer_list}
            sorted_peers = [(pid, addr_map[pid]) for pid, _ in sorted_ids if pid in addr_map]

            q = queue.Queue()
            for i in missing: q.put(i)

            threads = []
            for _ in range(4):
                t = threading.Thread(target=self._download_worker, args=(q, file_hash, f_meta, sorted_peers))
                t.start()
                threads.append(t)
            
            q.join()
            for t in threads: t.join()

            if self.storage.is_download_complete(file_hash):
                default_out = os.path.join(self.storage.completed_dir, f_meta['name'])
                if self.storage.has_physical_chunks(file_hash, f_meta['chunk_count']):
                    success = file_utils.reassemble_file(file_hash, f_meta['chunk_count'], self.storage.downloads_dir, default_out)
                    if success:
                        self._finalize_download(file_hash, default_out, destination_path)
                        self.metrics.log("download_complete", {"file": f_meta['name'], "status": "success"})
                    else:
                        self.active_downloads[file_hash]['status'] = 'Reassembly Failed'
                        self.metrics.log("download_complete", {"file": f_meta['name'], "status": "reassembly_failed"})
                else:
                     self.active_downloads[file_hash]['status'] = 'Missing Chunks'
            else:
                self.active_downloads[file_hash]['status'] = 'Stalled'
                self.metrics.log("download_complete", {"file": f_meta['name'], "status": "stalled"})

        except Exception as e:
            logger.error(f"Download failed: {e}")
            if file_hash in self.active_downloads:
                self.active_downloads[file_hash]['status'] = 'Error'

    def _finalize_download(self, file_hash: str, current_path: str, destination_path: str = None, move: bool = True):
        final_path = os.path.abspath(current_path)
        if not os.path.exists(current_path): return

        if destination_path:
            if not os.path.exists(destination_path):
                try: os.makedirs(destination_path, exist_ok=True)
                except: destination_path = None 
            if destination_path:
                try:
                    fname = os.path.basename(current_path)
                    custom_out = os.path.join(destination_path, fname)
                    if os.path.exists(custom_out): os.remove(custom_out)
                    if move: shutil.move(current_path, custom_out)
                    else: shutil.copy2(current_path, custom_out)
                    final_path = os.path.abspath(custom_out)
                except Exception: pass

        self.register_with_tracker()
        if file_hash in self.active_downloads:
            completed_info = self.active_downloads.pop(file_hash)
            completed_info['status'] = 'Complete'
            completed_info['progress'] = 100
            completed_info['final_path'] = final_path
            completed_info['timestamp'] = time.time()
            self.download_history.insert(0, completed_info)

    def stop(self):
        self.is_running = False
        if self.tracker_sock: self.tracker_sock.close()