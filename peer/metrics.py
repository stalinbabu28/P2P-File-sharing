import json
import time
import os
import threading

class MetricsLogger:
    def __init__(self, peer_id):
        self.peer_id = peer_id
        self.log_dir = "metrics_logs"
        os.makedirs(self.log_dir, exist_ok=True)
        self.log_file = os.path.join(self.log_dir, f"{peer_id}.jsonl")
        self.lock = threading.Lock()

    def log(self, event_type, data):
        """
        Logs an event to the JSONL file.
        event_type: str (e.g., 'download', 'upload', 'reputation')
        data: dict (key-value pairs of metrics)
        """
        entry = {
            "timestamp": time.time(),
            "peer_id": self.peer_id,
            "event": event_type,
            **data
        }
        
        with self.lock:
            with open(self.log_file, "a") as f:
                f.write(json.dumps(entry) + "\n")

    def log_download(self, file_hash, chunk_index, duration, size_bytes, source_peer, status, speed_mbps):
        self.log("download_chunk", {
            "file_hash": file_hash,
            "chunk_index": chunk_index,
            "duration_sec": round(duration, 4),
            "size_bytes": size_bytes,
            "source_peer": source_peer,
            "status": status,
            "speed_mbps": round(speed_mbps, 2)
        })

    def log_reputation(self, target_peer, change_reason, old_score, new_score):
        self.log("reputation_update", {
            "target_peer": target_peer,
            "reason": change_reason,
            "old_score": round(old_score, 2),
            "new_score": round(new_score, 2)
        })