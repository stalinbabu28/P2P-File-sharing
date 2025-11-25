import json
import os
import glob
import statistics

LOGS_DIR = "metrics_logs"

def load_logs():
    data = []
    files = glob.glob(os.path.join(LOGS_DIR, "*.jsonl"))
    print(f"📂 Found {len(files)} log files.")
    
    for fpath in files:
        with open(fpath, 'r') as f:
            for line in f:
                try:
                    data.append(json.loads(line))
                except: pass
    return data

def analyze_throughput(data):
    print("\n📊 NETWORK THROUGHPUT")
    downloads = [d for d in data if d['event'] == 'download_chunk' and d['status'] == 'success']
    
    if not downloads:
        print("   No successful downloads recorded.")
        return

    speeds = [d['speed_mbps'] for d in downloads]
    avg_speed = statistics.mean(speeds)
    max_speed = max(speeds)
    total_bytes = sum(d['size_bytes'] for d in downloads)
    
    print(f"   Total Data Transferred: {total_bytes / (1024*1024):.2f} MB")
    print(f"   Average Chunk Speed:    {avg_speed:.2f} Mbps")
    print(f"   Peak Chunk Speed:       {max_speed:.2f} Mbps")

def analyze_reputation(data):
    print("\n🤝 REPUTATION DYNAMICS")
    rep_updates = [d for d in data if d['event'] == 'reputation_update']
    
    # Group by target peer
    peer_scores = {}
    for r in rep_updates:
        target = r['target_peer']
        if target not in peer_scores: peer_scores[target] = []
        peer_scores[target].append(r['new_score'])
    
    print(f"   Recorded updates for {len(peer_scores)} distinct peers.")
    
    for peer, scores in peer_scores.items():
        start = scores[0]
        end = scores[-1]
        trend = "↗️" if end > start else "↘️" if end < start else "➡️"
        print(f"   Peer {peer[:8]}... : {start:.1f} -> {end:.1f} {trend} (Final: {end})")

def analyze_security(data):
    print("\n🛡️ SECURITY & FAILURES")
    corrupt = [d for d in data if d['event'] == 'download_chunk' and d['status'] == 'corrupt']
    failed = [d for d in data if d['event'] == 'download_chunk' and d['status'] == 'failed']
    
    print(f"   Corrupted Chunks Detected: {len(corrupt)}")
    print(f"   Failed/Refused Chunks:     {len(failed)}")
    
    if corrupt:
        # Find who sent the most trash
        bad_actors = {}
        for c in corrupt:
            p = c['source_peer']
            bad_actors[p] = bad_actors.get(p, 0) + 1
        print(f"   Detected Malicious Actors: {bad_actors}")

if __name__ == "__main__":
    if not os.path.exists(LOGS_DIR):
        print("❌ No logs found. Run 'scripts/automated_test.py' first.")
    else:
        logs = load_logs()
        analyze_throughput(logs)
        analyze_reputation(logs)
        analyze_security(logs)