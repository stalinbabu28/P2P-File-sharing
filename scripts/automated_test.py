import subprocess
import time
import os
import sys
import requests
import shutil
import json
import random
import signal

# --- Configuration ---
PYTHON_CMD = sys.executable
TRACKER_PORT = 9090
BASE_PEER_PORT = 6000
API_URL_TEMPLATE = "http://127.0.0.1:{port}/api"

# Paths
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT_DIR, "data")
LOGS_DIR = os.path.join(ROOT_DIR, "metrics_logs")
STORAGE_PREFIX = "peer_data_"

# --- Helper Functions ---

def setup_environment():
    """Cleans up old storage/logs and creates dummy data."""
    print("🧹 Cleaning up environment...")
    
    if os.path.exists(LOGS_DIR):
        shutil.rmtree(LOGS_DIR)
    os.makedirs(LOGS_DIR, exist_ok=True)

    for item in os.listdir(ROOT_DIR):
        if item.startswith(STORAGE_PREFIX):
            path = os.path.join(ROOT_DIR, item)
            if os.path.isdir(path):
                try: shutil.rmtree(path)
                except: pass 
    
    os.makedirs(DATA_DIR, exist_ok=True)
    
    f1 = os.path.join(DATA_DIR, "test_20mb.dat")
    if not os.path.exists(f1):
        print(f"📄 Generating 20MB test file: {f1}")
        with open(f1, 'wb') as f: f.write(os.urandom(20 * 1024 * 1024))
    
    return f1

def start_process(cmd, name):
    """Starts a background process and logs output."""
    log_path = os.path.join("logs", f"{name}.log")
    os.makedirs("logs", exist_ok=True)
    log_file = open(log_path, "w")
    return subprocess.Popen(cmd, stdout=log_file, stderr=log_file, cwd=ROOT_DIR)

def api_call(port, endpoint, data=None):
    """Helper to call peer APIs."""
    url = f"{API_URL_TEMPLATE.format(port=port)}/{endpoint}"
    try:
        if data:
            r = requests.post(url, json=data, timeout=5)
        else:
            r = requests.get(url, timeout=5)
        return r.json()
    except:
        return {}

def wait_for_file_hash(port):
    """Polls a seeder until they have finished hashing/sharing a file."""
    print(f"   ⏳ Waiting for Peer :{port} to finish hashing...")
    for _ in range(30):
        res = api_call(port, "my_files")
        if isinstance(res, list) and len(res) > 0:
            return res[0]['hash']
        time.sleep(1)
    return None

def wait_for_download(port, file_hash, timeout=120):
    """Polls a downloader until completion or error."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        res = api_call(port, "downloads")
        
        # 1. Check Completed History (The correct place for finished files)
        history = res.get('history', [])
        for item in history:
            if item.get('hash') == file_hash:
                return "Complete"

        # 2. Check Active Downloads
        active = res.get('active', {})
        if file_hash in active:
            status = active[file_hash].get('status', 'Unknown')
            if status == "Complete": return "Complete"
            if "Error" in status or "Failed" in status: return status
        
        time.sleep(1)
    return "Timeout"

def kill_processes(procs):
    print("💀 Killing processes...")
    for p in procs:
        try:
            if sys.platform == 'win32':
                subprocess.call(['taskkill', '/F', '/T', '/PID', str(p.pid)])
            else:
                os.killpg(os.getpgid(p.pid), signal.SIGTERM)
        except:
            try: p.terminate()
            except: pass
    time.sleep(2)

# --- SCENARIOS ---

def run_scenario_1_baseline(test_file):
    print("\n🧪 SCENARIO 1: BASELINE (10 Cooperative Peers)")
    print("   Goal: Measure max throughput and ideal reputation convergence.")
    
    procs = []
    try:
        procs.append(start_process([PYTHON_CMD, "tracker/tracker.py"], "tracker"))
        time.sleep(2)

        peers = []
        for i in range(10):
            port = BASE_PEER_PORT + i
            name = f"Peer_{i}_Good"
            p = start_process([PYTHON_CMD, "web_app.py", "--port", str(port), "--name", name, "--behavior", "good"], name)
            procs.append(p)
            peers.append(port)
        
        print("   🚀 10 Peers started. Waiting for initialization...")
        time.sleep(5)

        print(f"   📤 Peer 0 sharing file...")
        api_call(peers[0], "share", {"path": test_file})
        f_hash = wait_for_file_hash(peers[0])
        if not f_hash: raise Exception("Seeding failed")

        print(f"   ⬇️  Peers 1-9 starting download...")
        for port in peers[1:]:
            api_call(port, "download", {"hash": f_hash})
            time.sleep(0.5)

        print("   👀 Monitoring downloads...")
        pending = peers[1:]
        start_wait = time.time()
        while pending and (time.time() - start_wait < 300): # 5 min max total
            for p in pending[:]:
                status = wait_for_download(p, f_hash, timeout=1) 
                if status == "Complete":
                    print(f"      ✅ Peer :{p} Finished!")
                    pending.remove(p)
                elif status != "Timeout" and status != None:
                    print(f"      ❌ Peer :{p} Failed: {status}")
                    pending.remove(p)
            if len(pending) == 0: break
            time.sleep(2)
            
    finally:
        kill_processes(procs)

def run_scenario_2_fairness(test_file):
    print("\n🧪 SCENARIO 2: FAIRNESS (Freeloaders)")
    print("   Goal: 3 Freeloaders vs 7 Good Peers. Observe reputation penalties.")
    
    procs = []
    try:
        procs.append(start_process([PYTHON_CMD, "tracker/tracker.py"], "tracker"))
        time.sleep(2)

        peers = []
        for i in range(7):
            port = BASE_PEER_PORT + i
            name = f"Peer_{i}_Good"
            procs.append(start_process([PYTHON_CMD, "web_app.py", "--port", str(port), "--name", name, "--behavior", "good"], name))
            peers.append(port)
        
        for i in range(7, 10):
            port = BASE_PEER_PORT + i
            name = f"Peer_{i}_Freeloader"
            procs.append(start_process([PYTHON_CMD, "web_app.py", "--port", str(port), "--name", name, "--behavior", "freeloader"], name))
            peers.append(port)

        time.sleep(5)

        api_call(peers[0], "share", {"path": test_file})
        f_hash = wait_for_file_hash(peers[0])

        print("   ⬇️  Phase 1: Everyone gets file from Peer 0...")
        for port in peers[1:]:
            api_call(port, "download", {"hash": f_hash})
        
        time.sleep(15) 

        print("   🆕 Phase 2: New Peer joins and downloads from swarm...")
        new_port = BASE_PEER_PORT + 99
        new_name = "Peer_New_Victim"
        p_new = start_process([PYTHON_CMD, "web_app.py", "--port", str(new_port), "--name", new_name, "--behavior", "good"], new_name)
        procs.append(p_new)
        time.sleep(3)
        
        api_call(new_port, "download", {"hash": f_hash})
        print("   👀 Watching interactions (Freeloaders should reject requests)...")
        wait_for_download(new_port, f_hash)

    finally:
        kill_processes(procs)

def run_scenario_4_security(test_file):
    print("\n🧪 SCENARIO 4: SECURITY (Integrity Attack)")
    print("   Goal: 1 Malicious Seeder vs 1 Good Seeder vs 5 Victims.")
    
    procs = []
    try:
        procs.append(start_process([PYTHON_CMD, "tracker/tracker.py"], "tracker"))
        time.sleep(2)

        p_mal = start_process([PYTHON_CMD, "web_app.py", "--port", "6000", "--name", "Seeder_Evil", "--behavior", "malicious"], "p_mal")
        p_good = start_process([PYTHON_CMD, "web_app.py", "--port", "6001", "--name", "Seeder_Good", "--behavior", "good"], "p_good")
        procs.extend([p_mal, p_good])
        
        victims = []
        for i in range(5):
            port = 6002 + i
            name = f"Victim_{i}"
            procs.append(start_process([PYTHON_CMD, "web_app.py", "--port", str(port), "--name", name, "--behavior", "good"], name))
            victims.append(port)
            
        time.sleep(5)

        print("   😈 Malicious Seeder registers file...")
        api_call(6000, "share", {"path": test_file})
        f_hash = wait_for_file_hash(6000)

        print("   😇 Good Seeder registers same file...")
        api_call(6001, "share", {"path": test_file})

        print("   🛡️ Victims starting download (Should detect corruption and switch)...")
        for v in victims:
            api_call(v, "download", {"hash": f_hash})
        
        time.sleep(20)

    finally:
        kill_processes(procs)

if __name__ == "__main__":
    test_file = setup_environment()
    
    run_scenario_1_baseline(test_file)
    time.sleep(5)
    run_scenario_2_fairness(test_file)
    time.sleep(5)
    run_scenario_4_security(test_file)
    
    print("\n✅ ALL SCENARIOS COMPLETE.")
    print(f"📂 Data generated in: {LOGS_DIR}")
    print("📊 Run 'python metrics/analyze_data.py' now!")