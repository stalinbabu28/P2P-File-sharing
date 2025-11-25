import subprocess
import time
import os
import signal
import sys

# Configuration
PEER_SCRIPT = "web_app.py" # or peer/cli.py if you prefer headless
PYTHON_CMD = "python3"

def spawn_peer(name, port, behavior='good'):
    print(f"Starting {name} on port {port} [Behavior: {behavior}]...")
    # We need to modify web_app.py slightly to accept --behavior arg, 
    # OR we can just use the peer/cli.py for testing non-UI peers.
    # Let's assume we use web_app.py for now but we need to pass behavior.
    # Since web_app.py initializes Peer(), we need to update web_app.py to read --behavior arg.
    
    cmd = [
        PYTHON_CMD, "web_app.py",
        "--name", name,
        "--port", str(port),
        "--behavior", behavior
    ]
    
    # Log output to files so we don't clutter terminal
    with open(f"{name}.log", "w") as f:
        return subprocess.Popen(cmd, stdout=f, stderr=f)

def main():
    procs = []
    try:
        # Scenario: 1 Good Seeder, 1 Malicious, 1 Freeloader, 1 Victim (Good Downloader)
        
        # Start Tracker
        print("Starting Tracker...")
        tracker = subprocess.Popen([PYTHON_CMD, "tracker/tracker.py"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        procs.append(tracker)
        time.sleep(2)

        # Start Peers
        # You need to manually update web_app.py to accept --behavior first (see below)
        p1 = spawn_peer("seeder_good", 5001, "good")
        p2 = spawn_peer("seeder_malicious", 5002, "malicious")
        p3 = spawn_peer("downloader_free", 5003, "freeloader")
        p4 = spawn_peer("downloader_victim", 5004, "good")
        
        procs.extend([p1, p2, p3, p4])
        
        print("\nAll peers started!")
        print("Open http://127.0.0.1:5004 to control the 'Victim' peer.")
        print("1. Share a file from http://127.0.0.1:5001 (Good Seeder)")
        print("2. Share the SAME file from http://127.0.0.1:5002 (Malicious Seeder) - make sure it has same name/content to hash same? No, malicious needs to spoof hash or share same file but send garbage.")
        print("   * To test security: Malicious peer must register the SAME file hash but send bad data.")
        print("   * Since we calculate hash from file content, Malicious peer needs a valid file to register, but the code sends garbage.")
        
        print("\nPress Ctrl+C to stop all.")
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping all processes...")
        for p in procs:
            p.kill()

if __name__ == "__main__":
    main()