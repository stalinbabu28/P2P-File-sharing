# Project Feature TODO List

Here is a breakdown of the new features requested and the technical steps required to implement them.

---

## 1. 🚀 Parallel Downloads (Utilize Full Bandwidth)

### **Goal:**
Download different chunks of the same file from multiple peers simultaneously, similar to BitTorrent.

### **Current State:**
The system is sequential. It downloads one chunk at a time, trying all peers for that single chunk before moving to the next.

### **Required Changes:**

* **Refactor `peer.py` -> `download_file`:**
    * This function needs to be rewritten to manage a pool of workers.
    * Create a thread-safe `Queue` or set of `missing_chunks`.
    * Create a thread-safe set of `chunks_in_progress` to prevent two workers from downloading the same chunk.
* **Create a `DownloadWorker` Thread:**
    * Create a pool of worker threads (e.g., 4 workers).
    * Each worker will:
        * Pop a `chunk_index` from the `missing_chunks` queue (and add it to `chunks_in_progress`).
        * Iterate through the `sorted_peer_addrs` list.
        * Attempt to download that specific chunk from the best available peer.
        * If successful, it will verify, store, and update reputation.
        * If it fails, it will penalize the peer and put the `chunk_index` back into the `missing_chunks` queue for another worker to try.
* **Peer Selection Logic:**
    * This is the new challenge: how to assign peers to chunks.
    * **Simple Method:** All workers share the same sorted peer list.
    * **Advanced Method (Rarest-First):** This would require asking all peers which chunks they have first, which adds a lot of complexity. (Recommend starting with the simple method).

---

## 2. 🖥️ Web Front-End & User Accounts

### **Goal:**
Create a user-friendly web interface for login, search, sharing, and downloading, with visible reputation scores.

### **Current State:**
The system is headless (CLI-only) and anonymous (peers are identified by UUIDs).

### **Required Changes:**
This is a major undertaking that splits the project into a new backend and frontend.

* **Backend API (New Component):**
    * Use a web framework like **Flask** or **FastAPI** to wrap the `Peer` class.
    * **Authentication:**
        * Add user login/signup (e.g., storing user passwords in the tracker's database).
        * Peers will authenticate with the backend, which will then tell the `Peer` object to connect to the tracker.
    * **New API Endpoints:**
        * `POST /login`: Authenticates a user.
        * `GET /files/search?q=...`: A new search endpoint. This will require modifying the Tracker to support searching by filename or (if we link users to peers) by username.
        * `POST /share`: An endpoint that tells the user's `Peer` instance to share a file.
        * `POST /download`: An endpoint that starts a download.
    * **Real-time Updates:**
        * Implement **WebSockets** to push download progress, new files, and reputation changes to the front-end without refreshing.
* **Frontend UI (New Component):**
    * Build a Single Page Application (SPA) using HTML, CSS, and JavaScript (or a framework like **React** or **Angular**).
    * **Pages:**
        * **Login Page:** For user authentication.
        * **Dashboard Page:** Shows "My Shared Files," "My Downloads" (with progress bars), and this peer's reputation score as seen by others (this would require a new API).
        * **Search Page:** A search bar that hits the `/files/search` endpoint and displays results.
        * **User Profile Page:** Shows a user's own reputation database (their scores for other peers).

---

## 3. 🧪 Network-Wide Robustness Testing

### **Goal:**
Test the system's fairness and security at scale, as described in your PDF (Scenarios 2, 3, 4).

### **Current State:**
We have a working "happy path" with two peers. We need to simulate the "unhappy path."

### **Required Changes:**

* **Create Malicious Peer Types:**
    * `freeloader_peer.py`:
        * A copy of `peer.py` that can download normally.
        * Its `handle_peer_request` function in `network_utils.py` will be modified to always return an error or simply `pass` (timeout), simulating a **"freeloader"** (Scenario 2).
    * `malicious_peer.py`:
        * A copy of `peer.py` that can download.
        * Its `handle_peer_request` will be modified to send `sendall(os.urandom(1024*1024))`—intentionally sending corrupt data (Scenario 4).
* **Create Automation Scripts** (as in your file structure):
    * `scripts/spawn_peers.sh`:
        * A bash script to launch multiple peers at once.
        * Example:
            ```bash
            ./spawn_peers.sh --good 5 --bad 2 --free 3
            ```
        * This script will run the `cli.py` (or malicious/freeloader scripts) in the background (`&`) and manage their process IDs (PIDs).
        * This will allow you to simulate a 10-peer network and test **"peer churn"** (Scenario 3) by killing some of the processes.
* **Create Metrics & Visualization** (as in your file structure):
    * `metrics/collector.py`:
        * A new Python script that will:
        * Find all `peer_storage_*/reputation.db` files.
        * Connect to each SQLite DB.
        * Run a SQL query to extract the final scores:
            ```sql
            SELECT peer_id, score FROM reputation
            ```
        * Aggregate all this data into a single JSON or CSV file.
    * `metrics/visualizer_stub.py`:
        * A script that reads the aggregated data from the collector.
        * Use **matplotlib** or **seaborn** (new requirements) to plot the final reputation scores.
        * This will visually prove your hypothesis: the plot should show all "good" peers with high scores and all "bad"/"freeloader" peers with very low scores. This directly addresses **"Reputation Convergence"** and **"Fairness Index"** from your PDF.
