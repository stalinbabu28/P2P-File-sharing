# P2P File Sharing — Project README

Summary
This repository implements a tracker-assisted peer-to-peer file sharing system with a local web UI. It provides peer discovery via a tracker, direct peer-to-peer file transfer, a per-peer reputation system, and a Flask + Socket.IO interface for control and realtime status.

Key features
- Tracker-assisted peer discovery and file indexing.
- Peer server to serve file chunks and client logic to download chunks from other peers.
- Per-peer persistent reputation manager (SQLite) that updates on transfer events.
- REST API and WebSocket (Socket.IO) endpoints for search, share, download, and realtime updates.
- Basic experiment and metrics tooling (scripts and metrics/collector).

Architecture overview
- Tracker: central process that maintains a registry of peers and available files.
- Peers: each peer runs a P2P server to serve files and a client component to request chunks from other peers. Peers periodically register with the tracker.
- Frontend: a Flask web app (web_app.py) served per-peer that exposes REST endpoints and a Socket.IO channel for live updates.
- Storage & Utilities: modules for file handling, networking utilities, and reputation management live under peer/.

Main components and important files
- peer/
  - peer.py — Peer lifecycle, download and upload orchestration.
  - network_utils.py — Low-level socket helpers, tracker and peer message routines.
  - storage.py, file_utils.py — File storage and integrity helpers.
  - reputation.py — Persistent reputation store and update logic (SQLite).
- tracker/
  - tracker.py, tracker_db.py, tracker_utils.py — Tracker process and DB.
- web_app.py — Flask + Socket.IO web UI and HTTP API used to interact with the local Peer.
- templates/index.html — Frontend UI served to the browser.
- scripts/
  - run_experiment.py, spawn_peers.sh — Helpers to run multi-peer experiments.
- metrics/
  - collector.py, visualizer_stub.py — Basic metrics collection and plotting helpers.
- tests/
  - test_basic_transfer.py, test_basic_utils.py, test_reputation_updates.py — Unit/integration tests.

How to run (basic)
1. Configure:
   - Edit config.yaml to set tracker host/port and other parameters.
2. Start the tracker (if running centralized tracker):
   - python tracker/tracker.py
3. Start a peer (starts P2P server and web UI):
   - python peer/cli.py --name=<peer_name>   (or run peer.peer directly)
   - By default web UI listens on port 5000; override in web_app CLI args.
4. Open the web UI:
   - Point a browser to http://localhost:5000 to search, share, and download files.
5. Use the UI or REST endpoints to upload/share files, search for file hashes, and start downloads.
6. Run tests:
   - pytest tests/

Reputation model (brief)
- Implemented in peer/reputation.py using an SQLite DB per peer.
- Event-based score updates using fixed deltas and exponential smoothing:
  - Events such as SUCCESSFUL_UPLOAD, SUCCESSFUL_DOWNLOAD add positive score.
  - Negative events (REFUSED_UPLOAD, CORRUPTED_DATA) subtract score.
- Reputation data is exposed to the UI via Socket.IO.

Networking and protocols
- REST (Flask) for user-initiated actions (search, share, download).
- WebSocket (Socket.IO) for realtime updates (download progress, reputation).
- Custom TCP-based messages for tracker and peer-to-peer communication (JSON over TCP sockets).
- Files identified by content hash; downloads operate at chunk granularity.

