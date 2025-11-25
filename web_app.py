import argparse
from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
from peer.peer import Peer
import threading
import time
import os
import logging
import subprocess
from werkzeug.utils import secure_filename

# --- Parse CLI Arguments ---
parser = argparse.ArgumentParser()
parser.add_argument('--port', type=int, default=5000, help='Web server port')
parser.add_argument('--name', type=str, default=None, help='Unique name for this peer instance')
parser.add_argument('--behavior', type=str, default='good', choices=['good', 'freeloader', 'malicious'], help='Peer behavior')
args = parser.parse_args()

# Initialize Flask
app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
# Reduce logging noise
logging.getLogger('werkzeug').setLevel(logging.ERROR)

socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Initialize Global Peer Instance with unique name/storage and behavior
peer = Peer(instance_name=args.name, behavior=args.behavior)

def open_folder_dialog():
    try:
        cmd = [
            "powershell.exe", 
            "-NoProfile", 
            "-Command", 
            "Add-Type -AssemblyName System.Windows.Forms; "
            "$f = New-Object System.Windows.Forms.FolderBrowserDialog; "
            "$f.ShowDialog() | Out-Null; "
            "$f.SelectedPath"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        path = result.stdout.strip()
        if path: return path
    except: pass
    return None

# --- Background Tasks ---

def start_peer_background():
    peer.start_server()
    peer.start_tracker_connection()
    while True:
        peer.register_with_tracker()
        time.sleep(30)

def push_updates():
    while True:
        socketio.emit('downloads_update', {
            'active': peer.active_downloads,
            'history': peer.download_history
        })
        socketio.emit('status_update', {
            'peer_id': peer.peer_id,
            'port': peer.server_port,
            'shared_files': len(peer.storage.get_shared_files_info())
        })
        socketio.emit('reputation_update', peer.reputation.get_all_reputations())
        socketio.sleep(1)

threading.Thread(target=start_peer_background, daemon=True).start()
socketio.start_background_task(target=push_updates)

# --- Routes ---

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/my_files')
def my_files():
    return jsonify(peer.storage.get_shared_files_info())

@app.route('/api/downloads', methods=['GET'])
def get_downloads():
    """API for automation scripts to check status."""
    return jsonify({
        'active': peer.active_downloads,
        'history': peer.download_history
    })

@app.route('/api/search')
def search():
    query = request.args.get('q', '')
    if not query: return jsonify([])
    results = peer.search_files(query)
    return jsonify(results)

@app.route('/api/share', methods=['POST'])
def share():
    # Case 1: Handle JSON Path (For Automation Scripts)
    if request.is_json:
        data = request.get_json()
        file_path = data.get('path')
        if file_path and os.path.exists(file_path):
            threading.Thread(target=peer.share_file, args=(file_path,), daemon=True).start()
            return jsonify({'status': 'success', 'message': f'Sharing started for: {file_path}'})
        return jsonify({'status': 'error', 'message': 'Invalid path provided in JSON'})

    # Case 2: Handle File Upload (For Web UI)
    if 'file' in request.files:
        file = request.files['file']
        if file.filename == '':
            return jsonify({'status': 'error', 'message': 'No selected file'})
        
        if file:
            filename = secure_filename(file.filename)
            upload_dir = os.path.join(peer.storage.base_dir, 'my_uploads')
            os.makedirs(upload_dir, exist_ok=True)
            save_path = os.path.join(upload_dir, filename)
            try:
                file.save(save_path)
                threading.Thread(target=peer.share_file, args=(save_path,), daemon=True).start()
                return jsonify({'status': 'success', 'message': f'File uploaded and sharing started: {filename}'})
            except Exception as e:
                return jsonify({'status': 'error', 'message': str(e)})

    return jsonify({'status': 'error', 'message': 'No file or path provided'})

@app.route('/api/browse_destination', methods=['GET'])
def browse_destination():
    path = open_folder_dialog()
    if path: return jsonify({'status': 'success', 'path': path})
    else: return jsonify({'status': 'canceled', 'path': ''})

@app.route('/api/download', methods=['POST'])
def download():
    data = request.json
    file_hash = data.get('hash')
    dest_path = data.get('destination_path', '').strip()
    if dest_path == '': dest_path = None
    
    peer.start_download_thread(file_hash, dest_path)
    return jsonify({'status': 'success', 'message': 'Download started'})

@app.route('/api/open_path', methods=['POST'])
def open_path():
    data = request.json
    path = data.get('path')
    if not path or not os.path.exists(path):
        return jsonify({'status': 'error', 'message': 'Path does not exist.'})
    try:
        if hasattr(os, 'startfile'): os.startfile(path)
        else: subprocess.Popen(['explorer.exe', path])
        return jsonify({'status': 'success', 'message': 'Opened successfully'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

if __name__ == '__main__':
    print(f"Starting Peer Web UI on http://127.0.0.1:{args.port} [Behavior: {args.behavior}]")
    socketio.run(app, debug=True, port=args.port, use_reloader=False)