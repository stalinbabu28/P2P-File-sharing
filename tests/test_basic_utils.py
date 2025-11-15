import unittest
import os
import shutil
import sys
import sqlite3

# --- Add project root to Python path ---
# This allows us to import from the 'peer' directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from peer import file_utils
from peer.storage import StorageManager
from peer.reputation import ReputationManager

class TestPeerUtils(unittest.TestCase):

    # --- Test Setup & Teardown ---
    
    def setUp(self):
        """Set up a clean environment for each test."""
        self.test_dir = "test_temp"
        self.peer_id = "test_peer_123"
        self.storage_dir = f"peer_storage_{self.peer_id}"
        self.dummy_file_name = "dummy_test_file.txt"
        self.dummy_file_path = os.path.join(self.test_dir, self.dummy_file_name)
        self.chunk_size = 1024 # Small chunk size for testing

        # Create test directory
        os.makedirs(self.test_dir, exist_ok=True)
        
        # Create a dummy file
        with open(self.dummy_file_path, "wb") as f:
            f.write(b"A" * self.chunk_size)  # First chunk
            f.write(b"B" * self.chunk_size)  # Second chunk
            f.write(b"C" * 512)             # Third chunk (partial)
        
        self.dummy_file_size = os.path.getsize(self.dummy_file_path)
        self.dummy_file_hash = file_utils.get_file_hash(self.dummy_file_path)

    def tearDown(self):
        """Clean up all created files and directories after each test."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        if os.path.exists(self.storage_dir):
            shutil.rmtree(self.storage_dir)

    # --- file_utils.py Tests ---

    def test_01_file_hashing(self):
        print("\nTesting file_utils: Hashing...")
        self.assertIsNotNone(self.dummy_file_hash)
        self.assertEqual(len(self.dummy_file_hash), 64) # SHA-256 length

    def test_02_file_splitting_and_reassembly(self):
        print("\nTesting file_utils: Splitting and Reassembly...")
        # 1. Split the file
        output_chunk_dir = os.path.join(self.test_dir, "split_chunks")
        meta = file_utils.split_file(self.dummy_file_path, self.chunk_size, output_chunk_dir)
        
        self.assertIsNotNone(meta)
        self.assertEqual(meta['name'], self.dummy_file_name)
        self.assertEqual(meta['size'], self.dummy_file_size)
        self.assertEqual(meta['hash'], self.dummy_file_hash)
        self.assertEqual(meta['chunk_count'], 3)
        self.assertEqual(len(meta['chunk_hashes']), 3)

        # 2. Reassemble the file
        reassembled_path = os.path.join(self.test_dir, "reassembled_file.txt")
        success = file_utils.reassemble_file(meta['hash'], meta['chunk_count'], output_chunk_dir, reassembled_path)
        self.assertTrue(success)

        # 3. Verify reassembled file
        reassembled_hash = file_utils.get_file_hash(reassembled_path)
        self.assertEqual(self.dummy_file_hash, reassembled_hash)
        self.assertTrue(file_utils.verify_file_integrity(reassembled_path, self.dummy_file_hash))

    # --- storage.py Tests ---
    
    def test_03_storage_manager_init(self):
        print("\nTesting storage: Initialization...")
        storage = StorageManager(self.peer_id)
        self.assertTrue(os.path.exists(storage.base_dir))
        self.assertTrue(os.path.exists(storage.shared_dir))
        self.assertTrue(os.path.exists(storage.downloads_dir))
        self.assertTrue(os.path.exists(storage.completed_dir))
        self.assertTrue(os.path.exists(storage.metadata_file))

    def test_04_storage_add_file_to_share(self):
        print("\nTesting storage: Adding shared file...")
        storage = StorageManager(self.peer_id)
        meta = storage.add_file_to_share(self.dummy_file_path, self.chunk_size)
        
        self.assertIsNotNone(meta)
        self.assertEqual(meta['hash'], self.dummy_file_hash)
        
        # Check metadata
        self.assertIn(self.dummy_file_hash, storage.file_metadata)
        self.assertIn(self.dummy_file_hash, storage.chunk_tracker)
        self.assertEqual(storage.chunk_tracker[self.dummy_file_hash], {0, 1, 2})
        self.assertTrue(storage.is_download_complete(self.dummy_file_hash))
        
        # Check if chunks were created
        self.assertTrue(os.path.exists(storage.get_chunk_path(self.dummy_file_hash, 0)))
        self.assertTrue(os.path.exists(storage.get_chunk_path(self.dummy_file_hash, 1)))
        self.assertTrue(os.path.exists(storage.get_chunk_path(self.dummy_file_hash, 2)))
        self.assertIsNone(storage.get_chunk_path(self.dummy_file_hash, 3)) # Out of bounds
        
        # Check shared file info
        shared_files = storage.get_shared_files_info()
        self.assertEqual(len(shared_files), 1)
        self.assertEqual(shared_files[0]['hash'], self.dummy_file_hash)

    def test_05_storage_download_tracking(self):
        print("\nTesting storage: Download tracking...")
        storage = StorageManager(self.peer_id)
        file_meta = {
            "name": "download_test.dat",
            "size": 12345,
            "hash": "filehash_to_download_123",
            "chunk_count": 5
        }
        
        storage.add_downloading_file(file_meta)
        
        self.assertIn(file_meta['hash'], storage.file_metadata)
        self.assertEqual(storage.chunk_tracker[file_meta['hash']], set())
        self.assertFalse(storage.is_download_complete(file_meta['hash']))
        
        # Simulate storing a chunk
        storage.store_chunk(file_meta['hash'], 2, b"chunk_data_goes_here")
        self.assertEqual(storage.chunk_tracker[file_meta['hash']], {2})
        self.assertTrue(storage.has_chunk(file_meta['hash'], 2))
        self.assertFalse(storage.has_chunk(file_meta['hash'], 1))
        
        # Check missing chunks
        missing = storage.get_missing_chunks(file_meta['hash'])
        self.assertEqual(missing, {0, 1, 3, 4})

    # --- reputation.py Tests ---

    def test_06_reputation_manager_init(self):
        print("\nTesting reputation: Initialization...")
        rep_manager = ReputationManager(self.peer_id)
        self.assertTrue(os.path.exists(rep_manager.db_path))
        
        # Check if table exists
        conn = sqlite3.connect(rep_manager.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='reputation'")
        self.assertIsNotNone(cursor.fetchone())
        conn.close()
        rep_manager.close()

    def test_07_reputation_scoring(self):
        print("\nTesting reputation: Scoring logic...")
        rep_manager = ReputationManager(self.peer_id)
        other_peer_id = "peer_abc"
        
        # 1. Check default reputation
        # From reputation.py: DEFAULT_REPUTATION = 10
        self.assertEqual(rep_manager.get_reputation(other_peer_id), 10)
        
        # 2. Test a good event
        # R_new = (0.8 * 10) + (0.2 * 3) = 8 + 0.6 = 8.6
        rep_manager.update_reputation(other_peer_id, "SUCCESSFUL_UPLOAD")
        self.assertAlmostEqual(rep_manager.get_reputation(other_peer_id), 8.6)
        
        # 3. Test another good event
        # R_new = (0.8 * 8.6) + (0.2 * 2) = 6.88 + 0.4 = 7.28
        rep_manager.update_reputation(other_peer_id, "VERIFIED_INTEGRITY")
        self.assertAlmostEqual(rep_manager.get_reputation(other_peer_id), 7.28)
        
        # 4. Test a bad event
        # R_new = (0.8 * 7.28) + (0.2 * -5) = 5.824 - 1.0 = 4.824
        rep_manager.update_reputation(other_peer_id, "CORRUPTED_DATA")
        self.assertAlmostEqual(rep_manager.get_reputation(other_peer_id), 4.824)
        
        rep_manager.close()

    def test_08_reputation_sorting(self):
        print("\nTesting reputation: Peer sorting...")
        rep_manager = ReputationManager(self.peer_id)
        
        peer_good = "peer_good"
        peer_bad = "peer_bad"
        peer_new = "peer_new"
        
        rep_manager.update_reputation(peer_good, "SUCCESSFUL_DOWNLOAD") # 8.6
        rep_manager.update_reputation(peer_bad, "CORRUPTED_DATA")     # 4.0 (0.8*10 + 0.2*-5)
        
        peer_list = [peer_bad, peer_new, peer_good]
        sorted_list = rep_manager.get_peers_sorted_by_reputation(peer_list)
        
        # Expected order: peer_new (10), peer_good (8.6), peer_bad (4.0)
        self.assertEqual(sorted_list[0][0], peer_new)
        self.assertEqual(sorted_list[1][0], peer_good)
        self.assertEqual(sorted_list[2][0], peer_bad)
        
        self.assertAlmostEqual(sorted_list[0][1], 10.0)
        self.assertAlmostEqual(sorted_list[1][1], 8.6)
        
        # R_new = (0.8 * 10) + (0.2 * -5) = 8.0 - 1.0 = 7.0
        self.assertAlmostEqual(sorted_list[2][1], 7.0) # <-- Was 4.0
        
        rep_manager.close()

if __name__ == '__main__':
    unittest.main()