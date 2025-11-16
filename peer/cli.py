import sys
import argparse
import time
import logging
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from peer.peer import Peer

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - [%(name)s] - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="P2P File Sharing Peer")
    
    subparsers = parser.add_subparsers(dest='command', required=True)
    
    share_parser = subparsers.add_parser('share', help="Share a file with the network")
    share_parser.add_argument('file_path', type=str, help="The path to the file you want to share")
    
    download_parser = subparsers.add_parser('download', help="Download a file from the network")
    download_parser.add_argument('file_hash', type=str, help="The SHA-256 hash of the file to download")
    
    daemon_parser = subparsers.add_parser('daemon', help="Run as a daemon to seed files")

    args = parser.parse_args()
    
    try:
        peer = Peer()
        peer.start_server()
        
        peer.start_tracker_connection()
        
        peer.register_with_tracker()

        if args.command == 'share':
            logger.info(f"CLI: Executing 'share' command for {args.file_path}")
            peer.share_file(args.file_path)
            logger.info("Share command complete. Peer will continue running as a daemon.")
            
        elif args.command == 'download':
            logger.info(f"CLI: Executing 'download' command for {args.file_hash}")
            peer.download_file(args.file_hash)
            logger.info("Download command complete. Peer will continue running as a daemon.")
            
        elif args.command == 'daemon':
            logger.info("CLI: Running in daemon mode. Seeding files...")

        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("CLI: Shutdown signal received.")
    finally:
        if 'peer' in locals():
            peer.stop()
        logger.info("CLI: Exiting.")

if __name__ == "__main__":
    main()