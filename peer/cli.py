"""Minimal CLI to run a peer."""
import argparse
from .peer import Peer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', default='peer1')
    parser.add_argument('--dir', default='./data')
    args = parser.parse_args()

    p = Peer(args.id, storage_dir=args.dir)
    print('Files:', p.list_files())

if __name__ == '__main__':
    main()
 
