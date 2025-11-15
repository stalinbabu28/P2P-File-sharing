#!/bin/bash

# Create the peer directory if it doesn't exist
mkdir -p peer

# Run the peer CLI, passing all script arguments ($@) to the python script
# Example: ./run_peer.sh share my_file.txt
# This will execute: python3 peer/cli.py share my_file.txt
python3 peer/cli.py "$@"