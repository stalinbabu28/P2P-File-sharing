#!/bin/bash
echo "Starting Tracker Server..."

# Create the tracker directory if it doesn't exist
# (although you'll likely create it to save the file)
mkdir -p tracker

# Run the tracker python script
python3 tracker/tracker.py