#!/bin/bash
echo "Running Peer Utility Tests..."

# Create the tests directory if it doesn't exist
mkdir -p tests

# Run the test script using Python's unittest module
# The -v flag adds verbosity, so we can see which tests are running
python3 -m unittest -v tests/test_basic_utils.py