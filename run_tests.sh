#!/bin/bash
echo "Running Peer Utility Tests..."

mkdir -p tests

python3 -m unittest -v tests/test_basic_utils.py