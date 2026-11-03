#!/usr/bin/env bash
# Downloads the Gowalla check-ins and friendship network from Stanford SNAP (about 100 MB).
# Source: https://snap.stanford.edu/data/loc-gowalla.html
set -euo pipefail
mkdir -p data/raw
cd data/raw
wget -c https://snap.stanford.edu/data/loc-gowalla_totalCheckins.txt.gz
wget -c https://snap.stanford.edu/data/loc-gowalla_edges.txt.gz
gunzip -f loc-gowalla_totalCheckins.txt.gz loc-gowalla_edges.txt.gz
ls -lh
echo "First lines of each file:"
head -3 loc-gowalla_totalCheckins.txt
head -3 loc-gowalla_edges.txt
