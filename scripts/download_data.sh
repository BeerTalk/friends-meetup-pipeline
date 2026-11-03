#!/usr/bin/env bash
# Downloads the LBSN2Vec++ Foursquare dataset (about 2.5 GB) into data/raw.
# If the link fails, open https://doi.org/10.4121/15112308, right-click "Download all",
# copy the link address and paste it in place of the URL below.
set -euo pipefail
mkdir -p data/raw
cd data/raw
wget -O lbsn.zip "https://figshare.com/ndownloader/articles/15112308/versions/1"
unzip -o lbsn.zip
find . -name "*.zip" ! -name "lbsn.zip" -exec unzip -o {} \;
echo "Files now in data/raw:"
find . -type f -name "*.txt" -exec ls -lh {} \;
