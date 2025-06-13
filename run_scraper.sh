#!/bin/bash

# Change to the script directory
cd "$(dirname "$0")"

# Activate virtual environment if it exists (uncomment if you use one)
# source venv/bin/activate

# Install/update requirements
pip install -r requirements.txt

# Run the scraper
python scraper.py

# Log the completion
echo "Scraper completed at $(date)" >> scraper.log 