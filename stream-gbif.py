#!/usr/bin/env python3
"""
Streams species occurrence records from the GBIF API. Every 5 seconds,
streams 300 JSON records separated by newlines to stdout. Stops streaming
after approximately one hour.
"""

import json
from time import sleep

import requests
import logging

for page in range(720):
    try:
        response = requests.get(
            "https://api.gbif.org/v1/occurrence/search"
            "?limit=300"
            f"&offset={page * 300}"
            "&shuffle=40"
        )
        data = response.json()
        
        for record in data.get("results", []):
            print(json.dumps(record).lower(), flush=True)
                
    except TypeError as e:
        logging.error("Failed to parse JSON", exc_info=e)
        
    except requests.RequestException as e:
        logging.error(f"Request failed", exc_info=e)
    
    sleep(5)
