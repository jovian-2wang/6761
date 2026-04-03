#!/usr/bin/env python3
"""
Streams species occurrence records from the iDigBio API. Every 5 seconds,
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
            "https://search.idigbio.org/v2/search/records"
            "?limit=300"
            f"&offset={page * 300}"
            "&sort=uuid"
        )
        data = response.json()
        
        for record in data.get("items", []):
            raw_data = record.get("data")
            if raw_data:
                print(json.dumps(raw_data).lower(), flush=True)
                
    except TypeError as e:
        logging.error("Failed to parse JSON", exc_info=e)
        
    except requests.RequestException as e:
        logging.error(f"Request failed", exc_info=e)
    
    sleep(5)
