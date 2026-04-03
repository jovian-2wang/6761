#!/usr/bin/env python3
"""
Streams species occurrence records from the OBIS API. Every 5 seconds,
streams 300 JSON records separated by newlines to stdout. Stops streaming
after approximately one hour.
"""

import json
from time import sleep

import requests
import logging

last_id = ""
for page in range(720):
    try:
        response = requests.get(
            "https://api.obis.org/occurrence"
            "?size=300"
            f"&offset={page * 300}"
            "&shuffle=40"
            + (f"&after={last_id}" if last_id else "")
        )
        data = response.json())
        
        for record in data.get("results", []):
            print(json.dumps(record).lower(), flush=True)
            last_id = record.get("id")
                
    except TypeError as e:
        logging.error("Failed to parse JSON", exc_info=e)
        
    except requests.RequestException as e:
        logging.error(f"Request failed", exc_info=e)
    
    sleep(5)
