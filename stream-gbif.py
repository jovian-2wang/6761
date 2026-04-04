#!/usr/bin/env python3

import json
import logging
from time import sleep

import requests
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

for page in range(720):
    try:
        response = requests.get(
            "https://api.gbif.org/v1/occurrence/search"
            "?limit=300"
            f"&offset={page * 300}"
            "&shuffle=40",
            timeout=30
        )
        print("status:", response.status_code, "page:", page, flush=True)
        response.raise_for_status()
        data = response.json()

        count = 0
        for record in data.get("results", []):
            producer.send("gbif", record)
            count += 1

        producer.flush()
        print(f"sent {count} gbif records from page {page}", flush=True)

    except (TypeError, ValueError) as e:
        logging.error("Failed to parse JSON", exc_info=e)

    except requests.RequestException as e:
        logging.error("Request failed", exc_info=e)

    sleep(5)

producer.flush()
producer.close()
