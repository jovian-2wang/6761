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

last_id = ""

for page in range(720):
    try:
        response = requests.get(
            "https://api.obis.org/occurrence"
            "?size=300"
            f"&offset={page * 300}"
            "&shuffle=40"
            + (f"&after={last_id}" if last_id else ""),
            timeout=30
        )
        response.raise_for_status()
        data = response.json()

        for record in data.get("results", []):
            producer.send("obis", record)
            last_id = record.get("id", last_id)

        producer.flush()

    except (TypeError, ValueError) as e:
        logging.error("Failed to parse JSON", exc_info=e)

    except requests.RequestException as e:
        logging.error("Request failed", exc_info=e)

    sleep(5)

producer.flush()
producer.close()
