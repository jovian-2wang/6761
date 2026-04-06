#!/usr/bin/env python3
import os
import json
import logging
from time import sleep

import requests
from kafka import KafkaProducer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

for page in range(720):
    try:
        response = requests.get(
            "https://search.idigbio.org/v2/search/records"
            "?limit=300"
            f"&offset={page * 300}"
            "&sort=uuid",
            timeout=30
        )
        response.raise_for_status()
        data = response.json()

        for record in data.get("items", []):
            raw_data = record.get("data")
            if raw_data:
                producer.send("idigbio", raw_data)

        producer.flush()

    except (TypeError, ValueError) as e:
        logging.error("Failed to parse JSON", exc_info=e)

    except requests.RequestException as e:
        logging.error("Request failed", exc_info=e)

    sleep(5)

producer.flush()
producer.close()
