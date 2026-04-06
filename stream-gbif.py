import json
import logging
from time import sleep
import os
import requests
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

page = 0

while True:
    try:
        response = requests.get(
            "https://api.gbif.org/v1/occurrence/search"
            "?limit=300"
            f"&offset={page * 300}"
            "&shuffle=40",
            timeout=30
        )
        response.raise_for_status()
        data = response.json()

        results = data.get("results", [])
        if not results:
            logging.info("No more GBIF results for this page, reset to page 0")
            page = 0
            sleep(5)
            continue

        for record in results:
            producer.send("gbif", record)

        producer.flush()
        logging.info(f"Sent {len(results)} GBIF records to topic gbif (page={page})")
        page += 1

    except TypeError as e:
        logging.error("Failed to parse JSON", exc_info=e)

    except requests.RequestException as e:
        logging.error("Request failed", exc_info=e)

    except Exception as e:
        logging.error("Unexpected error", exc_info=e)

    sleep(5)
