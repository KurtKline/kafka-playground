import json
import uuid
from datetime import datetime
from time import sleep
from typing import List

from pykafka import KafkaClient


with open("data/bus1.json", "r") as bus_data:
    data = json.load(bus_data)
    coordinates = data["features"][0]["geometry"]["coordinates"]


def generate_uuid():
    return uuid.uuid4()


client = KafkaClient(hosts="localhost:9092")
topic = client.topics["testBusData"]
producer = topic.get_sync_producer()

data = {}
data["busline"] = "00001"


def generate_checkpoint(coordinates: List[List[str]]):

    i = 0
    while i < len(coordinates):
        data["key"] = data["busline"] + "_" + str(generate_uuid())
        data["timestamp"] = str(datetime.utcnow())
        data["latitude"] = coordinates[i][1]
        data["longitude"] = coordinates[i][0]
        message = json.dumps(data)
        print(message)
        producer.produce(message.encode("ascii"))

        if i == len(coordinates) - 1:
            i = 0
        else:
            i += 1


generate_checkpoint(coordinates)
