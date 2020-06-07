import json
from time import sleep
from datetime import datetime
import uuid
from pykafka import KafkaClient


with open('data/bus1.json', 'r') as bus_data:
    data = json.load(bus_data)
    coords = data['features'][0]['geometry']['coordinates']


client = KafkaClient(hosts="localhost:9092")
topic = client.topics['testBusData']

producer = topic.get_sync_producer()

for x, y in coords: 
    producer.produce(f'X: {x}, Y: {y}'.encode('ascii'))
    sleep(1)