import json
import sys

import kafka
import yaml


def send(msg):
    key = "ABCD"
    producer.send("transform-definitions", key, json.dumps(msg))

if len(sys.argv) != 4:
    print("usage: definition.py <config> add|delete <id>")
    sys.exit(1)

kafka_url = "192.168.10.4:9092"

client = kafka.client.KafkaClient(kafka_url)
producer = kafka.producer.KeyedProducer(
    client,
    async=False,
    req_acks=kafka.producer.KeyedProducer.ACK_AFTER_LOCAL_WRITE,
    ack_timeout=2000)


conf = yaml.load(open(sys.argv[1], 'r'))

if sys.argv[2] == "add":
    msg = {}
    msg['transform_id'] = sys.argv[3]
    msg['transform_definition'] = conf
    send(msg)

if sys.argv[2] == "delete":
    msg = {}
    msg['transform_id'] = sys.argv[3]
    msg['transform_definition'] = []
    send(msg)
