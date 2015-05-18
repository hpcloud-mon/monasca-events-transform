import json
import sys

import kafka
import yaml


kafka_url = "192.168.10.4:9092"

client = kafka.client.KafkaClient(kafka_url)
producer = kafka.producer.KeyedProducer(
    client,
    async=False,
    req_acks=kafka.producer.KeyedProducer.ACK_AFTER_LOCAL_WRITE,
    ack_timeout=2000)

conf = yaml.load(open(sys.argv[1], 'r'))

key = "ABCD"
producer.send("transform-definitions", key, json.dumps(conf))
