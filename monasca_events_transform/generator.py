import datetime
import json
import time

import kafka
import notigen


kafka_url = "192.168.10.4:9092"

client = kafka.client.KafkaClient(kafka_url)
producer = kafka.producer.KeyedProducer(
    client,
    async=False,
    req_acks=kafka.producer.KeyedProducer.ACK_AFTER_LOCAL_WRITE,
    ack_timeout=2000)

g = notigen.EventGenerator("/vagrant_home/projects/stacktach-notigen/templates",
                           operations_per_hour=100000)
now = datetime.datetime.utcnow()
start = now
nevents = 0

length = 0

while nevents < 1000000:
    e = g.generate(now)
    if e:
        nevents += len(e)
        key = time.time() * 1000

        msg = json.dumps(e)

        if len(msg) > length:
            length = len(msg)
            print("Max notification size: {}".format(length))

        producer.send("raw-events", key, msg)

    now = datetime.datetime.utcnow()
    time.sleep(0.01)
