import json
import threading
import time

import kafka

from stackdistiller import condenser
from stackdistiller import distiller


kafka_url = "192.168.10.4:9092"


class Transform(object):
    def __init__(self):
        self._kafka = kafka.client.KafkaClient(kafka_url)

        self._event_consumer = kafka.consumer.SimpleConsumer(
            self._kafka,
            str(time.time()),
            "raw-events",
            auto_commit=True,
            max_buffer_size=None)

        self._definition_consumer = kafka.consumer.SimpleConsumer(
            self._kafka,
            str(time.time()),
            "transform-definitions",
            auto_commit=True,
            max_buffer_size=None)

        self._producer = kafka.producer.KeyedProducer(self._kafka)

        self._event_consumer.seek(0, 0)
        self._definition_consumer.seek(0, 0)

        self._distiller = distiller.Distiller([])
        self._condenser = condenser.DictionaryCondenser()

        self._lock = threading.Lock()

        self._transform_def_thread = threading.Thread(
            name='transform_defs',
            target=self._transform_definitions)

        self._transform_def_thread.daemon = True

    def run(self):
        def date_handler(obj):
            return obj.isoformat() if hasattr(obj, 'isoformat') else obj

        self._transform_def_thread.start()

        for event in self._event_consumer:
            event_payload = json.loads(event[1].value)

            self._lock.acquire()
            result = self._distiller.to_event(event_payload[0], self._condenser)
            self._lock.release()

            if result:
                distilled_event = self._condenser.get_event()
                key = time.time() * 1000
                message = json.dumps(distilled_event, default=date_handler)
                self._producer.send("transformed-events", key, message)

    def _transform_definitions(self):
        for definition in self._definition_consumer:
            definition_payload = json.loads(definition[1].value)

            self._lock.acquire()
            self._distiller = distiller.Distiller(definition_payload)
            self._lock.release()
