import json
import logging
import logging.config
import os
import threading
import time

import kafka

from oslo_config import cfg

from stackdistiller import condenser
from stackdistiller import distiller

from database import retrieve_transforms

log = logging.getLogger(__name__)


class Transform(object):
    def __init__(self):
        self._kafka = kafka.client.KafkaClient(cfg.CONF.kafka.url)

        self._event_consumer = kafka.consumer.SimpleConsumer(
            self._kafka,
            cfg.CONF.kafka.transform_group,
            cfg.CONF.kafka.events_topic,
            auto_commit=True,
            buffer_size=250000,
            max_buffer_size=None)

        self._event_consumer.seek(0, 2)
        self._event_consumer.provide_partition_info()
        self._event_consumer.fetch_last_known_offsets()

        self._definition_consumer = kafka.consumer.SimpleConsumer(
            self._kafka,
            str(time.time() * 1000) + str(os.getpid()),
            cfg.CONF.kafka.transform_def_topic,
            auto_commit=True,
            buffer_size=250000,
            max_buffer_size=None)

        self._definition_consumer.seek(0, 2)
        self._definition_consumer.provide_partition_info()
        self._definition_consumer.fetch_last_known_offsets()

        self._producer = kafka.producer.SimpleProducer(self._kafka)

        self._distiller_table = {}

        for row in retrieve_transforms():
            transform_id = row[0]
            transform_def = row[1]
            self._distiller_table[transform_id] = (
                distiller.Distiller(transform_def))

        self._condenser = condenser.DictionaryCondenser()

        self._lock = threading.Lock()

        self._transform_def_thread = threading.Thread(
            name='transform_defs',
            target=self._transform_definitions)

        self._transform_def_thread.daemon = True

    def run(self):
        self._transform_def_thread.start()

        def date_handler(obj):
            return obj.isoformat() if hasattr(obj, 'isoformat') else obj

        for event in self._event_consumer:
            partition = event[0]
            event_payload = json.loads(event[1].message.value)
            event_data = event_payload['event']

            result = []

            self._lock.acquire()
            for v in self._distiller_table.values():
                e = v.to_event(event_data, self._condenser)
                if e:
                    result.append(json.dumps(self._condenser.get_event(),
                                             default=date_handler))

            self._lock.release()

            if result:
                # key = time.time() * 1000
                self._producer.send_messages(
                    cfg.CONF.kafka.transformed_events_topic, *result)

            self._event_consumer.commit([partition])

    def _transform_definitions(self):
        for definition in self._definition_consumer:
            partition = definition[0]
            definition_payload = json.loads(definition[1].message.value)

            self._lock.acquire()

            transform_id = definition_payload['transform_id']
            transform_def = definition_payload['transform_definition']

            if transform_def == []:
                log.info("Delete definition {}".format(transform_id))
                if transform_id in self._distiller_table:
                    del self._distiller_table[transform_id]
            else:
                log.info("Add definition {}".format(transform_id))
                self._distiller_table[transform_id] = (
                    distiller.Distiller(transform_def))

            self._lock.release()

            self._definition_consumer.commit([partition])
