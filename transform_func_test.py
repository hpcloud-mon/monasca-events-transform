import json
import time

import kafka

import logging

logging.basicConfig()

kafka_url = "192.168.10.4:9092"

client = kafka.client.KafkaClient(kafka_url)
producer = kafka.producer.KeyedProducer(
    client,
    async=False,
    req_acks=kafka.producer.KeyedProducer.ACK_AFTER_LOCAL_WRITE,
    ack_timeout=2000)

consumer = kafka.consumer.SimpleConsumer(
    client,
    str(time.time()),
    "transformed-events",
    auto_commit=True,
    max_buffer_size=None)

consumer.seek(0, 2)
consumer.provide_partition_info()
consumer.fetch_last_known_offsets()

event = [{u'_context_request_id': u'req-dc4371e3-5fb6-4ae5-a2de-865703f85b68',
          u'_context_quota_class': None,
          u'event_type': u'compute.instance.update',
          u'_context_auth_token': u'8cc8387654b7466db0966e20eae16a88',
          u'_context_user_id': u'765849',
          u'payload': {u'state_description': u'rebuilding',
                       u'availability_zone': None,
                       u'terminated_at': u'',
                       u'ephemeral_gb': 0,
                       u'instance_type_id': 3,
                       u'bandwidth': {},
                       u'deleted_at': u'',
                       u'reservation_id': u'res-7038',
                       u'memory_mb': 1024,
                       u'display_name': u'Instance_451481',
                       u'hostname': u'server-59821',
                       u'state': u'active',
                       u'old_state': u'active',
                       u'launched_at': u'2015-05-20 20:33:33.114066',
                       u'metadata': {},
                       u'node': u'node-805970',
                       u'ramdisk_id': u'',
                       u'access_ip_v6': u'a109:ba71:28ec:d607:1571:68fe:a34a:ce32',
                       u'disk_gb': 40,
                       u'access_ip_v4': u'221.175.133.163',
                       u'kernel_id': u'',
                       u'host': u'host-140813',
                       u'user_id': u'765849',
                       u'image_ref_url': u'http://167.176.152.228:9292/images/a18b6ce1-0ce1-4a00-9edf-0997e2737f54',
                       u'audit_period_beginning': u'2015-05-20 19:19:37.114066',
                       u'root_gb': 40,
                       u'tenant_id': u'transform_func_test',
                       u'created_at': u'2015-05-20 20:32:25.114066',
                       u'old_task_state': None,
                       u'instance_id': u'024f380d-2b92-402f-9873-8634cd3417e9',
                       u'instance_type': u'1GB Standard Instance',
                       u'vcpus': 1,
                       u'image_meta': {u'vm_mode': u'hvm',
                                       u'org.openstack__1__os_version': u'7',
                                       u'org.openstack__1__os_distro': u'org.debian',
                                       u'os_distro': u'debian',
                                       u'image_type': u'base',
                                       u'container_format': u'ovf',
                                       u'min_ram': u'512',
                                       u'cache_in_nova': u'True',
                                       u'min_disk': u'40',
                                       u'disk_format': u'vhd',
                                       u'auto_disk_config': u'disabled',
                                       u'os_type': u'linux',
                                       u'base_image_ref': u'53d50763-942b-43b5-89a8-77d09b7cd3a6',
                                       u'org.openstack__1__architecture': u'x64'},
                       u'architecture': u'x64',
                       u'new_task_state': u'rebuilding',
                       u'audit_period_ending': u'2015-05-20 20:40:27.114066',
                       u'os_type': u'linux',
                       u'instance_flavor_id': u'3'},
          u'priority': u'INFO',
          u'_context_is_admin': False,
          u'_context_user': u'765849',
          u'publisher_id': u'publisher-896649',
          u'message_id': u'8c2d94ac-fba9-4ce5-b952-28b5de209024',
          u'_context_remote_address': u'198.150.217.192',
          u'_context_roles': [u'checkmate',
                              u'object-store:default',
                              u'compute:default',
                              u'identity:user-admin'],
          u'timestamp': u'2015-05-20 20:40:20.963066',
          u'_context_timestamp': u'2015-05-20 20:41:37.165066',
          u'_unique_id': u'121e45f8029c4826a3218bbcf930cfb3',
          u'_context_glance_api_servers': None,
          u'_context_project_name': u'329996',
          u'_context_read_deleted': u'no',
          u'_context_tenant': u'329996',
          u'_context_instance_lock_checked': False,
          u'_context_project_id': u'329996',
          u'_context_user_name': u'765849'}]

conf = [{'event_type': 'compute.instance.*',
         'traits': {'tenant_id': {'fields': 'payload.tenant_id'},
                    'service': {'fields': 'publisher_id',
                                'plugin': 'split'}}}]


def send(topic, msg):
    key = "ABCD"
    producer.send(topic, key, json.dumps(msg))


def add(_id):
    print("Add transform definition {}".format(_id))
    msg = {}
    msg['transform_id'] = _id
    msg['transform_definition'] = conf
    send("transform-definitions", msg)


def delete(_id):
    print("Delete transform definition {}".format(_id))
    msg = {}
    msg['transform_id'] = _id
    msg['transform_definition'] = []
    send("transform-definitions", msg)


def wait_for_events(num):
    rx_events = 0
    expected_key_set = [u'event_type', u'service', u'tenant_id', u'when', u'request_id', u'message_id']
    data = consumer.get_messages(count=1000, timeout=5)
    for e in data:
        partition = e[0]
        consumer.commit([partition])
        event_payload = json.loads(e[1].message.value)
        if event_payload['tenant_id'] == "transform_func_test":
            if len(event_payload.keys()) == 6:
                rx_events += 1
            else:
                print("Event has invalid key set")
                print("Expected: {}".format(expected_key_set))
                print("Recieved: {}".format(event_payload.keys()))

    return rx_events


def test_transform():
    expected_events = 3

    add('A')
    add('B')

    time.sleep(2)
    send("raw-events", event)

    rx_events = 0
    rx_events += wait_for_events(2)

    delete('A')
    time.sleep(2)
    send("raw-events", event)

    rx_events += wait_for_events(1)

    delete('B')

    if rx_events == expected_events:
        print("Transform success.  Expected {} found {}".
              format(expected_events, rx_events))
    else:
        print("Transform failure.  Expected {} found {}".
              format(expected_events, rx_events))

test_transform()
