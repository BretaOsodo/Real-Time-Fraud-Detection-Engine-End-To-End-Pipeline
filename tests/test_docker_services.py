"""
Intergration tests for docker-compose services.
Bring up all services and verifies each one is healhty and reachable

"""

import socket
import time 
import pytest
import requests 
from confluent_kafka.admin import AdminClient


#Helpers 
def wait_for_port(host: str,port: int, timeout:float=60.0) ->bool:
    """Poll until a TCP port is open or timeout expires."""
    deadline = time.time() + timeout

    while time.time() < deadline:
        try:
            with socket.create_connection((host,port), timeout =2):
                return True
        except OSError:
            time.sleep(2)
    return False 

def wait_for_http(url: str, timeout:float = 120.0, expected_status:int=200)->bool:
    """Poll an HTTP endpoint until it returns expected_status or timeout expires """

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r=requests.get(url,timeout=5)
            if r.status_code == expected_status:
                return True
        except requests.exceptions.RequestException:
            pass
        time.sleep(3)

    return False

#Kafka
class TestKafka:

    def test_kafka_port_is_open(self):
        """Kafka broker should be listening on port 9092"""
        assert wait_for_port("localhost",9092, timeout=60),\
        "kafka port 9092 is not open after 60s"

    def test_kafka_broker_is_functional(self):
        """Admin client should be able to list topics from the broker"""
        admin = AdminClient({
            "bootstrap.servers":"localhost:9092"
        })
        metadata=admin.list_topics(timeout=15)
        assert metadata is not None,"Kafka broker did not return metadata"

    def test_kafka_can_create_topic(self):
        """Broker should allow topic creation"""

        from confluent_kafka.admin import NewTopic
        admin = AdminClient({
            "bootstrap.servers":"localhost:9092"
        })
        topic_name ="test-health-check-topic"
        futures=admin.create_topics(
            [NewTopic(topic_name,num_partitions=1,replication_factor=1)]
        )

        for topic, future in futures.items():
            try:
                future.result()
            except Exception as e:
                #Topic may already exists: That's fine
                assert "already exists" in str(e).lower(),\
                f"Unexpected error creating topic:{e}"
        #confirm topic appears in metadata
        metadata=admin.list_topics(timeout=10)
        assert topic_name in metadata.topics,\
        f"Topic {topic_name} not found after creation"

#Kafdrop 
class TestKafdrop:

    def test_kafdrop_ui_is_reachable(self):
        """Kafdrop web ui should respond on port 9000"""
        assert wait_for_port("localhost",9000,timeout=60),\
        "Kafdrop port 9000 is not open after 60s"

        assert wait_for_http("http://localhost:9000", timeout=60),\
        "Kafdrop UI did not return HTTP 200"