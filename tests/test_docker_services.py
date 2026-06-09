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

#Spark

class TestSpark:

    def test_spark_master_ui_is_reachable(self):
        """Spark master wed ui should respond om port 9090"""
        assert wait_for_port("localhost",9090,timeout=90),\
        "Spark master port 9090 is not open after 90s"
        assert wait_for_http("http://localhost:9090",timeout=90),\
        "Spark master Ui did not return HTTP 200"

    def test_spark_master_rpc_port_is_open(self):
        assert wait_for_port("localhost",7077,timeout=60),\
        "Spark master RPC port 7077 is not open after 60s"

    def test_spark_workers_registered(self):
        """Spark master Ui should report at least 2 workers alive"""
        assert wait_for_http("http://localhost:9090", timeout=90), \
            "Spark master UI not reachable"
        r = requests.get("http://localhost:9090/json", timeout=10)
        assert r.status_code == 200, "Spark master /json endpoint failed"
        data = r.json()
        alive_workers = data.get("aliveworkers", 0)
        assert alive_workers >= 2, \
            f"Expected at least 2 alive Spark workers, got {alive_workers}"
        
#Zookeeper
class TestZookeeper:
 
    def test_zookeeper_port_is_open(self):
        """Zookeeper should be listening on port 2181."""
        assert wait_for_port("localhost", 2181, timeout=60), \
            "Zookeeper port 2181 is not open after 60s"
 
    def test_zookeeper_responds_to_ruok(self):
        """Zookeeper four-letter 'ruok' command should return 'imok'."""
        assert wait_for_port("localhost", 2181, timeout=60)
        with socket.create_connection(("localhost", 2181), timeout=5) as s:
            s.sendall(b"ruok")
            response = s.recv(1024).decode("utf-8")
        assert response == "imok", \
            f"Zookeeper ruok returned unexpected response: '{response}'"
 
 
# Postgres
 
class TestPostgres:
 
    def test_postgres_port_is_open(self):
        """Postgres should be listening on port 5432."""
        assert wait_for_port("localhost", 5432, timeout=60), \
            "Postgres port 5432 is not open after 60s"
 
    def test_postgres_accepts_connection(self):
        """Should be able to connect to Postgres with druid credentials."""
        try:
            import psycopg2
            conn = psycopg2.connect(
                host="localhost",
                port=5432,
                dbname="druid",
                user="druid",
                password="Kidathegreat",
                connect_timeout=10,
            )
            conn.close()
        except ImportError:
            pytest.skip("psycopg2 not installed — run: pip install psycopg2-binary")
        except Exception as e:
            pytest.fail(f"Could not connect to Postgres: {e}")
 
 
# Druid 
 
class TestDruid:
 
    def test_druid_router_is_reachable(self):
        """Druid router (main UI) should respond on port 8888."""
        assert wait_for_port("localhost", 8888, timeout=120), \
            "Druid router port 8888 is not open after 120s"
        assert wait_for_http("http://localhost:8888/status", timeout=120), \
            "Druid router /status did not return HTTP 200"
 
    def test_druid_broker_is_reachable(self):
        """Druid broker should respond on port 8082."""
        assert wait_for_port("localhost", 8082, timeout=120), \
            "Druid broker port 8082 is not open after 120s"
        assert wait_for_http("http://localhost:8082/status", timeout=120), \
            "Druid broker /status did not return HTTP 200"
 
    def test_druid_coordinator_is_reachable(self):
        """Druid coordinator should respond on port 8081."""
        assert wait_for_port("localhost", 8081, timeout=120), \
            "Druid coordinator port 8081 is not open after 120s"
        assert wait_for_http("http://localhost:8081/status", timeout=120), \
            "Druid coordinator /status did not return HTTP 200"
 
    def test_druid_historical_is_reachable(self):
        """Druid historical node should respond on port 8083."""
        assert wait_for_port("localhost", 8083, timeout=120), \
            "Druid historical port 8083 is not open after 120s"
        assert wait_for_http("http://localhost:8083/status", timeout=120), \
            "Druid historical /status did not return HTTP 200"
 
    def test_druid_cluster_is_healthy(self):
        """Druid router health endpoint should report cluster as healthy."""
        assert wait_for_http("http://localhost:8888/status/health", timeout=120), \
            "Druid cluster health endpoint did not return HTTP 200"
 
 
# Superset 
 
class TestSuperset:
 
    def test_superset_port_is_open(self):
        """Superset should be listening on port 8088."""
        assert wait_for_port("localhost", 8088, timeout=120), \
            "Superset port 8088 is not open after 120s"
 
    def test_superset_ui_is_reachable(self):
        """Superset web UI should return HTTP 200 on the root path."""
        assert wait_for_http("http://localhost:8088/health", timeout=120), \
            "Superset /health did not return HTTP 200"
