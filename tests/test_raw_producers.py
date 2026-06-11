"""
Integration tests for all raw kafka topics producers
Very messages are produced and consumed correctly for all 5 topics
"""
import json
import uuid
import time
import pytest
from confluent_kafka import Producer, Consumer,KafkaError
from confluent_kafka.admin import AdminClient,NewTopic
from data_generator.data_generator import TransactionDataGenerator


#Constants 
BOOTSTRAP_SERVERS="127.0.0.1:9092"
TOPICS=[
    "visa_raw",
    "mastercard_raw",
    "mpesa_raw",
    "pesapal_raw",
    "flutter_raw",
]

PRODUCER_CONFIG={
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "acks": "all",
    "retries": 10,
    "enable.idempotence": True,
}

#Helper Functions 

    

def make_producer() -> Producer:
    return Producer(PRODUCER_CONFIG)

def produce_messages(topic : str,records:list)-> None:
    """produce a list of records to a topic and flush"""
    producer = make_producer()
    for record in records:
        producer.produce(
            topic=topic,
            value=json.dumps(record).encode('utf-8')
        )
    remaining = producer.flush(timeout=15)
    print(f"Messages remaining after flush: {remaining}")

def produce_and_consume(topic:str,records:list,timeout:float=30.0)->list:
    """Subscribe first , produce and consume"""
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id":f"test-{topic}-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    consumer.subscribe([topic])

    deadline = time.time() + 30

    while not consumer.assignment():
        consumer.poll(0.5)

        if time.time() > deadline:
            raise RuntimeError(
                f"Consumer never received partition assignment for {topic}"
            )

    print("Assigned:", consumer.assignment())

    #Now produce 
    produce_messages(topic,records)

    #Consume 
    messages =[]
    deadline = time.time() + timeout
    try:
        while len(messages) < len(records) and time.time() < deadline:
            msg= consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"Kafka error:{msg.error()}")
            messages.append(json.loads(msg.value().decode("utf-8")))
    finally:
        consumer.close()

    return messages

#Fixtures 
@pytest.fixture(scope='session')
def generator():
    return TransactionDataGenerator()

@pytest.fixture(scope="session",autouse=True)
def esnure_topics():
    """Create all required topics before test run"""
    admin = AdminClient({"bootstrap.servers":BOOTSTRAP_SERVERS})
    existing = admin.list_topics(timeout=10).topics.keys()
    to_create=[
        NewTopic(t,num_partitions=1,replication_factor=1)
        for t in TOPICS if t not in existing
    ]

    if to_create:
        futures= admin.create_topics(to_create)
        for topic, future in futures.items():
            try:
                future.result()
            except Exception as e:
                if "already exists" not in str(e).lower():
                    raise




#Test producers 
class TestVisaProducer:
    
    def test_produces_correct_count(self,generator):
        records =[generator.generate_visa_transaction() for _ in range(10)]
        received = produce_and_consume('visa_raw',records)
        assert len(received)==10

    def test_messages_are_valid_json(self,generator):
        records=[generator.generate_visa_transaction() for _ in range(10)]
        received = produce_and_consume("visa_raw",records)
        for msg in received:
            assert isinstance(msg,dict)

    def test_message_source_is_visa(self, generator):
        records = [generator.generate_visa_transaction() for _ in range(5)]
        received = produce_and_consume("visa_raw", records)
        for msg in received:
            assert msg["source"] == "VISA"

    def test_message_has_bitmap(self, generator):
        records = [generator.generate_visa_transaction() for _ in range(5)]
        received = produce_and_consume("visa_raw", records)
        for msg in received:
            assert "bitmap" in msg
            assert "DE002" in msg["bitmap"]

class TestMastercardProducer:

    def test_produces_correct_count(self, generator):
        records = [generator.generate_mastercard_transaction() for _ in range(10)]
        received = produce_and_consume("mastercard_raw", records)
        assert len(received) == 10, f"Expected 10 messages, got {len(received)}"

    def test_messages_are_valid_json(self, generator):
        records = [generator.generate_mastercard_transaction() for _ in range(5)]
        received = produce_and_consume("mastercard_raw", records)
        for msg in received:
            assert isinstance(msg, dict)

    def test_message_source_is_mastercard(self, generator):
        records = [generator.generate_mastercard_transaction() for _ in range(5)]
        received = produce_and_consume("mastercard_raw", records)
        for msg in received:
            assert msg["source"] == "MASTERCARD"

    def test_message_has_bitmap(self, generator):
        records = [generator.generate_mastercard_transaction() for _ in range(5)]
        received = produce_and_consume("mastercard_raw", records)
        for msg in received:
            assert "bitmap" in msg
            assert "DE002" in msg["bitmap"]


class TestMpesaProducer:

    def test_produces_correct_count(self, generator):
        records = [generator.generate_mpesa_transaction() for _ in range(10)]
        received = produce_and_consume("mpesa_raw", records)
        assert len(received) == 10, f"Expected 10 messages, got {len(received)}"

    def test_messages_are_valid_json(self, generator):
        records = [generator.generate_mpesa_transaction() for _ in range(5)]
        received = produce_and_consume("mpesa_raw", records)
        for msg in received:
            assert isinstance(msg, dict)

    def test_message_source_is_mpesa(self, generator):
        records = [generator.generate_mpesa_transaction() for _ in range(5)]
        received = produce_and_consume("mpesa_raw", records)
        for msg in received:
            assert msg["source"] == "MPESA"

    def test_webhook_payload_present(self, generator):
        records = [generator.generate_mpesa_transaction() for _ in range(5)]
        received = produce_and_consume("mpesa_raw", records)
        for msg in received:
            assert "webhook_payload" in msg
            assert "TransID" in msg["webhook_payload"]
            assert "TransAmount" in msg["webhook_payload"]

    def test_signature_present(self, generator):
        records = [generator.generate_mpesa_transaction() for _ in range(5)]
        received = produce_and_consume("mpesa_raw", records)
        for msg in received:
            assert "signature" in msg
            assert len(msg["signature"]) > 0


class TestFlutterwaveProducer:

    def test_produces_correct_count(self, generator):
        records = [generator.generate_flutterwave_transaction() for _ in range(10)]
        received = produce_and_consume("flutter_raw", records)
        assert len(received) == 10, f"Expected 10 messages, got {len(received)}"

    def test_messages_are_valid_json(self, generator):
        records = [generator.generate_flutterwave_transaction() for _ in range(5)]
        received = produce_and_consume("flutter_raw", records)
        for msg in received:
            assert isinstance(msg, dict)

    def test_message_source_is_flutterwave(self, generator):
        records = [generator.generate_flutterwave_transaction() for _ in range(5)]
        received = produce_and_consume("flutter_raw", records)
        for msg in received:
            assert msg["source"] == "FLUTTERWAVE"

    def test_webhook_payload_present(self, generator):
        records = [generator.generate_flutterwave_transaction() for _ in range(5)]
        received = produce_and_consume("flutter_raw", records)
        for msg in received:
            assert "webhook_payload" in msg
            assert "data" in msg["webhook_payload"]
            assert "amount" in msg["webhook_payload"]["data"]

    def test_verification_hash_present(self, generator):
        records = [generator.generate_flutterwave_transaction() for _ in range(5)]
        received = produce_and_consume("flutter_raw", records)
        for msg in received:
            assert "verification_hash" in msg
            assert len(msg["verification_hash"]) > 0


class TestPesapalProducer:

    def test_produces_correct_count(self, generator):
        records = [generator.generate_pesapal_transaction() for _ in range(10)]
        received = produce_and_consume("pesapal_raw", records)
        assert len(received) == 10, f"Expected 10 messages, got {len(received)}"

    def test_messages_are_valid_json(self, generator):
        records = [generator.generate_pesapal_transaction() for _ in range(5)]
        received = produce_and_consume("pesapal_raw", records)
        for msg in received:
            assert isinstance(msg, dict)

    def test_message_source_is_pesapal(self, generator):
        records = [generator.generate_pesapal_transaction() for _ in range(5)]
        received = produce_and_consume("pesapal_raw", records)
        for msg in received:
            assert msg["source"] == "PESAPAL"

    def test_ipn_payload_present(self, generator):
        records = [generator.generate_pesapal_transaction() for _ in range(5)]
        received = produce_and_consume("pesapal_raw", records)
        for msg in received:
            assert "ipn_payload" in msg
            assert "amount" in msg["ipn_payload"]
            assert "pesapal_transaction_tracking_id" in msg["ipn_payload"]

    def test_ipn_signature_present(self, generator):
        records = [generator.generate_pesapal_transaction() for _ in range(5)]
        received = produce_and_consume("pesapal_raw", records)
        for msg in received:
            assert "ipn_signature" in msg
            assert len(msg["ipn_signature"]) > 0


