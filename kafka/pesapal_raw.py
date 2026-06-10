from confluent_kafka import Producer
from data_generator.data_generator import TransactionDataGenerator
import logging
import json

logging.basicConfig(level=logging.INFO)
logger= logging.getLogger(__name__)

#A function to produce the flutter wave transaction
def produce_flutter_transaction():
    producer_config={
        'bootstrap.servers':'localhost:9092',
        'acks':'all',
        'retries':10,
        'enable.idempotence':True

    }

    producer= Producer(producer_config)
    generator=TransactionDataGenerator()
    count = 0

    while count < 100000:
        pesapal_data=generator.generate_pesapal_transaction()

        producer.produce(
            topic='pesapal_raw',
            value=json.dumps(pesapal_data).encode('utf-8')
        )

        count +=1
        producer.poll(0) #serve delivery callbacks without blocking
        print(f'Record [{count}]: {pesapal_data}')
        logger.info(f'Successfully Produced record [{count}]')

    producer.flush()
    logger.info('All records produced successfully')

if __name__=='__main__':
    produce_flutter_transaction()