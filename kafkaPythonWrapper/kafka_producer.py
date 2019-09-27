from kafka import KafkaProducer
from kafka.errors import KafkaError
import functools
import time
from collections.abc import Iterable
from .logger import Logger
logger = Logger('kafka_producer')

class MessageSender:
    '''Kafka messenger'''

    def __init__(self, topic, bootstrap_server=['localhost:9092'], retries=1):
        '''initialize a producer with no serilizer'''
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_server,
            retries=retries)

    def send_sync(self, retry=1, delay=0):
        '''send a message synchronously'''
        def decorator(function):

            @functools.wraps(function)
            def wrapper(*args, **kwargs):
                '''wrap a function into producer'''  
                nonlocal retry
                while retry > 0:
                    time.sleep(delay)
                    try: 
                        for key, value in function(*args, **kwargs):
                            if value:
                                self._send(value, key)
                            else:
                                logger.warning('No value assgined')
                                return 'No value assgined'
                    except Exception as e:
                        logger.error(f'{e}')
                        raise Exception(f'key and value cannot be assgined: {e}')
                    
                    retry -= 1
            return wrapper
        return decorator

    def _send(self, value, key=None):
        # Asynchronous by default
        if value and key:
            future = self.producer.send(self.topic, key=key.encode(), value=value.encode())
        elif value and key is None:
            future = self.producer.send(self.topic, value=value.encode())
            # Block for 'synchronous' sends
        try:
            record_metadata = future.get(timeout=10)
            logger.info(f'task sent|topic: {self.topic}, key: {key}, value: {value}')
            return record_metadata.topic, record_metadata.partition, record_metadata.offset
        except KafkaError as e:
            # Decide what to do if produce request failed...
            logger.error(f'Kafka Error {e}')
            raise Exception(f'Kafka Error {e}')


    def send_async(self, topic, value, key=''):
        '''send a message asynchronously'''

        def on_send_success(record_metadata):
            logger.info(record_metadata.topic, record_metadata.partition, record_metadata.offset)
            return record_metadata.topic, record_metadata.partition, record_metadata.offset

        def on_send_error(excp):
            logger.error('producer error', exc_info=excp)
            raise Exception(f'producer error: {excp}')

        self.producer.send(
                    topic, key=key.encode(), value=value.encode()
            ).add_callback(on_send_success).add_errback(on_send_error)
        self.producer.flush()
    
    def disconnect(self):
        '''disconnect producer'''

        try:
            self.producer.close()
        except Exception as e:
            logger.error(f'disconnecting failed: {e}')
