from kafka import KafkaConsumer
import functools
from .logger import Logger

logger = Logger('kafka_consumer')

class MessageCollector:
    '''Kafka messenger'''
    
    def __init__(self, topic, group_id, bootstrap_server=['localhost:9092']):
        '''initiate a consumer with auto-offset'''
        self.consumer = KafkaConsumer(topic,
          group_id=group_id, bootstrap_servers=bootstrap_server,
          auto_commit_interval_ms=1000)

    def consume(self, function):
        '''start to consume messages from topics'''
        @functools.wraps(function)
        def wrapper(**kwargs):
            '''wrap a consumer function into the consumer'''
            for message in self.consumer: 
                logger.info("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                      message.offset, message.key,
                                                      message.value))
                if message.key:
                    kwargs[message.key.decode()] = message.value.decode()
                else:
                    kwargs['no_key_value'] = message.value.decode()
                try:
                    function(**kwargs)
                except TypeError as e:
                    logger.error(f"function error: {e}")
        return wrapper

    def disconnect(self):
        '''disconnect consumer'''
        
        try:
            self.consumer.close()
        except Exception as e:
            logger.error(f'disconnecting failed: {e}')

