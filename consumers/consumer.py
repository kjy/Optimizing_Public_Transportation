"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen

# Topics are divided into partitions. A partition represents the unit of parallelism in Kafka. In general, a higher 
# number of partitions means higher throughput. Within each partition, each job has a specific offset that consumers 
# use to keep track of how far they have progressed through the stream. Consumers may use Kafka partitions as semantic partitions as well.

# If you provide a key to messages in Kafka, they will be partitioned by that key allowing you to do per-key processing.
# In some sense, you can think of Kafka as categorizing your data and providing it to you ordered by key. 

#Yes, one partition is consumed by one consumer in one group. Y.C.

logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            'bootstrap.servers': BROKER_URL,
            'group.id':'0',
            'auto.offset.reset':'earliest' if self.offset_earliest else 'latest'
        }

        # TODO: Create the Consumer, using the appropriate type.
        if is_avro is True:
            self.broker_properties["schema.registry.url"] = "http://localhost:8081"
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        #
        #
        # TODO: Configure the AvroConsumer and subscribe to the topics. Make sure to think about
        # how the `on_assign` callback should be invoked.
logging.info(f'Subscribing to topic pattern: {self.topic_name_pattern}'')      self.consumer.subscribe([topic_name_pattern],on_assign=self.on_assign)
        
        # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""

        for partition in partitions:
            partition.offset = OFFSET_BEGINNING

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)
        logger.info("partitions successfully assigned")

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        #
        #
        # TODO: Poll Kafka for messages. Make sure to handle any errors or exceptions.
        # Additionally, make sure you return 1 when a message is processed, and 0 when no message
        # is retrieved.
        #
        #
        try:
            message = self.consumer.poll(self.consume_timeout)
        except Exception:
            logger.info(f'Error while polling')

        if message is None:
            logging.info(f'No message found')
            return 0
        elif message.error():
            logger.info(f'Error while consuming message')
        else:
            self.message_handler(message)
            return 1

        # logger.info("_consume is incomplete - skipping")


    def close(self):
        """Cleans up any open kafka consumers"""
        #
        #
        # TODO: Cleanup the kafka consumer
        #
        self.consumer.close()

