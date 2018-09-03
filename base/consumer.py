import logging
import pika
from time import sleep
from .base import RabbitConfig


class BaseConsumer(RabbitConfig):

    def connect(self):
        self.LOGGER.info('Connecting to %s', self._url)
        return pika.SelectConnection(pika.URLParameters(self._url),
                                     self.on_connection_open,
                                     stop_ioloop_on_close=False)

    def on_connection_open(self, unused_connection):
        self.LOGGER.info('Connection opened')
        self.add_on_connection_close_callback()
        self.open_channel()

    def add_on_connection_close_callback(self):
        self.LOGGER.info('Adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self.LOGGER.warning('Connection closed, reopening in 5 seconds: (%s) %s',
                           reply_code, reply_text)
            self._connection.add_timeout(5, self.reconnect)

    def reconnect(self):
        # This is the old connection IOLoop instance, stop its ioloop
        self._connection.ioloop.stop()

        if not self._closing:

            # Create a new connection
            self._connection = self.connect()

            # There is now a new connection, needs a new ioloop to run
            self._connection.ioloop.start()

    def open_channel(self):
        self.LOGGER.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        self.LOGGER.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange()

    def add_on_channel_close_callback(self):
        self.LOGGER.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        self.LOGGER.warning('Channel %i was closed: (%s) %s',
                       channel, reply_code, reply_text)
        self._connection.close()

    def setup_exchange(self):
        self.LOGGER.info('Declaring exchange %s', self._exchange)
        self._channel.exchange_declare(self.on_exchange_declareok,
                                       self._exchange,
                                       self._exchange_type)

    def on_exchange_declareok(self, unused_frame):
        self.LOGGER.info('Exchange declared')
        self.setup_queue()

    def setup_queue(self):
        self.LOGGER.info('Declaring queue %s', self._queue)
        self._channel.queue_declare(self.on_queue_declareok, self._queue)

    def on_queue_declareok(self, method_frame):
        self.LOGGER.info('Binding %s to %s with %s',
                    self._exchange, self._queue, self._routing_key)
        self._channel.queue_bind(self.on_bindok, self._queue,
                                 self._exchange, self._routing_key)

    def on_bindok(self, unused_frame):
        self.LOGGER.info('Queue bound')
        self.start_consuming()

    def start_consuming(self):
        self.LOGGER.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self.on_message,
                                                         self._queue)

    def add_on_cancel_callback(self):
        self.LOGGER.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        self.LOGGER.info('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, unused_channel, basic_deliver, properties, body):
        message = dict(
            delivery_tag=basic_deliver.delivery_tag,
            app_id=properties.app_id,
            body=body,
            )
        self._message_handle(body)
        self.LOGGER.info('Received message # %s from %s: %s',
                    basic_deliver.delivery_tag, properties.app_id, body)
        self.acknowledge_message(basic_deliver.delivery_tag)

    def acknowledge_message(self, delivery_tag):
        self.LOGGER.info('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        if self._channel:
            self.LOGGER.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def on_cancelok(self, unused_frame):
        self.LOGGER.info('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def close_channel(self):
        self.LOGGER.info('Closing the channel')
        self._channel.close()

    def run(self, message_handle):
        self._message_handle = message_handle
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        self.LOGGER.info('Stopping')
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()
        self.LOGGER.info('Stopped')

    def close_connection(self):
        self.LOGGER.info('Closing connection')
        self._connection.close()
