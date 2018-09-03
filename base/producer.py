import pika
import json
from .base import RabbitConfig


class BaseProducer(RabbitConfig):

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
        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0

        # This is the old connection IOLoop instance, stop its ioloop
        self._connection.ioloop.stop()

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
        self.LOGGER.warning('Channel was closed: (%s) %s', reply_code, reply_text)
        if not self._closing:
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
        self.start_publishing()

    def start_publishing(self):
        self.LOGGER.info('Issuing consumer related RPC commands')
        self.enable_delivery_confirmations()
        self.publish_message()

    def enable_delivery_confirmations(self):
        self.LOGGER.info('Issuing Confirm.Select RPC command')
        self._channel.confirm_delivery(self.on_delivery_confirmation)

    def on_delivery_confirmation(self, method_frame):
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        self.LOGGER.info('Received %s for delivery tag: %i',
                    confirmation_type,
                    method_frame.method.delivery_tag)
        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1
        self._deliveries.remove(method_frame.method.delivery_tag)
        self.LOGGER.info('Published %i messages, %i have yet to be confirmed, '
                    '%i were acked and %i were nacked',
                    self._message_number, len(self._deliveries),
                    self._acked, self._nacked)
        self.stop()

    def publish_message(self):
        if self._stopping:
            return

        properties = pika.BasicProperties(app_id='example-publisher',
                                          content_type='application/json')

        self._channel.basic_publish(self._exchange, self._routing_key,
                                    json.dumps(self.message),
                                    properties)
        self._message_number += 1
        self._deliveries.append(self._message_number)
        self.LOGGER.info('Published message # %i', self._message_number)

    def close_channel(self):
        self.LOGGER.info('Closing the channel')
        if self._channel:
            self._channel.close()

    def run(self):
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        self.LOGGER.info('Stopping')
        self._stopping = True
        self.close_channel()
        self.close_connection()
        self._connection.ioloop.start()
        self.LOGGER.info('Stopped')

    def close_connection(self):
        self.LOGGER.info('Closing connection')
        self._closing = True
        self._connection.close()
