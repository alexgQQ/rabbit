import logging
import yaml

class RabbitConfig(object):
    def __init__(self, message='', config_path='/usr/src/app/base/config/rabbit.yaml'):
        self._connection = None
        self._channel = None
        self._closing = False
        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self.message = message
        self._message_number = 0
        self._stopping = False
        self._consumer_tag = None

        LOG_FORMAT = ('%(asctime)s - %(levelname)s - %(message)s')
        self.LOGGER = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

        with open(config_path, 'r') as ymlfile:
            config = yaml.load(ymlfile)

        self._url = config['url']
        self._exchange = config['exchange']['id']
        self._exchange_type = config['exchange']['type']
        self._queue = config['queue']['id']
        self._routing_key = config['queue']['routing_key']
