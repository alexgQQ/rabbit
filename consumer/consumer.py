from pprint import pprint
from time import sleep
from base.consumer import BaseConsumer
import logging

LOG_FORMAT = ('%(asctime)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

def print_message(message_dict):
    LOGGER.info('Running print_message with: %s', message_dict)

if __name__ == '__main__':
    consumer = BaseConsumer()
    consumer.run(print_message)
