
from time import sleep
from base.producer import BaseProducer

if __name__ == '__main__':
    message = dict(
        item='test',
        context='test-run',
        arg='test-arg',
        )
    sleep(10)
    producer = BaseProducer(message=message)
    for _ in range(5):
        sleep(2)
        producer.run()
