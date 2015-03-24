# -*-coding:utf8-*-
from Queue import Empty, Queue
from threading import Thread
import time

from .logger import logger
import multiprocessing

multiprocessing.cpu_count()


class Transmitter(Thread):
    def __init__(self, queue, factory):
        self.queue = queue
        self.factory = factory
        Thread.__init__(self)

    def run(self):
        while True:
            try:
                message = self.queue.get(timeout=3)
            except Empty:
                continue
            for client in self.factory.channels.get(message['channel'], []):
                try:
                    client.transport.write(message['data'])
                    client.attended_channel_join_info.last_sent_at = time.time()
                except Exception, e:
                    logger.error('TransmissionError: %s' % str(e), exc_info=True)
            self.queue.task_done()


class TransmitDistributor(Thread):
    def __init__(self, factory):
        Thread.__init__(self)
        self.factory = factory

    def run(self):
        pubsub = self.factory.redis_session.pubsub()
        pubsub.psubscribe('*')
        pubsub.listen().next()
        queue = Queue()
        for x in xrange(multiprocessing.cpu_count() * 8):
            Transmitter(queue, self.factory).start()
        for message in pubsub.listen():
            queue.put(message)
