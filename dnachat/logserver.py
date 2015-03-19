# -*-coding:utf8-*-
from Queue import Queue, Empty
import bson
from multiprocessing import cpu_count
from redis import StrictRedis
from threading import Thread

from .models import Message
from .logger import logger


class ChatLogger(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            try:
                message = self.queue.get(timeout=3)
            except Empty:
                continue
            data = bson.loads(message['data'])
            if data['method'] == 'ack':
                continue
            logger.debug(data)
            try:
                Message.put_item(**data)
            except Exception, e:
                logger.error('Error on save message', exc_info=True)


class ChatLogDistributor(object):
    def __init__(self, redis_host):
        self.session = StrictRedis(host=redis_host)

    def start(self):
        pubsub = self.session.pubsub()
        pubsub.psubscribe('*')
        pubsub.listen().next()
        queue = Queue()

        for _ in xrange(cpu_count() * 2):
            ChatLogger(queue).start()

        for message in pubsub.listen():
            queue.put(message)