# -*-coding:utf8-*-
import bson
import time

from Queue import Queue, Empty
from multiprocessing import cpu_count
from redis import StrictRedis
from threading import Thread, Timer, RLock

from .models import Message, ChannelJoinInfo
from .logger import logger

last_read_at_buffer = []
lock = RLock()


def log_message(queue):
    while True:
        try:
            message = queue.get(timeout=3)
        except Empty:
            continue
        data = bson.loads(message['data'])
        if data['method'] == 'ack':
            with lock:
                last_read_at_buffer.append((data['channel'], data['sender']))
            continue
        logger.debug(data)
        try:
            Message.put_item(**data)
        except Exception, e:
            logger.error('Error on save message', exc_info=True)


def flush_last_read_at_periodically(second):
    def wrap():
        flush_last_read_at_periodically(second)
        flush_last_read_at()
    Timer(second, wrap).start()


def flush_last_read_at():
    global last_read_at_buffer, lock

    with lock:
        buffer_ = list(last_read_at_buffer)
        last_read_at_buffer = []

    for channel, user_id in buffer_:
        join_info = ChannelJoinInfo.get_item(channel, user_id)
        join_info.last_read_at = time.time()
        join_info.save()


class LogServer(object):
    def __init__(self, redis_host):
        self.session = StrictRedis(host=redis_host)

    def start(self):
        pubsub = self.session.pubsub()
        pubsub.psubscribe('*')
        pubsub.listen().next()
        queue = Queue()

        for _ in xrange(cpu_count() * 2):
            Thread(target=log_message, args=(queue,)).start()

        flush_last_read_at_periodically(1)

        for message in pubsub.listen():
            queue.put(message)
