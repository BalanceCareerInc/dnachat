# -*-coding:utf8-*-
import bson

from Queue import Queue, Empty
from multiprocessing import cpu_count
from redis import StrictRedis
from threading import Thread, Timer, RLock

from .models import Message, ChannelJoinInfo
from .logger import logger

last_read_at_buffer = dict()
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
                last_read_at_buffer[(data['channel'], data['sender'])] = data['published_at']
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
        buffer_ = last_read_at_buffer.copy()
        last_read_at_buffer.clear()

    for (channel, user_id), published_at in buffer_.iteritems():
        join_info = ChannelJoinInfo.get_item(channel, user_id)
        join_info.last_read_at = published_at
        join_info.save()

    Timer(1, flush_last_read_at).start()


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
