# -*-coding:utf8-*-
import time
import json

from multiprocessing import cpu_count, Process, Lock
from threading import Timer

from boto import sqs
from dnachat.settings import conf

from .models import Message, ChannelJoinInfo
from .logger import logger

last_read_at_buffer = []
lock = Lock()


def log_message(queue):
    while True:
        message = queue.read(wait_time_seconds=5)
        if not message:
            continue
        queue.delete_message(message)
        data = json.loads(message.get_body())
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
        sqs_conn = sqs.connect_to_region('ap-northeast-1')
        self.queue = sqs_conn.get_queue(conf['LOG_QUEUE_NAME'])

    def start(self):
        flush_last_read_at_periodically(1)

        for _ in xrange(cpu_count() - 1):
            Process(target=log_message, args=(self.queue,)).start()
        log_message(self.queue)

