# -*-coding:utf8-*-
import time
import json

from Queue import Empty
from multiprocessing import cpu_count, Process, Queue
from threading import Timer

from boto import sqs
from dnachat.settings import conf

from .models import Message, ChannelJoinInfo
from .logger import logger


def log_message(message_queue, last_read_at_queue):
    while True:
        message = message_queue.read(wait_time_seconds=conf['QUEUE_POLLING_INTERVAL'])
        if not message:
            continue
        message_queue.delete_message(message)
        data = json.loads(message.get_body())
        if data['method'] == 'ack':
            last_read_at_queue.put((data['channel'], data['sender']))
            continue
        logger.debug(data)
        try:
            Message.put_item(**data)
        except Exception, e:
            logger.error('Error on save message\n%s' % str(data), exc_info=True)


def flush_last_read_at_periodically(second, last_read_at_queue):
    def wrap():
        flush_last_read_at_periodically(second, last_read_at_queue)
        flush_last_read_at(last_read_at_queue)
    Timer(second, wrap).start()


def flush_last_read_at(last_read_at_queue):
    while True:
        try:
            channel, user_id = last_read_at_queue.get_nowait()
        except Empty:
            break
        join_info = ChannelJoinInfo.get_item(channel, user_id)
        join_info.last_read_at = time.time()
        join_info.save()


class LogServer(object):
    def __init__(self):
        sqs_conn = sqs.connect_to_region('ap-northeast-1')
        self.queue = sqs_conn.get_queue(conf['LOG_QUEUE_NAME'])

    def start(self):
        last_read_at_queue = Queue()
        flush_last_read_at_periodically(1, last_read_at_queue)

        for _ in xrange(cpu_count() - 1):
            Process(target=log_message, args=(self.queue, last_read_at_queue)).start()
        log_message(self.queue, last_read_at_queue)

