# -*-coding:utf8-*-
import bson
import threading
from redis import StrictRedis

from .models import Message
from .logger import logger


def put_message(data):
    try:
        Message.put_item(**data)
    except Exception, e:
        logger.error('Error on save message', exc_info=True)


class ChatLogger(object):
    def __init__(self, redis_host):
        self.session = StrictRedis(host=redis_host)

    def start(self):
        pubsub = self.session.pubsub()
        pubsub.psubscribe('*')
        pubsub.listen().next()
        for message in pubsub.listen():
            data = bson.loads(message['data'])
            if data['method'] == 'ack':
                continue
            logger.debug(data)
            threading.Thread(target=put_message, args=(data,)).start()
