# -*-coding:utf8-*-
import bson
from redis import StrictRedis

from .models import Message
from .logger import logger


class ChatLogger(object):
    def __init__(self, redis_host):
        self.session = StrictRedis(host=redis_host)

    def start(self):
        pubsub = self.session.pubsub()
        pubsub.psubscribe('*')
        pubsub.listen().next()
        for message in pubsub.listen():
            data = bson.loads(message['data'])
            logger.debug(data)
            try:
                Message.put_item(**data)
            except Exception, e:
                logger.error('Error on save message', exc_info=True)
