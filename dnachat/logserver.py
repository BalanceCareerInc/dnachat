# -*-coding:utf8-*-
import bson
from redis import StrictRedis

from dnachat.models import Message


class ChatLogger(object):
    def __init__(self, redis_host):
        self.session = StrictRedis(host=redis_host)

    def start(self):
        pubsub = self.session.pubsub()
        pubsub.psubscribe('*')
        pubsub.listen().next()
        for message in pubsub.listen():
            data = bson.loads(message['data'])
            print Message.put_item(
                type=message['type'],
                channel=message['channel'],
                user=data['writer'],
                published_at=data['published_at'],
                message=data['message']
            )
