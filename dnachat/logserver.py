# -*-coding:utf8-*-
import bson
from bynamodb import patch_dynamodb_connection
from redis import StrictRedis

from dnachat.settings import conf
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
                channel=message['channel'],
                user=data['writer'],
                published_at=data['published_at'],
                message=data['message']
            )


def run_logger():
    patch_dynamodb_connection(
        host=conf['DYNAMODB_HOST'],
        port=conf['DYNAMODB_PORT'],
        is_secure=conf['DYNAMODB_IS_SECURE']
    )
    ChatLogger(conf['REDIS_HOST']).start()

if __name__ == '__main__':
    run_logger()
