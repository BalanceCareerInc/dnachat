# -*-coding:utf8-*-
import bson
from bynamodb import patch_dynamodb_connection
from redis import StrictRedis
from chat import get_config
from models import Message


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
    config = get_config('conf/localconfig.py')
    patch_dynamodb_connection(
        host=config['DYNAMODB_HOST'],
        port=config['DYNAMODB_PORT'],
        is_secure=config['DYNAMODB_IS_SECURE']
    )
    ChatLogger(config['REDIS_HOST']).start()

if __name__ == '__main__':
    run_logger()
