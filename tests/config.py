# -*-coding:utf8-*-
from dnachat.server import BaseChatProtocol


class User(object):
    def __init__(self, id_):
        self.id = id_


class ChatProtocol(BaseChatProtocol):

    def authenticate(self, request):
        return User(request['id'])

    @staticmethod
    def get_user_by_id(user_id):
        return User(user_id)

PORT = 9310
PROTOCOL = ChatProtocol

DYNAMODB_PREFIX = 'JoshuaStudysearch'
DYNAMODB_CONNECTION = {
    'host': 'localhost',
    'port': 8001,
    'is_secure': False
}

NOTIFICATION_QUEUE_NAME = 'JoshuaNotificationQueue'

REDIS_HOST = 'localhost'

