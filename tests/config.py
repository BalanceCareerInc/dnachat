# -*-coding:utf8-*-
from dnachat.models import ChannelJoinInfo
from dnachat.server import BaseChatProtocol


class User(object):
    def __init__(self, id_):
        self.id = id_


class TestChatProtocol(BaseChatProtocol):

    def authenticate(self, request):
        return User(request['id'])

    @classmethod
    def get_activated_channels(cls, user):
        return [ji.channel for ji in ChannelJoinInfo.by_user(user.id)]

    @staticmethod
    def get_user_by_id(user_id):
        return User(user_id)


class TestApiProcessor(object):
    def __init__(self, factory):
        self.factory = factory

    def handle(self, message):
        pass

PORT = 9310
PROTOCOL = TestChatProtocol
API_PROCESSOR = TestApiProcessor

DYNAMODB_PREFIX = 'JoshuaStudysearch'
DYNAMODB_CONNECTION = {
    'host': 'localhost',
    'port': 8001,
    'is_secure': False
}

NOTIFICATION_QUEUE_NAME = 'JoshuaNotificationQueue'
LOG_QUEUE_NAME = 'JoshuaTestLogQueue'
API_QUEUE_NAME = 'JoshuaTestApiQueue'
QUEUE_POLLING_INTERVAL = 2

REDIS_HOST = 'localhost'

