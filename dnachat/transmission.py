# -*-coding:utf8-*-
import bson
import time
from threading import Thread

from .logger import logger


class Transmitter(Thread):
    def __init__(self, factory):
        Thread.__init__(self)
        self.factory = factory

    def run(self):
        pubsub = self.factory.redis_session.pubsub()
        pubsub.psubscribe('*')
        pubsub.listen().next()
        for message in pubsub.listen():
            if message['channel'] == 'create_channel':
                try:
                    self.make_client_attend_channel(message)
                except Exception, e:
                    logger.error('CreatingChannelError: %s' % str(e), exc_info=True)
                continue

            for client in self.factory.channels.get(message['channel'], []):
                try:
                    client.transport.write(message['data'])
                except Exception, e:
                    logger.error('TransmissionError: %s' % str(e), exc_info=True)

    def make_client_attend_channel(self, message):
        data = bson.loads(message['data'])
        clients = reduce(lambda x, y: x.union(y), self.factory.channels.values(), set())
        listening_users = dict((c.user.id, c) for c in clients if c.user)
        channel_users = set(data['users'])
        applying_users = set(listening_users.keys()) & channel_users
        if applying_users:
            self.factory.channels.setdefault(data['channel'], []).extend(
                [listening_users[user] for user in applying_users]
            )
