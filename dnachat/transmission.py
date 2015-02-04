# -*-coding:utf8-*-
from threading import Thread
import time


class Transmitter(Thread):
    def __init__(self, factory):
        Thread.__init__(self)
        self.factory = factory

    def run(self):
        pubsub = self.factory.redis_session.pubsub()
        pubsub.psubscribe('*')
        pubsub.listen().next()
        for message in pubsub.listen():
            for client in self.factory.channels.get(message['channel'], []):
                client.transport.write(message['data'])
                client.channel.last_sent_at = time.time()
