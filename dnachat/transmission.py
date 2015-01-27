# -*-coding:utf8-*-
from threading import Thread
import bson
from twisted.internet.threads import deferToThread
from dnachat.models import Channel


class Transmitter(Thread):
    def __init__(self, factory):
        Thread.__init__(self)
        self.factory = factory

    def run(self):
        pubsub = self.factory.redis_session.pubsub()
        pubsub.psubscribe('*')
        pubsub.listen().next()
        for message in pubsub.listen():
            data = bson.loads(message['data'])
            for client in self.factory.channels[message['channel']]:
                client.transport.write(message['data'])
            deferToThread(self.mark_read, message['channel'], data['published_at'])

    def mark_read(self, channel_name, published_at):
        for client in self.factory.channels[channel_name]:
            for channel in Channel.channels_of(client.user.id):
                if channel.name == channel_name:
                    channel.last_read_at = published_at
                    channel.save()
                    break
