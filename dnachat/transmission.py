# -*-coding:utf8-*-
from threading import Thread


class Transmitter(Thread):
    def __init__(self, factory):
        Thread.__init__(self)
        self.factory = factory

    def run(self):
        pubsub = self.factory.session.pubsub()
        pubsub.psubscribe('*')
        pubsub.listen().next()
        for message in pubsub.listen():
            for client in self.factory.channels[message['channel']]:
                if client.status == 'stable':
                    client.transport.write(message['data'])
                elif client.status == 'pending':
                    with client.pending_messages_lock:
                        client.pending_messages.append(message)