# -*-coding:utf8-*-
import threading
import time

import bson
import redis
from twisted.internet.protocol import Factory
from twisted.internet.threads import deferToThread

from decorators import must_be_in_channel
from dnachat.dna.protocol import DnaProtocol, ProtocolError
from transmission import Transmitter
from .settings import conf
from dnachat.models import Message


class ChatProtocol(DnaProtocol):
    def __init__(self):
        self.user = None
        self.status = 'pending'
        self.pending_messages = []
        self.pending_messages_lock = threading.Lock()

    def requestReceived(self, request):
        processor = getattr(self, 'do_%s' % request.method, None)
        if processor is None:
            raise ProtocolError('Unknown method')
        processor(request)

    def do_authenticate(self, request):
        def send_unread_messages(channel, published_at):
            messages = [
                message.to_dict()
                for message in Message.query(channel__eq=channel, published_at__gt=published_at)
            ]

            with self.pending_messages_lock:
                pending_messages = list(self.pending_messages)
                self.pending_messages = []

            for message in pending_messages:
                message = bson.loads(message)
                del message['method']
                messages.append(message)

            self.transport.write(bson.dumps(dict(method=u'unread', messages=messages)))

        def ready_to_receive(result):
            self.status = 'stable'

        names = conf['AUTHENTICATOR'].split('.')
        module = __import__('.'.join(names[:-1]))
        authenticate = module
        for name in names[1:]:
            authenticate = getattr(authenticate, name)
        self.user = authenticate(request)
        if not self.user:
            raise ProtocolError('Authentication failed')
        self.factory.channels.setdefault(self.user.channel, []).append(self)
        d = deferToThread(send_unread_messages, self.user.channel, request['last_published_at'])
        d.addCallback(ready_to_receive)

    @must_be_in_channel
    def do_publish(self, request):
        message = dict(
            message=request['message'],
            writer=self.user.id,
            published_at=time.time(),
            method=u'publish'
        )
        message = bson.dumps(message)
        self.factory.session.publish(self.user.channel, message)

    def connectionLost(self, reason=None):
        print reason
        if self.user and self.user.channel:
            self.factory.channels[self.user.channel].remove(self)


class ChatFactory(Factory):
    protocol = ChatProtocol
    channels = dict()

    def __init__(self, redis_host='localhost'):
        self.session = redis.StrictRedis(host=redis_host)
        Transmitter(self).start()

