# -*-coding:utf8-*-
import threading
import time

import bson
import redis
from boto import sqs
from boto.sqs.message import Message as QueueMessage
from twisted.internet.protocol import Factory
from twisted.internet.threads import deferToThread

from decorators import must_be_in_channel
from dnachat.dna.protocol import DnaProtocol, ProtocolError
from transmission import Transmitter
from .settings import conf, func_from_package_name
from dnachat.models import Message as MessageModel


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
                for message in MessageModel.query(channel__eq=channel, published_at__gt=published_at)
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

        authenticate = func_from_package_name(conf['AUTHENTICATOR'])
        self.user = authenticate(request)
        if not self.user:
            raise ProtocolError('Authentication failed')
        self.factory.channels.setdefault(self.user.channel, []).append(self)
        d = deferToThread(send_unread_messages, self.user.channel, request['last_published_at'])
        d.addCallback(ready_to_receive)

    @must_be_in_channel
    def do_publish(self, request):
        def publish_to_client(channel, message_):
            self.factory.redis_session.publish(channel, message_)

        def write_to_sqs(result, message_):
            self.factory.queue.write(QueueMessage(body=message_))

        message = dict(
            message=request['message'],
            writer=self.user.id,
            published_at=time.time(),
            method=u'publish',
            channel=request.user.channel
        )
        message = bson.dumps(message)
        d = deferToThread(publish_to_client, message)
        d.addCallback(write_to_sqs, message)

    def connectionLost(self, reason=None):
        print reason
        if self.user and self.user.channel:
            self.factory.channels[self.user.channel].remove(self)


class ChatFactory(Factory):
    protocol = ChatProtocol
    channels = dict()

    def __init__(self, redis_host='localhost'):
        self.redis_session = redis.StrictRedis(host=redis_host)
        self.queue = sqs.connect_to_region('ap-northeast-1').get_queue(conf['NOTIFICATION_QUEUE_NAME'])
        Transmitter(self).start()

