# -*-coding:utf8-*-

import bson
import json
from bynamodb.exceptions import ItemNotFoundException
import redis
import threading
import time

from boto import sqs
from boto.sqs.message import Message as QueueMessage
from uuid import uuid1
from twisted.internet.protocol import Factory
from twisted.internet.threads import deferToThread

from .decorators import in_channel_required, auth_required
from .dna.protocol import DnaProtocol, ProtocolError
from dnachat.adapter import authenticate
from .transmission import Transmitter
from .settings import conf
from .models import Message as DnaMessage, Joiner


class ChatProtocol(DnaProtocol):
    def __init__(self):
        self.user = None
        self.channel = None
        self.status = 'pending'
        self.pending_messages = []
        self.pending_messages_lock = threading.Lock()

    def requestReceived(self, request):
        processor = getattr(self, 'do_%s' % request.method, None)
        if processor is None:
            raise ProtocolError('Unknown method')
        processor(request)

    def do_authenticate(self, request):
        self.user = authenticate(request)
        self.user.channels = [joiner.channel for joiner in Joiner.channels_of(self.user.id)]
        if not self.user:
            raise ProtocolError('Authentication failed')
        self.transport.write(bson.dumps(dict(method=u'authenticate', status='OK')))

    @auth_required
    def do_create(self, request):
        def get_from_exists_channels(channels, partner_id):
            for channel in channels:
                for joiner in Joiner.users_of(channel):
                    if joiner.user_id == partner_id:
                        return channel
            raise ItemNotFoundException

        def create_channel(err, user_ids):
            channel = Joiner.create_channel(user_ids)
            return channel

        def send_channel(channel):
            self.transport.write(bson.dumps(dict(method=u'create', channel=channel)))

        d = deferToThread(get_from_exists_channels, self.user.channels, request['partner_id'])
        d.addErrback(create_channel, (self.user.id, request['partner_id']))
        d.addCallback(send_channel)

    @auth_required
    def do_unread(self, request):
        messages = []
        for channel in self.user.channels:
            messages += [
                message.to_dict()
                for message in DnaMessage.query(
                    channel__eq=channel,
                    published_at__gt=request['last_published_at']
                )
            ]

        self.transport.write(bson.dumps(dict(method=u'unread', messages=messages)))

    @auth_required
    def do_join(self, request):
        def check_is_able_to_join(channel):
            permission_to_join = False

            for joiner in Joiner.users_of(channel):
                if joiner.user_id == self.user.id:
                    permission_to_join = True
                    break
            else:
                raise ProtocolError('Channel is not exists')

            if not permission_to_join:
                raise ProtocolError('No permission to join')

        def join_channel(result, channel):
            self.channel = channel
            self.factory.channels.setdefault(channel, []).append(self)

        d = deferToThread(check_is_able_to_join, request['channel'])
        d.addCallback(join_channel, request['channel'])

    @in_channel_required
    def do_publish(self, request):
        def publish_to_client(channel, message_):
            self.factory.redis_session.publish(channel, bson.dumps(message_))

        def write_to_sqs(result, message_):
            self.factory.queue.write(QueueMessage(body=json.dumps(message_)))

        message = dict(
            message=request['message'],
            writer=self.user.id,
            published_at=time.time(),
            method=u'publish',
            channel=self.channel
        )
        d = deferToThread(publish_to_client, self.channel, message)
        d.addCallback(write_to_sqs, message)

    def connectionLost(self, reason=None):
        print reason
        if self.user and self.channel:
            self.factory.channels[self.channel].remove(self)


class ChatFactory(Factory):
    protocol = ChatProtocol
    channels = dict()

    def __init__(self, redis_host='localhost'):
        self.redis_session = redis.StrictRedis(host=redis_host)
        self.queue = sqs.connect_to_region('ap-northeast-1').get_queue(conf['NOTIFICATION_QUEUE_NAME'])
        Transmitter(self).start()

