# -*-coding:utf8-*-

import bson
import json
from bynamodb.exceptions import ItemNotFoundException
import redis
import threading
import time

from boto import sqs
from boto.sqs.message import Message as QueueMessage
from twisted.internet.protocol import Factory
from twisted.internet.threads import deferToThread

from .decorators import in_channel_required, auth_required
from .dna.protocol import DnaProtocol, ProtocolError
from .adapter import authenticate
from .transmission import Transmitter
from .settings import conf
from .models import Message as DnaMessage, Channel


class ChatProtocol(DnaProtocol):
    def __init__(self):
        self.user = None
        self.channel = None
        self.status = 'pending'
        self.last_read_at = dict()
        self.pending_messages = []
        self.pending_messages_lock = threading.Lock()

    def requestReceived(self, request):
        processor = getattr(self, 'do_%s' % request.method, None)
        if processor is None:
            raise ProtocolError('Unknown method')
        processor(request)

    def do_authenticate(self, request):
        self.user = authenticate(request)
        self.user.channels = list(Channel.channels_of(self.user.id))
        if not self.user:
            raise ProtocolError('Authentication failed')
        self.transport.write(bson.dumps(dict(method=u'authenticate', status='OK')))

    @auth_required
    def do_create(self, request):
        def get_from_exists_channels(channels, partner_id):
            for channel in channels:
                for partner_channel in Channel.users_of(channel.name):
                    if partner_channel.user_id == partner_id:
                        return partner_channel.name
            raise ItemNotFoundException

        def create_channel(err, user_ids):
            channels = Channel.create_channel(user_ids)
            my_channel = [channel for channel in channels if channel.user_id == self.user.id][0]
            self.user.channels.append(my_channel)
            return my_channel.name

        def send_channel(channel):
            self.transport.write(bson.dumps(dict(
                method=u'create',
                channel=channel,
                partner_id=request['partner_id']
            )))

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
                    channel__eq=channel.name,
                    published_at__gt=channel.last_read_at
                )
            ]

        self.transport.write(bson.dumps(dict(method=u'unread', messages=messages)))

    @auth_required
    def do_join(self, request):
        def check_is_able_to_join(channel):
            for joiner in Channel.users_of(channel):
                if joiner.user_id == self.user.id:
                    break
            else:
                raise ProtocolError('Channel is not exists')

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
        if self.user:
            if self.channel:
                self.factory.channels[self.channel].remove(self)
            for channel in self.user.channels:
                if channel.name in self.last_read_at:
                    channel.last_read_at = self.last_read_at.pop(channel.name)['last_read_at']
                    channel.save()


class ChatFactory(Factory):
    protocol = ChatProtocol
    channels = dict()

    def __init__(self, redis_host='localhost'):
        self.redis_session = redis.StrictRedis(host=redis_host)
        self.queue = sqs.connect_to_region('ap-northeast-1').get_queue(conf['NOTIFICATION_QUEUE_NAME'])
        Transmitter(self).start()

