# -*-coding:utf8-*-

import bson
import json
import redis
import threading
import time

from boto import sqs
from boto.sqs.message import Message as QueueMessage
from bynamodb.exceptions import ItemNotFoundException
from twisted.internet.protocol import Factory
from twisted.internet.threads import deferToThread

from .decorators import in_channel_required, auth_required
from .dna.protocol import DnaProtocol, ProtocolError
from .logger import logger
from .transmission import Transmitter
from .settings import conf
from .models import Message as DnaMessage, Channel


class BaseChatProtocol(DnaProtocol):
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
        self.user = self.authenticate(request)
        self.user.id = str(self.user.id).decode('utf8')
        self.user.channels = list(Channel.channels_of(self.user.id))
        if not self.user:
            raise ProtocolError('Authentication failed')
        self.transport.write(bson.dumps(dict(method=u'authenticate', status=u'OK')))

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
            if type(channel) is str:
                channel = unicode(channel)
            if type(request['partner_id']) is str:
                request['partner_id'] = unicode(request['partner_id'])

            self.transport.write(bson.dumps(dict(
                method=u'create',
                channel=channel,
                partner_id=request['partner_id']
            )))

        d = deferToThread(get_from_exists_channels, self.user.channels, request['partner_id'])
        d.addErrback(create_channel, (self.user.id, request['partner_id']))
        d.addCallback(send_channel)

    @auth_required
    def do_ack(self, request):
        def publish_to_client(channel_name_, message_):
            self.factory.redis_session.publish(channel_name_, bson.dumps(message_))

        def refresh_last_read_at(channel_name, published_at):
            for channel in self.user.channels:
                if channel.name != channel_name:
                    continue
                channel.last_read_at = published_at
                channel.save()
                break

        message = dict(
            sender=self.user.id,
            published_at=request['published_at'],
            method=u'ack',
            channel=request['channel']
        )
        deferToThread(publish_to_client, request['channel'], message)
        deferToThread(refresh_last_read_at, request['channel'], request['published_at'])

    @auth_required
    def do_unread(self, request):
        def save_last_read_at(channel_):
            channel_.last_sent_at = time.time()
            channel_.save()

        messages = []
        channels = self.user.channels
        if 'channel' in request:
            channels = [
                channel
                for channel in self.user.channels
                if channel.name == request['channel']
            ]
            if not channels:
                raise ProtocolError('Not a valid channel')
        for channel in channels:
            new_messages = [
                message.to_dict()
                for message in DnaMessage.query(
                    channel__eq=channel.name,
                    published_at__gt=channel.last_sent_at
                )
            ]
            if new_messages:
                deferToThread(save_last_read_at, channel)
                messages += new_messages

        self.transport.write(bson.dumps(dict(method=u'unread', messages=messages)))

    @auth_required
    def do_join(self, request):
        def check_is_able_to_join(channel):
            for joiner in Channel.users_of(channel):
                if joiner.user_id == self.user.id:
                    break
            else:
                raise ProtocolError('Channel is not exists')

        def join_channel(result, channel_name):
            self.channel = [channel for channel in self.user.channels
                            if channel.name == channel_name][0]
            clients = [
                client
                for client in self.factory.channels.get(channel_name, [])
                if client.user.id != self.user.id
            ] + [self]
            self.factory.channels[channel_name] = clients

        def send_last_read(result, channel):
            for joiner in Channel.users_of(channel):
                if joiner.user_id != self.user.id:
                    self.transport.write(bson.dumps(dict(method=u'join', channel=joiner.name, last_read=joiner.last_read_at)))

        d = deferToThread(check_is_able_to_join, request['channel'])
        d.addCallback(join_channel, request['channel'])
        d.addCallback(send_last_read, request['channel'])

    @in_channel_required
    def do_exit(self, request):
        self.exit_channel()

    @in_channel_required
    def do_publish(self, request):
        def refresh_last_read_at(published_at):
            self.channel.last_read_at = published_at
            self.channel.save()

        def publish_to_client(result, channel_name, message_):
            self.factory.redis_session.publish(channel_name, bson.dumps(message_))

        def write_to_sqs(result, message_):
            self.factory.queue.write(QueueMessage(body=json.dumps(message_)))

        self.ensure_valid_message(request)

        message = dict(
            type=request['type'],
            message=request['message'],
            writer=self.user.id,
            published_at=time.time(),
            method=u'publish',
            channel=unicode(self.channel.name)
        )
        d = deferToThread(refresh_last_read_at, message['published_at'])
        d.addCallback(publish_to_client, self.channel.name, message)
        d.addCallback(write_to_sqs, message)

    def exit_channel(self):
        if not self.user:
            return
        if not self.channel:
            return

        self.channel.save()
        self.factory.channels[self.channel.name].remove(self)
        self.channel = None

    def connectionLost(self, reason=None):
        logger.info('Connection Lost : %s' % reason)
        self.exit_channel()

    def authenticate(self, request):
        """
        Authenticate this connection and return a User
        :param request: dnachat.dna.request.Request object
        :return: A user object that has property "id". If failed, returns None
        """
        raise NotImplementedError

    def ensure_valid_message(self, request):
        if not request['message'].strip():
            raise ProtocolError('Blank message is not accepted')

    @staticmethod
    def get_user_by_id(user_id):
        """
        Return a user by user_id
        :param user_id: id of user
        :return: A user object
        """
        raise NotImplementedError


class ChatFactory(Factory):

    def __init__(self, redis_host='localhost'):
        self.protocol = conf['PROTOCOL']
        self.channels = dict()
        self.redis_session = redis.StrictRedis(host=redis_host)
        self.queue = sqs.connect_to_region('ap-northeast-1').get_queue(conf['NOTIFICATION_QUEUE_NAME'])
        Transmitter(self).start()

