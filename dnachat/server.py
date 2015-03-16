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
from .models import Message as DnaMessage, Channel, ChannelJoinInfo, ChannelWithdrawalLog


class BaseChatProtocol(DnaProtocol):
    def __init__(self):
        self.user = None
        self.protocol_version = None
        self.attended_channel_join_info = None
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
        self.user.join_infos = list(ChannelJoinInfo.by_user(self.user.id))
        if not self.user:
            raise ProtocolError('Authentication failed')
        self.protocol_version = request.get('protocol_version')
        self.transport.write(bson.dumps(dict(method=u'authenticate', status=u'OK')))

    @auth_required
    def do_create(self, request):
        def main():
            if 'partner_id' in request:
                channel_names = [join_info.channel for join_info in self.user.join_infos]
                d = deferToThread(get_from_exists_channels, channel_names, request['partner_id'])
                chat_members = [self.user.id, request['partner_id']]
                d.addErrback(create_channel, chat_members, False)
            else:
                chat_members = [self.user.id] + request['partner_ids']
                d = deferToThread(create_channel, None, chat_members, True)
            d.addCallback(send_channel, [m for m in chat_members if m != self.user.id])

        def get_from_exists_channels(channel_names, partner_id):
            for join_info in [join_info for channel in channel_names
                              for join_info in ChannelJoinInfo.by_channel(channel)]:
                if join_info.user_id == partner_id:
                    return Channel.get_item(join_info.channel)
            raise ItemNotFoundException

        def create_channel(err, user_ids, is_group_chat):
            channel, join_infos = Channel.create_channel(user_ids, is_group_chat)
            my_join_info = [join_info for join_info in join_infos if join_info.user_id == self.user.id][0]
            self.user.join_infos.append(my_join_info)
            return channel

        def send_channel(channel, partner_ids):
            response = dict(method=u'create', channel=unicode(channel.name))
            if channel.is_group_chat:
                response['partner_ids'] = partner_ids
            else:
                response['partner_id'] = partner_ids[0]

            self.transport.write(bson.dumps(response))

        main()

    @auth_required
    def do_ack(self, request):
        def publish_to_client(channel_name_, message_):
            self.factory.redis_session.publish(channel_name_, bson.dumps(message_))

        def refresh_last_read_at(channel_name, published_at):
            if self.attended_channel_join_info.channel == channel_name:
                self.attended_channel_join_info.last_read_at = published_at

            for join_info in self.user.join_infos:
                if join_info.channel != channel_name:
                    continue
                join_info.last_read_at = published_at
                join_info.save()
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
        def main():
            messages = []
            join_infos = self.user.join_infos
            if 'channel' in request:
                join_infos = [
                    join_info
                    for join_info in self.user.join_infos
                    if join_info.channel == request['channel']
                ]
                if not join_infos:
                    raise ProtocolError('Not a valid channel')

            for join_info in join_infos:
                new_messages = [
                    message.to_dict()
                    for message in DnaMessage.query(
                        channel__eq=join_info.channel,
                        published_at__gt=join_info.last_sent_at
                    )
                ]
                if new_messages:
                    deferToThread(update_last_sent_at, join_info)
                    messages += new_messages

            self.transport.write(bson.dumps(dict(method=u'unread', messages=messages)))

        def update_last_sent_at(join_info):
            join_info.last_sent_at = time.time()
            join_info.save()

        main()

    @auth_required
    def do_join(self, request):
        try:
            channel = Channel.get_item(request['channel'])
        except ItemNotFoundException:
            raise ProtocolError('Not exist channel: "%s"' % request['channel'])
        if not channel.is_group_chat:
            raise ProtocolError('Not a group chat: "%s"' % request['channel'])
        partner_ids = [join_info.user_id for join_info in ChannelJoinInfo.by_channel(channel.name)]
        self.publish_message('join', channel.name, '', self.user.id)
        ChannelJoinInfo.put_item(
            channel=channel.name,
            user_id=self.user.id,
        )
        self.transport.write(bson.dumps(dict(
            method='join',
            channel=channel.name,
            partner_ids=partner_ids
        )))

    @auth_required
    def do_withdrawal(self, request):
        def get_join_info(channel_name):
            try:
                channel = Channel.get_item(channel_name)
            except ItemNotFoundException:
                raise ProtocolError('Not exist channel: "%s"' % channel_name)
            if not channel.is_group_chat:
                raise ProtocolError('Not a group chat: "%s"' % channel_name)
            try:
                join_info = ChannelJoinInfo.get_item(channel.name, self.user.id)
            except ItemNotFoundException:
                raise ProtocolError('Not a member of channel: "%s"' % channel.name)
            return join_info

        def withdrawal(join_info):
            ChannelWithdrawalLog.put_item(
                channel=join_info.channel,
                user_id=join_info.user_id,
                joined_at=join_info.joined_at,
                last_read_at=join_info.last_read_at,
            )
            join_info.delete()
            self.transport.write(bson.dumps(dict(method='withdrawal', channel=join_info.channel)))

        d = deferToThread(get_join_info, request['channel'])
        d.addCallback(withdrawal)




    @auth_required
    def do_attend(self, request):
        def check_is_able_to_attend(channel_name):
            for join_info in ChannelJoinInfo.by_channel(channel_name):
                if join_info.user_id == self.user.id:
                    return join_info
            else:
                raise ProtocolError('Channel is not exists')

        def attend_channel(join_info):
            self.attended_channel_join_info = join_info
            clients = [
                client
                for client in self.factory.channels.get(join_info.channel, [])
                if client.user.id != self.user.id
            ] + [self]
            self.factory.channels[join_info.channel] = clients

            others_join_infos = [
                channel_ for channel_ in ChannelJoinInfo.by_channel(join_info.channel)
                if channel_.user_id != self.user.id
            ]

            response = dict(method=request.method, channel=join_info.channel)
            if Channel.get_item(self.attended_channel_join_info.channel).is_group_chat:
                response['last_read'] = dict(
                    (join_info.user_id, join_info.last_read_at)
                    for join_info in others_join_infos
                )
            else:
                response['last_read'] = others_join_infos[0].last_read_at

            self.transport.write(bson.dumps(response))

        d = deferToThread(check_is_able_to_attend, request['channel'])
        d.addCallback(attend_channel)

    @in_channel_required
    def do_exit(self, request):
        self.exit_channel()

    @in_channel_required
    def do_publish(self, request):
        self.ensure_valid_message(request)
        self.publish_message(request['type'], self.attended_channel_join_info.channel, request['message'], self.user.id)

    def publish_message(self, type_, channel_name, message, writer):

        def refresh_last_read_at(published_at):
            self.attended_channel_join_info.last_read_at = published_at
            self.attended_channel_join_info.save()

        def publish_to_client(result, channel_name, message_):
            self.factory.redis_session.publish(channel_name, bson.dumps(message_))

        def write_to_sqs(result, message_):
            self.factory.queue.write(QueueMessage(body=json.dumps(message_)))

        message = dict(
            type=unicode(type_),
            channel=unicode(channel_name),
            message=unicode(message),
            writer=writer,
            published_at=time.time(),
            method=u'publish',
        )
        d = deferToThread(refresh_last_read_at, message['published_at'])
        d.addCallback(publish_to_client, channel_name, message)
        d.addCallback(write_to_sqs, message)

    def exit_channel(self):
        if not self.user:
            return
        if not self.attended_channel_join_info:
            return

        self.attended_channel_join_info.save()
        self.factory.channels[self.attended_channel_join_info.channel].remove(self)
        self.attended_channel_join_info = None

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

