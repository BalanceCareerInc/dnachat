# -*-coding:utf8-*-
import bson
import datetime
import json
import redis
import time

from boto import sqs
from boto.sqs.message import Message as QueueMessage
from bynamodb.exceptions import ItemNotFoundException
from twisted.internet.protocol import Factory
from twisted.internet.threads import deferToThread
from twisted.internet.error import ConnectionDone

from .decorators import auth_required
from .dna.protocol import DnaProtocol, ProtocolError
from .logger import logger
from .transmission import Transmitter
from .settings import conf
from .models import (Message as DnaMessage, Channel, ChannelJoinInfo,
                     ChannelWithdrawalLog, ChannelUsageLog)
from .api import ApiListener
from .adapter.protocol_2_to_3_adapter import Protocol2To3Adapter
from .utils import to_comparable


class BaseChatProtocol(DnaProtocol):
    def __init__(self):
        self.user = None
        self.protocol_version = None
        self.last_sent_ats = {}

    def requestReceived(self, request):
        request = self.adapt_old_versions(request)
        if not request:
            return

        processor = getattr(self, 'do_%s' % request.method, None)
        if processor is None:
            raise ProtocolError('Unknown method')
        processor(request)

    def adapt_old_versions(self, request):
        if not self.protocol_version:
            return request

        adapter = None
        if to_comparable(self.protocol_version) == to_comparable("2.0"):
            adapter = Protocol2To3Adapter(self, request)

        if adapter:
            return adapter.adapt()
        else:
            return request

    def do_authenticate(self, request):
        self.user = self.authenticate(request)
        if self.user is None:
            return
        self.user.id = str(self.user.id).decode('utf8')
        self.factory.clients_by_user_id.setdefault(self.user.id, []).append(self)

        self.user.join_infos_dict = dict(
            (join_info.channel, join_info)
            for join_info in ChannelJoinInfo.by_user(self.user.id)
        )
        self.protocol_version = request.get('protocol_version')
        for join_info in self.user.join_infos_dict.values():
            self.factory.clients_by_channel_name.setdefault(join_info.channel, []).append(self)
        self.transport.write(bson.dumps(dict(method=u'authenticate', status=u'OK')))

    @auth_required
    def do_create(self, request):
        def main():
            if 'partner_id' in request:
                d = deferToThread(get_from_exists_private_channel, self.user.join_infos_dict.keys(), request['partner_id'])
                chat_members = [self.user.id, request['partner_id']]
                d.addErrback(create_channel, chat_members, False)
            else:
                chat_members = [self.user.id] + request['partner_ids']
                d = deferToThread(create_channel, None, chat_members, True)
            d.addCallback(send_channel, [m for m in chat_members if m != self.user.id])

        def get_from_exists_private_channel(channel_names, partner_id):
            partner_id = unicode(partner_id)
            is_group_chat = dict(
                (channel.name, channel.is_group_chat)
                for channel in Channel.batch_get(*[(name,) for name in channel_names])
            )
            for join_info in [join_info for channel in channel_names
                              for join_info in ChannelJoinInfo.by_channel(channel)]:
                if join_info.user_id == partner_id and not is_group_chat[join_info.channel]:
                    return Channel.get_item(join_info.channel)
            raise ItemNotFoundException

        def create_channel(err, user_ids, is_group_chat):
            channel, join_infos = Channel.create_channel(user_ids, is_group_chat)
            my_join_info = [join_info for join_info in join_infos if join_info.user_id == self.user.id][0]
            self.user.join_infos_dict[my_join_info.channel] = my_join_info
            self.factory.clients_by_channel_name.setdefault(my_join_info.channel, []).append(self)

            other_user_ids = [join_info.user_id for join_info in join_infos if join_info.user_id != self.user.id]
            self.factory.redis_session.publish(
                'create_channel',
                bson.dumps(dict(channel=channel.name, users=other_user_ids))
            )
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
    def do_get_channels(self, request):

        def get_recent_messages(channel):
            return [
                dict(
                    message=message.message,
                    writer=message.writer,
                    type=message.type,
                    published_at=message.published_at
                )
                for message in DnaMessage.query(channel__eq=channel, scan_index_forward=False, limit=20)
            ]

        def get_partner_join_info_dicts(channel_name):
            return [
                dict(
                    user=join_info.user_id,
                    joined_at=join_info.joined_at,
                    last_read_at=join_info.last_read_at
                )
                for join_info in ChannelJoinInfo.get_partners(channel_name, self.user.id)
            ]

        channel_dicts = []
        users = set()
        for channel_name in self.get_activated_channels(self.user):
            recent_messages = get_recent_messages(channel_name)
            if not recent_messages:
                continue

            join_info = self.user.join_infos_dict[channel_name]

            join_info.last_sent_at = time.time()
            join_info.save()

            partner_join_info_dicts = get_partner_join_info_dicts(channel_name)
            channel_dicts.append(dict(
                channel=join_info.channel,
                is_group_chat=False,
                unread_count=DnaMessage.query(
                    channel__eq=join_info.channel,
                    published_at__gt=join_info.last_read_at
                ).count(),
                recent_messages=recent_messages,
                join_infos=partner_join_info_dicts,
            ))
            users = users.union([partner_join_info_dict['user'] for partner_join_info_dict in partner_join_info_dicts])

        response = dict(
            method=u'get_channels',
            users=list(users),
            channels=channel_dicts
        )
        self.transport.write(bson.dumps(response))

    @auth_required
    def do_unread(self, request):
        def main():
            join_infos = self.user.join_infos_dict.values()
            if 'channel' in request:
                join_infos = [
                    join_info
                    for join_info in join_infos
                    if join_info.channel == request['channel']
                ]
                if not join_infos:
                    raise ProtocolError('Not a valid channel')
            deferToThread(send_messages, join_infos, request.get('before'), request.get('after'))

        def send_messages(join_infos, before=None, after=None):
            messages = []
            updated_join_infos = []
            for join_info in join_infos:
                if before:
                    new_messages = messages_before(join_info.channel, before)
                elif after:
                    new_messages = messages_after(join_info.channel, min(after, join_info.last_sent_at))
                else:
                    new_messages = messages_after(join_info.channel, join_info.last_sent_at)

                if new_messages:
                    updated_join_infos.append(join_info)
                    messages += new_messages

            self.transport.write(bson.dumps(dict(method=u'unread', messages=messages)))

            for join_info in updated_join_infos:
                join_info.last_sent_at = time.time()
                join_info.save()

        def messages_before(channel, before):
            return [
                message.to_dict()
                for message in DnaMessage.query(
                    channel__eq=channel,
                    published_at__lte=before,
                    scan_index_forward=False,
                    limit=100
                )
            ]

        def messages_after(channel, after):
            return [
                message.to_dict()
                for message in DnaMessage.query(
                    channel__eq=channel,
                    published_at__gt=after
                )
            ]

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
                self.transport.write(bson.dumps(dict(method=u'withdrawal', channel=channel_name)))
                return None
            return join_info

        def withdrawal(join_info):
            if not join_info:
                return
            ChannelWithdrawalLog.put_item(
                channel=join_info.channel,
                user_id=join_info.user_id,
                joined_at=join_info.joined_at,
                last_read_at=join_info.last_read_at,
            )
            join_info.delete()
            self.transport.write(bson.dumps(dict(method=u'withdrawal', channel=join_info.channel)))
            self.publish_message(u'withdrawal', join_info.channel, '', self.user.id)

        d = deferToThread(get_join_info, request['channel'])
        d.addCallback(withdrawal)

    @auth_required
    def do_get_last_read_at(self, request):
        for join_info in ChannelJoinInfo.by_channel(request['channel']):
            if join_info.user_id == self.user.id:
                self.transport.write(bson.dumps(dict(
                    method=request.method,
                    channel=join_info.channel,
                    last_read_at=join_info.partners_last_read_at
                )))
                return
        raise ProtocolError('Given channel "%s" is not a channel of user %s' % (request['channel'], self.user.id))

    @auth_required
    def do_publish(self, request):
        if request['type'] != 'text':
            raise ProtocolError('Invalid message type')
        if request['channel'] not in self.user.join_infos_dict:
            raise ProtocolError('Permission denied')

        self.publish_message(
            request['type'],
            request['channel'],
            request['message'],
            self.user.id
        )

    @auth_required
    def do_ack(self, request):
        message = dict(
            sender=self.user.id,
            published_at=request['published_at'],
            method=u'ack',
            channel=request['channel']
        )
        self.factory.redis_session.publish(request['channel'], bson.dumps(message))
        self.factory.log_queue.write(QueueMessage(body=json.dumps(message)))

    def do_ping(self, request):
        self.transport.write(bson.dumps(dict(method=u'ping', time=time.time())))

    def publish_message(self, type_, channel_name, message, writer):
        published_at = self.factory.publish_message(type_, channel_name, message, writer)
        self.last_sent_ats[channel_name] = published_at

    def connectionLost(self, reason=None):
        if reason.type is ConnectionDone:
            logger.info('Connection Done : %s' % reason)
        else:
            logger.warning('Connection Lost : %s' % reason)

        if not self.user:
            return

        for channel, published_at in self.last_sent_ats.items():
            ChannelUsageLog.put_item(
                date=datetime.datetime.fromtimestamp(published_at).strftime('%Y-%m-%d'),
                channel=channel,
                last_published_at=published_at
            )

        self.last_sent_ats = dict()
        for join_info in self.user.join_infos_dict.values():
            self.factory.clients_by_channel_name[join_info.channel].remove(self)
        self.factory.clients_by_user_id[self.user.id].remove(self)

    def authenticate(self, request):
        """
        Authenticate this connection and return a User
        :param request: dnachat.dna.request.Request object
        :return: A user object that has property "id". If failed, returns None
        """
        raise NotImplementedError

    def get_activated_channels(self, user):
        raise NotImplementedError


class ChatFactory(Factory):

    def __init__(self, redis_host='localhost'):
        self.protocol = conf['PROTOCOL']
        self.clients_by_channel_name = dict()
        self.clients_by_user_id = dict()

        self.redis_session = redis.StrictRedis(host=redis_host)
        sqs_conn = sqs.connect_to_region('ap-northeast-1')
        self.notification_queue = sqs_conn.get_queue(conf['NOTIFICATION_QUEUE_NAME'])
        self.log_queue = sqs_conn.get_queue(conf['LOG_QUEUE_NAME'])
        self.api_queue = sqs_conn.get_queue(conf['API_QUEUE_NAME'])

        Transmitter(self).start()
        if conf['API_PROCESSOR']:
            ApiListener(self, conf['API_PROCESSOR']).start()

    def publish_message(self, type_, channel_name, message, writer):
        def write_to_sqs(message_):
            queue_message = QueueMessage(body=json.dumps(message_))
            self.notification_queue.write(queue_message)
            self.log_queue.write(queue_message)

        published_at = time.time()
        message = dict(
            type=unicode(type_),
            channel=unicode(channel_name),
            message=unicode(message),
            writer=unicode(writer),
            published_at=published_at,
            method=u'publish',
        )
        self.redis_session.publish(channel_name, bson.dumps(message))
        deferToThread(write_to_sqs, message)
        return published_at
