import bson
import datetime
import time
from socket import AF_INET, SOCK_STREAM, socket
from bynamodb.exceptions import ItemNotFoundException
from dnachat.models import Channel, ChannelJoinInfo, Message, ChannelUsageLog
from pytest import fixture

import config

bson.patch_socket()


class AuthenticatedClient(object):
    def __init__(self, user):
        self.conn = socket(AF_INET, SOCK_STREAM)
        self.conn.connect(('localhost', config.PORT))
        self.conn.sendobj(dict(method='authenticate', id=user))
        assert self.conn.recvobj() == dict(method=u'authenticate', status=u'OK')

    def __enter__(self):
        return self.conn

    def __exit__(self, type, value, traceback):
        self.conn.close()


@fixture
def user1():
    return 'id1'


@fixture
def user2():
    return 'id2'


@fixture
def channel1(user1, user2):
    channels = Channel.create_channel([user1, user2])
    return channels[0].name


@fixture
def group_chat_channel1(user1):
    channel, _ = Channel.create_channel([user1], is_group_chat=True)
    return channel.name


def test_authenticate(user1):
    with AuthenticatedClient(user1) as client_sock:
        pass


def test_create(user1):
    with AuthenticatedClient(user1) as client_sock:
        client_sock.sendobj(dict(method='create', partner_id=u'id2'))
        response = client_sock.recvobj()
    assert response['method'] == 'create'
    assert 'channel' in response
    assert response['partner_id'] == 'id2'


def test_join(group_chat_channel1, user1):
    with AuthenticatedClient(user1) as client_sock:
        client_sock.sendobj(dict(method='join', channel=group_chat_channel1))
        response = client_sock.recvobj()

    assert response['method'] == 'join'
    assert response['channel'] == group_chat_channel1
    assert response['partner_ids'] == [user1]


def test_withdrawal(group_chat_channel1, user1):
    with AuthenticatedClient(user1) as client_sock:
        client_sock.sendobj(dict(method='withdrawal', channel=group_chat_channel1))
        response = client_sock.recvobj()

    assert response['method'] == 'withdrawal'
    try:
        ChannelJoinInfo.get_item(group_chat_channel1, user1)
    except ItemNotFoundException:
        pass
    else:
        assert False


def test_attend(channel1, user1):
    with AuthenticatedClient(user1) as client_sock:
        client_sock.sendobj(dict(method='attend', channel=channel1))
        response = client_sock.recvobj()

    assert response['method'] == 'attend'
    assert response['channel'] == channel1
    assert time.time() - response['last_read'] < 1.0


def test_publish(channel1, user1, user2):
    with AuthenticatedClient(user1) as sock1:
        sock1.sendobj(dict(method='attend', channel=channel1))
        sock1.recvobj()

        with AuthenticatedClient(user2) as sock2:
            sock2.sendobj(dict(method='attend', channel=channel1))
            sock2.recvobj()

            sock1.sendobj(dict(method='publish', message='Hi!', type='text'))
            response1 = sock1.recvobj()
            response2 = sock2.recvobj()

    assert response1 == response2
    assert response1['method'] == 'publish'
    assert response1['writer'] == user1
    assert response1['message'] == 'Hi!'

    time.sleep(0.1)
    channel_usage_log = ChannelUsageLog.get_item(
        datetime.datetime.fromtimestamp(response1['published_at']).strftime('%Y-%m-%d'),
        response1['channel'])
    assert channel_usage_log.last_published_at == response1['published_at']


def test_unread(channel1, user1, user2):
    Message.put_item(
        channel=channel1,
        type=u'text',
        message=u'Text Message',
        writer=user1,
        published_at=time.time()
    )

    with AuthenticatedClient(user2) as client_sock:
        client_sock.sendobj(dict(method='unread', channel=channel1))
        response = client_sock.recvobj()
        assert response['method'] == u'unread'
        new_messages = response['messages']
        assert len(new_messages) == 1
        assert new_messages[0]['message'] == u'Text Message'
