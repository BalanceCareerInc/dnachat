import json
import bson
import datetime
import time
from socket import AF_INET, SOCK_STREAM, socket, SHUT_RDWR

from boto import sqs
from boto.sqs.message import Message as QueueMessage
from bynamodb.exceptions import ItemNotFoundException
from pytest import fixture

from dnachat.models import Channel, ChannelJoinInfo, Message, ChannelUsageLog
from dnachat.settings import conf
from tests.config import TestChatProtocol

bson.patch_socket()


class AuthenticatedClient(object):
    def __init__(self, user, version=u'3.0'):
        self.user = user
        self.version = version
        self.conn = socket(AF_INET, SOCK_STREAM)

    def __enter__(self):
        self.conn.connect(('localhost', conf['PORT']))
        self.conn.sendobj(dict(method='authenticate', id=self.user, protocol_version=self.version))
        assert self.conn.recvobj() == dict(method=u'authenticate', status=u'OK')
        return self.conn

    def __exit__(self, type, value, traceback):
        time.sleep(0.1)
        self.conn.shutdown(SHUT_RDWR)
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


@fixture
def group_chat_channel2(user1, user2):
    channel, _ = Channel.create_channel([user1, user2], is_group_chat=True)
    return channel.name


def test_authenticate(user1):
    with AuthenticatedClient(user1) as client_sock:
        pass


def test_create(user1, user2):
    with AuthenticatedClient(user1) as client_sock:
        client_sock.sendobj(dict(method='create', partner_id=user2))
        response = client_sock.recvobj()
    assert response['method'] == 'create'
    assert 'channel' in response
    assert response['partner_id'] == 'id2'


def test_publish_with_create(user1, user2):
    with AuthenticatedClient(user1) as sock1:
        with AuthenticatedClient(user2) as sock2:
            sock1.sendobj(dict(method='create', partner_id=user2))
            response = sock1.recvobj()

            sock1.sendobj(dict(method='publish', message='Hi!', type='text', channel=response['channel']))
            response1 = sock1.recvobj()
            response2 = sock2.recvobj()

            assert response1['method'] == 'publish'
            assert response2['method'] == 'publish'

def test_create_gets_private_channel(user1, user2, group_chat_channel2):
    with AuthenticatedClient(user1) as client_sock:
        client_sock.sendobj(dict(method='create', partner_id=user2))
        response = client_sock.recvobj()
    assert response['method'] == 'create'
    assert 'channel' in response
    assert response['channel'] != group_chat_channel2
    assert response['partner_id'] == 'id2'

    exist_channel_name = response['channel']
    with AuthenticatedClient(user1) as client_sock:
        client_sock.sendobj(dict(method='create', partner_id=user2))
        response = client_sock.recvobj()
    assert response['channel'] == exist_channel_name


def test_join(group_chat_channel1, user1):
    with AuthenticatedClient(user1) as client_sock:
        client_sock.sendobj(dict(method='join', channel=group_chat_channel1))
        client_sock.recvobj()
        response = client_sock.recvobj()

    assert response['method'] == 'join'
    assert response['channel'] == group_chat_channel1
    assert response['partner_ids'] == [user1]


def test_withdrawal(group_chat_channel2, user1, user2):
    with AuthenticatedClient(user1) as user1_sock:
        with AuthenticatedClient(user2) as user2_sock:
            user1_sock.sendobj(dict(method='withdrawal', channel=group_chat_channel2))
            response = user1_sock.recvobj()
            assert response['method'] == 'withdrawal'

            response = user2_sock.recvobj()
            assert response['method'] == 'publish'
            assert response['type'] == 'withdrawal'

    try:
        ChannelJoinInfo.get_item(group_chat_channel2, user1)
    except ItemNotFoundException:
        pass
    else:
        assert False


def test_publish_version_3(channel1, user1, user2):
    with AuthenticatedClient(user1) as sock1:
        with AuthenticatedClient(user2) as sock2:
            sock1.sendobj(dict(method='publish', message='Hi!', type='text', channel=channel1))
            response1 = sock1.recvobj()
            response2 = sock2.recvobj()

    assert response1 == response2
    assert response1['method'] == 'publish'
    assert response1['writer'] == user1
    assert response1['message'] == 'Hi!'

    time.sleep(0.3)
    channel_usage_log = ChannelUsageLog.get_item(
        datetime.datetime.fromtimestamp(response1['published_at']).strftime('%Y-%m-%d'),
        response1['channel']
    )
    assert channel_usage_log.last_published_at == response1['published_at']


def test_publish_version_2(channel1, user1, user2):
    with AuthenticatedClient(user1, version=u'2.0') as sock1:
        sock1.sendobj(dict(method='attend', channel=channel1))
        sock1.recvobj()
        with AuthenticatedClient(user2, version=u'2.0') as sock2:
            sock2.sendobj(dict(method='attend', channel=channel1))
            sock2.recvobj()

            sock1.sendobj(dict(method='publish', message='Hi!', type='text', channel=channel1))
            response1 = sock1.recvobj()
            response2 = sock2.recvobj()

    assert response1 == response2
    assert response1['method'] == 'publish'
    assert response1['writer'] == user1
    assert response1['message'] == 'Hi!'

    time.sleep(0.3)
    channel_usage_log = ChannelUsageLog.get_item(
        datetime.datetime.fromtimestamp(response1['published_at']).strftime('%Y-%m-%d'),
        response1['channel']
    )
    assert channel_usage_log.last_published_at == response1['published_at']


def test_unread_specific_channel(channel1, user1, user2):
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


def test_unread_all_channel(channel1, user1, user2):
    Message.put_item(
        channel=channel1,
        type=u'text',
        message=u'Text Message',
        writer=user1,
        published_at=time.time()
    )

    with AuthenticatedClient(user2) as client_sock:
        client_sock.sendobj(dict(method='unread'))
        response = client_sock.recvobj()
        assert response['method'] == u'unread'
        new_messages = response['messages']
        assert len(new_messages) == 1
        assert new_messages[0]['channel'] == channel1
        assert new_messages[0]['message'] == u'Text Message'


def test_unread_before(channel1, user1, user2):
    Message.put_item(
        channel=channel1,
        type=u'text',
        message=u'Text Message',
        writer=user1,
        published_at=time.time()
    )

    with AuthenticatedClient(user2) as client_sock:
        client_sock.sendobj(dict(method='unread', before=time.time()))
        response = client_sock.recvobj()
        assert response['method'] == u'unread'
        new_messages = response['messages']
        assert len(new_messages) == 1
        assert new_messages[0]['channel'] == channel1
        assert new_messages[0]['message'] == u'Text Message'

        client_sock.sendobj(dict(method='unread', before=time.time() - 999))
        response = client_sock.recvobj()
        assert response['method'] == u'unread'
        new_messages = response['messages']
        assert len(new_messages) == 0


def test_get_channels(user1, channel1):
    class MockUser(object):
        def __init__(self, user_id):
            self.id = user_id

    Message.put_item(**dict(channel=channel1, published_at=time.time(), type="text", writer=user1, message="hi"))

    with AuthenticatedClient(user1) as client_sock:
        client_sock.sendobj(dict(method='get_channels'))
        response = client_sock.recvobj()
        assert response['method'] == u'get_channels'
        assert 'users' in response
        assert 'channels' in response
        channel_names = [channel['channel'] for channel in response['channels']]
        assert channel1 in channel_names

        activated_channels = TestChatProtocol.get_activated_channels(MockUser(user1))
        assert set(activated_channels) == set(channel_names)


def test_get_channels_unread_count(user1, user2, channel1):
    with AuthenticatedClient(user1) as sock1:
        sock1.sendobj(dict(method='publish', message='UnreadCountTest', type='text', channel=channel1))
        for x in xrange(10):
            time.sleep(conf['QUEUE_POLLING_INTERVAL'])
            count = Message.query(channel__eq=channel1).count()
            if count > 0:
                break
        else:
            raise ValueError('Logger seems not work')
        with AuthenticatedClient(user2) as sock2:
            sock2.sendobj(dict(method='get_channels'))
            response = sock2.recvobj()
            assert response['method'] == u'get_channels'
            channel = [channel for channel in response['channels'] if channel['channel'] == channel1][0]
            assert channel['unread_count'] == 1


def test_attend_version_2(channel1, user1):
    with AuthenticatedClient(user1, version=u'2.0') as client_sock:
        client_sock.sendobj(dict(method='attend', channel=channel1))
        response = client_sock.recvobj()

    assert response['method'] == 'attend'
    assert response['channel'] == channel1
    assert time.time() - response['last_read'] < 1.0


def test_api_consume():
    conn = sqs.connect_to_region('ap-northeast-1')
    queue = conn.get_queue(conf['API_QUEUE_NAME'])
    queue.write(QueueMessage(body=json.dumps(dict(method='test'))))
    time.sleep(conf['QUEUE_POLLING_INTERVAL'])
    assert queue.count() == 0
