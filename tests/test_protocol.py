import bson
from socket import AF_INET, SOCK_STREAM, socket
from dnachat.models import Channel
from pytest import fixture

import config

bson.patch_socket()


@fixture
def user1():
    return 'id1'


@fixture
def user2():
    return 'id2'


@fixture
def channel1(user1, user2):
    print 1
    channels = Channel.create_channel([user1, user2])
    return channels[0].name


def test_authenticate():
    client_sock = socket(AF_INET, SOCK_STREAM)
    client_sock.connect(('localhost', config.PORT))
    client_sock.sendobj(dict(method='authenticate', id='id'))
    response = client_sock.recvobj()
    client_sock.close()
    assert response['method'] == u'authenticate'
    assert response['status'] == u'OK'


def test_create():
    client_sock = socket(AF_INET, SOCK_STREAM)
    client_sock.connect(('localhost', config.PORT))
    client_sock.sendobj(dict(method='authenticate', id='id1'))
    client_sock.recvobj()

    client_sock.sendobj(dict(method='create', partner_id=u'id2'))
    response = client_sock.recvobj()
    client_sock.close()

    assert response['method'] == 'create'
    assert 'channel' in response
    assert response['partner_id'] == 'id2'


def test_join(channel1, user1):
    client_sock = socket(AF_INET, SOCK_STREAM)
    client_sock.connect(('localhost', config.PORT))
    client_sock.sendobj(dict(method='authenticate', id=user1))
    client_sock.recvobj()

    client_sock.sendobj(dict(method='join', channel=channel1))
    response = client_sock.recvobj()
    assert response['method'] == 'join'
    assert response['channel'] == channel1
    assert response['last_read'] == 0.0
    client_sock.close()


def test_publish(channel1, user1, user2):
    sock1 = socket(AF_INET, SOCK_STREAM)
    sock1.connect(('localhost', config.PORT))
    sock1.sendobj(dict(method='authenticate', id=user1))
    sock1.recvobj()
    sock1.sendobj(dict(method='join', channel=channel1))
    sock1.recvobj()

    sock2 = socket(AF_INET, SOCK_STREAM)
    sock2.connect(('localhost', config.PORT))
    sock2.sendobj(dict(method='authenticate', id=user2))
    sock2.recvobj()
    sock2.sendobj(dict(method='join', channel=channel1))
    sock2.recvobj()

    sock1.sendobj(dict(method='publish', message='Hi!', type='text'))
    response1 = sock1.recvobj()
    response2 = sock2.recvobj()

    assert response1 == response2
    assert response1['method'] == 'publish'
    assert response1['writer'] == user1
    assert response1['message'] == 'Hi!'

    sock1.close()
    sock2.close()

