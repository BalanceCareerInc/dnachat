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
