import bson
from socket import AF_INET, SOCK_STREAM, socket

import config

bson.patch_socket()


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

