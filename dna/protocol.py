# -*-coding:utf8-*-
from twisted.internet.protocol import Protocol

from .request import Request
from .exceptions import ProtocolError


class DnaProtocol(Protocol):
    def dataReceived(self, raw_bson):
        try:
            self.requestReceived(Request.from_bson(raw_bson))
        except ProtocolError, e:
            print e
            self.transport.loseConnection()
