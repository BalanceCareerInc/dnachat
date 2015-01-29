# -*-coding:utf8-*-
from twisted.internet.protocol import Protocol

from .request import Request
from .exceptions import ProtocolError
from ..logger import logger


class DnaProtocol(Protocol):
    def dataReceived(self, raw_bson):
        try:
            request = Request.from_bson(raw_bson)
            logger.info('"%s" Received' % request.method)
            self.requestReceived(request)
        except ProtocolError, e:
            logger.error('ProtocolError: %s' % str(e), exc_info=True)
            self.transport.loseConnection()
