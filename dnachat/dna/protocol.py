# -*-coding:utf8-*-
import struct
from twisted.internet.protocol import Protocol

from .request import Request
from .exceptions import ProtocolError
from ..logger import logger


class DnaProtocol(Protocol):
    def dataReceived(self, packet):
        while True:
            try:
                size = struct.unpack('<i', packet[:4])[0]
            except struct.error:
                self.transport.loseConnection()
            raw_bson = packet[:size]
            packet = packet[size:]
            try:
                request = Request.from_bson(raw_bson)
                logger.info('"%s" Received' % request.method)
                self.requestReceived(request)
            except ProtocolError, e:
                logger.error('ProtocolError: %s' % str(e), exc_info=True)
                self.transport.loseConnection()
            if packet == '':
                break
