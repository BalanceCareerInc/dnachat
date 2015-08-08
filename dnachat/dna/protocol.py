# -*-coding:utf8-*-
import struct
from twisted.internet.protocol import Protocol

from .request import Request
from .exceptions import ProtocolError
from ..logger import logger


class DnaProtocol(Protocol):
    def dataReceived(self, raw_packet):
        packet = raw_packet
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
                user_info = ''
                if self.user:
                    user_info = '({0}) - '.format(self.user.id)
                logger.error('ProtocolError: {0}, {1}0x{2}'.format(str(e), user_info, raw_packet.encode('hex')),
                             exc_info=True)
                self.transport.loseConnection()
            if packet == '':
                break
