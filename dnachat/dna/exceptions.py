# -*-coding:utf8-*-
class ProtocolError(Exception):
    def __init__(self, message='', errors=None):
        super(ProtocolError, self).__init__(message)
        self.errors = errors
