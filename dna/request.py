# -*-coding:utf8-*-
import bson
from .exceptions import ProtocolError


class Request(object):
    def __init__(self, method, data):
        self.method = method
        self._data = data

    def __getitem__(self, key):
        try:
            return self._data[key]
        except KeyError:
            raise ProtocolError

    def get(self, key, default=None):
        if key in self._data:
            return key
        return default

    @classmethod
    def from_bson(cls, raw_bson):
        try:
            data = bson.loads(raw_bson)
        except Exception:
            raise ProtocolError('Error on parsing bson')

        method = data.pop('method', None)
        if method is None:
            raise ProtocolError('Method is omitted')

        return Request(method, data)
