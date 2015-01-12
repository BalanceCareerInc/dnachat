# -*-coding:utf8-*-
from functools import wraps

from .dna.exceptions import ProtocolError


def must_be_in_channel(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.user and self.user.channel is None:
            raise ProtocolError
        return func(self, *args, **kwargs)
    return wrapper