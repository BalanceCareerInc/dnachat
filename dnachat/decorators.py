# -*-coding:utf8-*-
from functools import wraps

from dnachat.dna.exceptions import ProtocolError


def auth_required(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.user is None:
            raise ProtocolError('Not authenticated')
        return func(self, *args, **kwargs)
    return wrapper
