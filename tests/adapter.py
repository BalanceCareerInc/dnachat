# -*-coding:utf8-*-


class User(object):
    def __init__(self, id):
        self.id = id


def authenticate(request):
    return User(request['id'])


def user_by_id(id):
    return User('id')