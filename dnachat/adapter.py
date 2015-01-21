# -*-coding:utf8-*-

__all__ = 'authenticate', 'get_user_by_id'


def authenticate(request):
    """
    Authenticate this connection and return a User
    :param request: dnachat.dna.request.Request object
    :return: A user object that has property "id". If failed, returns None
    """
    raise NotImplementedError


def get_user_by_id(user_id):
    """
    Return a user by user_id
    :param user_id: id of user
    :return: A user object
    """
    raise NotImplementedError
