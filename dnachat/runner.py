from bynamodb import patch_dynamodb_connection
from twisted.internet import reactor

from dnachat.server import ChatFactory
from settings import conf


def run_dnachat(config_file='localconfig.py'):
    conf.load_from_file(config_file)
    patch_dynamodb_connection(
        host=conf['DYNAMODB_HOST'],
        port=conf['DYNAMODB_PORT'],
        is_secure=conf['DYNAMODB_IS_SECURE']
    )
    reactor.listenTCP(conf.get('PORT', 9339), ChatFactory(conf['REDIS_HOST']))
    reactor.run()
