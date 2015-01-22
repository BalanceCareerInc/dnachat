from bynamodb import init_bynamodb
from twisted.internet import reactor

from settings import conf


def run_dnachat(config_file='localconfig.py'):
    conf.load_from_file(config_file)
    init_bynamodb(conf)
    from dnachat.server import ChatFactory
    reactor.listenTCP(conf.get('PORT', 9339), ChatFactory(conf['REDIS_HOST']))
    reactor.run()


def run_logger(config_file='localconfig.py'):
    conf.load_from_file(config_file)
    init_bynamodb(conf)
    from dnachat.logserver import ChatLogger
    ChatLogger(conf['REDIS_HOST']).start()


def run_notisender(config_file='localconfig.py'):
    conf.load_from_file(config_file)
    init_bynamodb(conf)
    from dnachat.notiserver import NotificationSender
    NotificationSender().start()
