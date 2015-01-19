# -*-coding: utf8-*-
__all__ = 'conf', 'func_from_package_name'


class Settings(object):
    def __init__(self):
        self.config = dict(
            NOTIFICATION_QUEUE_NAME='NotificationQueue'
        )
        self.must_have_items = (
            'AUTHENTICATOR',  # receives request, returns object that has attr 'channel', 'id'
            'SUBSCRIBERS_RESOLVER'  # receives channel, returns dict iterable that has key 'id', 'endpoint_arn'
        )

    def __getitem__(self, key):
        return self.config[key]

    def get(self, key, default=None):
        return self.config.get(key, default)

    def load_from_file(self, config_file):
        with open(config_file, 'r') as f:
            data = f.read()
        config = dict()
        default = dict()
        exec '' in default
        exec data in config
        config = dict((k, v) for k, v in config.iteritems() if k not in default)
        for key in self.must_have_items:
            if key not in config:
                raise ValueError('Config "%s" is not found' % key)
        self.config.update(config)


def func_from_package_name(package_name):
    names = package_name.split('.')
    module = __import__('.'.join(names[:-1]))
    func = module
    for name in names[1:]:
        func = getattr(func, name)
    return func

conf = Settings()