# -*-coding: utf8-*-
import logging
import logger

__all__ = 'conf'


class Settings(object):
    def __init__(self):
        self.config = dict(
            LOG_QUEUE_NAME='LogQueue',
            NOTIFICATION_QUEUE_NAME='NotificationQueue',
            LOG_LEVEL=logging.INFO,
            CHAT_LOG_FILE_NAME='chat.log',
            LOGGER_LOG_FILE_NAME='logger.log',
            NOTISENDER_LOG_FILE_NAME='notisender.log',
        )
        self.must_have_items = ('PROTOCOL',)

    def values(self):
        return self.config.values()

    def keys(self):
        return self.config.keys()

    def __contains__(self, item):
        return item in self.config

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
        self.patch_all()

    def patch_all(self):
        if 'CHANNEL_MODEL' in self.config:
            import models
            models.Channel = self._func_from_package_name(self.config['CHANNEL_MODEL'])
            models.skip_create = True

    def update(self, dict_):
        self.config.update(dict_)

    def _func_from_package_name(self, package_name):
        names = package_name.split('.')
        module = __import__('.'.join(names[:-1]))
        func = module
        for name in names[1:]:
            func = getattr(func, name)
        return func

conf = Settings()