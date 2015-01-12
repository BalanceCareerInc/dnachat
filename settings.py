__all__ = 'settings'


class Settings(object):
    def __init__(self):
        self.conf = dict()

    def load_from_file(self, config_file):
        with open(config_file, 'r') as f:
            data = f.read()
        config = dict()
        default = dict()
        exec '' in default
        exec data in config
        self.conf.update(dict((k, v) for k, v in config.iteritems() if k not in default))

conf = Settings()