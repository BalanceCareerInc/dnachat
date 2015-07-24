import json
from threading import Thread

from .settings import conf
from .logger import logger

class ApiListener(Thread):
    def __init__(self, factory, ApiProcessor):
        Thread.__init__(self)
        self.factory = factory
        if not hasattr(ApiProcessor, 'handle') and callable(ApiProcessor.handle):
            raise TypeError('ApiProcessor has to have a "handle" method')
        self.api_processor = ApiProcessor(factory)
        self.daemon = True

    def run(self):
        queue = self.factory.api_queue
        while True:
            message = queue.read(wait_time_seconds=conf['QUEUE_POLLING_INTERVAL'])
            if not message:
                continue

            queue.delete_message(message)

            try:
                message_body = json.loads(message.get_body())
            except ValueError:
                logger.error('Error on decode message body')
                continue

            if 'method' not in message_body:
                logger.error('Invalid message: %s' % str(message_body))
                continue

            logger.info('Api "%s" Received' % message_body['method'])
            try:
                self.api_processor.handle(message_body)
            except Exception, e:
                logger.error('Error on handling api', exc_info=True)
