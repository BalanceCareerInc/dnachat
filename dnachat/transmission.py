# -*-coding:utf8-*-
import bson
import time
from threading import Thread
from dnachat.models import ChannelJoinInfo

from .logger import logger


class Transmitter(Thread):
    def __init__(self, factory):
        Thread.__init__(self)
        self.factory = factory
        self.daemon = True

    def run(self):
        pubsub = self.factory.redis_session.pubsub()
        pubsub.psubscribe('*')
        pubsub.listen().next()
        for message in pubsub.listen():
            if message['channel'] == 'create_channel':
                try:
                    self.make_client_attend_channel(message)
                except Exception, e:
                    logger.error('CreatingChannelError: %s' % str(e), exc_info=True)
                continue

            for client in self.factory.clients_by_channel_name.get(message['channel'], []):
                try:
                    join_info = client.user.join_infos_dict[message['channel']]
                    join_info.last_sent_at = time.time()
                    client.transport.write(message['data'])
                except Exception, e:
                    logger.error('TransmissionError: %s' % str(e), exc_info=True)

    def make_client_attend_channel(self, message):
        data = bson.loads(message['data'])
        channel_users = set(data['users'])
        applying_users = set(self.factory.clients_by_user_id.keys()) & channel_users
        for user_id in applying_users:
            mine = ChannelJoinInfo.get_item(data['channel'], user_id)
            user_clients = self.factory.clients_by_user_id[user_id]
            for client in user_clients:
                client.user.join_infos_dict[data['channel']] = mine
                self.factory.clients_by_channel_name.setdefault(data['channel'], []).append(client)
