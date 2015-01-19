# -*-coding:utf8-*-
import json
import bson

from boto import sqs, sns

from .settings import conf, func_from_package_name


subscribers = func_from_package_name(conf['SUBSCRIBERS_RESOLVER'])


class NotificationSender(object):
    def __init__(self):
        sqs_conn = sqs.connect_to_region('ap-northeast-1')
        self.queue = sqs_conn.get_queue(conf['NOTIFICATION_QUEUE_NAME'])
        self.sns_conn = sns.connect_to_region('ap-northeast-1')

    def start(self):
        """
        Message has to have key 'message', 'writer', 'channel', 'published_at'
        """

        while True:
            message = self.queue.read()
            if not message:
                continue
            message = bson.loads(message.get_body())
            channel = message.pop('channel')
            message['type'] = 'chat'
            gcm_json = json.dumps(dict(data=message), ensure_ascii=False)
            data = dict(default='default message', GCM=gcm_json)
            for subscriber in subscribers(channel):
                if subscriber['id'] == message['writer']:
                    continue
                self.sns_conn.publish(
                    message=json.dumps(data, ensure_ascii=False),
                    target_arn=subscriber['endpoint_arn'],
                    message_structure='json'
                )
