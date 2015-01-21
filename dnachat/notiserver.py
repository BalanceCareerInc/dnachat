# -*-coding:utf8-*-
import boto
import json

from boto import sqs, sns
from dnachat.models import Joiner

from .settings import conf, func_from_package_name


class NotificationSender(object):
    def __init__(self):
        sqs_conn = sqs.connect_to_region('ap-northeast-1')
        self.queue = sqs_conn.get_queue(conf['NOTIFICATION_QUEUE_NAME'])
        self.sns_conn = sns.connect_to_region('ap-northeast-1')
        self.get_user = func_from_package_name(conf['USER_RESOLVER'])

    def start(self):
        """
        Message has to have key 'message', 'writer', 'channel', 'published_at'
        """

        task = self.publish()
        task.next()
        while True:
            queue_message = self.queue.read()
            if not queue_message:
                continue
            task.send(queue_message)

    def publish(self):
        try:
            while True:
                queue_message = (yield)
                message = json.loads(queue_message.get_body())
                print message
                channel = message.pop('channel')
                message['type'] = 'chat'
                gcm_json = json.dumps(dict(data=message), ensure_ascii=False)
                data = dict(default='default message', GCM=gcm_json)
                for joiner in Joiner.query('ChannelIndex', channel__eq=channel):
                    if joiner.user_id == message['writer']:
                        continue
                    try:
                        print self.sns_conn.publish(
                            message=json.dumps(data, ensure_ascii=False),
                            target_arn=self.get_user(joiner.user_id).endpoint_arn,
                            message_structure='json'
                        )
                    except boto.exception.BotoServerError:
                        pass  # TODO: Error handling
                self.queue.delete_message(queue_message)
        except GeneratorExit:
            pass
