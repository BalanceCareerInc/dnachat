from uuid import uuid1
from bynamodb.attributes import StringAttribute, NumberAttribute
from bynamodb.indexes import GlobalAllIndex
from bynamodb.model import Model


class ChannelUser(Model):
    channel_name = StringAttribute(hash_key=True)
    user_id = StringAttribute(range_key=True)

    class UserIndex(GlobalAllIndex):
        hash_key = 'user_id'
        range_key = 'channel_name'

    @classmethod
    def users_of(cls, channel_name):
        return cls.query(channel_name__eq=str(channel_name))

    @classmethod
    def channels_of(cls, user_id):
        return cls.query('UserIndex', user_id__eq=str(user_id))


class Channel(Model):
    name = StringAttribute(hash_key=True)
    last_sent_at = NumberAttribute(default=0.0)
    last_read_at = NumberAttribute(default=0.0)

    @classmethod
    def create_channel(cls, user_ids):
        channel_name = str(uuid1())
        channels = []
        for user_id in user_ids:
            channels.append(cls.put_item(
                key='%s_%s' % (channel_name, user_id),
                name=channel_name,
                user_id=str(user_id)
            ))
        return channels


class Message(Model):
    channel = StringAttribute(hash_key=True)
    published_at = NumberAttribute(range_key=True)
    type = StringAttribute()
    writer = StringAttribute()
    message = StringAttribute()

    def to_dict(self):
        return dict(
            type=self.type,
            writer=self.writer,
            published_at=float(self.published_at),
            message=self.message,
            channel=self.channel
        )
