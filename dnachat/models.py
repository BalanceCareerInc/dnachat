import hashlib
from uuid import uuid1
from bynamodb.attributes import StringAttribute, NumberAttribute, BooleanAttribute
from bynamodb.indexes import GlobalAllIndex
from bynamodb.model import Model


class Channel(Model):
    key = StringAttribute(hash_key=True)
    name = StringAttribute()
    user_id = StringAttribute()
    last_sent_at = NumberAttribute(default=0.0)
    last_read_at = NumberAttribute(default=0.0)
    is_group_chat = BooleanAttribute(default=False)

    class UserIndex(GlobalAllIndex):
        hash_key = 'user_id'

        read_throughput = 1
        write_throughput = 1

    class ChannelIndex(GlobalAllIndex):
        hash_key = 'name'

        read_throughput = 1
        write_throughput = 1

    def to_dict(self):
        return dict(key=self.key, name=self.name, last_read_at=self.last_read_at)

    @classmethod
    def users_of(cls, channel_name):
        return cls.query('ChannelIndex', name__eq=str(channel_name))

    @classmethod
    def channels_of(cls, user_id):
        return cls.query('UserIndex', user_id__eq=str(user_id))

    @classmethod
    def create_channel(cls, user_ids, is_group_chat=False):
        channel_name = hashlib.sha512(str(uuid1())).hexdigest()
        channels = []
        for user_id in user_ids:
            channels.append(cls.put_item(
                key=str(uuid1()),
                name=channel_name,
                user_id=str(user_id),
                is_group_chat=is_group_chat
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
