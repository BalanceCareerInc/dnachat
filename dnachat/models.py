import time
from uuid import uuid1
from bynamodb.attributes import StringAttribute, NumberAttribute, BooleanAttribute
from bynamodb.indexes import GlobalAllIndex
from bynamodb.model import Model


class ChannelJoinInfo(Model):
    channel = StringAttribute(hash_key=True)
    user_id = StringAttribute(range_key=True)
    joined_at = NumberAttribute(default=time.time)
    last_sent_at = NumberAttribute(default=time.time)
    last_read_at = NumberAttribute(default=time.time)

    class UserIndex(GlobalAllIndex):
        hash_key = 'user_id'
        range_key = 'channel'

        read_throughput = 1
        write_throughput = 1

    class ChannelIndex(GlobalAllIndex):
        hash_key = 'channel'

        read_throughput = 1
        write_throughput = 1

    @classmethod
    def by_channel(cls, channel_name):
        return cls.query('ChannelIndex', channel__eq=str(channel_name))

    @classmethod
    def by_user(cls, user_id):
        return cls.query('UserIndex', user_id__eq=str(user_id))


class Channel(Model):
    name = StringAttribute(hash_key=True)
    is_group_chat = BooleanAttribute(default=False)

    @classmethod
    def users_of(cls, channel_name):
        return cls.query('ChannelIndex', name__eq=str(channel_name))

    @classmethod
    def channels_of(cls, user_id):
        return cls.query('UserIndex', user_id__eq=str(user_id))

    @classmethod
    def create_channel(cls, user_ids, is_group_chat=False):
        channel = cls.put_item(
            name=str(uuid1()),
            is_group_chat=is_group_chat
        )
        join_infos = []
        for user_id in user_ids:
            join_infos.append(ChannelJoinInfo.put_item(
                channel=channel.name,
                user_id=str(user_id),
            ))
        return channel, join_infos


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
