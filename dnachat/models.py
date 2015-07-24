import time
from uuid import uuid1
from bynamodb.attributes import StringAttribute, NumberAttribute, BooleanAttribute
from bynamodb.indexes import GlobalAllIndex
from bynamodb.model import Model


class ChannelWithdrawalLog(Model):
    channel = StringAttribute(hash_key=True)
    user_id = StringAttribute(range_key=True)
    joined_at = NumberAttribute()
    last_read_at = NumberAttribute()
    withdrawal_at = NumberAttribute(default=time.time)


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

    @property
    def partners_last_read_at(self):
        partners = self.get_partners(self.channel, self.user_id)
        if Channel.get_item(self.channel).is_group_chat:
            return dict(
                (join_info.user_id, join_info.last_read_at)
                for join_info in partners
            )
        else:
            return partners[0].last_read_at

    @classmethod
    def by_channel(cls, channel_name):
        return cls.query(channel__eq=str(channel_name))

    @classmethod
    def by_user(cls, user_id):
        return cls.query('UserIndex', user_id__eq=str(user_id))

    @classmethod
    def get_partners(cls, channel_name, user_id):
        return [
            join_info
            for join_info in cls.by_channel(channel_name)
            if join_info.user_id != user_id
        ]


class Channel(Model):
    name = StringAttribute(hash_key=True)
    is_group_chat = BooleanAttribute(default=False)

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


class ChannelUsageLog(Model):
    date = StringAttribute(hash_key=True)
    channel = StringAttribute(range_key=True)
    last_published_at = NumberAttribute()
