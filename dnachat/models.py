from uuid import uuid1
from bynamodb.attributes import StringAttribute, NumberAttribute
from bynamodb.indexes import GlobalAllIndex
from bynamodb.model import Model


class Channel(Model):
    key = StringAttribute(hash_key=True)
    name = StringAttribute()
    user_id = StringAttribute()

    class UserIndex(GlobalAllIndex):
        hash_key = 'user_id'

        read_throughput = 1
        write_throughput = 1

    class ChannelIndex(GlobalAllIndex):
        hash_key = 'name'

        read_throughput = 1
        write_throughput = 1

    @classmethod
    def users_of(cls, channel_name):
        return cls.query('ChannelIndex', name__eq=channel_name)

    @classmethod
    def channels_of(cls, user_id):
        return cls.query('UserIndex', user_id__eq=user_id)

    @classmethod
    def create_channel(cls, user_ids):
        channel_name = str(uuid1())
        for user_id in user_ids:
            cls.put_item(key='%s_%s' % (channel_name, user_id),
                         name=channel_name,
                         user_id=user_id)
        return channel_name


class Message(Model):
    channel = StringAttribute(hash_key=True)
    published_at = NumberAttribute(range_key=True)
    user = StringAttribute()
    message = StringAttribute()

    def to_dict(self):
        return dict(writer=self.user, published_at=float(self.published_at), message=self.message, channel=self.channel)
