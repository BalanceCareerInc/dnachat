from bynamodb.attributes import StringAttribute, NumberAttribute
from bynamodb.model import Model

from .settings import conf


class Message(Model):
    table_name = '%sMessage' % conf.get('prefix', '')

    channel = StringAttribute(hash_key=True)
    published_at = NumberAttribute(range_key=True)
    user = StringAttribute()
    message = StringAttribute()

    def to_dict(self):
        return dict(writer=self.user, published_at=float(self.published_at), message=self.message)
