import bson
from dnachat.dna.exceptions import ProtocolError
from dnachat.models import ChannelJoinInfo


class Protocol2To3Adapter(object):
    def __init__(self, protocol, request):
        self.protocol = protocol
        self.request = request

    def adapt(self):
        """
        :return: Changed request object. If None returned, protocol will not be continued.
        """
        if self.request.method == 'attend':
            for join_info in ChannelJoinInfo.by_channel(self.request['channel']):
                if join_info.user_id == self.protocol.user.id:
                    self.protocol.attended_channel = self.request['channel']
                    self.protocol.transport.write(bson.dumps(dict(
                        method=self.request.method,
                        channel=join_info.channel,
                        last_read=join_info.partners_last_read_at
                    )))
                    return
            raise ProtocolError(
                'Given channel "%s" is not a channel of user %s' %
                (self.request['channel'], self.protocol.user.id)
            )
        elif self.request.method == 'exit':
            self.protocol.attended_channel = None
        elif self.request.method == 'publish':
            self.request._data['channel'] = self.protocol.attended_channel  # Trick. Should not access _data directly
        return self.request
