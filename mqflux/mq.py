import paho.mqtt.client as mqtt
import logging
import json

from yurl import URL
from datetime import datetime

from .exc import WrongSchemeException

log = logging.getLogger(__name__)


class MQTT(object):
    def __init__(self, uri):
        self._init_mqtt(uri)

    def _init_mqtt(self, uri):
        log.debug('Parsing uri "{}"'.format(uri))
        p = URL(uri)

        if p.scheme != 'mqtt':
            raise WrongSchemeException(uri)

        self.client = mqtt.Client()
        if p.username:
            self.client.username_pw_set(p.username, p.authorization)

        self.client.enable_logger()

        self.client.connect(
            p.host,
            int(p.port) or 1883,
        )

    def subscribe(self, topic, callback):

        def parser(msg, callback):
            tmp = json.loads(msg.payload)
            tmp['Topic'] = msg.topic

            for k in tmp:
                if k == 'Time':
                    tmp[k] = datetime.fromisoformat(tmp[k])

            callback(tmp)

        self.client.on_message = lambda c, ud, msg: parser(msg, callback)
        self.client.on_connect = lambda c, *unused: c.subscribe(topic)

    def publish(self, *args, **kwargs):
        raise NotImplementedError

    def loop(self):
        return self.client.loop_forever()
