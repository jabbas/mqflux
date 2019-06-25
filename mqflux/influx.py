from influxdb import InfluxDBClient
from yurl import URL
import logging
from .exc import WrongSchemeException

log = logging.getLogger(__name__)


class InfluxDB(object):
    def __init__(self, uri):
        self._init_influx(uri)

    def _init_influx(self, uri):
        log.debug('Parsing uri "{}"'.format(uri))
        p = URL(uri)

        if p.scheme != 'influx':
            raise WrongSchemeException(uri)

        self.influx = InfluxDBClient(
                p.host,
                p.port or 8086,
                p.username,
                p.authorization,
                p.path.lstrip('/')
        )

    def write(self, msg):

        log.debug('message: {}'.format(msg))
        name, type_, tmp = msg['Topic'].split("/")

        tags = {
            'name': name,
            'kind': type_,
            'type': tmp.lower()
        }

        influx_data = []
        for sensor_name, measurements in msg.items():
            if type(measurements) == dict:
                tags['sensor_name'] = sensor_name
                for measurement, value in measurements.items():
                    body = {
                        "measurement": measurement.lower(),
                        "tags": tags,
                        "time": msg['Time'],
                        "fields": {'value': value}
                    }

                    if 'TempUnit' in msg and measurement == 'Temperature':
                        tags['temperature_unit'] = msg['TempUnit']

                    influx_data.append(body)

        log.debug('writing: {}'.format(influx_data))
        self.influx.write_points(influx_data)
