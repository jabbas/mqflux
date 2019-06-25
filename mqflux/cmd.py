import datetime
import logging
import sys
from argparse import ArgumentParser

from .influx import InfluxDB
from .mq import MQTT

logging.basicConfig(stream=sys.stderr,
                    format='%(asctime)s:%(levelname)s:%(name)s: - %(message)s')


def cmd():

    p = ArgumentParser(description='Grab MQTT data into ifluxdb')

    p.add_argument('--influx', '-i', type=str,
                   required=True, help='influxdb uri')
    p.add_argument('--mqtt', '-m', type=str,
                   required=True, help='mqtt uri')
    p.add_argument('--topic', '-t', type=str,
                   required=True, help='mqtt topic to subscribe')

    p.add_argument('--loglevel', '-l', type=str,
                   metavar='LEVEL',
                   default='CRITICAL',
                   help='log level (default: CRITICAL)')

    args = p.parse_args()

    try:
        log = logging.getLogger()
        log.setLevel(args.loglevel.upper())

        influx = InfluxDB(args.influx)

        mqtt = MQTT(args.mqtt)
        mqtt.subscribe(args.topic, callback=influx.write)

        mqtt.loop()

    except Exception as e:
        if args.loglevel == 'DEBUG':
            raise e

        logging.critical(e)
