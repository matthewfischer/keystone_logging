#!/usr/bin/env python

import argparse
import json
from keystoneclient.v3 import client
import logging
import pika
import sys
import socket

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)


class CADFEvent():
    _users_hash = {}
    _projects_hash = {}
    keystone_username = ""
    keystone_password = ""
    keystone_project = ""
    keystone_auth_url = ""
    keystone_domain = "Default"

    # this code makes me sad.
    def __init__(self, json_data):
        self.json_data = json_data
#        print "event_type: %s" % json_data['event_type']
        self.event_type = json_data['event_type']
        self.timestamp = json_data['timestamp']
        self.outcome = json_data['payload']['outcome']
        try:
            # V2 Doesn't provide initiator information.
            initiator = json_data['payload']['initiator']
            self.initiator_host = initiator['host']['address']
            self.initiator_host_agent = initiator['host']['agent']
            self.initiator_user_id = initiator['id']
        except Exception as e:
            self.initiator_host = "unknown"
            self.initiator_host_agent = "unknown"
            self.initiator_user_id = "unknown"
        try:
            self.initiator_project_id = initiator['project_id']
        except Exception as e:
            self.initiator_project_id = "unknown"
        try:
            target = json_data['payload']['target']
            self.target_id = target['id']
            self.target_type_uri = target['typeURI']
            self.target_type = self.target_type_uri.split('/')[-1:][0]
        except Exception as e:
            self.target_type_uri = "unknown"
            self.target_type = "unknown"

    def get_target_name(self):
        if 'user' in self.target_type:
            return self.get_user_name(self.target_id)
        elif 'project' in self.target_type:
            return self.get_project_name(self.target_id)
        else:
            return "unknown target type %s (id:%s)" % (self.target_type, self.target_id)

    def get_initiator_project_name(self):
        return self.get_project_name(self.initiator_project_id)

    def get_initiator_user_name(self):
        return self.get_user_name(self.initiator_user_id)

    def get_user_name(self, id):
        try:
            return self._users_hash[id].name
        except Exception:
            return "unknown (%s)" % id

    def get_project_name(self, id):
        try:
            return self._projects_hash[id].name
        except Exception:
            return "unknown (%s)" % id

    def __str__(self):
        return json.dumps(self.json_data, sort_keys=True, indent=4, separators=(',', ': '))

    @classmethod
    def build_project_dict(self):
        keystone = self.get_keystone_context()
        projects = keystone.projects.list()
        for project in projects:
            self._projects_hash[project.id] = project

    @classmethod
    def build_user_dict(self):
        keystone = self.get_keystone_context()
        users = keystone.users.list()
        for user in users:
            self._users_hash[user.id] = user

    @classmethod
    def get_keystone_context(self):
        try:
            keystone = client.Client(auth_url=self.keystone_auth_url,
                username=self.keystone_username,
                domain=self.keystone_domain,
                password=self.keystone_password,
                project_name=self.keystone_project)
        except Exception as e:
            print e
            sys.exit(1)
        return keystone


def get_crud_message(event):
    return "%s %s at %s by %s (project: %s)" % \
        (event.target_type, event.get_target_name(),
        event.timestamp,
        event.get_initiator_user_name(),
        event.get_initiator_project_name())


class CADFConsumer(object):
    EXCHANGE = 'keystone'
    EXCHANGE_TYPE = 'topic'
    QUEUE = 'keystone.info'

    def __init__(self, amqp_url):
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = amqp_url

    def connect(self):
        LOGGER.debug('Connecting to %s', self._url)
        return pika.SelectConnection(pika.URLParameters(self._url),
                                     self.on_connection_open,
                                     stop_ioloop_on_close=False)

    def on_connection_open(self, unused_connection):
        LOGGER.debug('Connection opened')
        self.add_on_connection_close_callback()
        self.open_channel()

    def add_on_connection_close_callback(self):
        LOGGER.debug('Adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning('Connection closed, reopening in 5 seconds: (%s) %s',
                           reply_code, reply_text)
            self._connection.add_timeout(5, self.reconnect)

    def reconnect(self):
        # This is the old connection IOLoop instance, stop its ioloop
        self._connection.ioloop.stop()

        if not self._closing:
            # Create a new connection
            self._connection = self.connect()

            # There is now a new connection, needs a new ioloop to run
            self._connection.ioloop.start()

    def open_channel(self):
        LOGGER.debug('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        LOGGER.debug('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def add_on_channel_close_callback(self):
        LOGGER.debug('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        LOGGER.warning('Channel %i was closed: (%s) %s',
                       channel, reply_code, reply_text)
        self._connection.close()

    def setup_exchange(self, exchange_name):
        LOGGER.debug('Declaring exchange %s', exchange_name)
        self._channel.exchange_declare(self.on_exchange_declareok,
                                       exchange_name,
                                       self.EXCHANGE_TYPE)

    def on_exchange_declareok(self, unused_frame):
        LOGGER.debug('Exchange declared')
        self.setup_queue(self.QUEUE)

    def setup_queue(self, queue_name):
        LOGGER.debug('Declaring queue %s', queue_name)
        self._channel.queue_declare(self.on_queue_declareok, queue_name)

    def on_queue_declareok(self, method_frame):
        LOGGER.debug('Binding %s to %s', self.EXCHANGE, self.QUEUE)
        self._channel.queue_bind(self.on_bindok, self.QUEUE,
                                 self.EXCHANGE)

    def on_bindok(self, unused_frame):
        LOGGER.debug('Queue bound')
        self.start_consuming()

    def start_consuming(self):
        LOGGER.debug('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self.on_message,
                                                         self.QUEUE)

    def add_on_cancel_callback(self):
        LOGGER.debug('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        LOGGER.debug('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, unused_channel, basic_deliver, properties, body):
#        LOGGER.debug('Received message # %s from %s: %s',
#                    basic_deliver.delivery_tag, properties.app_id, body)
        event = CADFEvent(json.loads(body))
        if event.event_type == 'identity.authenticate':
            print "USER AUTHENTICATED: %s" % get_crud_message(event)
        elif event.event_type == 'identity.user.created':
            CADFEvent.build_user_dict()
            print "USER CREATED: %s" % get_crud_message(event)
        elif event.event_type == 'identity.user.deleted':
            print "USER DELETED: %s" % get_crud_message(event)
        elif event.event_type == 'identity.project.created':
            CADFEvent.build_project_dict()
            print "PROJECT CREATED: %s" % get_crud_message(event)
        elif event.event_type == 'identity.project.deleted':
            print "PROJECT DELETED: %s" % get_crud_message(event)
        #self.acknowledge_message(basic_deliver.delivery_tag)

    def acknowledge_message(self, delivery_tag):
        LOGGER.debug('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        if self._channel:
            LOGGER.debug('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def on_cancelok(self, unused_frame):
        LOGGER.debug('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def close_channel(self):
        LOGGER.debug('Closing the channel')
        self._channel.close()

    def run(self):
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        LOGGER.debug('Stopping')
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()
        LOGGER.debug('Stopped')

    def close_connection(self):
        LOGGER.debug('Closing connection')
        self._connection.close()


def main():
    global keystone_auth_url, keystone_username, keystone_password, keystone_project

    parser = argparse.ArgumentParser()
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--auth_url", required=True)
    parser.add_argument("--rabbit_user", required=True)
    parser.add_argument("--rabbit_pass", required=True)
    parser.add_argument("--rabbit_host", required=True)
    args = parser.parse_args()

    # Generate build our keystone user/project hashes
    CADFEvent.keystone_auth_url = args.auth_url
    CADFEvent.keystone_username = args.username
    CADFEvent.keystone_password = args.password
    CADFEvent.keystone_project = args.project
    CADFEvent.build_user_dict()
    CADFEvent.build_project_dict()

    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    consumer = CADFConsumer('amqp://%s:%s@%s:5672/' % (args.rabbit_user,
        args.rabbit_pass, args.rabbit_host))
    try:
        consumer.run()
    except KeyboardInterrupt:
        consumer.stop()


if __name__ == '__main__':
    main()
