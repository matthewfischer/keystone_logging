#!/usr/bin/env python

import argparse
import json
from keystoneclient.v3 import client
import pika
import sys
import socket

users_hash = {}
projects_hash = {}

keystone_user = ""
keystone_password = ""
keystone_project = ""
keystone_auth_url = ""
keystone_domain = "Default"
rabbit_user = ""
rabbit_pass = ""
rabbit_host = ""

KEYSTONE_RABBIT_QUEUE='keystone.info'

class CADFEvent():
    # this code makes me sad.
    def __init__(self, json_data):
        self.json_data = json_data
#        print "event_type: %s" % json_data['event_type']
        self.event_type = json_data['event_type']
        self.timestamp = json_data['timestamp']
        self.outcome = json_data['payload']['outcome']
        try:
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
            return users_hash[id].name
        except Exception:
            return "unknown (%s)" % id

    def get_project_name(self, id):
        try:
            return projects_hash[id].name
        except Exception:
            return "unknown (%s)" % id

    def __str__(self):
        return json.dumps(self.json_data, sort_keys=True, indent=4, separators=(',', ': '))


def build_project_dict(keystone):
    global projects_hash
    projects = keystone.projects.list()
    for project in projects:
        projects_hash[project.id] = project


def build_user_dict(keystone):
    global users_hash
    users = keystone.users.list()
    for user in users:
        users_hash[user.id] = user


def get_keystone_context():
    try:
        keystone = client.Client(auth_url=keystone_auth_url,
            username=keystone_username,
            domain=keystone_domain,
            password=keystone_password,
            project_name=keystone_project)
    except Exception as e:
        print e
        sys.exit(1)
    return keystone


def setup_connection():
    credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
    parameters = pika.ConnectionParameters(credentials=credentials,
        host=rabbit_host)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue=KEYSTONE_RABBIT_QUEUE)
    return channel


def get_crud_message(event):
    return "%s %s at %s by %s (project: %s)" % \
        (event.target_type, event.get_target_name(), 
        event.timestamp, 
        event.get_initiator_user_name(),
        event.get_initiator_project_name())


def callback(ch, method, properties, body):
    event = CADFEvent(json.loads(body))
    if event.event_type == 'identity.authenticate':
       next
    elif event.event_type == 'identity.user.created':
        # need to rebuild our user list for this event, our
        # keystone context could be old (expired token), so
        # get a new one.
        keystone = get_keystone_context()
        build_user_dict(keystone)
        print "USER CREATED: %s" % get_crud_message(event)
    elif event.event_type == 'identity.user.deleted':
        print "USER DELETED: %s" % get_crud_message(event)
    elif event.event_type == 'identity.project.created':
        # need to rebuild our project list for this event, our
        # keystone context could be old (expired token), so
        # get a new one.
        keystone = get_keystone_context()
        build_project_dict(keystone)
        print "PROJECT CREATED: %s" % get_crud_message(event)
    elif event.event_type == 'identity.project.deleted':
        print "PROJECT DELETED: %s" % get_crud_message(event)


def main():
    channel = setup_connection()
    channel.basic_consume(callback, queue=KEYSTONE_RABBIT_QUEUE, no_ack=True)
    channel.start_consuming()
    print ' [*] Waiting for messages. To exit press CTRL+C'


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--auth_url", required=True)
    parser.add_argument("--rabbit_user", required=True)
    parser.add_argument("--rabbit_pass", required=True)
    parser.add_argument("--rabbit_host", required=True)
    args = parser.parse_args()

    # save these off for later use
    keystone_auth_url = args.auth_url
    keystone_username = args.username
    keystone_password = args.password
    keystone_project = args.project
    rabbit_user = args.rabbit_user
    rabbit_pass = args.rabbit_pass
    rabbit_host = args.rabbit_host

    keystone = get_keystone_context()
    build_user_dict(keystone)
    build_project_dict(keystone)
    main()
