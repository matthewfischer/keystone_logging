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


def callback(ch, method, properties, body):
    try:
        event = json.loads(body)
        if event['event_type'] == 'identity.authenticate':
           next
        elif event['event_type'] == 'identity.user.created':
            # need to rebuild our user list for this event, our
            # keystone context could be old (expired token), so
            # get a new one.
            keystone = get_keystone_context()
            build_user_dict(keystone)
            print "NEW USER CREATED: %r" % (body,)
        elif event['event_type'] == 'identity.user.deleted':
            deleted_user_id = event['payload']['resource_info']
            try:
                print "Deleted: "
                print(users_hash[deleted_user_id])
            except Exception as e:
                print e

    except:
        print "Invalid or unexpected format"


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
