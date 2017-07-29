#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
redisin.py
==========

@author: "Holger Mueller" <holgerm1969@gmx.de>

Wishbone input module to connect with redis servers.
"""


from wishbone import Actor
from wishbone.event import Event
from gevent import sleep, socket as gsocket
import redis
import redis.connection

redis.connection.socket = gsocket

class RedisIn(Actor):
    '''**Receive data from a redis server**

    Creates a connection to a redis server read data from it.

    Parameters:

        - host(str)("localhost")
           | Redis hostname
        - port(int)(6379)
           | Redis port
        - database(int)(0)
           | Index of db to use
        - key(str)("queue")
           | name of queue to pop data from

    Queues:

        - outbox
           |  Data coming from the outside world.
    '''

    #pylint: disable=too-many-arguments
    def __init__(self, actor_config,
                 host="localhost", port=6379, database=0, key="queue"):
        Actor.__init__(self, actor_config)

        self.host = host
        self.port = port
        self.rdb = database
        self.key = key

        self.pool.createQueue("outbox")

    def preHook(self):
        '''Sets up redis connection'''
        conn = redis.Redis(host=self.host, port=self.port)
        conn.execute_command("SELECT " + str(self.rdb))

        self.logging.info('Connection to %s created.' % (self.host))
        self.sendToBackground(self.drain, conn, self.key)

    def drain(self, connection, key):
        '''Reads the redis queue.'''

        self.logging.info('Started.')

        while self.loop():
            line = connection.rpop(key)
            if not line:
                sleep(0.1)
            else:
                evt = Event(line)
                #pylint: disable=no-member
                self.submit(evt, self.pool.queue.outbox)
