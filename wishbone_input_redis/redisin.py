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

        - redis_host(str)("localhost")
           |  Redis hostname
        - redis_port(int)(6379)
           | Redis port
        - redis_db(int)(0)
           | Index of db to use
        - queue(str)("wishbone.in")
           | name of queue to pop data from

    Queues:

        - outbox
           |  Data coming from the outside world.
    '''

    def __init__(self, actor_config,
                 redis_host="localhost", redis_port=6379, redis_db=0,
                 queue="wishbone.in"):
        Actor.__init__(self, actor_config)
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.queue = queue
        self.pool.createQueue("outbox")

    def preHook(self):
        '''Sets up redis connection'''
        conn = redis.Redis(host=self.redis_host, port=self.redis_port)
        conn.execute_command("SELECT " + str(self.redis_db))

        self.logging.info('Connection to %s created.' % (self.redis_host))
        self.sendToBackground(self.drain, conn, self.queue)

    def drain(self, connection, queue):
        '''Reads the redis queue.'''

        self.logging.info('Started.')

        while self.loop():
            line = connection.rpop(queue)
            if not line:
                sleep(0.1)
            else:
                evt = Event(line)
                #pylint: disable=no-member
                self.submit(evt, self.pool.queue.outbox)
