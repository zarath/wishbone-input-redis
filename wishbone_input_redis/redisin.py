#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
redisin.py.

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
    """Receive data from a redis server.

    Creates a connection to a redis server read data from it.

    Parameters
    ----------
        - host(str)("localhost")
           |  Redis hostname
        - port(int)(6379)
           | Redis port
        - database(int)(0)
           | Index of db to use
        - queue(str)("wishbone.in")
           | name of queue to pop data from

    Queues
    ------
        - outbox
           |  Data coming from the outside world.

    """

    def __init__(self, actor_config,
                 host="localhost", port=6379, database=0,
                 queue="wishbone.in"):
        """Input module to poll a redis dataabase."""
        Actor.__init__(self, actor_config)
        self.redis_host = host
        self.redis_port = port
        self.redis_db = database
        self.queue = queue
        self.pool.createQueue("outbox")

    def preHook(self):
        """Set up redis connection."""
        conn = redis.StrictRedis(
            host=self.redis_host, port=self.redis_port, db=self.redis_db)
        self.logging.info('Connection to %s created.' % self.redis_host)
        self.sendToBackground(self.drain, conn, self.queue)

    def drain(self, connection, queue):
        """Poll the redis queue."""
        self.logging.info('Started.')

        while self.loop():
            line = connection.rpop(queue)
            if not line:
                sleep(0.1)
            else:
                evt = Event(line)
                # pylint: disable=no-member
                self.submit(evt, self.pool.queue.outbox)
