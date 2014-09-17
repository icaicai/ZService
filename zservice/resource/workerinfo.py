#coding=utf8

import time

class WorkerInfo(object):

    def __init__(self, identity, address, service, lifetime):
        self.identity = identity
        self.address = address
        self.service = service
        self.lifetime = lifetime
        if lifetime:
            self.expiry = time.time() + 1e-3*lifetime
        else:
            self.expiry = None
        self.waiting = True

    def on_heartbeat(self):
        self.expiry = time.time() + 1e-3*self.lifetime

    def isalive(self):
        return self.lifetime is None or self.expiry > time.time()



