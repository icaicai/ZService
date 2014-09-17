#coding=utf8

import time

class PeerInfo(object):
    """docstring for PeerInfo"""
    def __init__(self, identity, **kwargs):
        super(PeerInfo, self).__init__()
        self.identity = identity
        self.address = identity
        self.client_uri = kwargs.get('client_uri')
        self.worker_uri = kwargs.get('worker_uri')
        self.cloudfe_uri = kwargs.get('cloudfe_uri')
        self.statebe_uri = kwargs.get('statebe_uri')
        self.services = {} # service => [worker total num, waiting]
        self.worker_total = 0
        self.client_total = 0

        self.lifetime = kwargs.get('lifetime')

        if self.lifetime:
            self.expiry = time.time() + 1e-3*kwargs['lifetime']
        else:
            self.expiry = None

        # if 'services' in kwargs:
        #     ss = kwargs['services']
        #     for s in ss:
        #         # if s not in self.services:
        #         #     self.services[service] = {'workers': set(), 'waiting': 0}

        #         self.services[s] = ss[s]


    def on_heartbeat(self):
        if self.lifetime:
            self.expiry = time.time() + 1e-3*self.lifetime
        
    def isalive(self):
        return self.lifetime is None or self.expiry > time.time()

    def add_client(self):
        self.client_total += 1

    def remove_client(self):
        self.client_total -= 1

    def add_worker(self, waddr, service):
        if service not in self.services:
            self.services[service] = {'workers': set(), 'waiting': 0}
        self.services[service]['workers'].add(waddr)
        self.services[service]['waiting'] += 1
        self.worker_total += 1

    def remove_worker(self, waddr, service):
        if service in self.services:
            self.services[service]['workers'].discard(waddr)
            # 
            if len(self.services[service]['workers']) > 0:
                if self.services[service]['waiting'] > 0:
                    self.services[service]['waiting'] -= 1
                    self.worker_total -= 1
            else:
                del self.services[service]


    def waiting_num(self, service):
        if service in self.services:
            return self.services[service]['waiting']
        return 0

    # def worker_total(self):
    #     return self.worker_total
