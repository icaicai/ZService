#coding=utf8

from binascii import hexlify
import time
# from base import with_metaclass, Singleton
from .serviceinfo import ServiceInfo
from .workerinfo import WorkerInfo
from .peerinfo import PeerInfo


class ResourceManager(object):
    """docstring for ServiceManager"""
    def __init__(self, worker_lifetime=0):
        super(ResourceManager, self).__init__()
        self.services = {}
        self.workers = {}
        self.clients = {}
        self.peers = {}
        self._on_changed_callback = None


    def get_service(self, name):
        return self.services.get(name)


    def get_service_status(self):
        status = {}
        for name in self.services:
            sobj = self.services[name]
            status[name] = len(sobj.worker_q)

        return status


    def add_request(self, service, cli, msg):
        if service in self.services:
            return self.services[service].add_request(cli, msg)


    def pop_request(self, service):
        if service in self.services:
            return self.services[service].pop_request()
        return None

    def add_client(self):
        pass

    def get_client(self):
        pass

    def remove_client(self):
        pass


    def get_worker(self, waddr):
        wid = hexlify(waddr)
        return self.workers.get(wid)

    def add_worker(self, waddr, service, lifetime=None):
        wid = hexlify(waddr)
        if wid in self.workers:
            return

        worker = WorkerInfo(wid, waddr, service, lifetime)
        self.workers[wid] = worker
        if service not in self.services:
            self.services[service] = ServiceInfo(service)

        self.services[service].add_worker(worker)
        self.on_changed()

        return worker


    def remove_worker(self, waddr):
        wid = hexlify(waddr)
        worker = self.workers.get(wid)
        if worker:
            del self.workers[wid]
            if worker.service in self.services:
                sobj = self.services[worker.service]
                sobj.remove_worker(worker)
                #delete invalid service
                if not sobj.isvalid():
                    del self.services[worker.service]
        self.on_changed()
        return worker

    def pop_worker(self, service):
        if service in self.services:
            worker = self.services[service].pop_worker()
            if worker:
                self.on_changed()
            return worker
        return None

    def waiting_worker(self, waddr):
        wid = hexlify(waddr)
        worker = self.workers.get(wid)
        if worker and worker.service in self.services:
            self.services[worker.service].waiting_worker(worker)
            self.on_changed()
        return worker

    #====
    def get_peer(self, paddr):
        return self.peers.get(paddr)

    def add_peer(self, paddr, **conf):
        if paddr not in self.peers:
            peer = PeerInfo(paddr, **conf)
            self.peers[paddr] = peer

            if 'services' in conf:
                services = conf['services']
                for service in services:
                    for waddr in services[service]['workers']:
                        self.add_peer_worker(paddr, waddr, service)

            return peer

    def remove_peer(self, paddr):
        if paddr in self.peers:
            peer = self.peers[paddr]
            del self.peers[paddr]
            for service in peer.services:
                sobj = self.services.get(service)
                if sobj:
                    sobj.remove_peer(peer)
                    # delete invalid service
                    if not sobj.isvalid():
                        del self.services[service]
            # self.on_changed()
            return True
        return False

    def find_peer(self, service):
        sobj = self.services.get(service)
        peer = None
        if sobj:
            peers = sobj.get_peers()
            num = 0
            p = None
            for p in peers:
                if num < p.waiting_num(service):
                    num = p.waiting_num(service)
                    peer = p

        return peer

    def add_peer_worker(self, paddr, waddr, service):
        peer = self.peers.get(paddr)
        if peer:
            peer.add_worker(waddr, service)

            if service not in self.services:
                self.services[service] = ServiceInfo(service)
            
            self.services[service].add_peer(peer)
            # self.on_changed()
            return True
        return False

    def remove_peer_worker(self, paddr, waddr, service):
        peer = self.peers.get(paddr)
        if peer :
            rlt = peer.remove_worker(waddr, service)
            ## delete self.services[service]
            if service not in peer.services and service in self.services:
                sobj = self.services[service]
                sobj.remove_peer(peer)
                if not sobj.isvalid():
                    del self.services[worker.service]
            # self.on_changed()
            return True
        return False


    def update_peer_service_status(self, paddr, ss):
        peer = self.peers.get(paddr)
        if peer :        
            for s in ss:
                if s in peer.services:
                    peer.services[s]['waiting'] = ss[s]


    def set_changed_callback(self, callback):
        self._on_changed_callback = callback


    def on_changed(self):
        if self._on_changed_callback:
            self._on_changed_callback()        


