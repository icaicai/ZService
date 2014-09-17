#coding=utf8


class ServiceInfo(object):
    """docstring for ServiceInfo"""
    def __init__(self, name):
        super(ServiceInfo, self).__init__()
        self.name = name
        self.request_q = []
        self.worker_q = []
        self.worker_all = set()
        self.peer_all = set()

    def isvalid(self):
        return (len(self.worker_all) + len(peer_all)) > 0

    def add_peer(self, peer):
        self.peer_all.add(peer)

    def get_peers(self):
        return self.peer_all
        
    def remove_peer(self, peer):
        self.peer_all.discard(peer)

    def add_request(self, cli, req):
        self.request_q.append((cli, req))

    def pop_request(self):
        if self.request_q:
            return self.request_q.pop(0)
        return None

    def waiting_worker(self, worker):
        self.worker_q.append(worker)

    def add_worker(self, worker):
        self.worker_q.append(worker)
        if worker not in self.worker_all:
            self.worker_all.add(worker)

    def pop_worker(self):
        if self.worker_q:
            return self.worker_q.pop(0)
        return None

    def remove_worker(self, worker):
        self.worker_all.discard(worker)
        if worker in self.worker_q:
            self.worker_q.remove(worker)
        
        