#coding=utf8
#cluster

import logging
from binascii import hexlify
import time
# import zmq
import gevent
import zmq.green as zmq
from .util import split_address
from .protocol import C_HELLO, C_READY, C_REQUEST, C_REPLY, C_HEARTBEAT, C_DISCONNECT, C_EXCEPTION, C_ERROR, C_SETUP, C_REGISTER
from .protocol import pack, unpack
from exception import InvalidBrokerUri, RPCSerivceNotFound, RPCServiceNotMethod, RPCServiceException






class CloudBroker(object):

    HEARTBEAT_LIVENESS = 5
    HEARTBEAT_INTERVAL = 1000
    HEARTBEAT_EXPIRY = HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS


    def __init__(self, identity, client_uri, worker_uri, cloudfe_uri, statebe_uri, manager_uri):
        self.identity = identity

        self.ctx = zmq.Context()

        self.client_uri = client_uri
        self.worker_uri = worker_uri
        self.cloudfe_uri = cloudfe_uri
        self.statebe_uri = statebe_uri
        self.manager_uri = manager_uri


        self.clientfe = self.ctx.socket(zmq.ROUTER)
        self.clientfe.bind(client_uri)

        self.workerbe = self.ctx.socket(zmq.ROUTER)
        self.workerbe.bind(worker_uri)     

        self.pollerfe = zmq.Poller()
        self.pollerbe = zmq.Poller()

        self.heartbeat_at = time.time() + 1e-3*self.HEARTBEAT_INTERVAL


        self._workers = {}
        self._services = {}
        self._peers = {}
        self._stateinfo = StateInfo()

        self.cloudfe = self.ctx.socket(zmq.ROUTER)
        self.cloudfe.setsockopt(zmq.IDENTITY, self.identity)
        self.cloudfe.bind(cloudfe_uri)

        self.cloudbe = self.ctx.socket(zmq.ROUTER)
        self.cloudbe.setsockopt(zmq.IDENTITY, self.identity)

        self.statefe = self.ctx.socket(zmq.SUB)
        self.statefe.setsockopt(zmq.SUBSCRIBE, b"")

        self.statebe = self.ctx.socket(zmq.PUB)
        self.statebe.bind(statebe_uri)


        self.pollerbe.register(self.clientfe, zmq.POLLIN)
        self.pollerbe.register(self.cloudfe, zmq.POLLIN)

        self.pollerbe.register(self.workerbe, zmq.POLLIN)
        self.pollerbe.register(self.cloudbe, zmq.POLLIN)
        self.pollerbe.register(self.statefe, zmq.POLLIN)    



    def register_to_manager(self):
        self.mgrsock = socket = self.ctx.socket(zmq.DEALER)
        socket.connect(self.manager_uri)
        poller = zmq.Poller()
        poller.register(socket)
        to_send = ['', C_REGISTER, 'BROKER', self.identity, self.client_uri, self.worker_uri, self.cloudfe_uri, self.statebe_uri]
        socket.send_multipart(to_send)

        retries = 3

        while True:

            try:
                events = dict(poller.poll(3000))
            except zmq.ZMQError:
                break  # interrupted
            except KeyboardInterrupt:
                break

            print '==>', retries, events

            if socket in events:
                msg = socket.recv_multipart()
                # print 'broker conf recv', msg
                empty = msg.pop(0)
                cmd = msg.pop(0)
                conf = unpack(msg[0])

                # print conf
                other_brokers = []
                if 'other_brokers':
                    other_brokers = conf['other_brokers']
                    del conf['other_brokers']

                for key in conf:
                    setattr(self, key, conf[key])



                poller.unregister(socket)



                if hasattr(self, 'ctrl_uri'):
                    self.statefe.connect(self.ctrl_uri)

                
                # self.pollerbe.register(self.mgrsock)

                for data in other_brokers:
                    identity = data['identity']
                    peer = Peer(identity, data['cloudfe_uri'], data['statebe_uri'])
                    self._peers[identity] = peer
                    self.connect_to_peer(data['cloudfe_uri'], data['statebe_uri'])

                    # say hello
                    print 'say hello to', identity
                    # self.cloudbe.send_multipart([identity, '', C_HELLO])
                    gevent.spawn_later(1000, self.cloudbe.send_multipart, [identity, '', C_HELLO])


                break

            else:
                retries -= 1

                if retries == 0:
                    logging.warn('cannt connect to manager %s ' % self.manager_uri)
                    break



    def connect_to_peer(self, cloudfe_uri, statebe_uri):
        logging.info("I: connecting to cloud frontend at %s" % cloudfe_uri)
        self.cloudbe.connect(cloudfe_uri)
        logging.info("I: connecting to state backend at %s" % statebe_uri)
        self.statefe.connect(statebe_uri)


    def find_available_peer(self, service):
        try:
            print 'find_available_peer', service, self._services
            sobj = self._services[service]
            peers = sobj.get_peers()
            print peers
            waiting_num = 0
            ap = None
            for peer_id in peers:
                peer = self._peers.get(peer_id)
                # print '==', peer_id, peer, peer.services
                if peer and service in peer.services:
                    worker_num = peer.services[service]
                    if worker_num > waiting_num:
                        waiting_num = worker_num
                        ap = peer
                else:
                    sobj.remove_peer(peer_id)

            return ap
        except KeyError:
            pass
        return None


    def publish_state(self, topic, data):
        # self.statebe.send()
        print 'publish_state', topic, data
        to_send = [topic, self.identity, pack(data)]
        self.statebe.send_multipart(to_send)


    def send_heartbeats(self):
        """Send heartbeats to idle workers if it's time"""
        if (time.time() > self.heartbeat_at):
            rw = []
            for wid in self._workers:
                worker = self._workers[wid]
                if worker.waiting:

                    to_send = [worker.address, b'', C_HEARTBEAT]
                    self.workerbe.send_multipart(to_send)

                    if worker.expiry < time.time():
                        rw.append(worker)

            for w in rw:
                self.delete_worker(w, True)

            # send to manager
            self.mgrsock.send_multipart(['', C_HEARTBEAT, 'BROKER', self.identity])
            self.heartbeat_at = time.time() + 1e-3*self.HEARTBEAT_INTERVAL



    def delete_worker(self, worker, disconnect):

        if disconnect:
            to_send = [worker.address, b'', C_DISCONNECT]
            self.workerbe.send_multipart(to_send)

        if worker.service is not None:
            service = worker.service
            self._services[service].remove(worker.identity)
            # self._services[service]
        self._workers.pop(worker.identity, None)
        
        # self.publish_state('service.delete', worker.service)
        self.publish_state('worker.disconnected', [worker.identity, worker.service])


    # process worker command
    def on_worker_ready(self, waddr, msg):
        wid = hexlify(waddr)

        if wid not in self._workers:

            service = msg[0]

            self._workers[wid] = Worker(wid, waddr, service, self.HEARTBEAT_EXPIRY)

            if service in self._services:
                sq = self._services[service]
                sq.put(wid)
            else:
                q = Service(service, self._stateinfo)
                q.put(wid)
                self._services[service] = q


            self.publish_state('worker.connected', [wid, service])

            logging.info('new worker <%s, %s>' % (wid, service))


    def on_worker_reply(self, waddr, msg, cmd):
        wid = hexlify(waddr)
        worker = self._workers.get(wid)
        if not worker:
            return


        logging.debug('on_worker_reply %s %s %s' % (wid, msg, cmd))

        worker.waiting = True
        service = worker.service
        # make worker available again
        try:
            sq = self._services[service]
            cp, msg = split_address(msg)  # cp maybe [cloud, client]
            msg.insert(0, cmd)
            self.respond_client(cp, msg)
            sq.put(worker.identity)
            if sq.request_q:
                rp, msg = sq.pop_request()
                self.on_client_request(rp, msg)
        except KeyError:
            # unknown service
            logging.warn('unknown service %s' % service)
            self.disconnect_worker(wid)
        return

    def on_worker_heartbeat(self, sender, msg):
        wid = hexlify(sender)
        try:
            worker = self._workers[wid]
            worker.expiry = time.time() + 1e-3*self.HEARTBEAT_EXPIRY
        except KeyError:
            # ignore HB for unknown worker
            logging.warn('unknown worker %s' % sender)      


    def on_worker_disconnect(self, sender, msg):
        wid = hexlify(sender)
        self.delete_worker(wid)
        return

    # worker command end ---------------------------

    def process_worker(self, waddr, msg):
        # rp, msg = split_address(msg)
        # msg => waddr, '', cmd, cli_addr, '', body
        # msg => waddr, '', cmd, peer_addr, cli_addr, '', body
        cmd = msg.pop(0)

        if cmd == C_READY:
            self.on_worker_ready(waddr, msg)
        elif cmd in (C_REPLY, C_EXCEPTION, C_ERROR):
            self.on_worker_reply(waddr, msg, cmd)
        elif cmd == C_HEARTBEAT:
            self.on_worker_heartbeat(waddr, msg)
        elif cmd == C_DISCONNECT:
            self.on_worker_disconnect(waddr, msg)
        else:
            logging.warn("unknown command %s", cmd)


    def process_cloud_worker(self, paddr, msg):
        # rp, msg = split_address(msg)
        # pid = hexlify(paddr)
        cmd = msg.pop(0)

        logging.debug( 'process_cloud_worker recv %s %s' % (paddr, msg))

        if cmd == C_HELLO:
            pass

        else:
            caddr, msg = split_address(msg)
            msg.insert(0, cmd)
            self.respond_client(caddr, msg)


    def process_cloud_state(self, topic, sender, msg):
        
        
        # topic, msg = msg.split(' ', 2)

        data = unpack(msg)

        logging.debug( 'process_cloud_state recv %s %s %s' % (topic, sender, data))


        if topic == 'services.state':
            peer = self._peers.get(sender)
            if not peer:
                return

            for service in data:
                if service not in self._services:
                    self._services[service] = Service(service, self._stateinfo)
                
                self._services[service].put_peer(sender)
                peer.services[service] = data[service]

        elif topic.startswith('worker.'):
            peer = self._peers.get(sender)

            if not peer:
                return

            print '>>', peer.services
            service = data[1]

            if topic == 'worker.connected':
                print 'worker.connected', data
                
                # if service in peer.services:
                #     peer.services[service] += 1
                # else:
                #     peer.services[service] = 1
                try:
                    sobj = self._services[service]
                    sobj.put_peer(peer.identity)
                except KeyError:
                    pass
            elif topic == 'worker.disconnected':
                pass

        elif topic.startswith('broker.'):
            identity = data['identity']

            if identity == self.identity:
                return

            if topic == 'broker.join':
                peer = Peer(identity, data['cloudfe_uri'], data['statebe_uri'])
                self._peers[identity] = peer
                self.connect_to_peer(data['cloudfe_uri'], data['statebe_uri'])

                # print '-----hello', identity
                # self.cloudbe.send_multipart([identity,'', C_HELLO])

            elif topic == 'broker.failure':
                if identity in self._peers:
                    p = self._peers[identity]
                    self.cloudbe.disconnect(p.cloudfe_uri)
                    self.statefe.disconnect(p.statebe_uri)
                    del self._peers[identity]


    # ----------------------
    def on_client_mmi(self, sender, service, msg):
        returncode = "501"
        if "mmi.service" == service:
            name = unpack(msg[1])
            returncode = "200" if name in self._services else "404"
        #msg[0] = returncode

        # msg = msg[:2] + [MDP.C_CLIENT, service] + msg[2:]
        msg = [C_REPLY, "mmi.service", pack((name, returncode))]
        # self.socket.send_multipart(msg)
        self.respond_client(sender, msg)

    def on_client_request(self, caddr, msg):
        service = msg[0]

        if service.startswith(b'mmi.'):
            self.on_client_mmi(caddr, service, msg)
            return

        try:
            full_service_name = service
            if '.' in service:
                service, method = service.split('.', 2)

            sq = self._services[service]
            wid = sq.get()
            worker = self._workers.get(wid)

            address = None
            if worker:
                address = worker.address
            else:
                peer = self.find_available_peer(service)
                if peer:
                    address = peer.identity
 

            if not address:
                # no worker ready
                # queue message                
                # msg.insert(0, full_service_name)
                sq.put_request(caddr, msg)   
                return
            
            to_send = [address, b'', C_REQUEST]
            to_send.extend(caddr)
            to_send.append(b'')
            to_send.extend(msg)

            print 'on_client_request', self.identity, to_send, worker

            if worker:
                worker.waiting = False
                self.workerbe.send_multipart(to_send)
            else:
                self.cloudbe.send_multipart(to_send)

        except KeyError:
            # unknwon service
            # ignore request
            msg = [C_ERROR, b'broker', (u'broker has no service "%s"' % full_service_name).encode('ascii')]
            self.respond_client(caddr, msg)        
    # ----------------------

    def process_client(self, caddr, msg):
        cmd = msg.pop(0)
        if cmd == C_HEARTBEAT:
            pass
        self.on_client_request(caddr, msg)


    def process_cloud_client(self, paddr, msg):
        cmd = msg.pop(0)  # always 0x01

        logging.debug('process_cloud_client %s %s' % (paddr, msg))

        if cmd == C_HELLO:
            
            addr = paddr[0]
            if msg:
                
                data = unpack(msg[0])

                peer = self._peers.get(addr)

                print data, peer
                if not peer:
                    return

                for service in data:
                    if service not in self._services:
                        self._services[service] = Service(service, self._stateinfo)
                    
                    self._services[service].put_peer(addr)
                    peer.services[service] = data[service] 

            else:
                #发送服务信息到新的broker
                sv = {}
                for service in self._services:
                    sv[service] = len(self._services[service].worker_q)

                self.cloudbe.send_multipart([addr, '', C_HELLO, pack(sv)])

        elif cmd == C_REQUEST:
            caddr, msg = split_address(msg)
            # addr = [paddr, caddr]
            addr = []
            addr.extend(paddr)
            addr.extend(caddr)

            self.on_client_request(addr, msg)


        # service = msg.pop(0)


    def respond_client(self, addr, msg):  #addr maybe [cloud, client], msg => 
        a0 = addr.pop(0)
        to_send = [a0, b'']
        

        logging.debug( 'respond_client %s %s %s' % (a0, msg, a0 in self._peers))

        a0id = hexlify(a0)
        if a0 in self._peers:
            cmd = msg.pop(0)
            to_send.append(cmd)
            to_send.extend(addr)
            to_send.append(b'')
            to_send.extend(msg)
            self.cloudfe.send_multipart(to_send)
        else:
            to_send.extend(msg)
            self.clientfe.send_multipart(to_send)

    def response_client_error(self, addr, service, err):
        to_send = [addr, '', C_ERROR, service, err]
        self.respond_client()

    def response_client_exception(self, exc):
        pass



    def start(self):
    

        logging.info("I: start broker: client <%s>   worker <%s>" % (self.client_uri, self.worker_uri))


        self.started = True
        while self.started:
            try:
                events = dict(self.pollerbe.poll(self.HEARTBEAT_INTERVAL))
            except zmq.ZMQError:
                break  # interrupted
            except KeyboardInterrupt:
                break

            # previous = local_capacity
            self._stateinfo.clean()

            msg = None
            if self.workerbe in events:
                msg = self.workerbe.recv_multipart()
                # msg => waddr, '', cmd, cli_addr, '', body
                # msg => waddr, '', cmd, peer_addr, cli_addr, '', body
                waddr, msg = split_address(msg)
                self.process_worker(waddr[0], msg)

            elif self.cloudbe in events:
                msg = self.cloudbe.recv_multipart()
                peer, msg = split_address(msg)
                # (address, empty), msg = msg[:2], msg[2:]
                self.process_cloud_worker(peer, msg)


            if self.statefe in events:
                topic, sender, msg = self.statefe.recv_multipart()
                self.process_cloud_state(topic, sender, msg)



            # process client and cloudfe

            if self.cloudfe in events:
                msg = self.cloudfe.recv_multipart()
                #msg => peer_addr, '', cmd, cli_addr, '', body
                paddr, msg = split_address(msg)
                self.process_cloud_client(paddr, msg)
            elif self.clientfe in events:
                msg = self.clientfe.recv_multipart()
                #msg => cli_addr, '', cmd, body
                # print 'clientfe recv', time.time(), msg
                sender, msg = split_address(msg)
                self.process_client(sender, msg)        
         

            self.send_heartbeats()

            changed = self._stateinfo.get_changed()
            if changed:
                self.publish_state('services.state', changed)

    def stop(self):
        self.started = False




class StateInfo(object):

    def __init__(self):
        self._info = {}

    def clean(self):
        self._info = {}

    def inc_worker(self, service, total):
        if service in self._info:
            self._info[service][0] += 1
            self._info[service][1] = total
        else:
            self._info[service] = [1, total]

    def dec_worker(self, service, total):
        if service in self._info:
            self._info[service][0] -= 1
            self._info[service][1] = total
        else:
            self._info[service] = [-1, total]

    def get_changed(self):
        cs = {}
        for s in self._info:
            if self._info[s][0] != 0:
                cs[s] = self._info[s][1]

        return cs


class Worker(object):
    """a Worker, idle or active"""

    def __init__(self, identity, address, service, lifetime):
        self.identity = identity
        self.address = address
        self.service = service
        self.expiry = time.time() + 1e-3*lifetime
        self.waiting = True


class Peer(object):

    def __init__(self, identity, cloudfe_uri, statebe_uri):
        self.identity = identity
        self.cloudfe_uri = cloudfe_uri
        self.statebe_uri = statebe_uri
        self.services = {} # service => [worker total num, waiting]
        self.worker_total = 0
        self.worker_waiting = 0



class Service(object):

    def __init__(self, name, stateinfo=None):
        self.name = name
        self._stateinfo = stateinfo
        self.worker_q = []
        self.request_q = []
        self.peer_q = []

        self.worker_all = set()


    def remove_peer(self, pid):
        if isinstance(pid, Peer):
            pid =pid.identity        
        try:
            self.peer_q.remove(pid)
        except ValueError:
            pass

    def put_peer(self, pid):
        if isinstance(pid, Peer):
            pid =pid.identity
        if pid not in self.peer_q:
            self.peer_q.append(pid)

    def get_peers(self):
        return self.peer_q

    def put_request(self, addr, msg):
        self.request_q.append((addr, msg))


    def pop_request(self):
        if self.request_q:
            return self.request_q.pop(0)

        return None


    def remove(self, wid):
        try:
            self.worker_q.remove(wid)
            self._stateinfo.dec_worker(self.name, len(self.worker_q))
        except ValueError:
            pass


    def put(self, wid, *args, **kwargs):
        if wid not in self.worker_q:
            self.worker_q.append(wid)
            self._stateinfo.inc_worker(self.name, len(self.worker_q))

    def get(self):
        if not self.worker_q:
            return None
        
        wid = self.worker_q.pop(0)
        self._stateinfo.dec_worker(self.name, len(self.worker_q))
        return wid