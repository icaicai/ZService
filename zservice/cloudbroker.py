#coding=utf8

import logging
from binascii import hexlify
import time
import zmq.green as zmq
from .base import Base
from .resource import ResourceManager
from .utils.function import split_address
from .utils.timer import Timer
from .protocol import C_HELLO, C_READY, C_REQUEST, C_REPLY, C_HEARTBEAT, C_DISCONNECT, C_EXCEPTION, C_ERROR, C_SETUP, C_REGISTER
from .serializer import loads, dumps
from .exceptions import InvalidUri, SerivceNotFound, ServiceNotMethod, ServiceException



class CloudBroker(Base):
    """docstring for CloudBroker"""

    role = 'BROKER'

    HEARTBEAT_LIVENESS = 5
    HEARTBEAT_INTERVAL = 1000
    HEARTBEAT_EXPIRY = HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS
    
    def __init__(self, identity, manager_uri, client_uri, worker_uri, cloudfe_uri, statebe_uri):
        super(CloudBroker, self).__init__(identity, manager_uri)

        self._timer = None
        

        self.client_uri = client_uri
        self.worker_uri = worker_uri
        self.cloudfe_uri = cloudfe_uri
        self.statebe_uri = statebe_uri
        self.manager_uri = manager_uri

        self._ext_reg_msg = [self.client_uri, self.worker_uri, self.cloudfe_uri, self.statebe_uri]

        self._resmgr = ResourceManager()
        self._resmgr.set_changed_callback(self.on_service_status_changed)
        
        self.pollerbe = zmq.Poller()

        self.init()

        # self.heartbeat_at = time.time() + 1e-3*self.HEARTBEAT_INTERVAL
        # self._workers = {}
        # self._services = {}
        # self._peers = {}
        # self._stateinfo = StateInfo()

    def init(self):

        self.clientfe = self.ctx.socket(zmq.ROUTER)
        self.clientfe.bind(self.client_uri)

        self.workerbe = self.ctx.socket(zmq.ROUTER)
        self.workerbe.bind(self.worker_uri)   

        self.cloudfe = self.ctx.socket(zmq.ROUTER)
        self.cloudfe.setsockopt(zmq.IDENTITY, self.identity)
        self.cloudfe.bind(self.cloudfe_uri)

        self.cloudbe = self.ctx.socket(zmq.ROUTER)
        self.cloudbe.setsockopt(zmq.IDENTITY, self.identity)

        self.statefe = self.ctx.socket(zmq.SUB)
        self.statefe.setsockopt(zmq.SUBSCRIBE, b"")

        self.statebe = self.ctx.socket(zmq.PUB)
        self.statebe.bind(self.statebe_uri)

        self.pollerbe.register(self.clientfe, zmq.POLLIN)
        self.pollerbe.register(self.workerbe, zmq.POLLIN)
        self.pollerbe.register(self.cloudfe, zmq.POLLIN)
        self.pollerbe.register(self.cloudbe, zmq.POLLIN)
        self.pollerbe.register(self.statefe, zmq.POLLIN)    


    # def register_to_manager(self):


    def on_setup(self, conf, socket=None):
        print conf
        self.mgrsock = socket
        other_brokers = []
        if 'other_brokers':
            other_brokers = conf['other_brokers']
            del conf['other_brokers']

        for key in conf:
            setattr(self, key, conf[key])        


        if hasattr(self, 'ctrl_uri'):
            self.statefe.connect(self.ctrl_uri)


        for data in other_brokers:
            identity = data.pop('identity', None)
            peer = self._resmgr.add_peer(identity, **data)
            if peer:
                self.connect_peer(peer)



    def connect_peer(self, peer):
        logging.info("connecting to cloud frontend at %s" % peer.cloudfe_uri)
        self.cloudbe.connect(peer.cloudfe_uri)
        logging.info("connecting to state backend at %s" % peer.statebe_uri)
        self.statefe.connect(peer.statebe_uri)


    def disconnect_peer(self, peer):
        self.cloudbe.disconnect(peer.cloudfe_uri)
        self.statefe.disconnect(peer.statebe_uri)




    def publish_state(self, topic, data):
        # self.statebe.send()
        print 'publish_state', topic, data
        to_send = [topic, self.identity, dumps(data)]
        self.statebe.send_multipart(to_send)

    def on_service_status_changed(self):
        status = self._resmgr.get_service_status()
        self.publish_state('services.status', status)


    def heartbeat(self):

        rw = []
        workers = self._resmgr.workers
        for wid in workers:
            worker = workers[wid]
            if worker.waiting:
                to_send = [worker.address, b'', C_HEARTBEAT]
                self.workerbe.send_multipart(to_send)

                if not worker.isalive():
                    rw.append(worker)

        for w in rw:
            self._resmgr.remove_worker(w)
            self.publish_state('worker.disconnected', [w.identity, w.service])

        # send to manager
        self.mgrsock.send_multipart(['', C_HEARTBEAT, 'BROKER', self.identity])



    # process worker command
    def on_worker_ready(self, waddr, msg):
        # wid = hexlify(waddr)

        service = msg[0]
        self._resmgr.add_worker(waddr, service, self.HEARTBEAT_EXPIRY)
        self.publish_state('worker.ready', [waddr, service])


    def on_worker_reply(self, waddr, msg, cmd):

        worker = self._resmgr.waiting_worker(waddr)

        cp, msg = split_address(msg)  # cp maybe [cloud, client]
        msg.insert(0, cmd)
        self.respond_client(cp, msg)

        
        if worker:
            req = self._resmgr.pop_request(worker.service)
            if req:
                self.on_client_request(req[0], req[1])



    def on_worker_heartbeat(self, sender, msg):
        worker = self._resmgr.get_worker(sender)
        if worker:
            worker.on_heartbeat()
   


    def on_worker_disconnect(self, sender, msg):
        self._resmgr.remove_worker(sender)
        # wid = hexlify(sender)
        # self.delete_worker(wid)
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

        data = loads(msg)

        logging.debug( 'process_cloud_state recv %s %s %s' % (topic, sender, data))


        if topic == 'services.status':
            self._resmgr.update_peer_service_status(sender, data)

        elif topic.startswith('worker.'):
            # peer = self._peers.get(sender)

            # if not peer:
            #     return

            # print '>>', peer.services
            waddr = data[0]
            service = data[1]

            if topic == 'worker.ready':
                print 'worker.ready', data
                self._resmgr.add_peer_worker(sender, waddr, service)
                
            elif topic == 'worker.disconnected':
                self._resmgr.remove_peer_worker(sender, waddr, service)

        elif topic.startswith('broker.'):
            peer_id = data.pop('identity', None)

            if peer_id == self.identity:
                return

            if topic == 'broker.join':
                peer = self._resmgr.add_peer(peer_id, **data)
                if peer:
                    self.connect_peer(peer)

            elif topic == 'broker.disconnected':
                peer = self._resmgr.remove_peer(peer_id)
                if peer:
                    self.disconnect_peer(peer)



    # ----------------------
    def on_client_mmi(self, sender, service, msg):
        returncode = "501"
        if "mmi.service" == service:
            name = loads(msg[1])
            returncode = "200" if self._resmgr.get_service(name) else "404"
        #msg[0] = returncode

        # msg = msg[:2] + [MDP.C_CLIENT, service] + msg[2:]
        msg = [C_REPLY, "mmi.service", dumps((name, returncode))]
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


            worker = self._resmgr.pop_worker(service)
            if worker:
                address = worker.address
            else:
                peer = self._resmgr.find_peer(service)
                if peer:
                    address = peer.address
                else:
                    address = None

            if not address:
                return self._resmgr.add_request(service, caddr, msg)


            to_send = [address, b'', C_REQUEST]
            to_send.extend(caddr)
            to_send.append(b'')
            to_send.extend(msg)

            if worker:
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
            pass

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


        logging.debug( 'respond_client %s %s %s' % (a0, msg, self._resmgr.get_peer(a0)))

        # a0id = hexlify(a0)
        # if a0 in self._peers:
        if self._resmgr.get_peer(a0):
            cmd = msg.pop(0)
            to_send.append(cmd)
            to_send.extend(addr)
            to_send.append(b'')
            to_send.extend(msg)
            self.cloudfe.send_multipart(to_send)
        else:
            to_send.extend(msg)
            self.clientfe.send_multipart(to_send)




    def start(self):


        logging.info("I: start broker: client <%s>   worker <%s>" % (self.client_uri, self.worker_uri))

        self._timer = Timer(self.heartbeat, 1e-3*self.HEARTBEAT_INTERVAL)
        self._timer.start()

        self.started = True
        while self.started:
            try:
                events = dict(self.pollerbe.poll(self.HEARTBEAT_INTERVAL))
            except zmq.ZMQError:
                logging.exception('zmq.ZMQError')
                continue
            except KeyboardInterrupt:
                break


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
         



    def stop(self):
        self.started = False
        self._timer.stop()
        self.pollerbe.unregister(self.clientfe, zmq.POLLIN)
        self.pollerbe.unregister(self.workerbe, zmq.POLLIN)
        self.pollerbe.unregister(self.cloudfe, zmq.POLLIN)
        self.pollerbe.unregister(self.cloudbe, zmq.POLLIN)
        self.pollerbe.unregister(self.statefe, zmq.POLLIN)  
        self.clientfe.close()
        self.workerbe.close()
        self.cloudfe.close()
        self.cloudbe.close()
        self.statefe.close()
        

