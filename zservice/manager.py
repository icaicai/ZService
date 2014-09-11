#coding=utf8

import logging
from binascii import hexlify
import time
import zmq
from .util import split_address
from .protocol import C_READY, C_REQUEST, C_REPLY, C_HEARTBEAT, C_DISCONNECT, C_EXCEPTION, C_ERROR, C_SETUP, C_REGISTER
from .protocol import pack, unpack





CONFIG = {
    'WORKER': {
        'HEARTBEAT_LIVENESS': 5,
        'HEARTBEAT_INTERVAL': 1000
        # 'ctrl_uri'
        # 'broker_uri'
    },
    'CLIENT': {
        'HEARTBEAT_LIVENESS': 5,
        'HEARTBEAT_INTERVAL': 1000
        # 'ctrl_uri'
        # 'broker_uri'
    },
    'BROKER': {
        'HEARTBEAT_LIVENESS': 5,
        'HEARTBEAT_INTERVAL': 1000
        # 'ctrl_uri'
        # 'broker_uri'
    }
}





class Manager(object):

    HEARTBEAT_LIVENESS = 5
    HEARTBEAT_INTERVAL = 1000
    HEARTBEAT_EXPIRY = HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS

    def __init__(self, identity, conf_uri, ctrl_uri):
        self.identity = identity
        self.started = False
        self.conf_uri = conf_uri
        self.ctrl_uri = ctrl_uri

        self._cur_worker_idx = 0
        self._cur_client_idx = 0

        self.heartbeat_at = time.time() + 1e-3*self.HEARTBEAT_INTERVAL

        self.init()        


    def init(self):
        self._clients = {}
        self._workers = {}
        self._brokers = {}


        self.ctx = zmq.Context()
        self.conf_rut = self.ctx.socket(zmq.ROUTER)
        self.conf_rut.bind(self.conf_uri)
        self.ctrl_pub = self.ctx.socket(zmq.PUB)
        self.ctrl_pub.bind(self.ctrl_uri)

        self.state_sub = self.ctx.socket(zmq.SUB)
        self.state_sub.setsockopt_string(zmq.SUBSCRIBE, u'')

        self.poller = zmq.Poller()
        self.poller.register(self.conf_rut, zmq.POLLIN)
        self.poller.register(self.state_sub, zmq.POLLIN)

    def publish_control(self, topic, msg):
        logging.debug('publish control %s => %s' %  (topic, msg))
        to_send = [topic, self.identity, pack(msg)]
        self.ctrl_pub.send_multipart(to_send)


    def purge_broker(self):

        if (time.time() > self.heartbeat_at):
            bw = []
            for bid in self._brokers:
                broker = self._brokers[bid]
                if broker['expiry'] < time.time():
                    bw.append((bid, broker))

            for bid, broker in bw:
                self.publish_control('broker.failure', {'identity': bid})
                del self._brokers[bid]

            # send to broker
            # self.mgrsock.send_multipart([C_HEARTBEAT, 'BROKER', self.identity])
            self.heartbeat_at = time.time() + 1e-3*self.HEARTBEAT_INTERVAL




    def get_broker(self, role):
        m = None
        broker = None
        for bid in self._brokers:
            b = self._brokers[bid]
            if role == 'WORKER':
                num = b['workers']
            elif role == 'CLIENT':
                num = b['clients']
            else:
                break

            if m is None or m > num:
                m = num
                broker = b

        if not broker:
            return None

        if role == 'WORKER':
            addr = broker['worker_uri']
        else:
            addr = broker['client_uri']

        return addr




    def get_conf(self, role, identity):
        conf = CONFIG.get(role).copy()
        conf['ctrl_uri'] = self.ctrl_uri

        if role == 'WORKER':
            conf['broker_uri'] = self.get_broker(role)
        elif role == 'CLIENT':
            conf['broker_uri'] = self.get_broker(role)
        elif role == 'BROKER':
            pass

        if role != 'BROKER':
            print 'get_broker %s %s' % (identity, conf['broker_uri'])

        return conf

    def send_reply(self, addr, msg):
        to_send = [addr, b'']
        to_send.extend(msg)
        self.conf_rut.send_multipart(to_send)

    def on_conf_message(self, sender, msg):
        # address, empty, request = socket.recv_multipart()

        # print msg
        cmd = msg.pop(0)

        if cmd == C_REGISTER:
            role = msg.pop(0)
            identity = msg.pop(0)

            if role == 'WORKER':
                info = {
                    'service': msg[0]
                }
                self._workers[identity] = info
            elif role == 'CLIENT':
                info = {
                    'service': msg and msg[0] or None
                }
                self._clients[identity] = info
            elif role == 'BROKER':
                info = {
                    'address': sender,
                    'client_uri' : msg[0],
                    'worker_uri' : msg[1],
                    'cloudfe_uri' : msg[2],
                    'statebe_uri' : msg[3],
                    'services': {}, #wid service_name
                    'workers': 0,
                    'clients': 0,
                    'expiry': time.time() + 1e-3*self.HEARTBEAT_EXPIRY
                }
                self._brokers[identity] = info

                # 连接到broker的statebe，用于接收broker的状态信息
                self.state_sub.connect(info['statebe_uri'])

                data = {
                    'identity': identity,
                    'cloudfe_uri' : msg[2],
                    'statebe_uri' : msg[3]
                }
                
                # notice broker
                # self.ctrl_pub.send_multipart(['control.new_broker', self.identity, pack(data)])
                self.publish_control('broker.join', data)

            conf = self.get_conf(role, identity)

            if role == 'BROKER':
                ob = []
                for bid in self._brokers:
                    if bid != identity:
                        b = self._brokers[bid]
                        data = {
                            'identity': bid,
                            'address': b['address'],
                            'cloudfe_uri' : b['cloudfe_uri'],
                            'statebe_uri' : b['statebe_uri']
                        }
                        ob.append(data)
                conf['other_brokers'] = ob


            msg = [C_SETUP, pack(conf)]
            self.send_reply(sender, msg)

            print "%s %s %s connected" % (sender, role, identity)

        elif cmd == C_HEARTBEAT:
            role, identity = msg
            if role == 'BROKER':
                self._brokers[identity]['expiry'] = time.time() + 1e-3*self.HEARTBEAT_EXPIRY



    def on_state_message(self, topic, sender, msg):
        # print 'on_state_message', topic, sender, msg
        if topic.startswith('worker.'):
            b = self._brokers.get(sender)
            if not b:
                logging.warn(u'cannt find broker %s' % sender)
                return

            if topic == 'worker.connected':
                b['workers'] += 1
                wid = msg[0]
                service = msg[1]
                if service not in b['services']:
                    b['services'][service] = set()
                    b['services'][service].add(wid)
                else:
                    b['services'][service].add(wid)
            elif topic == 'worker.disconnected':
                b['workers'] -= 1
                wid = msg[0]
                service = msg[1]
                try:
                    b['services'][service].remove(wid)
                except:
                    pass
        elif topic.startswith('client.'):
            b = self._brokers.get(sender)
            if topic == 'client.connected':
                pass
            elif topic == 'client.disconnected':
                pass


    def start(self):
        self.started = True

        logging.info("I: start manager:  <%s>   <%s>" % (self.conf_uri, self.ctrl_uri))

        while self.started:
            try:
                events = dict(self.poller.poll(self.HEARTBEAT_INTERVAL))
            except zmq.ZMQError:
                logging.exception('zmq.ZMQError')
                continue
            except KeyboardInterrupt:
                break                

            if self.conf_rut in events:
                msg = self.conf_rut.recv_multipart()
                # print 'conf recv', msg
                sender, msg = split_address(msg)
                self.on_conf_message(sender[0], msg)
            if self.state_sub in events:
                topic, sender, msg = self.state_sub.recv_multipart()
                self.on_state_message(topic, sender, msg)


            self.purge_broker()


    def stop(self):
        self.started = False