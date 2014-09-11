#coding=utf8

import logging
import time
import zmq
from .util import split_address
from .protocol import C_READY, C_REQUEST, C_REPLY, C_HEARTBEAT, C_DISCONNECT, C_EXCEPTION, C_ERROR, C_SETUP, C_REGISTER
from .protocol import pack, unpack
from exception import InvalidBrokerUri, RPCSerivceNotFound, RPCServiceNotMethod, RPCServiceException

class Client(object):

    HEARTBEAT_LIVENESS = 5
    HEARTBEAT_INTERVAL = 1000
    HEARTBEAT_EXPIRY = HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS


    def __init__(self, identity, manager_uri, retries=3, timeout=2000):
        self.identity = identity
        self.manager_uri = manager_uri
        self._cache_services = {}

        self.retries = retries
        self.timeout = timeout

        self.ctx = zmq.Context()
        
        self.socket = None
        self.statefe = None
        self.poller = zmq.Poller()
        self.broker_uri = None

    def register_to_manager(self):
        socket = self.ctx.socket(zmq.REQ)
        socket.connect(self.manager_uri)
        poller = zmq.Poller()
        poller.register(socket)
        to_send = [C_REGISTER, 'CLIENT', self.identity]
        socket.send_multipart(to_send)

        retries = 3

        while True:

            try:
                events = dict(poller.poll(3000))
            except zmq.ZMQError:
                break  # interrupted
            except KeyboardInterrupt:
                break

            if socket in events:
                msg = socket.recv_multipart()
                cmd = msg.pop(0)
                conf = unpack(msg[0])

                for key in conf:
                    setattr(self, key, conf[key])

                if hasattr(self, 'ctrl_uri'):
                    self.statefe = self.ctx.socket(zmq.SUB)
                    self.statefe.setsockopt(zmq.SUBSCRIBE, b"")
                    self.statefe.connect(self.ctrl_uri)

                    self.poller.register(self.statefe, zmq.POLLIN)

                self.connect_to_broker()

                break

            else:
                retries -= 1

                if retries == 0:
                    logging.warn('cannt connect to manager %s ' % self.manager_uri)
                    break



    def connect_to_broker(self, broker_uri=None):
        if self.socket:
            self.poller.unregister(self.socket)
            self.socket.close()

        if broker_uri:
            self.broker_uri = broker_uri
        else:
            broker_uri = self.broker_uri

        if not broker_uri:
            raise InvalidBrokerUri()


        logging.info('connect to broker %s' % broker_uri)
        self.socket = self.ctx.socket(zmq.REQ)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.connect(broker_uri)
        self.poller.register(self.socket, zmq.POLLIN)
        # if self.statefe:
        #     self.poller.register(self.statefe, zmq.POLLIN)



    def use_service(self, service):

        cmd, result = self.send('mmi.service', service)
        name, code = result
        self._cache_services[name] = code
        print 'mmi.service', cmd, result
        if code == "200":
            return ServiceProxy(self, service)
        else:
            raise RPCSerivceNotFound('service %s is not exists' % service)


    def send(self, service, args=None, kwargs=None):
        to_send = [C_REQUEST, service]

        if args:
            to_send.append(pack(args))
        if kwargs:
            to_send.append(pack(kwargs))

        reply = None

        retries = self.retries
        while retries > 0:
            # print 'send ', time.time()
            self.socket.send_multipart(to_send)
            # print 'sent ', time.time()
            try:
                events = self.poller.poll(self.timeout)
            except zmq.ZMQError:
                logging.exception('zmq.ZMQError')
                continue
            except KeyboardInterrupt:
                break

            if events:
                msg = self.socket.recv_multipart()
                print 'recv ', time.time()

                # Don't try to handle errors, just assert noisily
                assert len(msg) >= 3

                cmd = msg.pop(0)
                reply_service = msg.pop(0)
                assert service == reply_service


                if cmd == C_REPLY:
                    result = unpack(msg[0])
                elif cmd == C_EXCEPTION:
                    e = unpack(msg[0])
                    raise e
                elif cmd == C_ERROR:
                    e = unpack(msg[0])
                    raise e
                else:
                    result = msg[0]


                return cmd, result

            else:
                if retries:
                    logging.warn("W: no reply, reconnecting...")
                    self.connect_to_broker()
                else:
                    logging.warn("W: permanent error, abandoning")
                    break
                retries -= 1

        return None, None



class ServiceProxy(object):

    def __init__(self, cli, service):
        self.cli = cli
        self.service = service

    def __call__(self, *args, **kwargs):
        cmd, result = self.cli.send(self.service, args, kwargs)
        return result

    def __getattr__(self, name):
        service = "%s.%s" % (self.service, name)
        return ServiceProxy(self.cli, service)
