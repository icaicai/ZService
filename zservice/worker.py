#coding=utf8

import logging
import time
import zmq
from .util import split_address
from .protocol import C_READY, C_REQUEST, C_REPLY, C_HEARTBEAT, C_DISCONNECT, C_EXCEPTION, C_ERROR, C_SETUP
from .protocol import pack, unpack


class Worker(object):

    HEARTBEAT_LIVENESS = 5
    HEARTBEAT_INTERVAL = 1000
    HEARTBEAT_EXPIRY = HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS


    def __init__(self, identity, service, manager_uri):
        self.service = service
        self.identity = identity
        self.manager_uri = manager_uri
        self._methods = {}

        self.ctx = zmq.Context()
        self.socket = None
        self.statefe = None
        self.poller = zmq.Poller()

        self.broker_uri = None


    def connect_to_manager(self):
        socket = self.ctx.socket(zmq.REQ)
        socket.connect(self.manager_uri)
        poller = zmq.Poller()
        poller.register(socket)
        to_send = [C_SETUP, 'WORKER', self.identity, self.service]
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

                if hasattr(self, 'ctrl_uri') and self.statefe is None:
                    self.statefe = self.ctx.socket(zmq.SUB)
                    self.statefe.setsockopt(zmq.SUBSCRIBE, b"control.")
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
            raise Exception(u'broker_uri is empty')

        logging.info('connect to broker %s' % broker_uri)
        self.socket = self.ctx.socket(zmq.DEALER)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.connect(broker_uri)

        self.poller.register(self.socket, zmq.POLLIN)

        self.liveness = self.HEARTBEAT_LIVENESS
        self.heartbeat_at = time.time() + 1e-3 * self.HEARTBEAT_INTERVAL

        self.send_ready()

    def add_method(self, callback, method=None):
        if not callable(callback):
            raise

        if not method:
            method = callback.__name__


        self._methods[method] = callback


    def on_request(self, sender, service, args, kwargs):
        if '.' in service:
            _, method = service.split('.')
        else:
            method = service

        try:
            callback = self._methods[method]
            result = callback(*args, **kwargs)
            msg = [service, pack(result)]
            self.send_reply(sender, msg)
        except KeyError:
            msg = [service, (u'%s has no method "%s"' % (self.service, method).encode('ascii'))]
            self.send_reply(sender, msg, err=True)
        except:
            msg = [service, (u'%s exception' % service).encode('ascii')]
            self.send_reply(sender, msg, exc=True)

    def on_disconnect(self):
        self.liveness = 0


    def process_message(self, cmd, msg):
        if cmd == C_REQUEST:
            sender, msg = split_address(msg)
            service = msg.pop(0)
            if msg:
                args_packed = msg.pop(0)
                args = unpack(args_packed)
            else:
                args = ()

            if msg:
                kwargs_packed = msg = msg.pop(0)
                kwargs = unpack(kwargs_packed)
            else:
                kwargs = {}

            self.on_request(sender, service, args, kwargs)
        elif cmd == C_HEARTBEAT:
            pass
        elif cmd == C_DISCONNECT:
            self.on_disconnect()
        else:
            print 'unknow command %s' % cmd

    def send_ready(self):
        logging.info('ready!') 
        ready_msg = [ b'', C_READY, self.service ]
        self.socket.send_multipart(ready_msg)

    def send_reply(self, addr, msg, err=False, exc=False):
        
        if exc:
            cmd = C_EXCEPTION
        elif err:
            cmd = C_ERROR
        else:
            cmd = C_REPLY

        to_send = [b'', cmd]
        to_send.extend(addr)
        to_send.append(b'')
        if isinstance(msg, list):
            to_send.extend(msg)
        else:
            to_send.append(msg)



        self.socket.send_multipart(to_send)


    def send_heartbeat(self):
        if time.time() > self.heartbeat_at:
            msg = [b'', C_HEARTBEAT]
            self.socket.send_multipart(msg)

            self.heartbeat_at = time.time() + 1e-3*self.HEARTBEAT_INTERVAL


    def start(self):
        self.started = True

        

        self.liveness = self.HEARTBEAT_LIVENESS
        self.heartbeat_at = time.time() + 1e-3 * self.HEARTBEAT_INTERVAL

        while self.started:
            try:
                events = dict(self.poller.poll(self.HEARTBEAT_INTERVAL))
            except zmq.ZMQError:
                break  # interrupted
            except KeyboardInterrupt:
                break

            if events:
                if self.socket in events:
                    msg = self.socket.recv_multipart()

                    # print 'recv', time.time(), msg
                    empty = msg.pop(0)
                    cmd = msg.pop(0)

                    self.liveness = self.HEARTBEAT_LIVENESS #

                    self.process_message(cmd, msg)

                if self.statefe and self.statefe in events:
                    topic, sender, msg = self.statefe.recv_multipart()
                    print 'worker recv', topic, sender, msg
            else:
                self.liveness -= 1


            if self.liveness == 0:
                logging.warn("W: disconnected from broker - retrying...")
                try:
                    time.sleep(3)
                except KeyboardInterrupt:
                    break
                ##
                self.connect_to_manager()
                # self.connect_to_broker()

            # Send HEARTBEAT if it's time
            self.send_heartbeat()
                


    def stop(self):
        self.started = False
        if self.socket:
            self.poller.unregister(self.socket)
            self.socket.close()
    
 