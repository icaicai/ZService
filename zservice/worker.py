#coding=utf8

import logging
import time
import zmq.green as zmq
from .base import Base
from .utils.function import split_address
from .utils.timer import Timer
from .protocol import C_READY, C_REQUEST, C_REPLY, C_HEARTBEAT, C_DISCONNECT, C_EXCEPTION, C_ERROR, C_REGISTER, C_SETUP
from .serializer import loads, dumps
from .exceptions import InvalidUri, SerivceNotFound, ServiceNotMethod, ServiceException


class Worker(Base):

    role = 'WORKER'


    def __init__(self, identity, service, manager_uri):
        super(Worker, self).__init__(identity, manager_uri)

        self.service = service

        self._methods = {}

        self.socket = None
        self.statefe = None
        self.poller = zmq.Poller()

        self.broker_uri = None

        self._ext_reg_msg = [self.service]


    def on_setup(self, conf, socket=None):
        super(Worker, self).on_setup(conf, socket)
        print conf
        self.connect_to_broker()


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
            msg = [service, dumps(result)]
            self.send_reply(sender, msg)
        except KeyError:
            msg = [service, (u'%s has no method "%s"' % (self.service, method).encode('ascii'))]
            self.send_reply(sender, msg, err=True)
        except:
            logging.exception('exe ')
            exc = sys.exc_info()
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
                args = loads(args_packed)
            else:
                args = ()

            if msg:
                kwargs_packed = msg = msg.pop(0)
                kwargs = loads(kwargs_packed)
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


    # def send_heartbeat(self):
    #     if time.time() > self.heartbeat_at:
    #         msg = [b'', C_HEARTBEAT]
    #         self.socket.send_multipart(msg)

    #         self.heartbeat_at = time.time() + 1e-3*self.HEARTBEAT_INTERVAL

    def heartbeat(self):
        msg = [b'', C_HEARTBEAT]
        self.socket.send_multipart(msg)


    def start(self):

        if self.broker_uri is None:
            self.register()
        else:
            self.connect_to_broker()


        self.started = True
        self._timer = Timer(self.heartbeat, 1e-3*self.HEARTBEAT_INTERVAL)
        self._timer.start()

        self.liveness = self.HEARTBEAT_LIVENESS
        # self.heartbeat_at = time.time() + 1e-3 * self.HEARTBEAT_INTERVAL

        while self.started:
            try:
                events = dict(self.poller.poll(self.HEARTBEAT_INTERVAL))
            except zmq.ZMQError:
                logging.exception('zmq.ZMQError')
                break  # interrupted
            except KeyboardInterrupt:
                break

            if events:
                self.liveness = self.HEARTBEAT_LIVENESS #

                if self.socket in events:
                    msg = self.socket.recv_multipart()

                    # print 'recv', time.time(), msg
                    empty = msg.pop(0)
                    cmd = msg.pop(0)

                    self.process_message(cmd, msg)

                # if self.statefe and self.statefe in events:
                #     topic, sender, msg = self.statefe.recv_multipart()
                #     logging.debug('worker recv %s %s %s' % (topic, sender, msg))
            else:
                self.liveness -= 1


            if self.liveness == 0:
                logging.warn("W: disconnected from broker - retrying...")
                try:
                    time.sleep(3)
                except KeyboardInterrupt:
                    break
                ##
                self.register()
                # self.connect_to_broker()

            # Send HEARTBEAT if it's time
            # self.send_heartbeat()
                


    def stop(self):
        self.started = False
        if self.socket:
            self.poller.unregister(self.socket)
            self.socket.close()
    
 