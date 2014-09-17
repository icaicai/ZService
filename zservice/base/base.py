#coding=utf8

import zmq.green as zmq
from ..protocol import C_REGISTER
from ..serializer import loads, dumps


class Base(object):
    role = ''
    HEARTBEAT_LIVENESS = 5
    HEARTBEAT_INTERVAL = 1000
    HEARTBEAT_EXPIRY = HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS    

    def __init__(self, identity, manager_uri):
        self.identity = identity
        self.manager_uri = manager_uri
        self.ctx = zmq.Context()

        self._ext_reg_msg = []



    def register(self):
        socket = self.ctx.socket(zmq.DEALER)
        socket.connect(self.manager_uri)
        poller = zmq.Poller()
        poller.register(socket)
        to_send = ['', C_REGISTER, self.role, self.identity]
        to_send.extend(self._ext_reg_msg)
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
                conf = loads(msg[0])

                # print conf

                poller.unregister(socket)

                self.on_setup(conf, socket)

                break

            else:
                retries -= 1

                if retries == 0:
                    logging.warn('cannt connect to manager %s ' % self.manager_uri)
                    break


    def on_setup(self, conf, socket=None):
        for key in conf:
            setattr(self, key, conf[key])








def with_metaclass(metaclass):
    """Class decorator for creating a class with a metaclass."""
    def wrapper(cls):
        orig_vars = cls.__dict__.copy()
        orig_vars.pop('__dict__', None)
        orig_vars.pop('__weakref__', None)
        slots = orig_vars.get('__slots__')
        if slots is not None:
            if isinstance(slots, str):
                slots = [slots]
            for slots_var in slots:
                orig_vars.pop(slots_var)
        return metaclass(cls.__name__, cls.__bases__, orig_vars)
    return wrapper   




