#coding=utf8

from __future__ import absolute_import

import codecs
import os
import sys

try:
    import cPickle as pickle
except ImportError:
    import pickle


from .exceptions import DecodeError, EncodeError, SerializerNotInstalled






def pickle_loads(s):
    return pickle.loads(s)

def pickle_dumps(o):
    return pickle.dumps(o)



class SerializerRegistry(object):

    def __init__(self):
        self._encoders = {}
        self._decoders = {}
        self._default_serializer = None


    def register(self, name, encoder, decoder):
        if encoder:
            self._encoders[name] = encoder
        if decoder:
            self._decoders[name] = decoder



    def unregister(self, name):
        try:
            self._decoders.pop(name, None)
            self._encoders.pop(name, None)
        except KeyError:
            raise SerializerNotInstalled(
                'No encoder/decoder installed for {0}'.format(name))

    def _set_default_serializer(self, name):

        if name in self._encoders:
            self._default_serializer = name
            return

        raise SerializerNotInstalled(
                'No encoder installed for {0}'.format(name))

    def dumps(self, data, serializer=None):
        if not serializer or serializer not in self._encoders:
            serializer = self._default_serializer

        encoder = self._encoders[serializer]

        return encoder(data)


    def loads(self, data, serializer=None):
        if not serializer or serializer not in self._encoders:
            serializer = self._default_serializer

        decode = self._decoders[serializer]

        return decode(data)






registry = SerializerRegistry()


dumps = registry.dumps
loads = decode = registry.loads
register = registry.register
unregister = registry.unregister



def register_json():
    from json import loads as json_loads, dumps as json_dumps

    registry.register('json', json_dumps, json_loads)


def register_yaml():
    try:
        import yaml
        registry.register('yaml', yaml.safe_dump, yaml.safe_load)
    except ImportError:
        pass




def register_pickle():
    registry.register('pickle', pickle_dumps, pickle_loads)


def register_msgpack():
    try:
        from msgpack import packb, unpackb
        unpack = lambda s: unpackb(s, encoding='utf-8')

        registry.register('msgpack', packb, unpack)
    except (ImportError, ValueError):
        pass

# Register the base serialization methods.
register_json()
register_pickle()
register_yaml()
register_msgpack()

# Default serializer 'pickle'
registry._set_default_serializer('pickle')


