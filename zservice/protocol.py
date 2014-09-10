#coding=utf8


try:
	import cPickle as pickle
except ImportError:
	import pickle


C_HELLO			=   b"\x0F"
C_READY         =   b"\x01"
C_REQUEST       =   b"\x02"
C_REPLY         =   b"\x04"
C_HEARTBEAT     =   b"\x05"
C_DISCONNECT    =   b"\x06"
C_EXCEPTION     =   b"\x010"
C_ERROR         =   b"\x011"

C_SETUP         =   b"\x020"



def pack(data):
	return pickle.dumps(data)

def unpack(bytes):
	return pickle.loads(bytes)