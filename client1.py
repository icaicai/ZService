#coding:utf8


import logging
from zservice import Client

logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S", level=logging.INFO)

client = Client('client1', 'tcp://localhost:9999')
client.connect_to_manager()
# client.connect_to_broker('tcp://localhost:5001')

echo = client.use_service('echo')

for i in range(10):
	print echo('%d : hello world' % i)