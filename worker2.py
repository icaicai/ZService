#coding:utf8

import logging
from zservice import Worker

logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S", level=logging.INFO)

def echo(msg):
	return "%s from worker 2" % msg

worker = Worker('worker2', 'echo', 'tcp://localhost:9999')
worker.add_method(echo)
# worker.connect_to_manager()
# worker.connect_to_broker('tcp://localhost:5002')
worker.start()



