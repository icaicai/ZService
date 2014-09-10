#coding:utf8

import logging
from zservice import Manager

logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S", level=logging.INFO)



mgr = Manager('manager', 'tcp://127.0.0.1:9999', 'tcp://127.0.0.1:9998')
# mgr.load()
mgr.start()



