#coding=utf8

import gevent


class Timer(object):

    def __init__(self, callback, interval, *args, **kwargs):
        self.callback = callback
        self.interval = interval
        self.args = args
        self.kwargs = kwargs

        self._thread = None

    def start(self):
        self.started = True
        self.run()


    def stop(self):
        self.started = False
        self._thread.kill()
        self._thread = None


    def run(self):
        if self.started:
            try:
                self.callback(*self.args, **self.kwargs)
            except Exception, e:
                raise

            self._thread = gevent.spawn_later(self.interval, self.run)


