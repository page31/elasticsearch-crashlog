#! /usr/bin/python2.7

from Queue import Queue
from elasticsearch import helpers
import config
import datetime
import elasticsearch
import json
import logging
import sys
import threading
import tornado.ioloop
import tornado.web


class Submitter(threading.Thread):
    def __init__(self, es):
        self.action_queue = Queue()
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)
        self.es = es
        super(Submitter, self).__init__(name='Submitter Thread')
        self.daemon = True

    def __bulk(self, actions):
        logging.debug('bulk')
        try:
            helpers.bulk(self.es, actions)
        except elasticsearch.ElasticsearchException:
            logging.error('bulk error %d actions discarded' % len(actions))

    def __do_submit(self):
        logging.debug('do submit')
        action_queue = []
        while not self.action_queue.empty():
            action_queue.append(self.action_queue.get())
            self.action_queue.task_done()
            if len(action_queue) == config.submit_per_batch:
                self.__bulk(action_queue)
                action_queue = []
        self.__bulk(action_queue)

    def __main_loop(self):
        while True:
            logging.debug('submiter loop tick')
            if not self.action_queue.empty():
                self.__do_submit()
            if self.cond.acquire():
                logging.debug('wait')
                self.cond.wait(config.submit_interval)
                self.cond.release()

    def run(self):
        logging.info('submiter started')
        self.__main_loop()

    def submit(self):
        logging.debug('submit actions')
        if self.cond.acquire():
            self.cond.notify()
            self.cond.release()

    def buffer_action(self, action):
        logging.debug('buffer action')
        self.action_queue.put(action)
        if self.action_queue.qsize() > config.submit_per_batch:
            self.submit()


if config.es_nodes is not None:
    es = elasticsearch.Elasticsearch(config.es_nodes)
else:
    es = elasticsearch.Elasticsearch()
submiter = Submitter(es)


def put_log(type, package, version, data):
    data['time'] = datetime.datetime.utcnow()
    submiter.buffer_action({
        '_index': 'log',
        '_type': '%s|%s|%s' % (type, package, version),
        '_ttl': {'enabled': True, 'default': '3d'},
        '_source': data
    })

# {
#             'time': datetime.datetime.utcnow(),
#             'channel': channel,
#             'model': model,
#             'udid': udid,
#             'os': os,
#             'detail': detail,
#         }


class LuanchLogHandler(tornado.web.RequestHandler):
    def post(self, package, version):
        data = json.loads(self.request.body)
        for key in ['model', 'udid', 'channel' 'os']:
            if not key in data:
                return
        put_log('launch', package, version, data)


class CrashLogHandler(tornado.web.RequestHandler):
    def post(self, package, version):
        data = json.loads(self.request.body)
        for key in ['model', 'udid', 'channel', 'os', 'detail']:
            if not key in data:
                return
        put_log('crash', package, version, data)


application = tornado.web.Application([
    (r'/luanch/([^/]+)/([^/]+)', LuanchLogHandler),
    (r'/crash/([^/]+)/([^/]+)', CrashLogHandler)
])


def init_logging():
    root = logging.getLogger()
    root.setLevel(logging.ERROR)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.ERROR)
    root.addHandler(handler)

if __name__ == '__main__':
    init_logging()
    application.listen(config.port)
    submiter.start()
    tornado.ioloop.IOLoop.instance().start()
