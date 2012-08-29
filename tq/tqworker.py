#!/usr/bin/python

import tq
import redis
import importlib

import socket
import os
import time
import pickle
import traceback

#-------------------------------------------------------------------------------

class Worker(object):
	
	def __init__(self):
		self.hash = os.urandom(8).encode('hex')

	def register(self, queues):
		dict = {}
		name = socket.gethostname()
		if name.endswith('.local'):
			name = 'local'
		pid  = os.getpid()
	
		dict['name'] = name
		dict['pid']  = pid
	
		r.hmset('tq:worker:%s' % self.hash, dict)
		r.sadd('tq:workers', self.hash)
		for q in queues:
			r.sadd('tq:queue:%s:workers' % q, self.hash)

	def unregister(self, queues):

		# push any unfinished tasks on failed

		id = r.hget('tq:worker:%s' % self.hash, 'task')
		if id is not None:
			tb = traceback.format_exc()
			r.hset('tq:task:%s' % id, 'traceback', tb)
			r.rpush('tq:queue:failed:fifo', id)

		# unregister worker		

		for q in queues:
			r.srem('tq:queue:%s:workers' % q, self.hash)
		r.srem('tq:workers', self.hash)
		r.delete('tq:worker:%s' % self.hash)

#-------------------------------------------------------------------------------

def work(worker, id):

	try:
		dict = r.hgetall('tq:task:%s' % id)
		func_name, args, kwargs = pickle.loads(dict['data'])

		module_name, func_name = func_name.rsplit('.', 1)
		module = importlib.import_module(module_name)

	#	arg_list = [repr(arg) for arg in args]
	#	arg_list += ['%s=%r' % (k, v) for k, v in kwargs.items()]
	#	arguments = ', '.join(arg_list)
	#	print '%s(%s)' % (func_name, arguments)

		now = int(time.time())
		r.hset('tq:task:%s' % id, 'started', now)
		r.hset('tq:worker:%s' % worker, 'task', id)

		result = getattr(module, func_name)(*args, **kwargs)
	except Exception:
		tb = traceback.format_exc()
		r.hset('tq:task:%s' % id, 'traceback', tb)
		r.rpush('tq:queue:failed', id)
	else:
		r.hset('tq:task:%s' % id, 'result', result)

	r.hdel('tq:worker:%s' % worker, 'task')
	return True

#-------------------------------------------------------------------------------

def main():
	if 0:
		keys = r.keys('tq:*')
		for k in keys:
			r.delete(k)
		return

	#	register worker

	queues = ['high', 'default', 'low']
	worker = Worker()
	worker.register(queues)

	#	work

	try:
		while True:
			for q in queues:
				task = tq.taskqueue.dequeue(q)
				if task is not None:
					work(worker.hash, task)
					break
			time.sleep(1)

	except KeyboardInterrupt:
		worker.unregister(queues)

#-------------------------------------------------------------------------------

r = redis.Redis(host='localhost')

if __name__ == '__main__':
	main()

#-------------------------------------------------------------------------------
