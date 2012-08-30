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
	
	def __init__(self, r, queues):
		self.hash	= os.urandom(8).encode('hex')
		self.r		= r
		self.queues = queues

	def register(self):
		dict = {}
		name = socket.gethostname()
		if name.endswith('.local'):
			name = 'local'
		pid  = os.getpid()
	
		dict['name'] = name
		dict['pid']  = pid
	
		self.r.hmset('tq:worker:%s' % self.hash, dict)
		self.r.sadd('tq:workers', self.hash)
		for q in self.queues:
			self.r.sadd('tq:queue:%s:workers' % q, self.hash)

	def unregister(self):

		# push any unfinished tasks on failed

		id = self.r.hget('tq:worker:%s' % self.hash, 'task')
		if id is not None:
			tb = traceback.format_exc()
			self.r.hset('tq:task:%s' % id, 'traceback', tb)
			self.r.rpush('tq:queue:failed:fifo', id)

		# unregister worker		

		for q in self.queues:
			self.r.srem('tq:queue:%s:workers' % q, self.hash)
		self.r.srem('tq:workers', self.hash)
		self.r.delete('tq:worker:%s' % self.hash)

	def perform_task(self, id):

		try:
			dict = self.r.hgetall('tq:task:%s' % id)
			func_name, args, kwargs = pickle.loads(dict['data'])

			module_name, func_name = func_name.rsplit('.', 1)
			module = importlib.import_module(module_name)

		#	arg_list = [repr(arg) for arg in args]
		#	arg_list += ['%s=%r' % (k, v) for k, v in kwargs.items()]
		#	arguments = ', '.join(arg_list)
		#	print '%s(%s)' % (func_name, arguments)

			now = int(time.time())
			self.r.hset('tq:task:%s' % id, 'started', now)
			self.r.hset('tq:worker:%s' % self.hash, 'task', id)

			result = getattr(module, func_name)(*args, **kwargs)

		except Exception:
			tb = traceback.format_exc()
			self.r.hset('tq:task:%s' % id, 'traceback', tb)
			self.r.rpush('tq:queue:failed', id)

		else:
			self.r.hset('tq:task:%s' % id, 'result', result)

		self.r.hdel('tq:worker:%s' % self.hash, 'task')
		return True

	def work(self):

		self.register()

		try:
			while True:
				for q in self.queues:
					task = tq.taskqueue.dequeue(q)
					if task is not None:
						self.perform_task(task)
						break
				time.sleep(1)

		except KeyboardInterrupt:
			self.unregister()

#-------------------------------------------------------------------------------

def main():

	r = redis.Redis(host='localhost')

	if 1:
		keys = r.keys('tq:*')
		for k in keys:
			r.delete(k)

	#	register worker

	queues = ['high', 'default', 'low']
	worker = Worker(r, queues)
	worker.work()

#-------------------------------------------------------------------------------

if __name__ == '__main__':
	main()

#-------------------------------------------------------------------------------
