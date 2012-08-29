#!/usr/bin/python

import tq
import redis

import socket
import os
import time

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
					tq.taskqueue.work(worker.hash, task)
					break
			time.sleep(1)

	except KeyboardInterrupt:
		worker.unregister(queues)

#-------------------------------------------------------------------------------

r = redis.Redis(host='localhost')

if __name__ == '__main__':
	main()

#-------------------------------------------------------------------------------
