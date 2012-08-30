
import redis
import pickle
from datetime import datetime
import time

import sys
import inspect
import os.path

#-------------------------------------------------------------------------------
#	tq:task:{id}				hash
#	
#	tq:queue:{name}				list
#			task_id
#	tq:queue:{name}:delayed		zset
#			time:task_id
#	tq:finished					set
#			task_id
#-------------------------------------------------------------------------------

#	TODO: need to figure out how to initialize this properly
_r = redis.Redis(host='localhost')

#-------------------------------------------------------------------------------

class Task(object):

	def __init__(self, func, *args, **kwargs):
		self.func	= func
		self.args	= args
		self.kwargs	= kwargs

#-------------------------------------------------------------------------------

class Queue(object):

	def __init__(self, name):
		self.name = name

	def add_task(self, task, **kwargs):
		module_name	= task.func.__module__
		func_name	= task.func.__name__

		if module_name == "__main__":
			stack = inspect.stack()
			for frame in stack:
				filename 	= frame[1]
				filename	= os.path.basename(filename)
				module_name	= filename.split('.')[0]
				if not __name__.endswith(module_name):
					break

		func_name = '%s.%s' % (module_name, func_name)
		data = pickle.dumps((func_name, task.args, task.kwargs))

		id = _r.incr('tq:task:last_id')
		key = 'tq:task:%s' % id

		now = int(time.time())
		dict = {}
		dict['data']   = data
		dict['queued'] = now
		dict['queue']  = self.name
		_r.hmset(key, dict)

		if 'time' in kwargs:
			t = kwargs['time']
			when = time.mktime(t.timetuple())
			_r.zadd('tq:queue:%s:delayed' % self.name, id, when)
		elif 'delay' in kwargs:
			when = now + kwargs['delay']
			_r.zadd('tq:queue:%s:delayed' % self.name, id, when)
		else:
			_r.rpush('tq:queue:%s:fifo' % self.name, id)

		return id

	def dequeue(self):
		fifo  = 'tq:queue:%s:fifo' % self.name
		delay = 'tq:queue:%s:delayed' % self.name

		if _r.zcard(delay):
			now = int(time.time())
			id, when = _r.zrange(delay, 0, 0, withscores = True)[0]
			if now >= when:
				_r.zrem(delay, id)
				return id

		id = _r.lpop(fifo)
		if id is not None:
			return id

		return

#-------------------------------------------------------------------------------

def add_task(q, task, **kwargs):
	return Queue(q).add_task(task, **kwargs)
#-------------------------------------------------------------------------------

def add(q, func, *args, **kwargs):
	task	= Task(func, *args, **kwargs)
	result	= add_task(q, task)
	return result

#-------------------------------------------------------------------------------

def dequeue(q):
	return Queue(q).dequeue()

#-------------------------------------------------------------------------------
