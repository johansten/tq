
import importlib
import redis
import pickle
from datetime import datetime
import time

import sys
import inspect
import os.path
import traceback

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

#-------------------------------------------------------------------------------

r = redis.Redis(host='localhost')

#-------------------------------------------------------------------------------

class Task(object):

	def __init__(self, func, *args, **kwargs):
		self.func	= func
		self.args	= args
		self.kwargs	= kwargs

#-------------------------------------------------------------------------------

def add_task(q, task, **kwargs):

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

	id = r.incr('tq:task:last_id')
	key = 'tq:task:%s' % id

	now = int(time.time())
	dict = {}
	dict['data']   = data
	dict['queued'] = now
	dict['queue']  = q
	r.hmset(key, dict)

	if 'time' in kwargs:
		t = kwargs['time']
		when = time.mktime(t.timetuple())
		r.zadd('tq:queue:%s:delayed' % q, id, when)
	elif 'delay' in kwargs:
		when = now + kwargs['delay']
		r.zadd('tq:queue:%s:delayed' % q, id, when)
	else:
		r.rpush('tq:queue:%s:fifo' % q, id)

	return id

#-------------------------------------------------------------------------------

def add(q, func, *args, **kwargs):
	task	= Task(func, *args, **kwargs)
	result	= add_task(q, task)
	return result

#-------------------------------------------------------------------------------

def dequeue(q):

	fifo  = 'tq:queue:%s:fifo' % q
	delay = 'tq:queue:%s:delayed' % q

	if r.zcard(delay):
		now = int(time.time())
		id, when = r.zrange(delay, 0, 0, withscores = True)[0]
		if now >= when:
			r.zrem(delay, id)
			return id

	id = r.lpop(fifo)
	if id is not None:
		return id

	return

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