#!/usr/bin/python

import cherrypy
import redis
import os
import re
import pickle
import simplejson as json
import time

from mako.template import Template
from mako.lookup import TemplateLookup

#lookup = TemplateLookup(directories=['templates'])
#lookup = TemplateLookup(directories=['/var/www/trends.kanadaforum.se/templates'])

#-------------------------------------------------------------------------------
#	John J. Lee, http://www.velocityreviews.com/forums/t511850-how-do-you-htmlentities-in-python.html
#-------------------------------------------------------------------------------

import htmlentitydefs
import re

def unescape_charref(ref):
	name = ref[2:-1]
	base = 10
	if name.startswith("x"):
		name = name[1:]
		base = 16
	return unichr(int(name, base))

def replace_entities(match):
	ent = match.group()
	if ent[1] == "#":
		return unescape_charref(ent)
	
	repl = htmlentitydefs.name2codepoint.get(ent[1:-1])
	if repl is not None:
		repl = unichr(repl)
	else:
		repl = ent
	return repl

def unescape(data):
	return re.sub(r"&#?[A-Za-z0-9]+?;", replace_entities, data)

#-------------------------------------------------------------------------------

class Queues:

	@cherrypy.expose
	def index(self):
		keys = r.keys('tq:queue:*:workers')
		queues = set()
		for k in keys:
			t = k.split(':')
			queues.add(t[2])

		q_list = []
		for q in list(queues):
			size  = 0
			size += r.llen('tq:queue:%s:fifo' % q)
			size += r.zcard('tq:queue:%s:delayed' % q)
			q_list.append((q, size))

		size = r.llen('tq:queue:failed')
		q_list.append(('failed', size))

		return json.dumps(q_list)

class Tasks:
	
	@cherrypy.expose
	def index(self):

		now = int(time.time())

		t_list = []
		workers = r.smembers('tq:workers')
		for w in workers:
			name = r.hget('tq:worker:%s' % w, 'name')
			pid  = r.hget('tq:worker:%s' % w, 'pid')
			task = r.hget('tq:worker:%s' % w, 'task')
			if task is not None:
				data    = r.hget('tq:task:%s' % task, 'data')
				queue   = r.hget('tq:task:%s' % task, 'queue')
				started = r.hget('tq:task:%s' % task, 'started')
				started = now - int(started)
				func_name, args, kwargs = pickle.loads(data)
				t_list.append(('%s(%s)' % (name, pid), queue, func_name, started))

		return json.dumps(t_list)

class Root:

	tasks  = Tasks()
	queues = Queues()

	@cherrypy.expose
	def queue(self, q):

		f_list = []
		size = r.llen('tq:queue:%s:fifo' % q)
		fifo = r.lrange('tq:queue:%s:fifo' % q, 0, size)
		for task in fifo:
			data = r.hget('tq:task:%s' % task, 'data')
			func_name, args, kwargs = pickle.loads(data)
			f_list.append((task, func_name))

		s_list = []
		size = r.zcard('tq:queue:%s:delayed' % q)
		sched = r.zrange('tq:queue:%s:delayed' % q, 0, size)
		for task in sched:
			data = r.hget('tq:task:%s' % task, 'data')
			func_name, args, kwargs = pickle.loads(data)
			s_list.append((task, func_name))

		tmpl = lookup.get_template("queue.html")
		return tmpl.render(qname=q, fifo=f_list, sched=s_list)

	@cherrypy.expose
	def index(self):
		keys = r.keys('tq:queue:*')
		queues = set()
		for k in keys:
			t = k.split(':')
			queues.add(t[2])

		q_list = []
		for q in list(queues):
			size  = r.llen('tq:queue:%s:fifo' % q)
			size += r.zcard('tq:queue:%s:delayed' % q)
#			q_list.append((q, size))

		w_list = []
		t_list = []
		workers = r.smembers('tq:workers')
		for w in workers:
			name = r.hget('tq:worker:%s' % w, 'name')
			pid  = r.hget('tq:worker:%s' % w, 'pid')
			task = r.hget('tq:worker:%s' % w, 'task')
			if task is not None:
				data = r.hget('tq:task:%s' % task, 'data')
				func_name, args, kwargs = pickle.loads(data)
#				t_list.append(('%s(%s)' % (name, pid), func_name))
			else:
				func_name = 'Idle'

#		out += '</html>'

#		return out.encode('utf-8')
		tmpl = lookup.get_template("index.html")
		return tmpl.render(queues=q_list, workers=w_list, tasks=t_list)

#-------------------------------------------------------------------------------

r = redis.Redis(host='localhost')
cwd = os.getcwd()

lookup = TemplateLookup(directories=[os.path.join(cwd, 'templates')])
root = Root()

conf = {
	'/static': {
		'tools.staticdir.on': True,
		'tools.staticdir.dir': os.path.join(cwd, 'static')
	},
}

cherrypy.quickstart(root, '/', config=conf)

#-------------------------------------------------------------------------------
