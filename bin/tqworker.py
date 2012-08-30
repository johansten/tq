#!/usr/bin/python

import tq
import redis

#-------------------------------------------------------------------------------

#tqworker.py --host=localhost --port=6379 --db=0 --queues=high, default, low

def main():

	r = redis.Redis(host='localhost')

#	keys = r.keys('tq:*')
#	for k in keys:
#		r.delete(k)

	#	register worker

	queues = ['high', 'default', 'low']
	worker = tq.Worker(r, queues)
	worker.work()

#-------------------------------------------------------------------------------

if __name__ == '__main__':
	main()

#-------------------------------------------------------------------------------
