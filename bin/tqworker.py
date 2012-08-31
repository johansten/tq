#!/usr/bin/python

import tq
import redis
import argparse

#-------------------------------------------------------------------------------

def main():

	parser = argparse.ArgumentParser()

	parser.add_argument("--host",
		default='localhost',
		help="hostname of the redis instance")

	parser.add_argument("--port",
		type=int,
		default=6379,
		help="port number of the redis instance")

	parser.add_argument("--db",
		type=int,
		default=0,
		help="db number of the redis instance")

	parser.add_argument('queues',
		metavar='queue',
		nargs='+',
		help='name of a queue to process')

	args = parser.parse_args()

	r = redis.Redis(host=args.host, port=args.port, db=args.db)
	worker = tq.Worker(r)
	worker.work(args.queues)

#-------------------------------------------------------------------------------

if __name__ == '__main__':
	main()

#-------------------------------------------------------------------------------
