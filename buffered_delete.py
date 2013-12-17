#!/usr/bin/env python 
#
# delete random data from large tables in batches. 
# Waits for slaves to sync up before continuing with the next batch
# uses text file for configuration
#

import os
import sys
import MySQLdb
import getpass
import time

dblist = []
config = {"sleeptime": 1.0, "debuglevel": 0, "testing": 0, "failsafe": 0}
required = ['user', 'pass', 'server', 'port', 'db', 'table', 'column', 'chunksize', 'query']
intparams = ['port', 'debuglevel', 'testing', 'failsafe'] 
floatparams = ['sleeptime']
hideparams = ['pass']

def mydebug(level, debugstr):
	if (level <= config['debuglevel']):
		print >> sys.stderr, debugstr
	return


def usage(name):
	print "usage: %s <configfile> [option1=a option2=b ... optionn=xyz]" % (name, )
	print "command line options override config file options"
	print "valid numeric options:",
	everything = sorted(config.keys() + required)
	for x in sorted(intparams + floatparams):
		print "%s" % (x, ),
		everything.remove(x)
	print "\nvalid string options:",
	for x in everything:
		print "%s" % (x, ),
	print ""


def parse_command_line_args(argv):
	global config
	for x in argv:
		mydebug(2, "arg %s" % (x,))
		try:
			(param, value) = x.split("=")
			mydebug(2, "arg %s param %s value %s" % (x, param, value))
			if param in intparams:
				config[param] = int(value)
			elif param in floatparams:
				config[param] = float(value)
			else:
				config[param] = value
		except:
			mydebug(0, "%s is an invalid option" % (x, ))


def load_config(filename):
	global config
	try:
		infile = open(filename, "r")
	except:
		mydebug(0, "open of config file '%s' failed, aborting" % (filename, ))
		sys.exit(1)

	for line in infile:
		cleanline = line.lstrip()
		if cleanline[0] == '#':
			continue
		z = cleanline.split()
		if z[0] == "check":
			add_slave(z[1:])
		else:
			if z[0] in intparams:
				config[z[0]] = int(" ".join(z[1:]))
			elif z[0] in floatparams:
				config[z[0]] = float(" ".join(z[1:]))
			else:
				config[z[0]] = " ".join(z[1:])
		# /else
	# /for

	return
# /load_config()


def check_config():
	for z in required:
		if z not in config:
			mydebug(0, "item %s missing in config" % (z,))
			sys.exit(1)
		else:
			if z in hideparams:
				mydebug(1, "%s: **********" % (z, ))
			else:
				mydebug(1, "%s: %s" % (z, config[z]))
	return


def print_slave_list():
	for z in dblist:
		mydebug(1, "server %s port %i db %s" % (z['name'], z['port'], z['db']))
	return


def add_slave(slaveinfo):
	global dblist
	x = {}

	if len(slaveinfo) < 5:
		mydebug(0, "slave %s misconfigured, not enough parameters (got %i, need 4)" % (slaveinfo[0], len(slaveinfo) - 1))
		return

	mydebug(2, "adding server %s" % (slaveinfo[0],))
	x['name'] = slaveinfo[0]
	try:
		x['port'] = int(slaveinfo[1])
	except:
		mydebug(0, "port must be numeric (%s)" % (x['port'],))
		sys.exit(1)
	x['user'] = slaveinfo[2]
	x['pass'] = slaveinfo[3]
	x['db'] = slaveinfo[4]
	x['dbhandle'] = connect_to_server(host = x['name'], port = x['port'], user = x['user'], passwd = x['pass'], db = x['db'])

	dblist.append(x)
	return
# /add_slave()


def check_slave_servers(checkval):
	for x in dblist:
		mydebug(2, "checking slave %s for %i" % (x['name'], checkval))
		if x['dbhandle'] == False:
			x['dbhandle'] = connect_to_server(host = x['name'], port = x['port'], user = x['user'], passwd = x['pass'], db = x['db'])

		if x['dbhandle'] != False:
			cursor = x['dbhandle'].cursor()
			q = "select %s from %s where %s = %i" % (config['column'], config['table'], config['column'], checkval)
			mydebug(3, q)
			cursor.execute(q)
			colval = cursor.fetchone()
			cursor.close()
			# apparently this needs a commit, even though it's another thread..
			x['dbhandle'].commit()
			mydebug(3, colval)
			if colval != None:
				mydebug(1, "server %s still has %s = %i: %s" % (x['name'], config['column'], checkval, colval[0]))
				return 1
			# else continue.. 
			else:
				mydebug(2, "server %s has deleted %s = %i" % (x['name'], config['column'], checkval))
		else:
			if config['failsafe'] != 0:
				return 1

	return 0
# /check_slave_servers()


def connect_to_server(host, port, user, passwd, db):
	try:
		mydb = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db)
	except MySQLdb.Error, e:
		mydebug(0, "connection to server %s:%i failed: %s" % (host, port, e[1]))
		return False
	return mydb


def connect_to_server_with_cursor(host, port, user, passwd, db):
	mydb = connect_to_server(host, port, user, passwd, db)
	if mydb == False:
		return(False, False)

	try:	
		dbcurs = mydb.cursor()
	except MySQLdb.Error, e:
		mydebug(0, "create cursor on server %s:%i failed: %s" % (host, port, e[1]))
		mydb.close()
		return (False, False)
	
	return (mydb, dbcurs)


def do_delete():
	totalcount = 0
	(mydb, dbcurs) = connect_to_server_with_cursor(host=config['server'], port=config['port'], user=config['user'], passwd=config['pass'], db=config['db'])
	if mydb == False:
		mydebug(0, "no DB connection, can't do much.")
		return 0

	havedata = 1
	while havedata == 1:
		# order by id is more than an order of magnitude faster when delete time comes. 
		# less seeking, pages cached already, etc.
		query = "select %s from %s where %s order by %s limit %s" % (config['column'], config['table'], config['query'], config['column'], config['chunksize'])
		mydebug(1, query)
		dbcurs.execute(query)
		
		ids = dbcurs.fetchall()
		# all done?
		if len(ids) == 0:
			dbcurs.close()
			mydb.close()
			return totalcount

		minid = ids[0][0]
		maxid = ids[len(ids) - 1][0]	
		idslist = ""
		count = 0
		for i in ids:
			idslist += str(i[0]);
			count += 1
			totalcount += 1
			if count != len(ids):
				idslist += ","
	
		# build list	
		query = "delete from %s where %s in (%s)" % (config['table'], config['column'], idslist)
		mydebug(1, "delete from %s where %s in ([%i members from %i to %i])" % (config['table'], config['column'], len(ids), minid, maxid))
		# use "between" sql.. slower, strangely. 
		#query = "delete from %s where %s between %i and %i" % (config['table'], config['column'], minid, maxid)
		#mydebug(1, query)
		
		checkid = maxid
		mydebug(2, "id to check: %i" % (checkid,))
		#outfile = open("output", "w");
		#outfile.write(query)
		#outfile.close()
		if config['testing'] != 0 and config['debuglevel'] >= 1:
			ok = raw_input("ok? y/n: ")
		else:
			ok = "y"
		if ok == "y":
			starttime = time.time()
			dbcurs.execute(query)
			mydb.commit()
			elapsedtime = time.time() - starttime
			mydebug(1, "elapsed time: %.3f" % (elapsedtime,))
		else:
			print "aborting"
			sys.exit(1)

		# sleep in this function until slaves catch up
		sleepmore = 1
		if elapsedtime < config['sleeptime']:
			time.sleep(config['sleeptime'] - elapsedtime);
		startcheck = time.time()
		while sleepmore == 1:
			sleepmore = check_slave_servers(checkid)
			if sleepmore:
				time.sleep(config['sleeptime'])

		mydebug(1, "slave delete took %.3f seconds" % (time.time() - startcheck, ))
		mydebug(1, "total delete took %.3f seconds, %.2f records/second" % (time.time() - starttime, int(config['chunksize']) / (time.time() - starttime)))
		# test/debug
		#havedata = 0	
	# /while
# /do_delete()

def do_lockfile(myname, config, create):
	lockfile = "/var/tmp/" + "".join(x for x in myname if x.isalnum()) + "." + "".join(y for y in config if y.isalnum()) + ".lock"
	docreate = 0

	#mydebug(0, "lockfile name %s, I am %i" % (lockfile, os.getpid()))

	if os.path.exists(lockfile):
		if create == 0:
			try:
				os.unlink(lockfile)
			except:
				mydebug(0, "couldn't remove lockfile")
			return

		try:
			f = open(lockfile, "r")
		except:
			mydebug(0, "can't open existing lock file %s" % (lockfile, ))
			sys.exit(1)

		pid = f.readline().strip()
		f.close()
		try:
			os.kill(int(pid), 0)
			mydebug(0, "pid %s is already running this job" % (pid, ))
			sys.exit(1)
		except OSError, e:
			mydebug(0, "this job was pid %s, but that process is gone" % (pid, ))
			docreate = 1
	else:
		docreate = 1

	if create == 1 and docreate == 1:
		try:
			f = open(lockfile, "w")
			f.write(str(os.getpid()))
			f.close()
		except IOError, e:
			mydebug(0, "can't create lockfile %s (%s)" % (lockfile, e[0]))
			sys.exit(1)



if __name__ == "__main__":
	if len(sys.argv) < 2:
		usage(sys.argv[0])
		sys.exit(1)

	do_lockfile(sys.argv[0], sys.argv[1], 1)
	load_config(sys.argv[1])
	if len(sys.argv) > 2:
		parse_command_line_args(sys.argv[2:])
	check_config()
	print_slave_list()
	deleted = do_delete()
	print "deleted %i rows from table %s" % (deleted, config['table'])
	do_lockfile(sys.argv[0], sys.argv[1], 0)
	sys.exit(0)
