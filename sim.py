#!/usr/bin/env python
import random
import simpy


RANDOM_SEED = 42
REPAIR_MEAN = 60.0 * 60    	# Avg. repair time in seconds
REPAIR_SIGMA = 5.0        	# Sigma of processing time
MTTF = 30.0 * 60 * 60  		# Mean time to failure in seconds
BREAK_MEAN = 1 / MTTF  		# Param. for expovariate distribution
WEEKS = 120             	# Simulation time in weeks
SIM_TIME = WEEKS * 7 * 24 * 60 * 60 	# Simulation time in seconds
MAINTAIN_GAP = 7 * 24 * 60 * 60
MAINTAIN_DURATION = 8 * 60 * 60 


NUM_NODE = 4
NUM_CLUSTER = 3
NUM_JOB = 5
NUM_REPLICA = 2
NUM_TASK = 1
NUM_ZONE = 4
NUM_LOCATION = 3

joblist = []
replicalist = []
tasklist = []
nodelist = []
clusterlist = []

class Job(object):
	def __init__(self, env, jid, replicalist):
		self.env = env
		self.jid = jid
		self.replicalist = list(replicalist)
		self.failure = False
		self.action = env.process(self.run())

	def run(self):
		print('Job %d starts running at %d' % (self.jid, env.now))
		while True:
			try:
				yield self.env.timeout(SIM_TIME)
			except simpy.Interrupt as i:
				if (i.cause == 'RD'):
					self.failure = True
					for replica in self.replicalist:
						if(replica.failure == False):
							self.failure = False
					if (self.failure == True):
						print('Job %d stops running at %d' % (self.jid, env.now))
				if (i.cause == 'RU'):
					if(self.failure == True):
						self.failure = False
						print('Job %d restarts running at %d' % (self.jid, env.now))

class Replica(object):
	def __init__(self, env, rid, jid, cid, tasklist):
		self.env = env
		self.rid = rid
		self.jid = jid
		self.cid = cid
		self.tasklist = list(tasklist)
		self.failure = False
		self.action = env.process(self.run())

	def run(self):
		print('Replica %d starts running at %d' % (self.rid, env.now))
		while True:
			try:
				yield self.env.timeout(SIM_TIME)
			except simpy.Interrupt as i:
				if (i.cause == 'TD'):
					self.failure = True
					for task in self.tasklist:
						if(task.failure == False):
							self.failure = False
					if (self.failure == True):
						print('Replica %d stops running at %d' % (self.rid, env.now))
						joblist[self.jid].action.interrupt('RD')
				if (i.cause == 'TU'):
					if(self.failure == True):
						self.failure = False
						print('Replica %d restarts running at %d' % (self.rid, env.now))
						joblist[self.jid].action.interrupt('RU')			

class Task(object):
	def __init__(self, env, tid, rid, nid):
		self.env = env
		self.tid = tid
		self.rid = rid
		self.nid = nid
		self.failure = False
		self.action = env.process(self.run())

	def run(self):
		print('Task %d starts running at %d' % (self.tid, env.now))
		while True:           
			try:
				yield self.env.timeout(SIM_TIME)

			except simpy.Interrupt as i:
				if (i.cause == 'FS'):
					self.failure = True
					print('Task %d stops running at %d' % (self.tid, env.now))
					replicalist[self.rid].action.interrupt('TD')
				if (i.cause == 'FE'):
					self.failure = False
					print('Task %d restarts running at %d' % (self.tid, env.now))
					replicalist[self.rid].action.interrupt('TU')

class Node(object):

	def __init__(self, env, nid, cid, tid):
		self.env = env
		self.nid = nid
		self.cid = cid
		self.tid = tid
		self.maintain = False
		self.down = False
		self.action = env.process(self.working())
		env.process(self.node_down())

	def working(self):
		"""Node is up as long as the simulation runs.

		While a failure happens, node stop working for some time period 
		and terminate the running task on it.

		"""
		print('Node %d starts running at %d'% (self.nid, env.now))
		while True:           

			try:
				yield self.env.timeout(SIM_TIME)
			except simpy.Interrupt as i:
				if (i.cause == 'FS'):
					self.down = True
					if(self.tid!=-1):
						tasklist[self.tid].action.interrupt('FS')
				if (i.cause == 'FE' and self.maintain == False):
					if(self.tid!=-1):
						tasklist[self.tid].action.interrupt('FE')
					self.down = False

	def node_down(self):
		"""Break the machine every now and then."""
		while True:
			yield self.env.timeout(time_to_failure())
			if not self.down:
				# Only break the machine if it is currently working.
				self.action.interrupt('FS')
				yield self.env.timeout(time_to_repair())
				self.action.interrupt('FE')


class Cluster(object):

	def __init__(self, env, cid, pid, zid, nodelist, capacity):
		self.env = env
		self.cid = cid
		self.pid = pid
		self.zid = zid
		self.avail = capacity
		self.nodelist = list(nodelist)
		self.down = False
		self.action = env.process(self.working())

	def working(self):
		print('Cluster %d starts running at %d' % (self.cid, env.now))		
		while True:           

			try:
				yield self.env.timeout(SIM_TIME)

			except simpy.Interrupt as i:
				if (i.cause == 'MS'):
					self.down = True
					print('Cluster %d starts maintainance at %d' %(self.cid, env.now))
					for node in self.nodelist:
						node.maintain = True
						node.action.interrupt('FS')
				if (i.cause == 'ME'):
					print('Cluster %d ends maintainance at %d' %(self.cid, env.now))
					for node in self.nodelist:
						node.maintain = False
						node.action.interrupt('FE')
					self.down = False		


def time_to_repair():
	"""Return repairing time for a unexpected failure."""
	return random.normalvariate(REPAIR_MEAN, REPAIR_SIGMA)


def time_to_failure():
	"""Return time until next unexpected failure."""
	return random.expovariate(BREAK_MEAN)

def assignReplica(cluster, replica):
	i=0
	j=0
	while(i < len(replica.tasklist) and j < len(cluster.nodelist)):
		if(cluster.nodelist[j].tid == -1):
			cluster.nodelist[j].tid = replica.tasklist[i].tid
			replica.tasklist[i].nid = cluster.nodelist[j].nid
			cluster.avail -= 1
			# print 'map task %d on node %d' %(replica.tasklist[i].tid, cluster.nodelist[j].nid)
			i += 1
			j += 1
		else:
			j += 1
	replica.cid = cluster.cid

def findAvailCluster(replica):
	avail=[]
	for cluster in clusterlist:
		if (cluster.avail >= len(replica.tasklist)):
			avail.append(cluster.cid)
	if not avail:
		return -1
	else:
		index = random.randint(0, len(avail)-1)
		return avail[index]

def maintainanceHandler(env):
	zid = 0
	while True:
		#maintainance
		yield env.timeout(MAINTAIN_GAP)
		zid = zid%NUM_ZONE
		# print('start maintainance on zone %d at %d' % (zid, env.now))
		for cluster in clusterlist:
			if(cluster.zid == zid):
				cluster.action.interrupt('MS')

		yield env.timeout(MAINTAIN_DURATION)
		# print('done maintainance on zone %d at %d' % (zid, env.now))
		for cluster in clusterlist:
			if(cluster.zid == zid):
				cluster.action.interrupt('ME')
		zid += 1

def createResource(env):
	# nodes:
	temp1 = []
	temp2 = []
	id1=0
	for c in range(NUM_CLUSTER):
		for i in range(NUM_NODE):
			aNode = Node(env, id1, c, -1)
			nodelist.append(aNode)
			id1 += 1
			temp1.append(aNode)
		clusterlist.append(Cluster(env, c, c/(NUM_CLUSTER/NUM_LOCATION), c%NUM_ZONE, temp1, NUM_NODE))
		del temp1[:]

	# jobs
	id1=0
	id2=0
	for j in range(NUM_JOB):
		for r in range(NUM_REPLICA):
			for t in range(NUM_TASK):
				aTask = Task(env, id1, id2, -1)
				tasklist.append(aTask)
				id1+=1
				temp1.append(aTask)
			aReplica = Replica(env, id2, j, -1, temp1)
			del temp1[:]
			replicalist.append(aReplica)
			id2+=1
			temp2.append(aReplica)
		joblist.append(Job(env, j, temp2))
		del temp2[:]

	#map jobs to nodes
	for job in joblist:
		for r in range(len(job.replicalist)):
			cid = findAvailCluster(job.replicalist[r])
			if (cid != -1):
				print 'add replica %d (Job %d) to cluster %d' %(job.replicalist[r].rid, job.jid, cid)
				assignReplica(clusterlist[cid], job.replicalist[r])


random.seed(RANDOM_SEED)
# Create an environment and start the setup process
env = simpy.Environment()
createResource(env)
# for node in nodelist:
# 	print node.tid
# for r in replicalist:
# 	print r.rid
env.process(maintainanceHandler(env))

# Execute!
env.run(until=SIM_TIME)

