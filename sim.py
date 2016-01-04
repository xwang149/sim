#!/usr/bin/env python
import random
import math
import simpy
from optparse import OptionParser

RANDOM_SEED = 42
REPAIR_MEAN = 450.0 * 60        # Avg. repair time in seconds
REPAIR_SIGMA = 4000.0            # Sigma of processing time
MTTF = 30.0 * 60 * 60          # Mean time to failure in seconds
BREAK_MEAN = 1 / MTTF          # Param. for expovariate distribution
WEEKS = 120                 			# Default simulation time in weeks
SIM_TIME = WEEKS * 7 * 24 * 60 * 60     # Simulation time in seconds
OUTPUT = "./output.log"					# Default output log file

joblist = {}
replicalist = {}
tasklist = {}
nodelist = {}
clusterlist = {}

class Job(object):
    def __init__(self, env, jid, replicalist):
        self.env = env
        self.jid = jid
        self.replicalist = list(replicalist)
        self.failure = False
        self.action = env.process(self.run())

    def run(self):
        print('Job %d starts running at %d' % (self.jid, env.now))
        outfile = open(OUTPUT, "a")
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
                        outfile.write("%s,%s,STOP" % (env.now, self.jid))
                if (i.cause == 'RU'):
                    if(self.failure == True):
                        self.failure = False
                        print('Job %d restarts running at %d' % (self.jid, env.now))
                        outfile.write("%s,%s,RESTART" % (env.now, self.jid))
                outfile.close()

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
        # alive = 0
        # threshold = math.ceil(len(self.tasklist)/2.0)
        outfile = open(OUTPUT, "a")
        while True:
            try:
                yield self.env.timeout(SIM_TIME)
            except simpy.Interrupt as i:
                if (i.cause == 'TD'):
                    self.failure = True
                    for task in self.tasklist:
                        if(task.failure == False):
                            self.failure = False
                    #         alive += 1
                    # if (alive < threshold):
                    # 	outfile.write("%s,%s,%s,DOWNGRADE" % (env.now, self.jid,self.rid))
                    if (self.failure == True):
                        print('Replica %d stops running at %d' % (self.rid, env.now))
                        outfile.write("%s,%s,%s,STOP" % (env.now, self.jid,self.rid))
                        joblist[self.jid].action.interrupt('RD')
                if (i.cause == 'TU'):
                    if(self.failure == True):
                        self.failure = False
                        print('Replica %d restarts running at %d' % (self.rid, env.now))
                        outfile.write("%s,%s,%s,RESTART" % (env.now, self.jid,self.rid))
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
        # env.process(self.node_down())

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

    # def node_down(self):
    #     """Break the machine every now and then."""
    #     while True:
    #         yield self.env.timeout(time_to_failure())
    #         if not self.down:
    #             # Only break the machine if it is currently working.
    #             self.action.interrupt('FS')
    #             yield self.env.timeout(time_to_repair())
    #             self.action.interrupt('FE')


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
                if (i.cause == 'FS'):
                    self.down = True
                    print('Cluster %d stop working at %d' %(self.cid, env.now))
                    for node in self.nodelist:
                        node.maintain = True
                        node.action.interrupt('FS')
                if (i.cause == 'FE'):
                    print('Cluster %d restart working at %d' %(self.cid, env.now))
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
    for cluster in clusterlist.values():
        if (cluster.avail >= len(replica.tasklist)):
            avail.append(cluster.cid)
    if not avail:
        return -1
    else:
        index = random.randint(0, len(avail)-1)
        return avail[index]

def failureHandler(env, failurelog):
	ffile = open(failurelog, "r")
    for line in ffile:
        line = line.strip('\n')
        line = line.strip('\r')
        if len(line)==0:
            continue
        parts = line.split(";")
        ftime = int(parts[0]) - env.now
        duration = int(parts[1])
        ftype = parts[2]
        yield env.timeout(ftime)
        if ftype == "MT":
        	zid = parts[4]
        	for c in clusterlist.values():
        		if(c.zid == zid):
        			c.action.interrupt('FS')
            yield env.timeout(duration)
	        for c in clusterlist.values():
	            if(c.zid == zid):
	                c.action.interrupt('FE')
	    elif ftype == "SNF":
	    	nid = parts[4]
			nodelist[nid].action.interrupt('FS')
			yield env.timeout(duration) 
			nodelist[nid].action.interrupt('FE')
		elif ftype == "MNF":
			targetType = parts[3]
			if targetType == "Cluster":
				cid = parts[4]
				clusterlist[cid].action.interrupt('FS')
				yield env.timeout(duration)
				clusterlist[cid].action.interrupt('FE')
			elif targetType == "Physical":
				pid = parts[4]
	        	for c in clusterlist.values():
	        		if(c.pid == pid):
	        			c.action.interrupt('FS')
	            yield env.timeout(duration)
		        for c in clusterlist.values():
		            if(c.pid == pid):
		                c.action.interrupt('FE')
		    elif targetType == "MultiNodeOutage":
		    	nodes = parts[4].split(",")
		    	for nid in nodes:
		    		nodelist[nid].action.interrupt("FS")
		    	yield env.timeout(duration)
		    	for nid in nodes:
		    		nodelist[nid].action.interrupt("FE")
    ffile.close()

def createResource(env, joblog, clusterlog):
    temp1 = []
    temp2 = []
    tid = 0
    i = 0
    # nodes:
    # read clusterlog file
    cfile = open(clusterlog, "r")
    
    for line in cfile:
        line = line.strip('\n')
        line = line.strip('\r')
        if len(line)==0:
            continue
        parts = line.split(";")
        cid = parts[0]
        pid = parts[1]
        zid = parts[2]
        nodes = parts[3].split(",")
        for nid in nodes:
        	aNode = Node(env, nid, cid, -1)
        	nodelist[nid] = aNode
        	temp1.append(aNode)
        clusterlist[cid] = Cluster(env, cid, pid, zid, temp1, len(nodes))
        del temp1[:]
    cfile.close()

    # jobs:
    # read joblog file
    jfile = open(joblog, "r")
    for line in jfile:
        line = line.strip('\n')
        line = line.strip('\r')
        if len(line)==0:
            continue
        parts = line.split(";")
        time = int(parts[0])
        jid = parts[1]
        replicas = parts[2].split(",")
        numTasks = parts[3].split(",")
        placement = parts[4].split(",")
        requirement = parts[5].split(",")
        for rid in replicas:
        	for t in range(int(numTasks[i])):
        		aTask = Task(env, tid, rid, -1)
        		tasklist[tid] = aTask
        		tid += 1        		
        		temp1.append(aTask)
    		i += 1
        	aReplica = Replica(env, rid, jid, -1, temp1)
        	del temp1[:]
        	replicalist[rid] = aReplica
        	temp2.append(aReplica)
        aJob = Job(env, jid, temp2)
        joblist[jid] = aJob
        del temp2[:]

	    #map job to nodes
	    if not placement:
	        for r in range(len(job.replicalist)):
	            cid = findAvailCluster(job.replicalist[r])
	            if (cid != -1):
	                print 'add replica %d (Job %d) to cluster %d' %(job.replicalist[r].rid, job.jid, cid)
	                assignReplica(clusterlist[cid], job.replicalist[r])
		else:
			for r in range(len(job.replicalist)):
				assignReplica(clusterlist[placement[r]], job.replicalist[r])
	jfile.close()


if __name__ == "__main__":
    p = OptionParser()
    p.add_option("-t", "--simtime", dest = "simtime", type = "int", 
                    help = "length of simulation time in weeks") 
    p.add_option("-j", "--joblog", dest = "joblog", type = "string", 
                    help = "path of job log file")
    p.add_option("-c", "--clusterlog", dest = "clusterlog", type = "string", 
                    help = "path of cluster log file")
    p.add_option("-f", "--failurelog", dest = "failurelog", type = "string", 
                    help = "path of failure log file") 
    p.add_option("-o", "--outputlog", dest = "outputlog", type = "string", 
                    help = "path of output log file")    
    (opts, args) = p.parse_args()
    
    if not opts.joblog:
        print "please specify path of job log file (-j)"
        p.print_help()
        exit()
    
    if not opts.clusterlog:
        print "please specify path of cluster log file (-c)"
        p.print_help()
        exit()    

    if not opts.failurelog:
        print "please specify path of failure log file (-f)"
        p.print_help()
        exit()

    if opts.outputlog:
    	OUTPUT = opts.outputlog

    if opts.simtime:
    	SIM_TIME = opts.simtime * 7 * 24 * 60 * 60

    random.seed(RANDOM_SEED)

    # Create an environment and start the setup process
    env = simpy.Environment()
    createResource(env, opts.joblog, opts.clusterlog)
    # for node in nodelist:
    #     print node.tid
    # for r in replicalist:
    #     print r.rid
    env.process(failureHandler(env, opts.failurelog))

    # Execute!
    env.run(until=SIM_TIME)
