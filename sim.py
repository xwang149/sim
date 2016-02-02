#!/usr/bin/env python
import random
import math
import simpy
from optparse import OptionParser

RANDOM_SEED = 42
REPAIR_MEAN = 450.0 * 60       # Avg. repair time in seconds
REPAIR_SIGMA = 4000.0          # Sigma of processing time
max_pid = 0
max_zid = 0

DAYS = 400                             # Default simulation time in weeks
SIM_TIME = DAYS * 24 * 60 * 60         # Simulation time in seconds
OUTPUT = "./output.log"                # Default output log file

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
        print("%s,Job %s,START" % (env.now, self.jid))
        # outfile = open(OUTPUT, "a")
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
                        # print('Job %s stops running at %d' % (self.jid, env.now))
                        print("%s,Job %s,STOP" % (env.now, self.jid))
                        # outfile.write("%s,%s,STOP" % (env.now, self.jid))
                        # outfile.close()
                if (i.cause == 'RU'):
                    if(self.failure == True):
                        self.failure = False
                        # print('Job %s restarts running at %d' % (self.jid, env.now))
                        print("%s,Job %s,RESTART" % (env.now, self.jid))
                        # outfile.write("%s,%s,RESTART" % (env.now, self.jid))
                        # outfile.close()

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
        # print('Replica %s starts running at %d' % (self.rid, env.now))
        # alive = 0
        # threshold = math.ceil(len(self.tasklist)/2.0)
        # outfile = open(OUTPUT, "a")
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
                    #     outfile.write("%s,%s,%s,DOWNGRADE" % (env.now, self.jid,self.rid))
                    if (self.failure == True):
                        # print("%s,Replica %s (Job %s),STOP" % (env.now, self.rid, self.jid))
                        # outfile.write("%s,%s,%s,STOP" % (env.now, self.jid,self.rid))
                        joblist[self.jid].action.interrupt('RD')
                if (i.cause == 'TU'):
                    if(self.failure == True):
                        self.failure = False
                        # print("%s,Replica %s (Job %s),RESTART" % (env.now, self.rid, self.jid))
                        # outfile.write("%s,%s,%s,RESTART" % (env.now, self.jid,self.rid))
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
        # print('Task %s starts running at %d' % (self.tid, env.now))
        while True:           
            try:
                yield self.env.timeout(SIM_TIME)

            except simpy.Interrupt as i:
                if (i.cause == 'FS' and self.failure==False):
                    self.failure = True
                    # print('Task %s stops running at %d' % (self.tid, env.now))
                    replicalist[self.rid].action.interrupt('TD')
                if (i.cause == 'FE' and self.failure==True):
                    self.failure = False
                    # print('Task %s restarts running at %d' % (self.tid, env.now))
                    replicalist[self.rid].action.interrupt('TU')

class Node(object):

    def __init__(self, env, nid, cid, tid):
        self.env = env
        self.nid = nid
        self.cid = cid
        self.tid = tid
        self.down = False
        # self.lambd = random.uniform(lambda_min, lambda_max)
        self.action = env.process(self.working())
        # env.process(self.node_down())

    def working(self):
        """Node is up as long as the simulation runs.

        While a failure happens, node stop working for some time period 
        and terminate the running task on it.

        """
        # print('Node %s starts running at %d'% (self.nid, env.now))
        while True:           

            try:
                yield self.env.timeout(SIM_TIME)
            except simpy.Interrupt as i:
                if (i.cause == 'FS' and self.down==False):
                    self.down = True
                    # print("%s,Node %s,STOP" % (env.now, self.nid))
                    if(self.tid!=-1):
                        tasklist[self.tid].action.interrupt('FS')
                if (i.cause == 'FE' and self.down == True):
                    if(self.tid!=-1):
                        tasklist[self.tid].action.interrupt('FE')
                    self.down = False
                    # print("%s,Node %s,RESTART" % (env.now, self.nid))

    # def node_down(self):
    #     """Break the machine every now and then."""
    #     while True:
    #         yield self.env.timeout(time_to_failure(self.lambd))
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
        # print('Cluster %s starts running at %d' % (self.cid, env.now))        
        while True:           

            try:
                yield self.env.timeout(SIM_TIME)

            except simpy.Interrupt as i:
                if (i.cause == 'FS'):
                    self.down = True
                    print("%s,Cluster %s,STOP" % (env.now, self.cid))
                    for node in self.nodelist:
                        node.maintain = True
                        node.action.interrupt('FS')
                if (i.cause == 'FE'):
                    for node in self.nodelist:
                        node.maintain = False
                        node.action.interrupt('FE')
                    self.down = False
                    print("%s,Cluster %s,RESTART" % (env.now, self.cid))        

def time_to_repair():
    """Return repairing time for a unexpected failure."""
    return random.normalvariate(REPAIR_MEAN, REPAIR_SIGMA)

def time_to_failure(lambd):
    """Return time until next unexpected failure."""
    return random.expovariate(lambd)

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
    # print 'add replica %s (Job %s) to cluster %s' %(replica.rid, replica.jid, cluster.cid)

def findAvailClusters(job, count):
    avail_cid=[]
    spot = 0
    for r in range(count):
        spot += len(job.replicalist[r].tasklist)
    for cluster in clusterlist.values():
        if (cluster.avail >= spot):
            avail_cid.append(cluster.cid)
    if not avail_cid:
        return -1
    else:
        return avail_cid

def failureHandler(env, failurelog):
    ffile = open(failurelog, "r")
    for line in ffile:
        line = line.strip('\n')
        line = line.strip('\r')
        if len(line)==0:
            continue
        parts = line.split(";")
        ftime = int(float(parts[0]) - env.now)
        flagType = parts[1]
        ftype = parts[2]
        yield env.timeout(ftime)
        if ftype == "MT":
            zid = parts[4]
            if(flagType=="FS"):
                for c in clusterlist.values():
                    if(c.zid == zid):
                        c.action.interrupt('FS')
            if(flagType=="FE"):
                for c in clusterlist.values():
                    if(c.zid == zid):
                        c.action.interrupt('FE')
        elif ftype == "SNF":
            nid = parts[4]
            if(flagType=="FS"):
                nodelist[nid].action.interrupt('FS')
            if(flagType=="FE"):
                nodelist[nid].action.interrupt('FE')
        elif ftype == "MNF":
            targetType = parts[3]
            if targetType == "Cluster":
                cid = parts[4]
                if(flagType=="FS"):
                    clusterlist[cid].action.interrupt('FS')
                if(flagType=="FE"):
                    clusterlist[cid].action.interrupt('FE')
            elif targetType == "Physical":
                pid = parts[4]
                if(flagType=="FS"):
                    for c in clusterlist.values():
                        if(c.pid == pid):
                            c.action.interrupt('FS')
                if(flagType=="FE"):
                    for c in clusterlist.values():
                        if(c.pid == pid):
                            c.action.interrupt('FE')
            elif targetType == "MultiNodeOutage":
                nodes = parts[4].split(",")
                for nid in nodes:
                    if(flagType=="FS"):
                        nodelist[nid].action.interrupt("FS")
                    if(flagType=="FE"):
                        nodelist[nid].action.interrupt("FE")
    ffile.close()

def createResource(env, joblog, clusterlog):
    temp1 = []
    temp2 = []
    tid = 0
    i = 0
    global max_pid
    global max_zid
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
        if(int(pid) > max_pid):
            max_pid = int(pid)
        zid = parts[2]
        if(int(zid) > max_zid):
            max_zid = int(zid)
        nodes = parts[3].split(",")
        for nid in nodes:
            nid = nid.strip()
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
        # placement = parts[4].split(",")
        # requirement = parts[5].split(",")
        i=0
        for rid in replicas:
            rid = rid.strip()
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
    jfile.close()

def isAvalable(tasknum, cluster):
    if(cluster.avail >= tasknum):
        return True
    else:
        return False

def placeJobOnCluster(strategy):
    strategies = strategy.split("-")
    diff_p = int(strategies[0])
    diff_l = int(strategies[1])
    if(diff_p==1 and diff_l==1):
        for job in joblist.values():
            tasknum=0
            for r in range(len(job.replicalist)):
                tasknum += len(job.replicalist[r].tasklist)
            initcid = int(job.jid) % len(clusterlist)
            cid = initcid
            found = True
            while (isAvalable(tasknum,clusterlist[str(cid)])==False):
                cid = (cid + 1)%len(clusterlist)
                if(cid == initcid):
                    found = False
                    break
            if(found):
                for r in range(3):
                    assignReplica(clusterlist[str(cid)], job.replicalist[r])
    if(diff_p==1 and diff_l==2):
        for job in joblist.values():
            initpid = int(job.jid) % (max_pid+1)
            initzid = int(job.jid) % (max_zid+1)            
            pid = initpid
            zid = initzid
            found = True
            tryagian = True
            while(tryagian):
                candidates = []
                for cluster in clusterlist.values():
                    if(int(cluster.pid) == pid and int(cluster.zid) == zid):
                        candidates.append(cluster)
                tasknum=0
                l = len(job.replicalist)-len(job.replicalist)/2
                for r in range(l):
                    tasknum += len(job.replicalist[r].tasklist)
                index=0
                while(isAvalable(tasknum,candidates[index])==False):
                    index += 1
                    if (index >= len(candidates)):
                        found = False
                        break
                if(found):
                    for r in range(l):
                        assignReplica(candidates[index],job.replicalist[r])
                    # for left replicas, found another logical zone
                    l1 = len(job.replicalist) - l
                    zid2 = (zid+1)%(max_zid+1)
                    while(found or zid2 != zid):
                        candidates = []
                        for cluster in clusterlist.values():
                            if(int(cluster.pid) == pid and int(cluster.zid) == zid2):
                                candidates.append(cluster)
                        tasknum=0
                        for r in range(l1):
                            tasknum += len(job.replicalist[r].tasklist)
                        index=0
                        while(isAvalable(tasknum,candidates[index])==False):
                            index += 1
                            if (index >= len(candidates)):
                                found = False
                                break
                        if(found):
                            for r in range(l1):
                                assignReplica(candidates[index],job.replicalist[l+r])
                                tryagian = False
                            break
                        else:
                            zid2 = (zid2+1)%(max_zid+1)   
                # next try:
                zid = (zid+1)%(max_zid+1)                 
                if(zid == initzid):
                    # next physical location
                    pid = (pid+1)%(max_pid+1)
                    zid = initzid
                    if(pid == initpid):
                        break

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
    p.add_option("-s", "--strategy", dest = "strategy", type = "string", 
                help = "job placement strategy") 
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

    if not opts.strategy:
        print "please specify strategy of job placement (-s)"
        p.print_help()
        exit()

    if opts.outputlog:
        OUTPUT = opts.outputlog

    if opts.simtime:
        SIM_TIME = opts.simtime * 24 * 60 * 60

    random.seed(RANDOM_SEED)

    # Create an environment and start the setup process
    env = simpy.Environment()
    createResource(env, opts.joblog, opts.clusterlog)
    placeJobOnCluster(opts.strategy)
    # for node in nodelist:
    #     print node.tid
    # for r in replicalist:
    #     print r.rid
    env.process(failureHandler(env, opts.failurelog))

    # Execute!
    env.run(until=SIM_TIME)
