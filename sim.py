#!/usr/bin/env python
import random
import math
import simpy
from optparse import OptionParser

# RANDOM_SEED = 42
# REPAIR_MEAN = 450.0 * 60       # Avg. repair time in seconds
# REPAIR_SIGMA = 4000.0          # Sigma of processing time
max_pid = 0
max_zid = 0

DAYS = 365                             # Default simulation time in weeks
SIM_TIME = DAYS * 24 * 60 * 60         # Simulation time in seconds
OUTPUT = "./output.log"                # Default output log file
logger = ""

joblist = {}
replicalist = {}
tasklist = {}
nodelist = {}
partlist = {}
clusterlist = {}

class Job(object):
    def __init__(self, env, jid, replicalist, requirement):
        self.env = env
        self.jid = jid
        self.replicalist = list(replicalist)
        self.failure = False
        self.requirement=requirement
        self.running = False
        self.action = env.process(self.run())

    def run(self):
        global logger
        if(self.running == True):
            print("%s,Job %s,START" % (env.now, self.jid))
            # logger += "%s,Job %s,START\n" % (env.now, self.jid)
        while True:
            try:
                yield self.env.timeout(SIM_TIME)
            except simpy.Interrupt as i:
                if (i.cause == 'RD' and self.failure==False):
                    self.failure = True
                    for replica in self.replicalist:
                        if(replica.failure == False):
                            self.failure = False
                    if (self.failure == True):
                        print("%s,Job %s,STOP" % (env.now, self.jid))
                        # logger += "%s,Job %s,STOP\n" % (env.now, self.jid)
                if (i.cause == 'RU' and self.failure==True):
                    self.failure = False
                    print("%s,Job %s,RESTART" % (env.now, self.jid))
                    # logger += "%s,Job %s,RESTART\n" % (env.now, self.jid)

class Replica(object):
    def __init__(self, env, rid, jid, pid, tasklist):
        self.env = env
        self.rid = rid
        self.jid = jid
        self.pid = pid
        self.tasklist = list(tasklist)
        self.failure = False
        self.action = env.process(self.run())

    def run(self):
        # print('Replica %s starts running at %d' % (self.rid, env.now))
        # alive = 0
        # threshold = math.ceil(len(self.tasklist)/2.0)
        global logger
        while True:
            try:
                yield self.env.timeout(SIM_TIME)
            except simpy.Interrupt as i:
                if (i.cause == 'TD' and self.failure==False):
                    self.failure = True
                    for task in self.tasklist:
                        if(task.failure == False):
                            self.failure = False
                    #         alive += 1
                    # if (alive < threshold):
                    if (self.failure == True):
                        # print("%s,Replica %s (Job %s),STOP" % (env.now, self.rid, self.jid))
                        # logger += "%s,Replica %s (Job %s),STOP\n" % (env.now, self.rid, self.jid)
                        joblist[self.jid].action.interrupt('RD')
                if (i.cause == 'TU' and self.failure==True):
                    self.failure = False
                    # print("%s,Replica %s (Job %s),RESTART" % (env.now, self.rid, self.jid))
                    # logger += "%s,Replica %s (Job %s),RESTART\n" % (env.now, self.rid, self.jid)
                    joblist[self.jid].action.interrupt('RU')            

class Task(object):
    def __init__(self, env, tid, rid, jid, nid):
        self.env = env
        self.tid = tid
        self.rid = rid
        self.jid = jid
        self.nid = nid
        self.failure = False
        self.action = env.process(self.run())

    def run(self):
        global logger
        while True:           
            try:
                yield self.env.timeout(SIM_TIME)

            except simpy.Interrupt as i:
                if (i.cause == 'FS' and self.failure==False):
                    self.failure = True
                    print("%s,Task %s_%s,STOP" % (env.now, self.jid, self.tid))
                    # logger += "%s,Task %s_%s,STOP\n" % (env.now, self.jid, self.tid)
                    replicalist[self.rid].action.interrupt('TD')
                if (i.cause == 'FE' and self.failure==True):
                    self.failure = False
                    print("%s,Task %s_%s,RESTART" % (env.now, self.jid, self.tid))
                    # logger += "%s,Task %s_%s,RESTART\n" % (env.now, self.jid, self.tid)
                    replicalist[self.rid].action.interrupt('TU')

class Node(object):

    def __init__(self, env, nid, cid, pid, tid):
        self.env = env
        self.nid = nid
        self.cid = cid
        self.pid = pid
        self.tid = tid
        self.down = False
        self.maintain = False
        self.action = env.process(self.working())
        # self.lambd = random.uniform(lambda_min, lambda_max)
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
                if (i.cause == 'FS' and self.maintain==False and self.down==False):
                    self.down = True
                    # print("%s,Node %s,STOP" % (env.now, self.nid))
                    if(self.tid!=-1):
                        tasklist[self.tid].action.interrupt('FS')
                if (i.cause == 'FE' and self.maintain==False and self.down == True):
                    if(self.tid!=-1):
                        tasklist[self.tid].action.interrupt('FE')
                    self.down = False
                    # print("%s,Node %s,RESTART" % (env.now, self.nid))
                if (i.cause == 'MTS' and self.maintain==False):
                    self.down = True
                    self.maintain = True
                    # print("%s,Node %s,MTSTART" % (env.now, self.nid))
                    if(self.tid!=-1):
                        tasklist[self.tid].action.interrupt('FS')
                if (i.cause == 'MTE' and self.maintain == True):
                    if(self.tid!=-1):
                        tasklist[self.tid].action.interrupt('FE')
                    self.down = False
                    self.maintain=False
                    # print("%s,Node %s,MTEND" % (env.now, self.nid))
    # def node_down(self):
    #     """Break the machine every now and then."""
    #     while True:
    #         yield self.env.timeout(time_to_failure(self.lambd))
    #         if not self.down:
    #             # Only break the machine if it is currently working.
    #             self.action.interrupt('FS')
    #             yield self.env.timeout(time_to_repair())
    #             self.action.interrupt('FE')

class Partition(object):
    def __init__(self, env, pid, cid, nodelist, capacity):
        self.env = env
        self.pid = pid
        self.cid = cid
        self.avail = capacity
        self.nodelist = list(nodelist)
        self.down = False
        self.maintain = False
        self.action = env.process(self.working())

    def working(self):
        global logger     
        while True:           
            try:
                yield self.env.timeout(SIM_TIME)
            except simpy.Interrupt as i:
                if (i.cause == 'FS' and self.maintain==False and self.down==False):
                    self.down = True
                    # print("%s,Domain %s,STOP" % (env.now, self.pid))
                    # logger += "%s,Cluster %s,STOP\n" % (env.now, self.cid)
                    for node in self.nodelist:
                        node.action.interrupt('FS')
                if (i.cause == 'FE' and self.maintain==False and self.down==True):
                    for node in self.nodelist:
                        node.action.interrupt('FE')
                    self.down = False
                    # print("%s,Domain %s,RESTART" % (env.now, self.pid))
                    # logger +=  "%s,Cluster %s,RESTART\n" % (env.now, self.cid)
                if (i.cause == 'MTS' and self.maintain==False):
                    self.down = True
                    self.maintain = True
                    # print("%s,Domain %s,MTSTART" % (env.now, self.pid))
                    # logger += "%s,Cluster %s,MTSTART\n" % (env.now, self.pid)
                    for node in self.nodelist:
                        node.action.interrupt('MTS')
                if (i.cause == 'MTE' and self.maintain==True):
                    for node in self.nodelist:
                        node.action.interrupt('MTE')
                    self.down = False
                    self.maintain = False
                    # print("%s,Domain %s,MTEND" % (env.now, self.pid))   
                    # logger += "%s,Cluster %s,MTEND\n" % (env.now, self.cid)    

class Cluster(object):

    def __init__(self, env, cid, zid, domainlist, capacity):
        self.env = env
        self.cid = cid
        self.zid = zid
        self.avail = capacity
        self.domainlist = list(domainlist)
        self.down = False
        self.maintain = False
        self.action = env.process(self.working())

    def working(self):
        global logger     
        while True:           
            try:
                yield self.env.timeout(SIM_TIME)
            except simpy.Interrupt as i:
                if (i.cause == 'FS' and self.maintain==False and self.down==False):
                    self.down = True
                    # print("%s,Cluster %s,STOP" % (env.now, self.cid))
                    # logger += "%s,Cluster %s,STOP\n" % (env.now, self.cid)
                    for domain in self.domainlist:
                        domain.action.interrupt('FS')
                if (i.cause == 'FE' and self.maintain==False and self.down==True):
                    for domain in self.domainlist:
                        domain.action.interrupt('FE')
                    self.down = False
                    # print("%s,Cluster %s,RESTART" % (env.now, self.cid))
                    # logger +=  "%s,Cluster %s,RESTART\n" % (env.now, self.cid)
                if (i.cause == 'MTS' and self.maintain==False):
                    self.down = True
                    self.maintain = True
                    print("%s,Cluster %s,MTSTART" % (env.now, self.cid))
                    # logger += "%s,Cluster %s,MTSTART\n" % (env.now, self.cid)
                    for domain in self.domainlist:
                        domain.action.interrupt('MTS')
                if (i.cause == 'MTE' and self.maintain==True):
                    for domain in self.domainlist:
                        domain.action.interrupt('MTE')
                    self.down = False
                    self.maintain = False
                    print("%s,Cluster %s,MTEND" % (env.now, self.cid))   
                    # logger += "%s,Cluster %s,MTEND\n" % (env.now, self.cid)

# def time_to_repair():
#     """Return repairing time for a unexpected failure."""
#     return random.normalvariate(REPAIR_MEAN, REPAIR_SIGMA)

# def time_to_failure(lambd):
#     """Return time until next unexpected failure."""
#     return random.expovariate(lambd)

def assignReplica(partition, replica):
    global logger
    i=0
    j=0
    cluster = clusterlist[partition.cid]
    while(i < len(replica.tasklist) and j < len(partition.nodelist)):
        if(partition.nodelist[j].tid == -1):
            partition.nodelist[j].tid = replica.tasklist[i].tid
            replica.tasklist[i].nid = partition.nodelist[j].nid
            partition.avail -= 1
            cluster.avail -= 1
            i += 1
            j += 1
        else:
            j += 1
    replica.pid = partition.pid
    print 'add replica %s (Job %s) to partition %s (Cluster %s)' %(replica.rid, replica.jid, partition.pid, partition.cid)
    # logger += 'add replica %s (Job %s) to cluster %s\n' %(replica.rid, replica.jid, cluster.cid)

def findAvailPartition(job, count):
    avail_pid=[]
    spot = 0
    for r in range(count):
        spot += len(job.replicalist[r].tasklist)
    for partition in partlist.values():
        if (partition.avail >= spot):
            avail_pid.append(partition.pid)
    if not avail_pid:
        return -1
    else:
        return avail_pid

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
                        c.action.interrupt('MTS')
            if(flagType=="FE"):
                for c in clusterlist.values():
                    if(c.zid == zid):
                        c.action.interrupt('MTE')
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
            elif targetType == "Partition":
                pid = parts[4]
                if(flagType=="FS"):
                    for p in partlist.values():
                        if(p.pid == pid):
                            p.action.interrupt('FS')
                if(flagType=="FE"):
                    for p in partlist.values():
                        if(p.pid == pid):
                            p.action.interrupt('FE')
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
        zid = parts[1]
        if(int(zid) > max_zid):
            max_zid = int(zid)
        pids = parts[2].split(",")
        s_pid = int(pids[0])
        e_pid = int(pids[1])
        pid_count = e_pid - s_pid + 1
        nodes = parts[3].split(",")
        s_nid = int(nodes[0])
        e_nid = int(nodes[1])
        nodes_per_part = (e_nid - s_nid + 1)/pid_count

        for i in range(pid_count):
            pid = str(s_pid+i)
            if(int(pid) > max_pid):
                max_pid = int(pid)
            for j in range(nodes_per_part):
                nid = str(s_nid+j)
                aNode = Node(env, nid, cid, pid, -1)
                nodelist[nid]= aNode
                temp1.append(aNode)
            aPartition = Partition(env, pid, cid, temp1, len(temp1))
            partlist[pid] = aPartition
            del temp1[:]
            temp2.append(aPartition)
            s_nid = s_nid + nodes_per_part
        clusterlist[cid] = Cluster(env, cid, zid, temp2, len(temp2))
        del temp2[:]
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
        requirement = parts[4]

        i=0
        for rid in replicas:
            rid = rid.strip()
            for t in range(int(numTasks[i])):
                aTask = Task(env, tid, rid, jid, -1)
                tasklist[tid] = aTask
                tid += 1
                temp1.append(aTask)
            i += 1
            aReplica = Replica(env, rid, jid, -1, temp1)
            del temp1[:]
            replicalist[rid] = aReplica
            temp2.append(aReplica)
        aJob = Job(env, jid, temp2, requirement)
        joblist[jid] = aJob
        del temp2[:]
    jfile.close()

def isAvalable(tasknum, partition):
    if(partition.avail >= tasknum):
        return True
    else:
        return False

def findCluster(zid, tasknum):
    candidates = []
    for cluster in clusterlist.values():
        if(int(cluster.zid) == zid):
            candidates.append(cluster)
    index=0
    found=True
    while(isAvalable(tasknum,candidates[index])==False):
        index += 1
        if (index >= len(candidates)):
            found = False
            break
    return found, candidates[index]

def findPlacement1(job, diversity, ptype):
    if(diversity==1):
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
            job.running = True
    if(diversity==2):
        initpid = int(job.jid) % (max_pid+1)
        initzid = int(job.jid) % (max_zid+1)            
        pid = initpid
        zid = initzid
        tryagian = True
        while(tryagian):
            l = len(job.replicalist)-len(job.replicalist)/2
            found, candidate = findInCandidates(pid, zid, job, 0, l)
            if(found):
                for r in range(l):
                    assignReplica(candidate,job.replicalist[r])
                # for left replicas, found another logical/physical zone
                l1 = len(job.replicalist)
                if(ptype=="pid"):
                    id2 = (pid+1)%(max_pid+1)
                    id1 = pid
                else:
                    id2 = (zid+1)%(max_zid+1)
                    id1 = zid
                while(found or id2 != id1):
                    if(ptype=="pid"):
                        found, candidate = findInCandidates(id2, zid, job, l, l1)
                    else:
                        found, candidate = findInCandidates(pid, id2, job, l, l1)
                    if(found):
                        for r in range(l, l1):
                            assignReplica(candidate,job.replicalist[r])
                        job.running = True
                        tryagian = False
                        break
                    else:
                        if(ptype=="pid"):
                            id2 = (id2+1)%(max_pid+1)
                        else:
                            id2 = (id2+1)%(max_zid+1)   
            # next try:
            if(ptype=="pid"):
                pid = (pid+1)%(max_pid+1)                 
                if(pid == initpid):
                    # next logical zone
                    zid = (zid+1)%(max_zid+1)
                    pid = initpid
                    if(zid == initzid):
                        break        
            else:
                zid = (zid+1)%(max_zid+1)                 
                if(zid == initzid):
                    # next physical location
                    pid = (pid+1)%(max_pid+1)
                    zid = initzid
                    if(pid == initpid):
                        break                     
    if(diversity==3):
        initpid = int(job.jid) % (max_pid+1)
        initzid = int(job.jid) % (max_zid+1)            
        pid = initpid
        zid = initzid
        tryagian = True
        l=[]
        l.append(0)
        if(len(job.replicalist)%3 == 0):
            l.append(len(job.replicalist)/3)
        else: 
            l.append(len(job.replicalist)/3+1)
        if((len(job.replicalist)-l[0])%2 == 0):
            l.append(l[0]+(len(job.replicalist) - l[0])/2)
        else:
            l.append(l[0]+(len(job.replicalist) - l[0])/2 + 1)
        l.append(len(job.replicalist))
        while(tryagian):
            if(ptype=="pid"):
                id_range = range(max_pid+1)
            else:
                id_range = range(max_zid+1)
            for i in range(3):
                sid = random.choice(id_range)
                if(ptype=="pid"):
                    found, candidate = findInCandidates(sid, zid, job, l[i], l[i+1])
                else:
                    found, candidate = findInCandidates(pid, sid, job, l[i], l[i+1])
                if(found):
                    for r in range(l[i], l[i+1]):
                        assignReplica(candidate,job.replicalist[r])
                    id_range.remove(sid)
                else:
                    for rid in id_range:
                        if(ptype=="pid"):
                            found, candidate = findInCandidates(rid, zid, job, l[i], l[i+1])
                        else:
                            found, candidate = findInCandidates(pid, rid, job, l[i], l[i+1])
                        if(found):
                            for r in range(l[i], l[i+1]):
                                assignReplica(candidate,job.replicalist[r])
                            id_range.remove(rid)
                            break
                    if(found==False):
                        tryagian=True
                        #next try
                        pid = (pid+1)%(max_pid+1)
                        zid = (zid+1)%(max_zid+1)
                        if(ptype=="pid"):
                            if(zid == initzid):
                                tryagian = False
                        else:
                            if(zid == initzid):
                                tryagian = False
            if(found):
                job.running = True
                tryagian = False

def findPlacement2(job, diversity, ptype):
    l=[]
    l.append(0)
    if(len(job.replicalist)%3 == 0):
        l.append(len(job.replicalist)/3)
    else: 
        l.append(len(job.replicalist)/3+1)
    if((len(job.replicalist)-l[0])%2 == 0):
        l.append(l[0]+(len(job.replicalist) - l[0])/2)
    else:
        l.append(l[0]+(len(job.replicalist) - l[0])/2 + 1)
    l.append(len(job.replicalist))
    if(diversity==2):
        tryagian = True
        count = 0 
        while(tryagian and count <= 20):
            count +=1
            pid_range = range(max_pid+1)
            zid_range = range(max_zid+1) 
            pid_list=[]
            zid_list=[]
            for i in range(2):
                pid = random.choice(pid_range)
                zid = random.choice(zid_range)
                found, candidate = findInCandidates(pid, zid, job, l[i], l[i+1])
                if(found):
                    for r in range(l[i], l[i+1]):
                        assignReplica(candidate,job.replicalist[r])
                    pid_list.append(pid)
                    zid_list.append(zid)
                    pid_range.remove(pid)
                    zid_range.remove(zid)
                else:
                    break
            if(found):
                #third replica
                found, candidate = findInCandidates(pid_list[0], zid_list[1], job, l[2], l[3])
                if(found):
                    for r in range(l[2], l[3]):
                        assignReplica(candidate,job.replicalist[r])
                    job.running = True
                    tryagian = False
                else:
                    found, candidate = findInCandidates(pid_list[1], zid_list[0], job, l[2], l[3])
                    if(found):
                        for r in range(l[2], l[3]):
                            assignReplica(candidate,job.replicalist[r])
                        job.running = True
                        tryagian = False
    if(diversity==3):
        tryagian = True
        count = 0
        while(tryagian and count <= 20):
            count +=1
            pid_range = range(max_pid+1)
            zid_range = range(max_zid+1) 
            if(ptype=="pid"):
                pid_list = random.sample(pid_range,3)
                zid_list = random.sample(zid_range, 2)
            else:
                pid_list = random.sample(pid_range,2)
                zid_list = random.sample(zid_range, 3)
            for i in range(3):
                if(ptype=="pid"):
                    pid = pid_list[i]
                    zid = zid_list[i%2]
                else:
                    pid = pid_list[i%2]
                    zid = zid_list[i]
                found, candidate = findInCandidates(pid, zid, job, l[i], l[i+1])
                if(found):
                    for r in range(l[i], l[i+1]):
                        assignReplica(candidate,job.replicalist[r])
                else: # next try
                    break
            if(found):
                job.running = True
                tryagian = False

def findPlacement3(job):
    l=[]
    l.append(0)
    if(len(job.replicalist)%3 == 0):
        l.append(len(job.replicalist)/3)
    else: 
        l.append(len(job.replicalist)/3+1)
    if((len(job.replicalist)-l[0])%2 == 0):
        l.append(l[0]+(len(job.replicalist) - l[0])/2)
    else:
        l.append(l[0]+(len(job.replicalist) - l[0])/2 + 1)
    l.append(len(job.replicalist))
    tryagian = True
    count = 0
    while(tryagian and count <= 20):
        count +=1
        pid_range = range(max_pid+1)
        zid_range = range(max_zid+1)
        for i in range(3):
            pid = random.choice(pid_range)
            zid = random.choice(zid_range)
            found, candidate = findInCandidates(pid, zid, job, l[i], l[i+1])
            if(found):
                for r in range(l[i], l[i+1]):
                    assignReplica(candidate,job.replicalist[r])
                pid_range.remove(pid)
                zid_range.remove(zid)
            else:
                break
        if(found):
            job.running = True
            tryagian = False

def findPlacement(job, diff_l, diff_p):
    count = 0
    length = len(job.replicalist)
    assigned = 0
    while(assigned < length and count <= 20):
        count +=1
        zid_range = range(max_zid+1)    
        start_index = 0
        zids = random.sample(zid_range, diff_l)
        pids = []
        for zid in zids:
            #add candidate partitions
            candidates = []
            for cluster in clusterlist.values():
                if(int(cluster.zid) == zid):
                    for domain in cluster.domainlist:
                        candidates.append(domain.pid)
            oneChoice = random.choice(candidates)
            pids.append(oneChoice)
            candidates.remove(oneChoice)
        add = random.sample(candidates, diff_p-diff_l)
        pids.extend(add)
        assigned = 0
        index=0
        for pid in pids:
            tasknum = len(job.replicalist[index].tasklist)
            if(partlist[pid].avail >= tasknum):
                assignReplica(partlist[pid], job.replicalist[index])
                assigned += 1
                index += 1

            else:
                break
        if(assigned==diff_p):
            remain = length - diff_p
            for i in range(remain):
                for pid in pids:
                    tasknum = len(job.replicalist[index+i].tasklist)
                    if(partlist[pid].avail >= tasknum):
                        assignReplica(partlist[pid], job.replicalist[index+i])
                        assigned += 1
                        break   
    if(assigned < length):
        print "add Job %s fail" % job.jid
    else:
        job.running = True

def placeJobOnCluster(strategy):
    for job in joblist.values():
        if(job.requirement == "NAN"):
            strategies = strategy.split("-")
        else:
            strategies = job.requirement.split("-")
        diff_l = int(strategies[0])
        diff_p = int(strategies[1])

        findPlacement(job, diff_l, diff_p)

if __name__ == "__main__":
    p = OptionParser()
    p.add_option("-t", "--simtime", dest = "simtime", type = "int", 
                    help = "length of simulation time in days") 
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

    # random.seed(RANDOM_SEED)

    # Create an environment and start the setup process
    env = simpy.Environment()
    createResource(env, opts.joblog, opts.clusterlog)
    placeJobOnCluster(opts.strategy)
    env.process(failureHandler(env, opts.failurelog))

    # Execute!
    env.run(until=SIM_TIME)

    # outfile = open(OUTPUT, "w")
    # outfile.write(logger)
    # outfile.close()