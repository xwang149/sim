#!/usr/bin/env python
import random
import math
from optparse import OptionParser
import numpy as np

RANDOM_SEED = 42

joblist = {}
replicalist = {}
tasklist = {}
nodelist = {}
clusterlist = {}

def parseConfig(filename):

    wlf = open(filename, "r")
    configs = {}
    for line in wlf:
        line = line.strip('\n')
        line = line.strip('\r')
        if len(line)==0:
            continue
        parts = line.split("=")
        configs[parts[0]] = parts[1]
    wlf.close()
    return configs

def generateCluster(configs, outfile):
    clusterlog = open(outfile, "w")

    clusters = int(configs["NUM_CLUSTER_PER_METRO"])
    metros = int(configs["NUM_METRO"])
    lzs = int(configs["NUM_LZ"])
    nodes = int(configs["NUM_NODE_PER_CLUSTER"])

    total_nodes = clusters * nodes * metros

    id1 = 0
    id2 = 0
    nids = []
    for k in range(metros):
        for i in range(clusters):
            line = ""
            for j in range(nodes):
                nids.append(id1)
                id1 += 1
            line = "%s" % nids
            line = line[1:line.rfind("]")]
            del nids[:]    
            cid = id2
            pid = k
            zid = cid % lzs
            clusterlog.write("%s;%s;%s;%s\n" % (cid, pid, zid, line))
            id2 += 1
    clusterlog.close()
    return total_nodes

def generateJob(configs, total_nodes, outfile):
    joblog = open(outfile, "w")

    num_job = int(configs["NUM_JOB"])
    num_replica = int(configs["NUM_REPLICA"])
    task_type = int(configs["NUM_TASK_TYPE"])
    util = float(configs["UTILIZATION"])
    
    id1 = 0
    tasks = []
    rids = []
    num_task = int(math.ceil(total_nodes * util))
    if task_type == 0:
        t = num_task / (num_job * num_replica)
        remain = num_task % (num_job * num_replica)
        for j in range(num_job):
            for r in range(num_replica):
                if (remain > 0):
                    tasks.append(t+1)
                    remain -= 1
                else:
                    tasks.append(t)
                rids.append(id1)
                id1 += 1
            jid = j
            l1 = "%s" % tasks
            l1 = l1[1:l1.rfind("]")]  
            l2 = "%s" % rids
            l2 = l2[1:l2.rfind("]")]
            joblog.write("%s;%s;%s;%s;NAN\n" % (0,jid, l2, l1))
            del tasks[:]
            del rids[:]
    elif task_type == 1:
        for j in range(num_job/2):
            for r in range(num_replica):
                tasks.append(1)
                num_task -= 1
                rids.append(id1)
                id1 += 1
            jid = id2
            id2 += 1
            l1 = "%s" % tasks
            l1 = l1[1:l1.rfind("]")]  
            l2 = "%s" % rids
            l2 = l2[1:l2.rfind("]")]
            joblog.write("%s;%s;%s;%s;NAN\n" % (0,jid, l2, l1))
            del tasks[:]
            del rids[:] 
        jobleft = num_job-num_job/2 
        t = num_task / (jobleft * num_replica)
        remain = num_task % (jobleft * num_replica)     
        for j in range(jobleft):
            for r in range(num_replica): 
                if (remain > 0):
                    tasks.append(t+1)
                    remain -= 1
                else:
                    tasks.append(t)
                rids.append(id1)
                id1 += 1
            jid = id2
            id2 += 1
            l1 = "%s" % tasks
            l1 = l1[1:l1.rfind("]")]  
            l2 = "%s" % rids
            l2 = l2[1:l2.rfind("]")]
            joblog.write("%s;%s;%s;%s;NAN\n" % (0,jid, l2, l1))
            del tasks[:]
            del rids[:]
    joblog.close()        

def generateFailure(configs, total_nodes, outfile):
    
    clusters = int(configs["NUM_CLUSTER_PER_METRO"])
    metros = int(configs["NUM_METRO"])
    lzs = int(configs["NUM_LZ"])
    nodes = int(configs["NUM_NODE_PER_CLUSTER"])

    sim_time = int(configs["SIM_TIME_IN_DAYS"]) * 24 * 60 * 60
    lambda_range = configs["LAMBDA_RANGE_PER_HOUR"].split(",")
    lambda_min = float(lambda_range[0]) / 60 / 60
    lambda_max = float(lambda_range[1]) / 60 / 60
    repair_mean = float(configs["REPAIR_MEAN"]) * 60
    repair_sigma = float(configs["REPAIR_SIGMA"])
    mt_interval = float(configs["MT_INTERVAL_IN_DAYS"]) * 24 * 60 * 60
    mt_duration = float(configs["MT_DURATION_IN_DAYS"]) * 24 * 60 * 60

    failures = []

    # generate maintainance schedule
    i=0
    for zid in range(lzs):
        t = zid * (mt_duration + 1)
        while (True): 
            time = t + mt_interval
            t = time+mt_duration
            if(t > sim_time):
                break            
            failures.append([])
            failures[i].append(time)
            failures[i].append("FS")
            failures[i].append("MT")
            failures[i].append("LZ")
            failures[i].append(zid)
            i += 1
            failures.append([])
            failures[i].append(time+mt_duration)
            failures[i].append("FE")
            failures[i].append("MT")
            failures[i].append("LZ")
            failures[i].append(zid) 
            i += 1           

    # generate node failures
    for nid in range(total_nodes):
        lambd = random.uniform(lambda_min, lambda_max)
        t = 0
        while(True):
            time = t + random.expovariate(lambd)
            k=random.normalvariate(repair_mean, repair_sigma)
            while (k<=0):
                k=random.normalvariate(repair_mean, repair_sigma)
            t = time + k
            if(t > sim_time):
                break
            failures.append([])
            failures[i].append(time)
            failures[i].append("FS")
            failures[i].append("SNF")
            failures[i].append("SN")
            failures[i].append(nid)
            i += 1
            failures.append([])
            failures[i].append(time+k)
            failures[i].append("FE")
            failures[i].append("SNF")
            failures[i].append("SN")
            failures[i].append(nid)
            i += 1

    # sort by time
    sorted_f = sorted(failures, key=lambda data_entry: float(data_entry[0]))
    failurelog = file(outfile, "w")
    np.savetxt(failurelog, sorted_f, fmt='%s', delimiter=';')
    failurelog.close()

if __name__ == "__main__":
    p = OptionParser()
    p.add_option("-c", "--config", dest = "config", type = "string", 
                    help = "name of config log files") 
    (opts, args) = p.parse_args()
    
    if not opts.config:
        print "please specify config file (-c)"
        p.print_help()
        exit()
    
    configs = parseConfig(opts.config)
    outfile = opts.config[0:opts.config.rfind(".")]
    
    random.seed(RANDOM_SEED)
    total_nodes = generateCluster(configs, outfile+"_cluster.log")
    generateJob(configs, total_nodes, outfile+"_job.log")
    generateFailure(configs, total_nodes, outfile+"_failure.log")
    exit()