#!/bin/bash

for i in {1..10}
do
    ./loggenerator.py -c test1.config
    wait
    ./sim.py -j test1_job.log -c test1_cluster.log -f test1_failure.log -s 1-1 > output.log
    wait
    cat -n output.log | sort -uk2 | sort -nk1 | cut -f2- > result.txt
    wait
    ./parseresult.py -f result.txt -s 1-1
    wait

    ./sim.py -j test1_job.log -c test1_cluster.log -f test1_failure.log -s 1-2 > output.log
    wait
    cat -n output.log | sort -uk2 | sort -nk1 | cut -f2- > result.txt
    wait
    ./parseresult.py -f result.txt -s 1-2
    wait

    # ./sim.py -j test1_job.log -c test1_cluster.log -f test1_failure.log -s 1-3 > output.log
    # wait
    # cat -n output.log | sort -uk2 | sort -nk1 | cut -f2- > result.txt
    # wait
    # ./parseresult.py -f result.txt
    # wait
done