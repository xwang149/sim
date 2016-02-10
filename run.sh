#!/bin/bash
TESTNAME="test2"
SIMTIME="365"

echo 'begin '"$TESTNAME"
./loggenerator.py -c "$TESTNAME".config
# wait
for i in {1..3}
do
    for j in {1..3}
    do
        echo 'start simulation '"$TESTNAME"'_'"$i"'-'"$j"'...'
        ./sim.py -t "$SIMTIME" -j "$TESTNAME"_job.log -c "$TESTNAME"_cluster.log -f "$TESTNAME"_failure.log -s "$i"-"$j" > "$TESTNAME"_"$i"-"$j"
        wait
        echo 'done simulation '"$TESTNAME"'_'"$i"'-'"$j"
    done
done
echo 'calculating results...'
./parseresult.py -t "$TESTNAME" -j "$TESTNAME"_job.log -c "$TESTNAME".config