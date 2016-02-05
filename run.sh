#!/bin/bash
TESTNAME="test1"

echo 'begin '"$TESTNAME"
./loggenerator.py -c "$TESTNAME".config
wait
for i in {1..3}
do
    for j in {1..3}
    do
        ./sim.py -j "$TESTNAME"_job.log -c "$TESTNAME"_cluster.log -f "$TESTNAME"_failure.log -s "$i"-"$j" > "$TESTNAME"_"$i"-"$j"
        wait
        echo 'done simulation '"$TESTNAME"'_'"$i"'-'"$j"
    done
done
echo 'calculating results...'
./parseresult.py -t "$TESTNAME" -j "$TESTNAME"_job.log