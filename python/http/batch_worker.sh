#!/bin/bash
NUM=100
PORT=5555


((NUM-=1))

command rm -vf pid.info

for i in `seq 0 $NUM`
do
    python worker_v0.py --port $PORT --id $i &> $i.log&
    echo $! >> pid.info
    ((PORT+=1))
    #echo $PORT":"$i
done
