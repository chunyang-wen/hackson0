#!/bin/bash
NUM=2
PORT=8080


((NUM-=1))

command rm -vf pid.info

for i in `seq 0 $NUM`
do
    ./hackson0 --port $PORT --id $i --type W &> $i.log&
    echo $! >> pid.info
    ((PORT+=1))
    #echo $PORT":"$i
done
