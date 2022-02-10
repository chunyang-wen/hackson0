while read line
do
    echo "Stop pid: ${line}"
    kill -9 $line
done < pid.info
