NUM=100
python router_v0.py \
    --data ../../data_large.txt \
    --bucket-num ${NUM} \
    --start-port 5555 > output_large.txt
