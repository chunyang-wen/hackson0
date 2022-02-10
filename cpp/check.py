#!/usr/bin/env python3

import sys

write_req_map = {}
read_req_map = {}
obj_map = {}
input_cnt = 0


def load_input(fname: str) -> None:
    global input_cnt
    with open(fname, 'r') as f:
        for line in f:
            if len(line) < 3:
                continue
            input_cnt += 1
            parts = line.strip().split(',')
            if parts[1] == 'R':
                read_req_map[parts[0]] = obj_map[parts[2]]
            else:
                write_req_map[parts[0]] = int(parts[3])
                obj_map[parts[2]] = parts[4]

bucket_map = {}

def check_output(fname: str) -> None:
    output_cnt = 0
    with open(fname, 'r') as f:
        for line in f:
            if len(line) < 3:
                continue
            output_cnt += 1
            parts = line.strip().split(',')
            if parts[0] in read_req_map:
                if parts[1] != read_req_map[parts[0]]:
                    print(f"read response wrong, required {read_req_map[parts[0]]}, go {parts[1]}")
            else:
                bid = int(parts[1])
                bucket_map[bid] = bucket_map.get(bid, 0) + write_req_map[parts[0]]
    if output_cnt != input_cnt:
        print(f"output count mismatch, required {input_cnt}, got {output_cnt}")

    print("top buckets:")
    for _, v in sorted(bucket_map.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"\t{v}")


if __name__ == '__main__':
    load_input(sys.argv[1])
    check_output(sys.argv[2])

