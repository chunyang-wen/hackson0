import asyncio
import argparse
import heapq
import pickle
import sys
import time
import logging

from collections import defaultdict

import zmq
import zmq.asyncio


logger = logging.getLogger(__name__)
formatter = logging.Formatter(
    "[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d:%(funcName)s] %(message)s"
)
handler = logging.StreamHandler(stream=sys.stderr)
handler.setFormatter(formatter)
logger.addHandler(handler)
level = "INFO"
logger.setLevel(level)


class BucketStatus:

    def __init__(self, bucket_id, used_bytes):
        self._bucket_id = bucket_id
        self._used_bytes = used_bytes

    def __lt__(self, other):
        return self._used_bytes < other._used_bytes



def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-start-port", default=5555)
    parser.add_argument("--data", default="data.txt")
    parser.add_argument("--bucket-num", default=1, type=int)
    return parser


def init_bucket(args):
    buckets = []
    tasks = []
    context = zmq.asyncio.Context()
    for bucket_id in range(args.bucket_num):
        buckets.append(BucketStatus(bucket_id, 0))
        task = context.socket(zmq.REQ)
        task.connect(f"tcp://localhost:{args.worker_start_port + bucket_id}")
        tasks.append(task)
    return buckets, tasks


def push_to_bucket(message, buckets, meta_info):
    bucket = heapq.heappop(buckets)
    bucket._used_bytes += message["size"]
    heapq.heappush(buckets, bucket)
    meta_info[message["object_id"]] = bucket._bucket_id
    return bucket._bucket_id


async def handle(messages, tasks):
    logger.debug(f"Message bucket size: {len(messages)}")
    for bucket_id, message in messages.items():
        logger.debug(f"Message content size: {len(message)}")
        if message:
            await tasks[bucket_id].send(pickle.dumps(message))
            result = await tasks[bucket_id].recv()
            for content in pickle.loads(result):
                print(content)
                # sys.stdout.flush()


async def main():
    parser = create_parser()
    args, _ = parser.parse_known_args()
    meta_info = {}  # object_id: bucket_id
    buckets, tasks = init_bucket(args)

    s = time.time()
    futures = []
    future_peak = 5000
    message_peak = 1000
    message_count = 0
    messages = defaultdict(list)
    logger.info("Start to ingesting file")
    with open(args.data) as reader:
        counter = 0
        for line in reader:
            counter += 1
            body = {}
            line = line.strip().split(",")
            body["request_id"] = line[0]
            body["action"] = line[1]
            body["object_id"] = line[2]
            if len(line) > 3:
                body["size"] = int(line[3])
                body["hash"] = line[4]
            if body["action"] == "W":
                bucket_id = push_to_bucket(body, buckets, meta_info)
                messages[bucket_id].append(body)
            else:
                bucket_id = meta_info[body["object_id"]]
                messages[bucket_id].append(body)
            message_count += 1
            if message_count == message_peak:
                futures.append(handle(messages, tasks))
                messages = defaultdict(list)
                message_count = 0
                if len(futures) == future_peak:
                    await asyncio.gather(*futures)
                    futures = []
            if counter % 1000000 == 0:
                logger.info(f"Processed to {counter}")
                sys.stdout.flush()
    if message_count:
        futures.append(handle(messages, tasks))
        messages = defaultdict(list)
        message_count = 0
    if futures:
        await asyncio.gather(*futures)
    e = time.time()
    logger.info(f"Time cost: {e - s}")
    for status in buckets:
        logger.info(f"{status._bucket_id} = {status._used_bytes} bytes")


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))

