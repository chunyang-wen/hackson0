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
logger.propagate = False


class BucketStatus:

    def __init__(self, bucket_id, used_bytes):
        self._bucket_id = bucket_id
        self._used_bytes = used_bytes

    def __lt__(self, other):
        return self._used_bytes < other._used_bytes



def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-pub-port", default=5555)
    parser.add_argument("--result-pub-port", default=[], action="append")
    parser.add_argument("--data", default="data.txt")
    parser.add_argument("--bucket-num", default=1, type=int)
    return parser


def init_bucket(args):
    buckets = []
    context = zmq.asyncio.Context()
    task = context.socket(zmq.PUB)
    task_address = f"tcp://*:{args.task_pub_port}"
    logger.info(f"Task address: {task_address}")
    task.bind(task_address)

    result = context.socket(zmq.SUB)
    result.setsockopt_string(zmq.SUBSCRIBE, "")
    for port in args.result_pub_port:
        result_pub_address = f"tcp://localhost:{port}"
        logger.info(f"Connect to result pub address: {result_pub_address}")
        result.connect(result_pub_address)
    for bucket_id in range(args.bucket_num):
        buckets.append(BucketStatus(bucket_id, 0))
    return buckets, task, result


def push_to_bucket(message, buckets, meta_info):
    bucket = heapq.heappop(buckets)
    bucket._used_bytes += message["size"]
    heapq.heappush(buckets, bucket)
    meta_info[message["object_id"]] = bucket._bucket_id
    return bucket._bucket_id

global_counter_read = 0

async def handle_request(bucket_id, task, message, result_queue):
    global global_counter_read
    message = f"{bucket_id}".encode() +  b"-" + pickle.dumps(message)
    # logger.debug(f"Send message: {message}")
    await task.send(message)
    logger.debug(f"Send finish for {bucket_id}")
    message = await result_queue.recv()
    messages = pickle.loads(message)
    global_counter_read += len(messages)
    for content in messages:
        print(content)


global_counter_write = 0

async def handle(messages, task, result_queue):
    global global_counter_write
    logger.debug(f"Message bucket size: {len(messages)}")
    tasks = []
    for bucket_id, message in messages.items():
        logger.debug(f"Message content size: {len(message)}")
        if message:
            tasks.append(handle_request(bucket_id, task, message, result_queue))
            global_counter_write += len(message)
    await asyncio.gather(*tasks)

async def main():
    parser = create_parser()
    args, _ = parser.parse_known_args()
    meta_info = {}  # object_id: bucket_id
    buckets, task, result_queue = init_bucket(args)

    s = time.time()
    futures = []
    future_peak = 5000
    message_peak = 100000
    message_count = 0
    messages = defaultdict(list)
    # Setup connection
    logger.info("Connection setup stage")
    for _ in range(100):
        for bucket_id in range(args.bucket_num):
            message = [{"action": "C", "object_id": "0"}]
            message = f"{bucket_id}".encode() +  b"-" + pickle.dumps(message)
            task.send(message)
            result_queue.recv()
        time.sleep(0.03)

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
                futures.append(handle(messages, task, result_queue))
                messages = defaultdict(list)
                message_count = 0
                if len(futures) == future_peak or True:
                    logger.debug(f"Fetching data: {len(futures)}")
                    await asyncio.gather(*futures)
                    futures = []
            if counter % 1000000 == 0:
                logger.info(f"Processed to {counter}")
                sys.stdout.flush()
    if message_count:
        futures.append(handle(messages, task, result_queue))
        messages = defaultdict(list)
        message_count = 0
    if futures or True:
        await asyncio.gather(*futures)
    e = time.time()
    logger.info(f"Time cost: {e - s}")
    for status in buckets:
        logger.info(f"{status._bucket_id} = {status._used_bytes} bytes")
    sys.stdout.flush()
    logger.info(f"Global counter: {global_counter_write}, {global_counter_read}")


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))

