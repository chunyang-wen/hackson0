import asyncio
import argparse
import heapq
# import json
import orjson as json
import sys
import time
import logging

from collections import defaultdict

import aiohttp


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
    parser.add_argument("--start-port", default=5555, type=int)
    parser.add_argument("--data", default="data.txt")
    parser.add_argument("--bucket-num", default=1, type=int)
    return parser


def init_bucket(args):
    buckets = []
    urls = []
    for bucket_id in range(args.bucket_num):
        buckets.append(BucketStatus(bucket_id, 0))
        port = args.start_port + bucket_id
        address = f"http://localhost:{port}/v1/messages"
        logger.info(f"Worker address for {bucket_id}: {address}")
        urls.append(address)
    return buckets, urls


def push_to_bucket(message, buckets, meta_info):
    bucket = heapq.heappop(buckets)
    bucket._used_bytes += message["size"]
    heapq.heappush(buckets, bucket)
    meta_info[message["object_id"]] = bucket._bucket_id
    return bucket._bucket_id


async def handle_request(bucket_id, urls, message, session):
    message = {"messages": message}
    # logger.debug(f"Send message: {message}")
    logger.debug(f"Send finish for {bucket_id}")
    message = json.dumps(message)
    async with session.post(urls[bucket_id], data=message) as response:
        result = await response.json()
        print("\n".join(result["result"]))
        # sys.stdout.flush()


async def handle(messages, urls, session):
    logger.debug(f"Message bucket size: {len(messages)}")
    tasks = []
    for bucket_id, message in messages.items():
        logger.debug(f"Message content size: {len(message)}")
        if message:
            tasks.append(handle_request(bucket_id, urls, message, session))
    await asyncio.gather(*tasks)

async def main():
    parser = create_parser()
    args, _ = parser.parse_known_args()
    meta_info = {}  # object_id: bucket_id
    # cache = {}
    buckets, urls = init_bucket(args)
    session = aiohttp.ClientSession()

    s = time.time()
    futures = []
    future_peak = 10
    message_peak = 1000000
    message_count = 0
    messages = defaultdict(list)
    logger.info("Start to ingesting file")
    with open(args.data) as reader:
        counter = 0
        for line in reader:
            counter += 1
            if counter % 1000000 == 0:
                logger.info(f"Processed to {counter}")
                sys.stdout.flush()
            body = {}
            line = line.strip().split(",")
            body["request_id"] = line[0]
            body["action"] = line[1]
            body["object_id"] = line[2]
            object_id = line[2]
            if len(line) > 3:
                body["size"] = int(line[3])
                body["hash"] = line[4]
            if body["action"] == "W":
                bucket_id = push_to_bucket(body, buckets, meta_info)
                messages[bucket_id].append(body)
                # cache[object_id] = line[4]
            else:
                # if object_id in cache:
                    # print(f"{line[0]},{cache[object_id]}")
                    # continue
                bucket_id = meta_info[object_id]
                messages[bucket_id].append(body)
            message_count += 1
            if message_count == message_peak:
                futures.append(handle(messages, urls, session))
                messages = defaultdict(list)
                message_count = 0
                if len(futures) == future_peak or True:
                    logger.debug(f"Fetching data: {len(futures)}")
                    await asyncio.gather(*futures)
                    futures = []
    if message_count:
        futures.append(handle(messages, urls, session))
        messages = defaultdict(list)
        message_count = 0
    if futures or True:
        await asyncio.gather(*futures)
    e = time.time()
    for status in buckets:
        logger.info(f"{status._bucket_id} = {status._used_bytes} bytes")
    logger.info(f"Time cost: {e - s}")
    await session.close()


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))


