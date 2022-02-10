import argparse
import logging
import pickle
import sys

import zmq


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



def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-pub-port", default="5555")
    parser.add_argument("--result-pub-port", default="5556")
    parser.add_argument("--id", default=0)
    return parser


def main():
    parser = create_parser()
    args, _= parser.parse_known_args()

    context = zmq.Context()
    #  Socket to talk to server
    logger.info(f"Create worker for {args.id}")
    logger.info(f"Task pub port: {args.task_pub_port}")
    logger.info(f"Result pub port: {args.result_pub_port}")

    socket = context.socket(zmq.SUB)
    address = f"tcp://localhost:{args.task_pub_port}"
    logger.info(f"Connect to address: {address}")
    socket.connect(address)
    topicfilter = f"{args.id}"
    logger.info(f"Topic filter = {topicfilter}")
    topicfilter = ""
    socket.setsockopt_string(zmq.SUBSCRIBE, topicfilter, encoding="utf-8")
    store = {}

    result_socket = context.socket(zmq.PUB)
    result_socket.bind(f"tcp://*:{args.result_pub_port}")
    connecting = False
    connected = False

    #  Do 10 requests, waiting each time for a response
    while True:
        #  Get the reply.
        logger.debug("Wait to receive message")
        messages = socket.recv()
        # print(messages)
        messages = messages.split(b"-", maxsplit=1)[1]
        messages = pickle.loads(messages)
        logger.debug(f"Receive message: {messages}")
        result = []
        for message in messages:
            object_id = message["object_id"]
            if message["action"] == "R":
                body = f"{message['request_id']},{store[object_id]}"
                result.append(body)
            elif message["action"] == "W":
                body = f"{message['request_id']},{args.id}"
                store[object_id] = message["hash"]
                result.append(body)
            elif message["action"] == "C":
                if not connecting:
                    logger.info("Connecting stage")
                    connecting = True
            else:
                print(f"Unknown message: {message}")
            if message["action"] == "C":
                result_socket.send(pickle.dumps([]))
            else:
                if not connected:
                    logger.info("Connected")
                    connected = True
        logger.debug("Finish process message")
        result_socket.send(pickle.dumps(result))


if __name__ == "__main__":
    sys.exit(main())

