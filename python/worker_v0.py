import argparse
import pickle
import sys

import zmq


def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-port", default="5555")
    parser.add_argument("--id", default=0)
    return parser


def main():
    parser = create_parser()
    args, _= parser.parse_known_args()

    context = zmq.Context()
    #  Socket to talk to server
    print(f"Create worker for {args.id}")
    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://*:{args.worker_port}")
    store = {}

    #  Do 10 requests, waiting each time for a response
    while True:
        #  Get the reply.
        messages = socket.recv()
        messages = pickle.loads(messages)
        # print(f"Receive message: {message}")
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
            else:
                print(f"Unknown message: {message}")
        socket.send(pickle.dumps(result))


if __name__ == "__main__":
    sys.exit(main())

