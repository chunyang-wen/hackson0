import argparse
import logging
import orjson as json
import sys

from tornado.web import Application
from tornado.web import RequestHandler
from tornado.ioloop import IOLoop



logger = logging.getLogger(__name__)
formatter = logging.Formatter(
    "[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d:%(funcName)s] %(message)s"
)
handler = logging.StreamHandler(stream=sys.stderr)
handler.setFormatter(formatter)
logger.addHandler(handler)
level = "DEBUG"
logger.setLevel(level)


def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", default="5555")
    parser.add_argument("--id", default=0)
    return parser

store = {}


class Handler(RequestHandler):

    def initialize(self, args):
        self.args = args

    async def post(self):
        body = json.loads(self.request.body)
        result = []
        for message in body["messages"]:
            object_id = message["object_id"]
            if message["action"] == "W":
                body = f"{message['request_id']},{self.args.id}"
                store[object_id] = message["hash"]
                result.append(body)
            elif message["action"] == "R":
                body = f"{message['request_id']},{store[object_id]}"
                result.append(body)
        result = {"result": result}
        self.write(result)


def create_application(args):
    url_specs = [("/v1/messages", Handler, dict(args=args))]
    for spec in url_specs:
        logger.info(f"Register url: {spec[0]}, handler: {spec[1].__name__}")
    app = Application(url_specs) # type: ignore
    return app


def main():
    parser = create_parser()
    args, _= parser.parse_known_args()

    app = create_application(args)
    logger.info(f"Start server at port = {args.port}")
    app.listen(args.port)

    try:
        IOLoop.current().start()
    except KeyboardInterrupt:
        logger.info("Stop tornado service")
        IOLoop.current().stop()


if __name__ == "__main__":
    sys.exit(main())


