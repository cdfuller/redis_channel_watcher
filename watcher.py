import argparse
import os
import time

from datetime import datetime as dt

import redis

def watch_redis(host, port, channel):
    """
    Watches the redis channel @ host:post
    """

    print(f"Connecting to {host}:{port}")
    r = redis.Redis(host=host, port=port)
    pubsub = r.pubsub()
    pubsub.subscribe(channel)

    message = pubsub.get_message(timeout=30)

    if message['type'] != 'subscribe':
        raise Exception(f"Unable to subscribe to channel '{channel}'")
    else:
        print(f"Subscribed to {message['channel'].decode('utf-8')}")

    while True:
        message = pubsub.get_message()
        if message:
            channel = message['channel'].decode('utf-8')
            data = message['data'].decode('utf-8')
            print(f"{dt.now()} channel={channel} msg={data}")
        else:
            time.sleep(0.001)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Debug tool for monitoring redis channels")
    parser.add_argument('-c', '--channel', default='production-requests', help="Channel to watch")
    parser.add_argument('-r', '--redis_host', default='localhost', help="Redis hostname")
    parser.add_argument('-p', '--port', default=6379, type=int, help="Redis port")

    args = parser.parse_args()
    redis_channel = args.channel
    redis_host = args.redis_host
    redis_port = args.port

    watch_redis(redis_host, redis_port, redis_channel)
