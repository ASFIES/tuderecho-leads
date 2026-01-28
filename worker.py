import os
from redis import Redis
from rq import Worker, Queue, Connection

REDIS_URL = os.environ.get("REDIS_URL", "").strip()
REDIS_QUEUE_NAME = os.environ.get("REDIS_QUEUE_NAME", "ximena").strip()

if not REDIS_URL:
    raise RuntimeError("Falta REDIS_URL.")

listen = [REDIS_QUEUE_NAME]
conn = Redis.from_url(REDIS_URL)

if __name__ == "__main__":
    with Connection(conn):
        worker = Worker([Queue(name) for name in listen])
        worker.work(with_scheduler=False)