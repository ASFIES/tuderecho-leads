import os
from redis import Redis

REDIS_URL = os.environ.get("REDIS_URL", "").strip()

def redis_client():
    if not REDIS_URL:
        return None
    return Redis.from_url(REDIS_URL)
