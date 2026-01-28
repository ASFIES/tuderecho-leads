import os
from redis import Redis
from rq import Worker, Queue, Connection

# ==========================================================
#  SUNTUOSAMENTE AQUÍ VAN TUS 2 VARIABLES CLAVE DE REDIS 👑
# ==========================================================
REDIS_URL = os.environ.get("REDIS_URL", "").strip()          # <-- LINK REDIS (rediss://...)
REDIS_QUEUE_NAME = os.environ.get("REDIS_QUEUE_NAME", "ximena").strip()  # <-- NOMBRE COLA (ximena)

if not REDIS_URL:
    raise RuntimeError("Falta REDIS_URL (link de Redis Key Value de Render).")

listen = [REDIS_QUEUE_NAME]
conn = Redis.from_url(REDIS_URL)

if __name__ == "__main__":
    with Connection(conn):
        worker = Worker([Queue(name) for name in listen])
        worker.work(with_scheduler=False)
