import redis
from config.load_env import rd_host, rd_port

redis_client = redis.Redis(host=rd_host, port=rd_port, decode_responses=True)

def get_redis_client():
    return redis_client