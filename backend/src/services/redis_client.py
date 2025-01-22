import redis
from config.load_env import rd_host, rd_port, rd_user, rd_pass

redis_client = redis.Redis(host=rd_host,
                           port=rd_port,
                           username=rd_user,
                           password=rd_pass,
                           decode_responses=True)

def get_redis_client():
    return redis_client