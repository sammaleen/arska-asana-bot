import redis
from config.load_env import rd_host, rd_port, db_user, db_pass

redis_client = redis.Redis(host=rd_host,
                           port=rd_port,
                           decode_responses=True,
                           username=db_user,
                           password=db_pass)

def get_redis_client():
    return redis_client

success = redis_client.set('foo', 'bar')
result = redis_client.get('foo')
print(result)