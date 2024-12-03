import redis

client = redis.Redis(host="localhost", port=6379)

try:
    client.ping()
    print("Connected to Redis!")
except redis.ConnectionError:
    print("Could not connect to Redis.")
    