import redis
import uuid
import urllib.parse
import requests

from config.load_env import client_id, client_secret, redirect_uri, auth_url

from services.redis_client import get_redis_client
redis_client = get_redis_client()


# dynamically generate oauth link
def gen_oauth_link():
    state = str(uuid.uuid4())
    
    payload = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "state": state,
    }

    oauth_link = f"{auth_url}?{urllib.parse.urlencode(payload)}"
    
    return oauth_link, state

# store oauth data in redis for 20 mins
def store_oauth_data(user_id, tg_user, state):
    key = f'oauth_state:{state}'
    redis_client.hset(key, mapping={"user_id": user_id, "tg_user": tg_user})
    redis_client.expire(key, 1200)
    
# get user_id and user_tg from redis using the state
def get_oauth_data(state):
    key = f'oauth_state:{state}'
    data = redis_client.hgetall(key)
    
    if data:
        return {k.decode("utf-8"): v.decode("utf-8") for k, v in data.items()}  # convert bytes to str
    return None
    
# exchange auth_code for access_token
def get_token(auth_code):
    
    token_url = "https://app.asana.com/-/oauth_token"
    
    payload = {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
        "code": auth_code,
    }
    
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(token_url, data=payload, headers=headers)
    
    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        return None
    
    