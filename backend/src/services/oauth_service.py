import redis
import uuid
import urllib.parse
import requests
from config.load_env import client_id, client_secret, redirect_uri, auth_url


# redis client
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

# dynamically generate oauth link
def gen_oauth_link():
    state = str(uuid.uuid4())
    
    payload = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "state": state,
    }

    oauth_url = f"{auth_url}?{urllib.parse.urlencode(payload)}"
    
    return oauth_url, state

# store state in redis for 5 mins
def store_state(user_id, state):
    redis_client.setex(f'oauth_state:{state}', 300, user_id)
    
# get user_id from redis using the state
def get_user_id(state):
    return redis_client.get(f'oauth_state:{state}')  

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
    
    