from config.load_env import client_id, client_secret, redirect_uri, auth_url, bot_token

import redis
import uuid
import urllib.parse

from telegram import Update
from telegram.ext import Application, CommandHandler, CallbackContext

from flask import Flask, request, jsonify
import requests
import os


'''Redis client'''
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)


'''Asana user Authorization'''

# dynamically generating OAuth link
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


'''Bot command /connect'''

# /connect 
async def connect_command(update: Update, context: CallbackContext):
    
    oauth_link, state = gen_oauth_link()  # generate oauth link
    
    # Store the state in Redis along with user_id mapping
    redis_client.setex(f'oauth_state:{state}', 300, update.effective_user.id)  # Store state with user ID mapping
    
    # Send the OAuth link to the user
    await update.message.reply_text(
        f"Connect to Asana:\n\n{oauth_link}"
    )


'''Oauth callback route'''

app = Flask(__name__)

@app.route('/callback', methods=['GET'])
def callback():
    auth_code = request.args.get('code')
    res_state = request.args.get('state')
    
    # Fetch the user_id associated with the state
    user_id = redis_client.get(f'oauth_state:{res_state}')
    
    if not user_id:
        return "Invalid / Expired state", 400
    
    # Exchange auth code for access token
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
        access_token = response.json().get("access_token")
        return jsonify({"message": "Authorization successful", "access_token": access_token})
    else:
        return f"Error exchanging code: {response.text}", response.status_code


'''Run bot'''
def main():
    # create app instance 
    application = Application.builder().token(bot_token).build()
    
    # add /connect command handler
    application.add_handler(CommandHandler("connect", connect_command))
    
    application.run_polling()


if __name__ == "__main__":

    import threading
    threading.Thread(target=lambda: app.run(port=5000, debug=True, use_reloader=False)).start()

    main()
