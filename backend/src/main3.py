from config.load_env import client_id, client_secret, redirect_uri, auth_url, bot_token

import redis
import uuid
import urllib.parse

from telegram import Update
from telegram.ext import Application, CommandHandler, CallbackContext

from flask import Flask, request, jsonify
import requests
import os
# Initialize Redis
redis_client = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)

# Flask app for handling Asana callback
app = Flask(__name__)

# Dynamically generate OAuth link
def gen_oauth_link(chat_id):
    state = str(uuid.uuid4())  # Generate a unique state
    redis_client.setex(f"oauth_state:{chat_id}", 300, state)  # Store state in Redis with 5 min expiry

    redirect_uri_upd = f'{redirect_uri}?chat_id={chat_id}'

    payload = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri_upd,
        "state": state,
        "chat_id": chat_id, 
    }

    oauth_url = f"{auth_url}?{urllib.parse.urlencode(payload)}"
    return oauth_url, state


# Telegram /connect command
async def connect_command(update: Update, context: CallbackContext):
    chat_id = update.effective_chat.id
    oauth_link, state = gen_oauth_link(chat_id)  # Generate OAuth link and state
    await update.message.reply_text(
        f"Connect to Asana:\n\n{oauth_link}"
    )



# Asana OAuth callback route
@app.route('/callback', methods=['GET'])

def callback():
    auth_code = request.args.get('code')
    res_state = request.args.get('state')
    chat_id = request.args.get('chat_id')  # extract chat_id from the query parameter
    
    # Validate state
    stored_state = redis_client.get(f"oauth_state:{chat_id}")
    if not stored_state or stored_state != res_state:
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
    response = requests.post(token_url, data=payload)

    if response.status_code == 200:
        access_token = response.json().get("access_token")
        return f"Authorization successful! Access token: {access_token}"
    else:
        return f"Error exchanging code: {response.content}", 400


# Start the Telegram bot
def main():
    application = Application.builder().token(bot_token).build()
    application.add_handler(CommandHandler("connect", connect_command))
    
    # Run Flask app and Telegram bot in parallel
    import threading
    threading.Thread(target=lambda: app.run(port=5000, debug=True, use_reloader=False)).start()
    
    application.run_polling()

if __name__ == "__main__":
    main()
