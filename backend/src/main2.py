from config.load_env import client_id, redirect_uri, auth_url, bot_token

import uuid
import urllib.parse

from telegram import Update
from telegram.ext import Application, CommandHandler, CallbackContext

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


'''Bot /connect command'''

# /connect 
async def connect_command(update: Update, context: CallbackContext):
    
    oauth_link, state = gen_oauth_link() # generate oauth link
    context.chat_data["oauth_state"] = state # store the state for validation
    
    # send the link to the user
    await update.message.reply_text(
        f"Connect to Asana:\n\n{oauth_link}"
    )


#
def main():
    # create app instance 
    application = Application.builder().token(bot_token).build()
    
    # add /connect command handler
    application.add_handler(CommandHandler("connect", connect_command))
    
    application.run_polling()

if __name__ == "__main__":
    main()
