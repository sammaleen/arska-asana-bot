from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import Application, CommandHandler, CallbackContext
from prettytable import PrettyTable

import pandas as pd
from flask import Flask, request, jsonify
import threading
import logging

from services.oauth_service import gen_oauth_link, store_state, get_user_id, get_token
from services.asana_data import get_user_name, get_user_data, save_asana_data, get_redis_data, get_tasks, format_df
from services.redis_client import get_redis_client

from config.load_env import bot_token, workspace_gid


# set logger and flask
logging.basicConfig(
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

app = Flask(__name__)


# redis health check
def redis_check():
    
    try:
        redis_client = get_redis_client()
        redis_client.ping()
        logger.info("redis is healthy")
        return True
    
    except Exception as err:
        logger.error(f"redis connection failed: {err}")
        return False
    

# COMMANDS ---

# /START command handler
async def start_command(update: Update, context: CallbackContext):
    
    oauth_link, state = gen_oauth_link()  # generate oauth link
    store_state(update.effective_user.id, state) # store the state in Redis along with user_id mapping
    logger.info(f"user - {update.effective_user.id}, state - {state}")
    
    keyboard = [[InlineKeyboardButton("Connect to Asana", url=oauth_link)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    start_message = (
        "*Привет*\!\n\n"
        "Бот организует связь с Асаной для получения данных по запланированным задачам\.\n\n"
        
        "*Авторизация*\n"
        "Для начала работы авторизуйте свой аккаунт в Асане через кнопку Connect to Asana ниже или с помощью команды [/connect] \\. "
        "Все доступные команды бота находятся в Menu внизу и здесь, в стартовом сообщении\.\n\n"
        
        "*Команды*\n"
        "[/start] \\- вернуться к стартовому сообщению\n"
        "[/connect] \\- авторизоваться в Асане\n"
        "[/mytasks] \\- посмотреть задачи на день\n\n"
        
        "*По вопросам*\n"
        "[@sammaleen] \\- Лена"

    )
        
    await context.bot.send_photo(
        chat_id=update.effective_chat.id,
        photo=open("C:/Users/samma/cursor/asana_bot/backend/src/assets/start.png", "rb"),
        caption=start_message,
        parse_mode="MarkdownV2",
        reply_markup=reply_markup
    )
    
    #await update.message.reply_text(start_message, 
                                    #reply_markup=reply_markup,
                                    #parse_mode="Markdown",
                                    #chat_id=update.effective_chat.id,
                                    #photo=open("C:/Users/samma/cursor/asana_bot/backend/src/assets/start.png", "rb"))


# /CONNECT command handler
async def connect_command(update: Update, context: CallbackContext):
    
    oauth_link, state = gen_oauth_link()  # generate oauth link
    store_state(update.effective_user.id, state) # store the state in Redis along with user_id mapping
    logger.info(f"user - {update.effective_user.id}, state - {state}")
    
    keyboard = [[InlineKeyboardButton("OAuth Link", url=oauth_link)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text("Перейдите по ссылке для авторизации:\n", reply_markup=reply_markup)
   
   
# /MYTASKS command handler
async def mytasks_command(update: Update, context: CallbackContext):
    
    keyboard = [[InlineKeyboardButton("Add notes", callback_data="add_notes")]]   
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    #fetch tasks and format message out of tasks df
    user_id = update.effective_user.id
    df = get_tasks(user_id, workspace_gid)
    mytasks_message = format_df(df)
    
    await context.bot.send_photo(
        chat_id=update.effective_chat.id,
        photo=open("C:/Users/samma/cursor/asana_bot/backend/src/assets/mytasks.png", "rb"),
        caption=mytasks_message,
        parse_mode="Markdown",
        reply_markup=reply_markup
    )
   
   
   
# bot post initialization, menu set
async def post_init(application: Application) -> None:
    
    await application.bot.set_my_commands(
        [BotCommand('start', 'go to start message'),
         BotCommand('connect', 'connect to Asana'),
         BotCommand('mytasks', 'get list of tasks for today')]
        )
    
    menu_button = {
    "type": "default",
    "text": "Menu",  
    }
    
    await application.bot.set_chat_menu_button()


# OAuth callback route

@app.route('/callback', methods=['GET'])

async def callback():
    
    auth_code = request.args.get('code')
    res_state = request.args.get('state')
    
    logger.info(f"recieved callback, state - {res_state}")
    user_id = get_user_id(res_state) # fetch the user_id associated with the state
    
    if not user_id:
        logger.error(f"invalid/expired state - {res_state}")
        return "Invalid / Expired state", 400
    logger.info(f"valid state for user - {user_id}")
     
    access_token = get_token(auth_code) # exchange auth code for access token

    if not access_token:
        logger.error(f"failed exchange auth code for token for user: {user_id}")
        auth_message = "`auth failed`\n`try to re-run /connect`"
        
    user_name = get_user_name(access_token)  
    
    if not user_name:
        logger.error(f"failed to get user name for user: {user_id}")
        auth_message = "`auth failed`\n`try to re-run /connect`"
    
    user_gid, user_token = get_user_data(user_name)
    
    if not user_token:
        logger.error(f"failed to get permanent token from db for user: {user_name}/{user_id}")
        auth_message = (
            "`auth successful`\n"
            f"`user_name: {user_name}`\n"
            "`user_token: missing\n`"
            "[click here to set personal token](https://docs.google.com/spreadsheets/d/1w9pbRfUU2pPqiB8oAIUs5wqPxHMjxzMcJQ6aTL0WMtM/edit?usp=sharing)"
            )
        
        try:
            application_instance = app.config['application_instance']
            chat = await application_instance.bot.get_chat(user_id)
            await chat.send_message(auth_message, parse_mode="Markdown")
            
        except Exception as err:
            logger.error(f"error sending message to user: {user_name}/{user_id}: {err}")
        
        return jsonify({"message": "auth successful", "user_token": "missing"}), 400
    
    data_saved = save_asana_data(user_name, user_gid, user_token, user_id)
    
    if not data_saved:
        logger.error(f"failed to save token for user: {user_name}/{user_id}")
        auth_message = (
            "`auth successful`\n"
            f"`user_name: {user_name}`\n"
            "`user_token: present, NOT saved (!)"
            "'please, retry /connect'"
        )
        try:
            application_instance = app.config['application_instance']
            chat = await application_instance.bot.get_chat(user_id)
            await chat.send_message(auth_message, parse_mode="Markdown")
            
        except Exception as err:
            logger.error(f"error sending message to user: {user_name}/{user_id}: {err}")
            
        return jsonify({"message": "auth failed"}), 500
    
    # full success, token present and saved
    auth_message = (
        "`auth successful`\n"
        f"`user_name: {user_name}`\n"
        "`user_token: present, saved`"
    )
    
    try:
        application_instance = app.config['application_instance']
        chat = await application_instance.bot.get_chat(user_id)
        await chat.send_message(auth_message, parse_mode="Markdown")
        
    except Exception as err:
        logger.error(f"error sending message to user: {user_name}/{user_id}: {err}")

    logger.info(f"token saved for user: {user_name}/{user_id}")
    return jsonify({"message": "auth successful", "user_name": user_name, "user_token": "present, saved"})
    
    
# run flask app in a separate thread to handle OAuth callback
def start_flask_app(application):
    app.config['application_instance'] = application  # pass application instance to Flask
    app.run(port=5000, debug=True, use_reloader=False)


# bot initialization 
def main():
    
    application = Application.builder().token(bot_token).post_init(post_init).build()
    
    application.add_handler(CommandHandler("start", start_command)) # add /start
    application.add_handler(CommandHandler("connect", connect_command)) # add /connect
    application.add_handler(CommandHandler("mytasks", mytasks_command)) # add /mytasks
    
    # start flask app in a separate thread
    flask_thread = threading.Thread(target=start_flask_app, args=(application,))
    flask_thread.start()

    # start bot
    application.run_polling()


if __name__ == "__main__":
    main() 
    