#!/usr/bin/env python3

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import Application, CommandHandler, CallbackContext, CallbackQueryHandler, MessageHandler, filters

import pandas as pd
from pathlib import Path
from datetime import datetime

from flask import Flask, request, jsonify
import threading
import logging

from services.oauth_service import gen_oauth_link, store_state, get_user_id, get_token

from services.asana_data import (get_user_name,
                                 get_user_data,
                                 save_asana_data,
                                 get_redis_data,
                                 get_tasks,
                                 get_note,
                                 format_df, 
                                 store_note,
                                 get_tasks_report,
                                 get_tg_user,
                                 format_report
                                 )

from services.redis_client import get_redis_client
from config.load_env import bot_token, workspace_gid, gs_url


# set logger and flask
logging.basicConfig(
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s", 
    level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)
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
    
    keyboard = [[InlineKeyboardButton("Connect to Asana 🔑", url=oauth_link)]]
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
        "[/mytasks] \\- посмотреть свои задачи на день\n"
        "[/report] \\- получить отчет по плану на день для всех\n\n "
        
        "*По вопросам*\n"
        "[@sammaleen] \\- Лена"
    )
        
    await context.bot.send_photo(
        chat_id=update.effective_chat.id,
        photo=open(Path(__file__).parent / "assets/start.png", "rb"),
        caption=start_message,
        parse_mode="MarkdownV2",
        reply_markup=reply_markup
    )
    
    
# /CONNECT command handler
async def connect_command(update: Update, context: CallbackContext):
    
    oauth_link, state = gen_oauth_link()  # generate oauth link
    store_state(update.effective_user.id, state) # store the state in Redis along with user_id mapping
    logger.info(f"auth for user - {update.effective_user.id}, state - {state}")
    
    keyboard = [[InlineKeyboardButton("OAuth Link", url=oauth_link)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text("Перейдите по ссылке для авторизации:\n", reply_markup=reply_markup)
   
   
# /MYTASKS command handler
async def mytasks_command(update: Update, context: CallbackContext):
    
    keyboard = [[InlineKeyboardButton("Add notes ✍", callback_data="add_notes")]]   
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    #fetch tasks and note, format message
    user_id = update.effective_user.id
    df = get_tasks(user_id, workspace_gid)
    extra_note = get_note(user_id)
    
    if not df.empty: 
        mytasks_message = format_df(df, extra_note, max_len=4000, max_note_len=150)
    else:
        mytasks_message = (
            f"*{datetime.now().strftime('%d %b %Y · %a')}*\n\n"
            "`No task for today`"
        )
    
    await context.bot.send_photo(
        chat_id=update.effective_chat.id,
        photo=open(Path(__file__).parent / "assets/mytasks.png", "rb"),
        caption=mytasks_message,
        parse_mode="Markdown",
        reply_markup=reply_markup
    )
 
 
# ADD NOTES button
# add notes callback

note_input_state = {}

async def add_notes_callback(update: Update, context: CallbackContext):
   query = update.callback_query
   await query.answer()
    
   user_id = query.from_user.id
   chat_id = query.message.chat.id
    
   # prompt the user
   note_input_state[user_id] = {"chat_id": chat_id}
   
   await context.bot.send_message(
        chat_id=chat_id,
        text="Пожалуйста, напишите вашу заметку к плану задач на сегодня ответом на это сообщение"
    )

  
# handle user response 
async def note_input(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    user_gid, user_name, user_token, tg_user = get_redis_data(user_id)
    chat_id = note_input_state.get(user_id, {}).get("chat_id")
    
    if chat_id:
        if update.effective_message:  # handling text input
            note_text = update.effective_message.text
            note_input_state[user_id]["note"] = note_text 
            
            keyboard = [
                [
                    InlineKeyboardButton("Save 💾", callback_data="confirm_note"),
                    InlineKeyboardButton("Re-write ✍", callback_data="edit_note"),
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"Ваша заметка:\n\n*{note_text}*\n\nПодтвердите сохранение или перепишите заметку",
                    reply_markup=reply_markup,
                    parse_mode="Markdown"
                )
                logger.info(f"note '{note_text}' sent to user {user_id}/{user_name} for confirmation")
            except Exception as err:
                logger.error(f"error while sending message to chat {chat_id}/{user_name}: {err}")
        else:
            await update.message.reply_text("To add notes use '/mytasks' command -> 'Add notes' button")
            logger.info("user tried to add notes without initiating from the correct button")
    else:
        await update.message.reply_text("To add notes use '/mytasks' command -> 'Add notes' button")
        logger.info("user tried to add notes without initiating from the correct button")

        
# handle note saving/rewriting 
async def process_note(update: Update, context: CallbackContext):
    query = update.callback_query
    user_id = query.from_user.id
    user_gid, user_name, user_token, tg_user = get_redis_data(user_id)

    if user_id in note_input_state:
        data = query.data
        chat_id = note_input_state[user_id]["chat_id"]
        note = note_input_state[user_id].get("note")
        
        logger.info(f"{user_id}/{user_name} clicked button {data}")

        # save note
        if data == "confirm_note": 
            success = store_note(note, user_id)
            
            if success:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="`Заметка сохранена успешно`",
                    parse_mode="Markdown"
                )
            else:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="`Возникла пробема с сохранением заметки. Пожалуйста, повторите процесс`",
                    parse_mode="Markdown"
                )
            del note_input_state[user_id]  # clear state
            
        # re-write note
        elif data == "edit_note": 
            note_input_state[user_id]["status"] = "awaiting_new_note"  
            note_input_state[user_id]["note"] = None  
            await context.bot.send_message(
                chat_id=chat_id,
                text="Напишите обновленную заметку"
            )
    else:
        await query.answer("Use command '/mytasks' again")

  
# /REPORT command handler    
async def report_command(update: Update, context: CallbackContext):
    
    user_id = update.effective_user.id
    user_gid, user_name, user_token, tg_user = get_redis_data(user_id)
    
    tasks_dict = get_tasks_report(user_name)
    
    if tasks_dict:
        users = list(tasks_dict.keys())
        mes_num = len(users)
        
        logger.info(f"got report data for {mes_num} users")
        
        # create formatted reports for each user from tasks_dict
        reports = []
        for user, user_df in tasks_dict.items():
            tg_user_name = get_tg_user(user)
            user_report = format_report(user_df, user, tg_user_name, max_len=4000, max_note_len=150)
            reports.append(user_report)
        
        # send each report as a separate message 
        for report in reports:
            try:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=report,
                    parse_mode='Markdown'
                )   
                logger.info(f"report sent to user: {user_id}/{user_name}")
            except Exception as err:
                logger.error(f"error sending report to user: {user_id}/{user_name}")
    else:
        report_message = (
            f"*{datetime.now().strftime('%d %b %Y - %a')}*\n\n"
            "`No data is present for now`"
        )
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text = report_message,
            parse_mode="Markdown"
        )
    
    
# MENU bot post initialization
async def post_init(application: Application) -> None:
    
    await application.bot.set_my_commands(
        [BotCommand('start', 'go to start message'),
         BotCommand('connect', 'connect to Asana'),
         BotCommand('mytasks', 'get list of tasks for today'),
         BotCommand('report', 'get report on tasks for all users')
         ]
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
    
    logger.info(f"recieved callback, state: {res_state}, auth_code: {auth_code}")
    user_id = get_user_id(res_state) # fetch the user_id associated with the state
    
    if not user_id:
        logger.error(f"invalid/expired state - {res_state}")
        return "Invalid / Expired state", 400
    logger.info(f"valid state for user - {user_id}")
    
    # fetch tg username
    try:
        application_instance = app.config['application_instance']
        chat = await application_instance.bot.get_chat(user_id)
        tg_user = chat.username
        logger.info(f"fetched TG username {tg_user} for user: {user_id}")
        
    except Exception as err:
        logger.error(f"failed to fetch TG username for user: {user_id}")
        tg_user = None 
    
    # exchange auth code for access token
    access_token = get_token(auth_code) 

    if not access_token:
        logger.error(f"failed exchange auth code for token for user: {user_id}")
        auth_message = "`auth failed`\n`try to re-run /connect`"
        
    user_name = get_user_name(access_token)  
    
    if not user_name:
        logger.error(f"failed to get user name for user: {user_id}")
        auth_message = "`auth failed`\n`try to re-run /connect`"
    
    user_gid, user_token = get_user_data(user_name)
    
    if not user_token:
        logger.error(f"failed to get permanent token from db for user: {user_id}/{user_name}")
        auth_message = (
            "`auth successful`\n"
            f"`user_name: {user_name}`\n"
            "`user_token: missing\n\n`"
            f"[Click here to set personal token 🡥]({gs_url})"
            )
        
        try:
            application_instance = app.config['application_instance']
            chat = await application_instance.bot.get_chat(user_id)
            await chat.send_message(auth_message, parse_mode="Markdown")
            
        except Exception as err:
            logger.error(f"error sending message to user: {user_id}/{user_name}: {err}")
        
        return jsonify({"message": "auth successful", "user_token": "missing", "check this": gs_url}), 400
    
    data_saved = save_asana_data(user_name, user_gid, user_token, user_id, tg_user)
    
    if not data_saved:
        logger.error(f"failed to save token for user: {user_id}/{user_name}")
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
            logger.error(f"error sending message to user: {user_id}/{user_name}: {err}")
            
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
        logger.error(f"error sending message to user: {user_id}/{user_name}: {err}")

    logger.info(f"token saved for user: {user_id}/{user_name}")
    return jsonify({"message": "auth successful", "user_name": user_name, "user_token": "present, saved"})
    
    
# run flask app in a separate thread to handle OAuth callback
def start_flask_app(application):
    app.config['application_instance'] = application  # pass application instance to Flask
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
    

# bot initialization 
def main():
    
    application = Application.builder().token(bot_token).post_init(post_init).build()
    
    # command handlers
    application.add_handler(CommandHandler("start", start_command)) 
    application.add_handler(CommandHandler("connect", connect_command)) 
    application.add_handler(CommandHandler("mytasks", mytasks_command)) 
    application.add_handler(CommandHandler("report", report_command)) 
    
    # callback handlers
    application.add_handler(CallbackQueryHandler(add_notes_callback, pattern="add_notes"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, note_input))
    application.add_handler(CallbackQueryHandler(process_note, pattern="^(confirm_note|edit_note)$"))
    
    # start flask app in a separate thread
    flask_thread = threading.Thread(target=start_flask_app, args=(application,))
    flask_thread.daemon = True
    flask_thread.start()

    # start bot
    application.run_polling()

if __name__ == "__main__":
    main() 
    