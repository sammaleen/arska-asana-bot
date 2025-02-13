#!/usr/bin/env python3
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import Application, CommandHandler, CallbackContext, CallbackQueryHandler, MessageHandler, filters, ContextTypes

import pandas as pd
from pathlib import Path
from datetime import datetime, time

from flask import Flask, request, jsonify
import logging

from services.redis_client import get_redis_client

from services.oauth_service import gen_oauth_link, store_oauth_data, get_oauth_data, get_token

from services.asana_data import (get_user_name,
                                 get_user_data,
                                 save_asana_data,
                                 get_redis_data,
                                 get_tasks,
                                 get_note,
                                 format_df, 
                                 store_note,
                                 get_report,
                                 get_report_pm,
                                 get_report_ba,
                                 get_tg_user,
                                 format_report
                                 )

from config.load_env import (bot_token,
                             workspace_gid,
                             gs_url,
                             report_chat_id, 
                             report_chat_id_pm, 
                             report_chat_id_ba,
                             pm_users,
                             ba_users
                             )

# set logger 
logging.basicConfig(
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s", 
    level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# set flask
app = Flask(__name__)

# COMMANDS ------

# /CHATID command handler
async def chat_id_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await update.message.reply_text(f"chat ID is: `{chat_id}`", parse_mode="Markdown")

# /START command handler
async def start_command(update: Update, context: CallbackContext):
    
    user_id = update.effective_user.id
    tg_user = update.effective_user.username
    
    oauth_link, state = gen_oauth_link()  # generate oauth link
    store_oauth_data(user_id, tg_user, state) # store the state in Redis along with user_id mapping
    logger.info(f"bot started by user: {user_id}/{tg_user}, state: {state}")
    
    keyboard = [[InlineKeyboardButton("Connect to Asana 🔑", url=oauth_link)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    start_message = (
        "*Привет*\!\n\n"
        "Бот организует связь с Асаной для получения данных по запланированным задачам\.\n\n"
        
        "\u2501\n"
        "*Авторизация*\n"
        "Для начала работы авторизуйте свой аккаунт в Асане через кнопку Connect to Asana ниже или с помощью команды [/connect]\\. "
        "Все доступные команды бота находятся в Menu внизу и здесь, в стартовом сообщении\.\n\n"
        
        "━\n"
        "*Команды*\n"
        "[/start] \\- вернуться к стартовому сообщению\n"
        "[/connect] \\- авторизоваться в Асане\n"
        "[/mytasks] \\- посмотреть свои задачи на день, добавить заметку\n\n"
        "[/report] \\- получить отчет по плану на день для всех\n"
        "[/pm\_report] \\- получить отчет по плану на день для РП\n"
        "[/ba\_report] \\- получить отчет по плану на день для БА\n\n"
        
        "━\n"
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
    
    user_id = update.effective_user.id
    tg_user = update.effective_user.username
    
    oauth_link, state = gen_oauth_link()  # generate oauth link
    store_oauth_data(user_id, tg_user, state) # store the state in Redis along with user_id mapping
    logger.info(f"auth process for user: {user_id}/{tg_user}, state: {state}")
    
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
        mytasks_message = format_df(df, extra_note, max_len=1024, max_note_len=85)
    else:
        mytasks_message = (
            f"<b>{datetime.now().strftime('%d %b %Y · %a')}</b>\n\n"
            "<code>No tasks for today</code>"
        )
    
    await context.bot.send_photo(
        chat_id=update.effective_chat.id,
        photo=open(Path(__file__).parent / "assets/mytasks.png", "rb"),
        caption=mytasks_message,
        parse_mode="HTML",
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
    
    tasks_dict = get_report(user_name, pm_users, ba_users)
    
    if tasks_dict:
        users = list(tasks_dict.keys())
        mes_num = len(users)
        logger.info(f"got report data for {mes_num} users: {users}")
        
        # create formatted reports for each user from tasks_dict
        reports = []
        for user, user_df in tasks_dict.items():
            tg_user_name = get_tg_user(user)
            user_report = format_report(user_df, user, tg_user_name, max_len=4000, max_note_len=100)
            reports.append(user_report)
        
        # send each report as a separate message 
        for report in reports:
            try:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=report,
                    parse_mode='HTML'  
                )   
            except Exception as err:
                logger.error(f"error sending report to user: {user_id}/{user_name}")
    else:
        report_message = (
            f"<b>{datetime.now().strftime('%d %b %Y · %a')}</b>\n\n" 
            "<code>No data is present for now</code>"
        )
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=report_message,
            parse_mode="HTML"  
        )
    
    
# /PM REPORT command handler    
async def pm_report_command(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    user_gid, user_name, user_token, tg_user = get_redis_data(user_id)
    
    tasks_dict = get_report_pm(user_name, pm_users)
    
    if tasks_dict:
        users = list(tasks_dict.keys())
        mes_num = len(users)
        logger.info(f"got PM report data for {mes_num} users: {users}")
        
        # create formatted reports for each user from tasks_dict
        reports = []
        for user, user_df in tasks_dict.items():
            tg_user_name = get_tg_user(user)
            user_report = format_report(user_df, user, tg_user_name, max_len=4000, max_note_len=85)
            reports.append(user_report)
        
        # send each report as a separate message 
        for report in reports:
            try:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=report,
                    parse_mode='HTML'  
                )   
            except Exception as err:
                logger.error(f"error sending PM report to user: {user_id}/{user_name}")
    else:
        report_message = (
            f"<b>{datetime.now().strftime('%d %b %Y · %a')}</b>\n\n" 
            "<code>No data is present for now</code>"
        )
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=report_message,
            parse_mode="HTML"  
        )    
    
 
# /BA REPORT command handler    
async def ba_report_command(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    user_gid, user_name, user_token, tg_user = get_redis_data(user_id)
    
    tasks_dict = get_report_ba(user_name, ba_users)
    
    if tasks_dict:
        users = list(tasks_dict.keys())
        mes_num = len(users)
        logger.info(f"got BA report data for {mes_num} users: {users}")
        
        # create formatted reports for each user from tasks_dict
        reports = []
        for user, user_df in tasks_dict.items():
            tg_user_name = get_tg_user(user)
            user_report = format_report(user_df, user, tg_user_name, max_len=4000, max_note_len=85)
            reports.append(user_report)
        
        # send each report as a separate message 
        for report in reports:
            try:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=report,
                    parse_mode='HTML'  
                )   
            except Exception as err:
                logger.error(f"error sending PM report to user: {user_id}/{user_name}")
    else:
        report_message = (
            f"<b>{datetime.now().strftime('%d %b %Y · %a')}</b>\n\n" 
            "<code>No data is present for now</code>"
        )
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=report_message,
            parse_mode="HTML"  
        )    
            
    
# MENU bot post initialization
async def post_init(application: Application) -> None:
    
    await application.bot.set_my_commands(
        [BotCommand('start', 'go to start message'),
         BotCommand('connect', 'connect to Asana'),
         BotCommand('mytasks', 'get list of tasks for today'),
         BotCommand('report', 'get report on tasks for all users'),
         BotCommand('pm_report', 'get report on tasks for PMs'),
         BotCommand('ba_report', 'get report on tasks for BAs'),
         BotCommand('chatid', 'get chat id of the group/channel')
         ]
        )
    
    menu_button = {
    "type": "default",
    "text": "Menu",  
    }
    await application.bot.set_chat_menu_button()


# OAuth callback route
@ app.route("/callback", methods=["GET"])

def callback():
    
    auth_code = request.args.get('code')
    res_state = request.args.get('state')
    logger.info(f"recieved callback, state: {res_state}, auth_code: {auth_code}")
    
    # fetch oauth info, user_id associated with the state
    user_oauth_data = get_oauth_data(res_state)
    if not user_oauth_data:
        logger.error(f"innvalid / expired state: {res_state}")
        return "Invalid / Expired state", 400
    
    user_id = int(user_oauth_data["user_id"])
    tg_user = user_oauth_data["tg_user"]
    logger.info(f"valid state for user: {user_id}/{tg_user}")

    # exchange auth code for access token
    access_token = get_token(auth_code) 
    if not access_token:
        logger.error(f"failed exchange auth code for token for user: {user_id}")
        return "Auth failed: couldn't get access token", 400
        
    # get asana user name via request to asana api
    user_name = get_user_name(access_token)  
    if not user_name:
        logger.error(f"failed to get asana user name for user: {user_id}")
        return "Auth failed: user is not in Asana ", 400
    
    # decode cyrillic 
    user_name = user_name.encode('utf-8').decode('unicode_escape')
    
    # get permanent token from DB
    user_gid, user_token = get_user_data(user_name)
    if not user_token:
        logger.error(f"failed to get permanent token from DB for user: {user_id}/{user_name}")
        return jsonify({
            "message": "auth successful, but personal token is missing",
            "user_name": user_name,
            "user_token": "missing",
            "note":"to get personal token search for 'asana_users' table in Google Drive"
        }), 400
    
    # saving extracted data to DB/cache
    data_saved = save_asana_data(user_name, user_gid, user_token, user_id, tg_user)
    if not data_saved:
        logger.error(f"failed to save data for user: {user_id}/{user_name}")
        return jsonify({"message": "auth failed - couldn't save data in DB/cache"}), 500
    
    logger.info(f"data saved for user: {user_id}/{user_name}")
    return jsonify({
        "message": "auth successful",
        "user_name": user_name,
        "user_token": "present, saved"
    }), 200
   

# /REPORT scheduler
async def scheduled_report(context: ContextTypes.DEFAULT_TYPE):
    
    logger.info("running scheduled report ...")
    
    tasks_dict = get_report(None, pm_users, ba_users)  

    if tasks_dict:
        users = list(tasks_dict.keys())
        logger.info(f"got scheduled report data for {len(users)} users: {users}")

        for user, user_df in tasks_dict.items():
            tg_user_name = get_tg_user(user)
            user_report = format_report(user_df, user, tg_user_name, max_len=4000, max_note_len=85)
            
            try:
                await context.bot.send_message(
                    chat_id=report_chat_id,
                    text=user_report,
                    parse_mode='HTML'
                )
            except Exception as err:
                logger.error(f"error sending scheduled report: {err}")
    else:
        report_message = (
            f"<b>{datetime.now().strftime('%d %b %Y · %a')}</b>\n\n"
            "<code>No data is present for now</code>"
        )
        await context.bot.send_message(
            chat_id=report_chat_id,
            text=report_message,
            parse_mode="HTML"
        )
    
   
# bot initialization 
def create_bot_app():
    
    bot_app = Application.builder().token(bot_token).post_init(post_init).build()
    
    # command handlers
    bot_app.add_handler(CommandHandler("chatid", chat_id_command)) 
    bot_app.add_handler(CommandHandler("start", start_command)) 
    bot_app.add_handler(CommandHandler("connect", connect_command)) 
    bot_app.add_handler(CommandHandler("mytasks", mytasks_command)) 
    bot_app.add_handler(CommandHandler("report", report_command)) 
    bot_app.add_handler(CommandHandler("pm_report", pm_report_command)) 
    bot_app.add_handler(CommandHandler("ba_report", ba_report_command)) 
    
    # callback handlers
    bot_app.add_handler(CallbackQueryHandler(add_notes_callback, pattern="add_notes"))
    bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, note_input))
    bot_app.add_handler(CallbackQueryHandler(process_note, pattern="^(confirm_note|edit_note)$"))
    
    return bot_app


# run bot polling
def main():
    bot_app = create_bot_app()   
    
    job_queue = bot_app.job_queue
    
    # scheduled run for general /report command
    job_queue.run_daily(
        scheduled_report,
        time=time(hour=7, minute=5),
        days=(0, 1, 2, 3, 4)  # Mon-Fri
    )
    
    # scheduled run for PM /pm_report
    job_queue.run_daily(
        scheduled_report,
        time=time(hour=7, minute=5),
        days=(0, 1, 2, 3, 4)  # Mon-Fri
    )
    
    # scheduled run for BA /ba_report
    job_queue.run_daily(
        scheduled_report,
        time=time(hour=7, minute=5),
        days=(0, 1, 2, 3, 4)  # Mon-Fri
    )
      
    job_queue.run_once(scheduled_report, when=5)  

    bot_app.run_polling()

if __name__ == "__main__":
    main() 
    