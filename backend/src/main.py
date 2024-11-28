from config.load_env import bot_token

import logging

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, Bot
from telegram.constants import ParseMode

from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes
from telegram.ext import filters

# Logging configuration
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# Pre-assign menu text
FIRST_MENU = "<b>Menu 1</b>\n\nA beautiful menu with a shiny inline button."
SECOND_MENU = "<b>Menu 2</b>\n\nA better menu with even more shiny inline buttons."

# Pre-assign button text
NEXT_BUTTON = "Next"
BACK_BUTTON = "Back"
TUTORIAL_BUTTON = "Tutorial"

# Build keyboards
FIRST_MENU_MARKUP = InlineKeyboardMarkup([[InlineKeyboardButton(NEXT_BUTTON, callback_data=NEXT_BUTTON)]])
SECOND_MENU_MARKUP = InlineKeyboardMarkup(
    [
        [InlineKeyboardButton(BACK_BUTTON, callback_data=BACK_BUTTON)],
        [InlineKeyboardButton(TUTORIAL_BUTTON, url="https://core.telegram.org/bots/api")],
    ]
)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /start command."""
    await update.message.reply_text("Hello! This is your bot.")


async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Echo back user messages, optionally in uppercase."""
    screaming = context.chat_data.get("screaming", False)

    if screaming and update.message.text:
        await update.message.reply_text(update.message.text.upper())
    else:
        await update.message.reply_text(update.message.text)


async def scream(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Activate screaming mode."""
    context.chat_data["screaming"] = True
    await update.message.reply_text("Screaming mode activated!")


async def whisper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Deactivate screaming mode."""
    context.chat_data["screaming"] = False
    await update.message.reply_text("Screaming mode deactivated!")


async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send the first menu with inline buttons."""
    await update.message.reply_text(FIRST_MENU, parse_mode=ParseMode.HTML, reply_markup=FIRST_MENU_MARKUP)


async def button_tap(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle inline button presses."""
    query = update.callback_query
    await query.answer()

    if query.data == NEXT_BUTTON:
        await query.edit_message_text(SECOND_MENU, parse_mode=ParseMode.HTML, reply_markup=SECOND_MENU_MARKUP)
    elif query.data == BACK_BUTTON:
        await query.edit_message_text(FIRST_MENU, parse_mode=ParseMode.HTML, reply_markup=FIRST_MENU_MARKUP)


def main() -> None:
    """Main function to run the bot."""
    # Initialize Application with token and default parse_mode
    application = Application.builder().token(bot_token).build()

    # Register handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("scream", scream))
    application.add_handler(CommandHandler("whisper", whisper))
    application.add_handler(CommandHandler("menu", menu))

    application.add_handler(CallbackQueryHandler(button_tap))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))

    # Run the bot
    application.run_polling()


if __name__ == "__main__":
    main()
