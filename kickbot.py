#! /usr/bin/python

"""
KickBot - Your Partner in Lurker-Slaughter
Authored by Shinanygans (shinanygans@proton.me)

This bot will gather data over time about which users are in your group and the last time they posted media.
You can command this bot to eject from your group those who have not posted in a given time span or have never posted.
"""

import logging
import sqlite3
import asyncio
import pytz

from config import (
    BOT_TOKEN,
    DEBUG_CHATS,
    DEBUG_ADMIN_MESSAGE,
    DEBUG_CAPTURE_MESSAGE,
    DEBUG_UPDATE_MESSAGE,
    START_PURGE,
    HELP_MESSAGE,
    AUTHORIZED_ADMINS,
    NUM_BATCHES,
    DATABASE_PATH
)
from datetime import datetime, timedelta
from functools import wraps
from tqdm import tqdm
from telegram import Update, ChatMember
from telegram.constants import ChatType
from telegram.error import RetryAfter
from telegram.ext import (
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
    CallbackContext,
    Application
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("app.log", mode="w"),
    ]
)

# Create a separate handler for console output with a higher level (WARNING)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)  # Set the level to WARNING or higher
console_formatter = logging.Formatter("KICKBOT: %(message)s")
console_handler.setFormatter(console_formatter)

# Attach the console handler to the root logger
logging.getLogger().addHandler(console_handler)

max_retries = 3
authorized_chats = set()
utc_timezone = pytz.utc
kick_started = False

# Initialize the SQLite database
with sqlite3.connect(DATABASE_PATH) as conn:
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

# Create the table with a composite unique constraint
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_activity (
            user_id INTEGER,
            channel_id INTEGER,
            last_activity TIMESTAMP,
            PRIMARY KEY (user_id, channel_id),
            FOREIGN KEY (channel_id) REFERENCES channels(id) ON DELETE CASCADE
        )
    """
    )

    # Create the index on user_id and channel_id
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS user_activity_index ON user_activity (user_id, channel_id);
        """
    )
    conn.commit()

#Delete user from database when he leaves or is kicked
def delete_user(user_id, chat_id):
        with sqlite3.connect(DATABASE_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Get users who haven't posted media since the cutoff date or have never posted
            cursor.execute(
                """
                DELETE FROM user_activity
                WHERE user_id = ? AND channel_id = ?
                """,
                (user_id, chat_id),
            )
            conn.commit()
        return


# Custom decorator function to check if the requesting user is authorized (use for commands).
def authorized_admin_check(handler_function):
    @wraps(handler_function)
    async def wrapper(update: Update, context: CallbackContext):
        rt = 0
        while rt < max_retries:
            try:
                user_id = update.effective_user.id
                if AUTHORIZED_ADMINS and user_id not in AUTHORIZED_ADMINS:
                    return 
                else:
                    return await handler_function(update, context)
            except RetryAfter as e:
                wait_seconds = e.retry_after
                logging.warning(f"Got a RetryAfter error. Waiting for {wait_seconds} seconds...")
                asyncio.sleep(wait_seconds)
                rt += 1
                if rt == max_retries:
                    logging.warning(f"Max retry limit reached. Message not sent.")
                    raise e
            except Exception as e:
                logging.warning(f"An error occured in authorized_admin_check(): {e}")
                break
    return wrapper


# Custom decorator function to check if a chat is authorized (use for room data collection).
def authorized_chat_check(handler_function):
    @wraps(handler_function)
    async def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        
        # If no authorized admins are in the list, the bot is open. Check passed.
        if not AUTHORIZED_ADMINS:
            return await handler_function(update, context, *args, **kwargs)
        
        # If there are admins on the list and this process has already approved a chat, check passed.
        global authorized_chats
        chat_id = update.effective_chat.id
        if chat_id in authorized_chats:
            return await handler_function(update, context, *args, **kwargs)
        
        # If there are admins and this chat is not pre-approved, get the chat admins and see if there's a match.
        rt = 0
        while rt < max_retries:
            try:      
                admins = await update.effective_chat.get_administrators()
                admin_ids = {admin.user.id for admin in admins}
                set_admin_ids = set(admin_ids)
                set_auth_admins = set(AUTHORIZED_ADMINS)
                if set_auth_admins.intersection(set_admin_ids):
                    authorized_chats.add(chat_id)
                    return await handler_function(update, context, *args, **kwargs)
                else:
                    return 
            except RetryAfter as e:
                wait_seconds = e.retry_after
                logging.warning(f"Got a RetryAfter error. Waiting for {wait_seconds} seconds...")
                asyncio.sleep(wait_seconds)
                rt += 1
                if rt == max_retries:
                    logging.warning(f"Max retry limit reached. Message not sent.")
                    raise e
            except Exception as e:
                logging.warning(f"An error occured in authorized_admin_check(): {e}")
                break
    return wrapper


# Send the help message to the user who triggered the /help command.
@authorized_admin_check
async def help_command(update: Update, context: CallbackContext) -> None:
    """Send a message with information about the bot's available commands."""
    chat_id = update.effective_chat.id
    await context.bot.send_message(chat_id=chat_id, text=HELP_MESSAGE)
    return


# Add a user_id to the database whenever the bot sees them enter a chat.
@authorized_chat_check
async def handle_new_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ Thank you to python-telegram-bot example chatmemberbot.py for the model of this function
        https://github.com/python-telegram-bot/python-telegram-bot/blob/master/examples/chatmemberbot.py
    """
    chat_id = update.effective_chat.id
    chat_name = update.effective_chat.title
    member = update.chat_member
    user = member.from_user
    user_name = user.first_name + (" " + user.last_name if user.last_name else "")
    user_id = user.id
    status_change = member.difference().get("status")
    old_is_member, new_is_member = member.difference().get("is_member", (None, None))
 
    if status_change is None:
        return None
    
    # If the user status went from 'non-member' to 'member,' record as new user in the database.
    old_status, new_status = status_change
    was_member = old_status in [
        ChatMember.MEMBER,
        ChatMember.OWNER,
        ChatMember.ADMINISTRATOR,
    ] or (old_status == ChatMember.RESTRICTED and old_is_member is True)
    is_member = new_status in [
        ChatMember.MEMBER,
        ChatMember.OWNER,
        ChatMember.ADMINISTRATOR,
    ] or (new_status == ChatMember.RESTRICTED and new_is_member is True)

    if not was_member and is_member:
        if not member.new_chat_member.status in ["administrator", "creator"]:
            logging.info(f"User ID {user_id} '{user_name}' entered chat {chat_id} '{chat_name}'. Not admin. Capturing.")
            await capture_user(context, user_id, chat_id, user_name, chat_name)
        else:
            logging.info(f"User ID {user_id} '{user_name}' entered chat {chat_id} '{chat_name}'. Admin. Ignoring.")
    if (not is_member and was_member) and user_id != context.bot.id and not kick_started:
        if not member.new_chat_member.status in ["administrator", "creator"]:
            logging.info(f"User ID {user_id} '{user_name}' (a non-admin) left the chat {chat_id} '{chat_name}'. Deleting.")
            delete_user(user_id, chat_id)
    return


# Check if it's a media message (photo, video, etc.) and update user activity
@authorized_chat_check
async def handle_message(update: Update, context: CallbackContext): 
    rt = 0
    while rt < max_retries:
        try:
            chat_id = update.effective_message.chat_id
            chat_name = update.effective_chat.title
            user = update.effective_user
            user_id = user.id
            user_name = user.first_name + (" " + user.last_name if user.last_name else "")

            # No matter what the message contains, capture the sender in the DB 
            logging.info(f"User ID {user_id} '{user_name}' activity in chat {chat_id} '{chat_name}'. Checking DB.")
            await capture_user(context, user_id, chat_id, user_name, chat_name)

            # If the message contained acceptable media, process further
            if update.effective_message.document or update.effective_message.photo or update.effective_message.video:
                date = update.effective_message.date
                chat_member = await context.bot.get_chat_member(chat_id, user_id)

                # If the sender was not an admin, update the last_activity in the database
                if not chat_member.status in ["administrator", "creator"]:
                    logging.warning(f"User ID {user_id} '{user_name}' in chat {chat_id} '{chat_name}' contains acceptable media and is not an admin. Updating last activity in DB")
                    await debug_message(context, chat_id, user_name, DEBUG_UPDATE_MESSAGE)
                    update_user_activity(user_id, chat_id, date)
                else:
                    logging.info(f"User ID {user_id} '{user_name}' in chat {chat_id} '{chat_name}' contains acceptable media but is an admin. Ignoring.")
                    await debug_message(context, chat_id, user_name, DEBUG_ADMIN_MESSAGE)
            else:
                logging.info(f"User ID {user_id} '{user_name}' post in chat {chat_id} '{chat_name}' does NOT contain acceptable media. No activity update.")
            break
        except RetryAfter as e:
                wait_seconds = e.retry_after
                logging.warning(f"Got a RetryAfter error. Waiting for {wait_seconds} seconds...")
                asyncio.sleep(wait_seconds)
                rt += 1
                if rt == max_retries:
                    logging.warning(f"Max retry limit reached. Message not sent.")
                    raise e
        except Exception as e:
            logging.error(f"An error occured in handle_message() parsing a post for db entry: {e}")        
    return


# Send special debug messages into the chats in the DEBUG_CHATS groups (config.py)
async def debug_message(context, chat_id, user_name, text, last_activity_str =None, user_id = None):
    if chat_id in DEBUG_CHATS:
            message = f"DEBUG: {user_name}, " 
            if user_id:
                message = message + f"({user_id}) "
            message = message + text
            if last_activity_str:
                message = message + f" Your most recent post was {last_activity_str}"
            await context.bot.send_message(chat_id=chat_id, text=message)
    return


# Add user_id / chat_id combination to the database
async def capture_user(context, user_id, chat_id, user_name, chat_name):
    try:
        with sqlite3.connect(DATABASE_PATH) as conn:
            cursor = conn.cursor()
            conn.execute('BEGIN')

            # Check if the specific combination of user_id and chat_id already exists
            cursor.execute(
                """
                SELECT 1 FROM user_activity
                WHERE user_id = ? AND channel_id = ?
                LIMIT 1
                """,
                (user_id, chat_id),
            )
            row = cursor.fetchone()

            if not row:
                # The combination doesn't exist, so insert it
                cursor.execute(
                    """
                    INSERT INTO user_activity (user_id, channel_id)
                    VALUES (?, ?)
                    """,
                    (user_id, chat_id),
                )
                conn.commit()
                logging.warning(f"User ID {user_id} '{user_name}' in chat {chat_id} '{chat_name}' was just added to the database.")
                await debug_message(context, chat_id, user_name, DEBUG_CAPTURE_MESSAGE)
            else:
                # The combination already exists
                conn.rollback()  # Roll back the transaction to discard the changes
                logging.info(f"User ID {user_id} '{user_name}' in chat {chat_id} '{chat_name}' was already in the database and was ignored.")
    except Exception as e:
        logging.error(f"An error occurred in capture_user() checking a user against the db: {e}")
    return


# Function to update the last_activity timestamp for a user_id / chat_id combination when someone posts media
def update_user_activity(user_id, channel_id, date):
    try:
        with sqlite3.connect(DATABASE_PATH) as conn:
            cursor = conn.cursor()

            # Check if the provided date is newer than the last_activity in the database
            cursor.execute(
                """
                SELECT last_activity FROM user_activity
                WHERE user_id = ? AND channel_id = ?
                """,
                (user_id, channel_id),
            )
            row = cursor.fetchone()
            
            if row:
                last_activity = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=utc_timezone) if row[0] else None
                if last_activity is None or date.timestamp() > last_activity.timestamp():
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO user_activity (user_id, channel_id, last_activity)
                        VALUES (?, ?, ?)
                        """,
                        (user_id, channel_id, date.strftime("%Y-%m-%d %H:%M:%S.%f")),
                    )
                    conn.commit()
            else:
                # The combination doesn't exist, so insert it with the provided date
                cursor.execute(
                    """
                    INSERT INTO user_activity (user_id, channel_id, last_activity)
                    VALUES (?, ?, ?)
                    """,
                    (user_id, channel_id, date.strftime("%Y-%m-%d %H:%M:%S.%f")),
                )
                conn.commit()

    except Exception as e:
        logging.error(f"An error occurred in update_user_activity() updating a last_activity record: {e}")
    return


# Command to begin the kick inactive users process. Starts a separate async event loop.
@authorized_admin_check
async def inactive_kick_loop(update: Update, context: CallbackContext):
    asyncio.create_task(kick_inactive_users(update, context, pretend=False))
    return


# Command to simulate kick purge without really doing it. Starts a separate async event loop.
@authorized_admin_check
async def pretend_kick_loop(update: Update, context: CallbackContext):
    asyncio.create_task(kick_inactive_users(update, context, pretend=True))
    return


#Command to purch the database of data from chats that are no longer active
@authorized_admin_check
async def clean_database_loop(update: Update, context: CallbackContext):
    asyncio.create_task(clean_database(update, context))
    return


# Function to remove data from chats that are no longer active
async def clean_database(update, context):
    issuer_chat_id = update.effective_chat.id
    chat_type = update.effective_chat.type
    if chat_type != ChatType.PRIVATE:
        await context.bot.send_message(chat_id=chat_id, text="This command only works in a private chat with the bot")
        return
    active_chats = []
    inactive_chats = []
    try:
        with sqlite3.connect(DATABASE_PATH) as conn:
            cursor = conn.cursor()

            # Get a list of unique chat_ids from the database
            cursor.execute("SELECT DISTINCT channel_id FROM user_activity")
            chat_ids_in_database = {row[0] for row in cursor.fetchall()}

            active_str = "CURRENT ACTIVE CHATS\n"
            inactive_str = "INACTIVE CHATS IN DATABASE\n"
            for chat_id in chat_ids_in_database:
                try:
                    chat = await context.bot.get_chat(chat_id)
                    active_chats.append([chat.id, chat.title])
                    active_str = active_str + f"{chat.id} - {chat.title}\n"
                except Exception as e:
                    inactive_chats.append(chat_id)
                    inactive_str = inactive_str + f"{chat_id}\n"

            
            if len(inactive_chats)>0:
                await context.bot.send_message(chat_id=issuer_chat_id, text = inactive_str)
                logging.warning(inactive_str)
                await context.bot.send_message(chat_id=issuer_chat_id, text = "Purging...\n")
                logging.warning("Purging...\n")

            for chat_id in inactive_chats:
                cursor.execute("DELETE FROM user_activity WHERE channel_id = ?", (chat_id,))
                #print(f"EXECUTING - DELETE FROM user_activity WHERE channel_id = ?", (chat_id,))
            conn.commit()

            if len(inactive_chats)>0:
                logging.warning("Inactive channels deleted.\n")
                await context.bot.send_message(chat_id=issuer_chat_id, text = "Inactive channels deleted.\n")
            logging.warning(active_str)
            await context.bot.send_message(chat_id=issuer_chat_id, text = active_str)
            return
    except Exception as e:
        logging.error(f"An error occured while cleaning the database: {e}")
    return


# Function to accept a batch of users and kick them. Used with asyncio.gather() in kick_inactive_users()
async def process_user_batch(batch, context, issuer_chat_id, issuer_chat_type, issuer_chat_name, pretend, pbar):
    
    # Kick the inactive users and count the number of users kicked
    banned_count = 0
    for user_info in batch:
        user_id = user_info[0]
        rt = 0
        while rt < max_retries:
            try:
                # If supergroup, use 'unban' for kick. Otherwise just ban.
                if not pretend:
                    if issuer_chat_type == ChatType.SUPERGROUP or issuer_chat_type == ChatType.CHANNEL:
                        await context.bot.unban_chat_member(issuer_chat_id, user_id)
                    else:
                        await context.bot.ban_chat_member(issuer_chat_id, user_id)
                    
                banned_count += 1
                
                # Update the progress bar
                pbar.update(1)    
                logging.info(f"User ID {user_id} KICKED from {issuer_chat_id} '{issuer_chat_name}'.")
                break
            except RetryAfter as e:
                wait_seconds = e.retry_after
                logging.warning(f"Got a RetryAfter error. Waiting for {wait_seconds} seconds...")
                asyncio.sleep(wait_seconds)
                rt += 1
                if rt == max_retries:
                    logging.warning(f"Max retry limit reached. Message not sent.")
                    raise e
            except Exception as e:
                logging.error(f"An error occurred in kick_inactive_users() during the ban process: {e}")
                break

    return banned_count

# Function to kick inactive users
async def kick_inactive_users(update: Update, context: CallbackContext, pretend=False):
    global kick_started
    kick_started = True
# Define a mapping of units to timedelta arguments
    unit_to_timedelta = {
        's': 'seconds',
        'm': 'minutes',
        'h': 'hours',
        'd': 'days',
        'w': 'weeks',
        'M': 'months',
        'y': 'years',
    }

    if not update.message:
        return

    try:
        issuer_user_id = update.effective_user.id
        issuer_chat_id = update.message.chat_id
        issuer_chat_name = update.effective_chat.title
        issuer_chat_type = update.effective_chat.type
        admins = await update.effective_chat.get_administrators()
        admin_ids = {admin.user.id for admin in admins}

        if issuer_user_id not in admin_ids:
            await context.bot.send_message(chat_id=issuer_chat_id, text="You are not an admin in this channel.")
            return

        logging.warning(f"\n\nHEADS UP! A valid inactivity purge has been started in {issuer_chat_name} ({issuer_chat_id})\n\n")

        time_span = context.args[0]
        unit = time_span[-1]
        duration = int(time_span[:-1])

        if unit not in unit_to_timedelta:
            raise ValueError("Invalid unit")

        timedelta_arg = {unit_to_timedelta[unit]: duration}
        cutoff_date = datetime.utcnow() - timedelta(**timedelta_arg)
        readable_string_of_duration = f"{duration} {unit_to_timedelta[unit]}"
        if duration == 1:
            readable_string_of_duration = readable_string_of_duration[:-1]

        logging.warning(f"Requested duration is {readable_string_of_duration}. Cutoff date is {cutoff_date}.")
    except (IndexError, ValueError):
        await context.bot.send_message(chat_id=issuer_chat_id, text="Invalid command format. Use /inactivekick <time> (e.g., /inactivekick 1d).")
        logging.error(f"An error occurred in kick_inactive_users(), probably due to an invalid time argument.")
        kick_started = False
        return
    
    try:
        logging.warning(f"STARTING DB QUERIES.")
        with sqlite3.connect(DATABASE_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Get users who haven't posted media since the cutoff date or have never posted
            cursor.execute(
                """
                SELECT user_id, last_activity FROM user_activity
                WHERE channel_id = ? AND (last_activity IS NULL OR last_activity < ?)
                """,
                (issuer_chat_id, cutoff_date),
            )
            user_data = cursor.fetchall()

        # Filter out admins
        users_to_ban = []
        for user_id, last_activity in tqdm(user_data, desc="Filtering Users", unit=" user"):
            if user_id not in admin_ids:
                users_to_ban.append((user_id, last_activity))

        # Count the ban list, and announce to the group.
        count_of_users_to_ban = len(users_to_ban)
        admin_message = START_PURGE + f" The KickBot is about to purge all users from {issuer_chat_name} who have not posted media in the last {readable_string_of_duration}."
        await context.bot.send_message(chat_id=issuer_chat_id, text=admin_message)

    except (IndexError, ValueError) as e:
        logging.error(f"An error occurred in kick_inactive_users(), while assembling the kick list. {e}")
        kick_started = False
        return
    
    # Define a shared variable to keep track of the total banned count
    total_banned_count = 0
    pbar = tqdm(total=len(users_to_ban), desc="Kicking Users", unit=" user")
    # Kick the inactive users and count the number of users kicked
    try:
        # Calculate the number of sub-lists based on a maximum batch size (e.g., 1000 users per batch)
        num_lists = NUM_BATCHES  # Specify the number of lists in the config.py file
        total_users = len(users_to_ban)  # Get the total number of users
        batch_size = max(1, total_users // num_lists)  # Calculate the batch size

        # Initialize an empty list to store the user batches
        user_batches = []

        # Split the users into batches
        for i in range(0, total_users, batch_size):
            batch = users_to_ban[i:i + batch_size]
            user_batches.append(batch)

        # Create asyncio tasks for each batch
        tasks = []
        for i, user_batch in enumerate(user_batches):
            task = process_user_batch(user_batch, context, issuer_chat_id, issuer_chat_type, issuer_chat_name, pretend, pbar)
            tasks.append(task)

        # Execute tasks concurrently using asyncio.gather
        banned_counts = await asyncio.gather(*tasks)

        # Sum up banned counts from all batches
        total_banned_count = sum(banned_counts)

        # Extract user_ids from the users_to_ban list
        user_ids = [user_info[0] for user_info in users_to_ban]

        # Create a tuple of (user_id, chat_id) for each user to be deleted
        delete_params = [(user_id, issuer_chat_id) for user_id in user_ids]

        # Construct the SQL query with placeholders
        delete_query = """
            DELETE FROM user_activity
            WHERE user_id = ? AND channel_id = ?
        """

        # Connect to the database and execute the delete query
        with sqlite3.connect(DATABASE_PATH) as conn:
            cursor = conn.cursor()
            if not pretend:
                cursor.executemany(delete_query, delete_params)
                conn.commit()

    except Exception as e:
        logging.error(f"An error occurred in kick_inactive_users() during the ban process: {e}")

    final_message = f"Kicked {total_banned_count} users for inactivity."

    await context.bot.send_message(chat_id=issuer_chat_id, text=final_message)
    kick_started = False
    return


def main() -> None:
    """Run bot."""
    # Create the Application and pass it your bot's token.
    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("inactivekick", inactive_kick_loop))
    application.add_handler(CommandHandler("pretendkick", pretend_kick_loop))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("cleandb", clean_database_loop))
    application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_message))
    application.add_handler(ChatMemberHandler(handle_new_member, ChatMemberHandler.CHAT_MEMBER))

    # Run the bot until the user presses Ctrl-C
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()