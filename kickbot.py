#! /usr/bin/python

"""
KickBot - Your Partner in Lurker-Slaughter
Authored by Shinanygans (shinanygans@proton.me)

This bot will gather data over time about which users are in your group and the last time they posted media.
You can command this bot to eject from your group those who have not posted in a given time span or have never posted.
"""
import sys
import os
import csv
import logging
from logging.handlers import TimedRotatingFileHandler
import asyncio
import pytz
import time
import traceback 


from config import (
    BOT_TOKEN,
    API_ID,
    API_HASH,
    DEBUG_CHATS,
    START_PURGE,
    HELP_MESSAGE,
    AUTHORIZED_ADMINS,
    NUM_BATCHES,
)
from db_utils import (
    initialize_db,
    insert_user_in_db,
    delete_user_from_db,
    get_chat_ids_and_names,
    list_chats_in_db,
    del_chats_from_db,
    get_user_activity,
    update_user_activity,
    deleted_kicks_from_user_activity,
    lookup_group_member,
    lookup_active_group_member,
    lookup_user_in_kick_db,
    lookup_kick_count_in_kick_db,
    insert_kicked_user_in_kick_db,
    lookup_most_recent_kick_timestamp,
    get_whitelist,
    get_whitelist_from_private,
    is_chat_authorized,
    insert_authorized_chat,
    get_three_strikes,
    update_three_strikes,
    get_ban_leavers_status,
    update_ban_leavers_status,
    batch_update_joined,
    batch_update_left,
    batch_update_kicked,
    batch_update_banned,
    list_member_ids_in_db,
    list_unkonwn_status_in_db,
    list_banned_users_in_db,
    keyword_search_from_db,
    return_blacklist,
    get_wholeft,
    get_wholeft_from_private,
    update_left_groups,
    insert_kicked_user_in_blacklist,
    insert_userlist_into_blacklist,
    remove_unbanned_user_from_blacklist,
    lookup_user_in_blacklist,
    batch_insert_or_update_chat_member,
    str_to_timedelta,
    format_timedelta,
    insert_obligation_chat,
    delete_obligation_chat,
    lookup_obligation_chat,
    lookup_last_scan,
    insert_last_scan,
    import_blacklist_from_csv,
    update_or_insert_group_member,
    insert_last_admin_update,
    lookup_last_admin_update,
    EventType
)
from datetime import datetime, timedelta, timezone
from random import choice
from functools import wraps
from tqdm import tqdm
import aioschedule as schedule
from telethon.sync import TelegramClient
from telethon.errors import ChannelPrivateError, BadRequestError, UserAdminInvalidError, TimedOutError
from telethon.tl.types import (
    ChannelParticipantAdmin, 
    ChannelParticipant, 
    ChannelParticipantCreator,
    ChatParticipant, 
    ChatParticipantAdmin, 
    ChatParticipantCreator,
    ChannelParticipantsKicked,
    ChannelParticipantBanned,
)
from telegram import Update, ChatMember, Message, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatType, ParseMode
from telegram.error import RetryAfter, Forbidden, TimedOut, BadRequest, NetworkError
from telegram.ext import (
    ChatMemberHandler,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
    CallbackContext,
    Application
)

# Configure logging
when = 'midnight'  # Rotate logs at midnight (other options include 'H', 'D', 'W0' - 'W6', 'MIDNIGHT', or a custom time)
interval = 1  # Rotate daily
backup_count = 7  # Retain logs for 7 days
log_handler = TimedRotatingFileHandler('app.log', when=when, interval=interval, backupCount=backup_count)
log_handler.suffix = "%Y-%m-%d"  # Suffix for log files (e.g., 'my_log.log.2023-10-22')

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        log_handler,
    ]
)

# Create a separate handler for console output with a higher level (WARNING)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)  # Set the level to WARNING or higher
console_formatter = logging.Formatter("KICKBOT: %(message)s")
console_handler.setFormatter(console_formatter)

# Attach the console handler to the root logger
logging.getLogger().addHandler(console_handler)

telethon = None
kickbot = None
app = None
kickbot_path = os.path.abspath(__file__)
tracking_chat_members = True
max_retries = 3
authorized_chats = set()
utc_timezone = pytz.utc
kick_started = False
scanning_underway = []
let_leave_without_banning = set()
admin_participant_types = (ChannelParticipantAdmin, ChannelParticipantCreator, ChatParticipantAdmin, ChatParticipantCreator)
attempting_telethon_restart = False

# Initialize the SQLite database
initialize_db()

# ********* LOCAL CACHING *********

chat_admins_cache = {}

async def update_chat_admins_cache(chat_id):
    global chat_admins_cache
    rt = 0
    while rt < max_retries:
        try:
            chat_admins = await kickbot.get_chat_administrators(chat_id)
            admin_ids = {admin.user.id for admin in chat_admins}
            chat_admins_cache[chat_id] = admin_ids
            insert_last_admin_update(chat_id)
            break
        except (BadRequest, Forbidden) as e:
            # Expecting deleted chats to get this error
            logging.warning(f"update_chat_admins_cache() - Error occurred for chat {chat_id}: {e}. Deleting from database.")
            del_chats_from_db([chat_id])
            break
        except (RetryAfter, TimedOut, NetworkError) as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            wait_seconds = 2 * e.retry_after if hasattr(e, 'retry_after') else 3
            logging.warning(f"Error in update_chat_admins_cache() - {e}.  Line: {exc_traceback.tb_lineno} - Type: {exc_type}. Waiting for {wait_seconds} seconds...")
            await asyncio.sleep(wait_seconds)
            rt += 1
            if rt == max_retries:
                logging.warning(f"Max retry limit reached. Admins for {chat_id} not locally cached.")
                break
        except Exception as e:
            logging.warning(f"Unhandled exception occurred in chat {chat_id}: {e}")
            break
    return

async def is_user_admin(user_id, chat_id):
    # If the chat's admins are not cached or if cache is deemed outdated, update it
    if chat_id not in chat_admins_cache:
        await update_chat_admins_cache(chat_id)
    
    # Now, check if the user is an admin
    return user_id in chat_admins_cache.get(chat_id, set())


async def authorize_chat_and_update_cache(chat_id, chat_title):
    try:
        admins = await kickbot.get_chat_administrators(chat_id)
        admin_ids = {admin.user.id for admin in admins}
        bot_admins = set(AUTHORIZED_ADMINS)

        # Check for overlap between chat admins and AUTHORIZED_ADMINS
        if admin_ids.intersection(bot_admins):
            # Update cache and database as necessary
            chat_admins_cache[chat_id] = admin_ids
            insert_last_admin_update(chat_id)  # Update the last admin update timestamp in the database
            if not is_chat_authorized(chat_id, chat_title):
                insert_authorized_chat(chat_id, chat_title)  # Insert the chat into the database if not already present
            return True
    except (BadRequest, BadRequestError, Forbidden, ChannelPrivateError):
        logging.error(f"Access error for chat {chat_id} - {chat_title}.")
    except Exception as e:
        logging.error(f"Unexpected error while authorizing chat {chat_id}: {e}")
    return False


async def check_chat_admins_against_named_users(update, chat_title, chat_id) -> bool:
    rt = 0
    while rt < max_retries:
        try:      
            admins = await update.effective_chat.get_administrators()
        
        except (BadRequest, BadRequestError, Forbidden, ChannelPrivateError) as e:
            logging.error(f"An access error occured in authorized_chat_check() for {chat_id} - {chat_title}. Cleaning databases.")
            active_chats, inactive_chats, active_str, inactive_str = await find_inactive_chats()
            if len(inactive_chats) > 0:
                logging.warning("Found inactive chats. Cleaning database.\n")
                logging.warning(inactive_str)
                del_chats_from_db(inactive_chats)
                logging.warning("Purging...\n")
                logging.warning("Inactive channels deleted.\n")
                logging.warning(active_str)  
            return False
        except Exception:
            return False

        try:
            admin_ids = {admin.user.id for admin in admins}
            set_admin_ids = set(admin_ids)
            set_auth_admins = set(AUTHORIZED_ADMINS)

            # If an authorized (named) admin is one of the chat admins, insert into cache and insert into DB as necessary
            if set_auth_admins.intersection(set_admin_ids):
                insert_authorized_chat(chat_id, chat_title)
                chat_admins_cache[chat_id] = set_admin_ids # Cache admin ids from chat
                insert_last_admin_update(chat_id) # Update most recent admin lookup
                return True
            else:
                return False
        except RetryAfter as e:
            wait_seconds = e.retry_after
            exc_type, exc_value, exc_traceback = sys.exc_info()
            await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
            logging.warning(f"Got a RetryAfter error. Waiting for {wait_seconds} seconds...")
            await asyncio.sleep(wait_seconds)
            rt += 1
            if rt == max_retries:
                logging.warning(f"Max retry limit reached. Authorized admin check error.")
                return False
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
            logging.warning(f"An error occured in authorized_chat_check(): {e}")
            return False
        

async def named_user_present_in_chat(chat_id, chat_title):
    #Returns true if any user on the AUTHORIZED_ADMINS list is present in the chat
    try:
        if not AUTHORIZED_ADMINS:
            return False
        in_chat = False
        for named_user in AUTHORIZED_ADMINS:
            try:
                member = await kickbot.get_chat_member(chat_id=chat_id, user_id=named_user)
                if member.status in ["administrator", "creator", "member"]:
                    in_chat = True
                    break
            except BadRequest:
                continue
            except Exception as e:
                continue
        if in_chat:
            insert_authorized_chat(chat_id, chat_title)
            insert_last_admin_update(chat_id) # Update most recent admin lookup
            logging.warning(f"New authorized chat: {chat_title}.")
            return True
        else:
            return False

    except Exception as e:
        logging.warning(f"An error occured in named_user_present_in_chat(): {e}")
        return False

# ********* WRAPPERS *********


# Custom decorator function to check if the requesting user is authorized (use for commands).
def authorized_admin_check(handler_function):
    @wraps(handler_function)
    async def wrapper(update: Update, context: CallbackContext):
        try:
            user_id = update.effective_user.id
            if AUTHORIZED_ADMINS and user_id not in AUTHORIZED_ADMINS:
                return 
            else:
                return await handler_function(update, context)

        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
            logging.warning(f"An error occured in authorized_admin_check(): {e}")
            return
    return wrapper


# Custom decorator function to check if a chat is authorized (use for room data collection).
def authorized_chat_check(handler_function):
    @wraps(handler_function)
    async def wrapper(update: Update, context: CallbackContext, *args, **kwargs):

        # If the chat is private, it is authorized. Check passed.
        chat_type = update.effective_chat.type
        if chat_type == ChatType.PRIVATE:
            return await handler_function(update, context, *args, **kwargs)
        
        # If no authorized admins are in the list, the bot is open. Check passed.
        if not AUTHORIZED_ADMINS:
            return await handler_function(update, context, *args, **kwargs)
        
        # Check if the chat is authorized based on the SQLite table
        chat_id = update.effective_chat.id
        chat_title = update.effective_chat.title

        # Check if chat is in authorized_chats db table (and update chat_name if none exists); if yes function proceeds
        if is_chat_authorized(chat_id, chat_title):
            return await handler_function(update, context, *args, **kwargs)
        
        # If there are admins and this chat is not pre-approved, get the chat admins and see if there's a match.
        try:

            # named_user_is_admin = await check_chat_admins_against_named_users(update, chat_title, chat_id)
            named_user_is_member = await named_user_present_in_chat(chat_id, chat_title)
            if named_user_is_member:
                return await handler_function(update, context, *args, **kwargs)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
            logging.warning(f"An error occured in authorized_chat_check(): {e}")
            return
    return wrapper


def is_admin_of_authorized_chat_check(handler_function):
    @wraps(handler_function)
    async def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        chat_type = update.effective_chat.type
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        chat_title = update.effective_chat.title

        # Pass directly for private chats or if the user is an authorized admin
        if chat_type == ChatType.PRIVATE or user_id in AUTHORIZED_ADMINS:
            return await handler_function(update, context, *args, **kwargs)

        # Function to attempt to authorize chat and handle common exceptions
        
        async def attempt_authorize_and_execute():
            try:
                # Runs get_chat_administrators()...if named bot admin is also chat admin, insert into cache and insert into DB as necessary
                if await authorize_chat_and_update_cache(chat_id, chat_title) and await is_user_admin(user_id, chat_id): #Reauthorize success and user is admin
                    return await handler_function(update, context, *args, **kwargs)
            except (BadRequest, BadRequestError, Forbidden, ChannelPrivateError):
                await handle_inactive_chats()
            except Exception as e:
                logging.error(f"An unexpected error occurred: {e}")
            return

        # Main authorization logic
        if chat_id in chat_admins_cache:
            # Cached chat: Check if the user is an admin directly
            if await is_user_admin(user_id, chat_id):
                return await handler_function(update, context, *args, **kwargs)
            else:
                # Attempt to re-authorize the chat if the user was expected to be an admin
                return await attempt_authorize_and_execute()
        else:
            # Chat not in cache: Attempt to authorize and update cache
            return await attempt_authorize_and_execute()

    return wrapper


# ********* UTILITIES *********

async def handle_inactive_chats():
    active_chats, inactive_chats, active_str, inactive_str = await find_inactive_chats()
    if len(inactive_chats) > 0:
        logging.warning("Found inactive chats. Cleaning database.\n")
        logging.warning(inactive_str)
        del_chats_from_db(inactive_chats)
        logging.warning("Purging...\n")
        logging.warning("Inactive channels deleted.\n")
        logging.warning(active_str)      
    return


async def check_telethon_connection() -> bool:
    global telethon
    global attempting_telethon_restart
    if attempting_telethon_restart:
        return
    if telethon.is_connected():
        #logging.warning(f"Error in telethon_restart. Client registers as connected.")
        return
    try:
        # Attempt to gracefully disconnect
        attempting_telethon_restart = True
        await telethon.disconnect()
    except Exception as disconnect_exception:
        logging.warning(f"Error disconnecting Telethon prior to restart. {disconnect_exception} - Attempting reconnection anyway.")
    try:
        # Reinitialize and reconnect the client
        telethon = TelegramClient('session_name', API_ID, API_HASH)
        await telethon.start(bot_token=BOT_TOKEN)
    except Exception as reconnect_exception:
        logging.warning(f"Error reestablishing Telethon client. {reconnect_exception} - Abandoning.")
        attempting_telethon_restart = False
        return False
    attempting_telethon_restart = False
    return telethon.is_connected


# Registered error handler for the app
async def error(update, context):
    err = f"Update: {update}\nError: {context.error}"
    logging.error(err, exc_info=context.error)
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


async def debug_to_chat(exc_type, exc_value, exc_traceback, update=None):
    chat_title = update.effective_chat.title if update else "N/A"
    bot_name = kickbot.name
    
    # Extracting information
    header = ""
    output_str = ""
    header += f"Exception Type: {exc_type}\n"
    header += f"Exception Value: {exc_value}\n"
    header += f"Top-level Code Line: {exc_traceback.tb_lineno}\n"
    header += f"Top-level Function Name: {exc_traceback.tb_frame.f_code.co_name}\n\n"
  
    if exc_traceback is not None:
        frame = exc_traceback.tb_frame
        locals_at_exception = frame.f_locals
        output_str += f"LOCALS:\n"
        for var_name, var_value in locals_at_exception.items():
            if var_name not in ["update", "context", "message", "user", "kicked_user", "participant"]: 
                output_str += f"{var_name}: {var_value}\n"

            
    # Send error to all debug chats
    try:
        max_message_length = 4096  # Adjust this based on your needs
        for debug_chat_id in DEBUG_CHATS:
            await telethon.send_message(debug_chat_id, f"DEBUG: Error from {bot_name} in {chat_title}\n")
            await telethon.send_message(debug_chat_id, f"{header}")
            if len(output_str)> 12288:
                await telethon.send_message(debug_chat_id, "Too much local variable data to return.")
                return

            # Split the long message into chunks
            message_chunks = [output_str[i:i + max_message_length] for i in range(0, len(output_str), max_message_length)]
            for chunk in message_chunks:
                await telethon.send_message(debug_chat_id, chunk)

            # Upload app.log file
            log_file_path = "app.log"  # Adjust the path accordingly
            if os.path.exists(log_file_path):
                with open(log_file_path, "rb") as log_file:
                    #await context.bot.send_document(chat_id=debug_chat_id, document=log_file)
                    await telethon.send_message(debug_chat_id, file=log_file)


    except Exception as e:
        logging.error(f"Error sending an earlier exception to debug chats: {e}")


async def delete_message_after_delay(context: CallbackContext, message:Message):
    try:
        await asyncio.sleep(3)  # Wait for 3 seconds
        await context.bot.delete_message(chat_id=message.chat_id, message_id=message.message_id)
    except Exception as e:
        logging.warning("Error deleting message.")
    return

def calculate_cutoff_date(arg):
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

    time_span = arg
    unit = time_span[-1]
    duration = int(time_span[:-1])

    if unit not in unit_to_timedelta:
        raise ValueError("Invalid unit")

    timedelta_arg = {unit_to_timedelta[unit]: duration}
    cutoff_date = datetime.utcnow() - timedelta(**timedelta_arg)
    readable_string_of_duration = f"{duration} {unit_to_timedelta[unit]}"
    if duration == 1:
        readable_string_of_duration = readable_string_of_duration[:-1]

    logging.warning(f"Requested duration is {readable_string_of_duration}. Cutoff date is {cutoff_date}.\n")

    return cutoff_date, readable_string_of_duration


# ********* BANNING AND UNBANNING UTILITIES *********

async def uniban_from_list(user_id_list, add_to_bl = True, reason = ''):
    chat_ids_in_database = list_chats_in_db()
    for chat_id in chat_ids_in_database:
        try:
            admins = chat_admins_cache.get(chat_id)
            titles = get_chat_ids_and_names()
            chat_title = titles.get(chat_id)
            if not admins:
                await authorize_chat_and_update_cache(chat_id, chat_title)
            if not await is_user_admin(kickbot.id, chat_id):
                continue         

            chat = await kickbot.get_chat(chat_id)
            if chat.type == ChatType.PRIVATE:
                logging.warning(f"Can't ban from {chat_title} - PRIVATE")
                continue     
            for banning_user_id in user_id_list:
                group_member_dict =  lookup_group_member(banning_user_id, chat_id)
                group_member_dict = group_member_dict[0] if group_member_dict else None
                if group_member_dict:
                    logging.warning(f"Banning {group_member_dict.get('user_name')} ({banning_user_id} - @{group_member_dict.get('username')} from {chat_title} --- {reason}")
                try:
                    await kickbot.ban_chat_member(chat_id, banning_user_id)
                except Exception as e:
                    logging.warning(f"Ban error for {banning_user_id} in {chat_title} - {e}")

        except (BadRequest, BadRequestError, Forbidden, ChannelPrivateError) as e:
            logging.warning(f"Can't ban from {chat_title} - Bad request or private channel error")
            continue
        except Exception as e:
            logging.warning(f"Uniban error: {e}")
            continue
    return


async def unban(update: Update=None, context: CallbackContext=None):
    @authorized_admin_check
    async def universal_unban(update: Update=None, context: CallbackContext=None):
        chat_ids_in_database = list_chats_in_db()
        for chat_id in chat_ids_in_database:
            try:
                # remove_unbanned_user_from_blacklist(unban_user_list, chat_id)
                chat = await telethon.get_entity(chat_id)
                if hasattr(chat, 'username') and chat.username:
                    logging.warning(f"Can't unban from {chat.title} - PRIVATE")
                else:
                    logging.warning(f"Unbanning left users from {chat.title}")
                    await telethon.edit_permissions(chat_id, unban_user_id)
                    batch_update_left(unban_user_list, chat_id)
            except ChannelPrivateError as e:
                logging.warning(f"Can't unban from {chat.title} - PRIVATE ERROR - Chat may no longer be active")
            except Exception as e:
                logging.warning(f"Unban error: {e}")       
    async def unban_from_current_chat():
        try:
            # remove_unbanned_user_from_blacklist(unban_user_list, issuer_chat_id)
            chat = await telethon.get_entity(issuer_chat_id)
            if hasattr(chat, 'username') and chat.username:
                logging.warning(f"Can't unban from {chat.title} - PRIVATE")
            else:
                logging.warning(f"Unbanning left users from {chat.title}")
                await telethon.edit_permissions(issuer_chat_id, unban_user_id)
                batch_update_left(unban_user_list, issuer_chat_id)
        except ChannelPrivateError as e:
            logging.warning(f"Can't unban from {chat.title} - PRIVATE ERROR in unban() - May may no longer be active")
        except Exception as e:
            logging.warning(f"Unban error: {e}")      
        return chat 
    unban_user_list=[]
    issuer_chat_id = update.message.chat_id
    issuer_user_id = update.effective_user.id
    chat_type = update.effective_chat.type
    try:
        id_or_username = context.args[0] 
    except (IndexError, ValueError) as e:
        await context.bot.send_message(chat_id=issuer_user_id, text="Invalid command format. Use /forgive <user> (e.g., /forgive @some_dude).")
        logging.error(f"An error occurred in kick_inactive_users(), probably due to an invalid time argument.")  
        return
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
        logging.warning(f"Unban argument error: {e}")  
        return
    try:
        id_or_username = int(id_or_username)
    except:
        pass

    try:
        global telethon
        await check_telethon_connection()
        unban_user_entity = await telethon.get_entity(id_or_username)
        unban_user_id = unban_user_entity.id
        unban_user_list.append(unban_user_id)
    except ValueError as e:
        logging.error(f" Unable to lookup user {unban_user_id} - {e}")
        return
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
        logging.error(f" Error getting entity and chat member information during lookup: {e}")
        return
    try:
        if chat_type is not ChatType.PRIVATE:
            message = await context.bot.send_message(
                chat_id=issuer_user_id,
                text="<i style='color:#808080;'> Response will be sent privately.</i>",
                parse_mode=ParseMode.HTML
            )
            asyncio.create_task(delete_message_after_delay(context, message))
            chat_entity = await unban_from_current_chat()
            await context.bot.send_message(
                chat_id=issuer_user_id,
                text=f"{id_or_username} has been unbanned from {chat_entity.title}."
            )
        else:
            try:
                await context.bot.send_message(
                    chat_id=issuer_chat_id,
                    text="Processing unbans..."
                )
            except Forbidden as e:
                logging.error(f"Bot chat not open - {e}")
            await universal_unban(update, context)
            try:
                await context.bot.send_message(
                    chat_id=issuer_chat_id,
                    text=f"{id_or_username} has been unbanned from all active groups."
                )
            except Forbidden as e:
                pass

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
        logging.error(f"{e}")
    return


# ********* PERIODIC CHAT SCANNER *********

async def update_chat_members(update: Update=None, context: CallbackContext=None):
    global let_leave_without_banning
    global scanning_underway
    try:
        # First, clean all the inactive chats out of the database
        await handle_inactive_chats()

        # Assemble a list of active chats in which the kickbot is an admin
        i_am_admin = []
        outdated_admin_lookups = []
        active_ids = list_chats_in_db()     
        chat_id = None
        
        for active_id in active_ids:
            try:
                admins = chat_admins_cache.get(active_id)
                titles = get_chat_ids_and_names()
                chat_title = titles.get(active_id)
                last_admin_update = lookup_last_admin_update(active_id)
                if (not last_admin_update) or (last_admin_update and (datetime.now(timezone.utc) - last_admin_update > timedelta(minutes=60))):
                    outdated_admin_lookups.append(active_id)
                if not admins:
                    await authorize_chat_and_update_cache(active_id, chat_title)
                if await is_user_admin(kickbot.id, active_id):
                    i_am_admin.append(active_id)        
                    logging.info(f"Bot is not an admin in {chat_title}. Skipping scan.")
            except Exception as e:
                logging.error(f"Error setting up scan for {chat_title} – {e}. Abandoning scan for this chat.")
                continue

        for lookup in outdated_admin_lookups:
            await update_chat_admins_cache(lookup)

        if len(i_am_admin) > 0:
            # Create a list of tasks
            results_list = []
            tasks = [process_chat_member_updates(chat_id, update, context) for chat_id in i_am_admin]

            # Execute group chat processing concurrently using asyncio.gather()
            results_list = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process obligation kicks in batch
            if results_list:
                for results in results_list:
                    if not results:
                        continue
                    if isinstance(results, Exception):
                        logging.error(f"SCAN: An error occurred: {results}")
                        continue
                    if 'chat_id' not in results or 'joined_user_ids' not in results:
                        continue
                    try:
                        await process_scanner_obligation_kicks(results)
                    except Exception as e:
                        logging.warning(f"SCAN: Exception raised with {chat_id} - {e}. Moving on to next chat in list.")
                        continue
                    results_chat_id = results.get('chat_id')
                    insert_last_scan(results_chat_id)

            # Update the left_groups table with those users who have recently left a chat
            update_left_groups()

        # Clear the lat_leave_without_banning list, in case any old values have not been properly erased
        # This list is expected to be empty because values should be cleared in real time as the user exits the group
        let_leave_without_banning.clear()

        # Clear the scanning_underway list, as the scanning is now complete for all chats
        # This list is expected to be empty by this point because individual chats are removed from it after processing
        scanning_underway.clear()
        logging.warning("SCAN: Update completed.")
        return

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
        logging.error(f"Error in the update_chat_members() function: {e}")
        scanning_underway.clear()
        return


async def process_chat_member_updates(chat_id, update: Update=None, context: CallbackContext=None):  
    start_time = time.time()  # Start timer at the very beginning of the function
    try:
        await check_telethon_connection()
        global scanning_underway
        global kick_started
        scanning_underway.append(chat_id)
        try:
            chat = await kickbot.get_chat(chat_id)
        except Exception as e:
            logging.error(f"Error in process_chat_member_updates() - {e}")
            return {}

        admin_ids = chat_admins_cache.get(chat_id, set())
        shin_ids = set(keyword_search_from_db('shinanygans'))
        whitelist_data = get_whitelist(chat_id)

        # Convert user_data and admin_ids into sets for faster lookups
        whitelist_set = {entry[0] for entry in whitelist_data}

        # Step 1: Fetch the list of user_ids from the chat_member table for the given chat_id
        member_ids_in_db = set(list_member_ids_in_db(chat_id)) # Users who are currently 'Member' or 'Admin' of 'Creator' status
        unknown_status_in_db = set(list_unkonwn_status_in_db(chat_id))
        banned_ids_in_db = set(list_banned_users_in_db(chat_id)) # Users who are currently 'Banned' status
        is_supergroup = True if chat.type == ChatType.SUPERGROUP or chat.type == ChatType.CHANNEL else False


        # Step 2: Iterate through the participants returned by iter_participants, and update a list of db insertion parameters
        logging.warning(f"Cataloging members of {chat_id}")


        if is_supergroup:
            participants, banned_users = await asyncio.gather(
                iterate_chat_participants(chat_id),
                iterate_banned_chat_participants(chat_id)
            )
            if participants is None or banned_users is None:
                error_message = "banned user scan" if banned_users is None else "room scan"
                logging.warning(f"{error_message} of {chat_id} returned a severe exception. Abandoning scan.")
                return {}
            batch_insert_parameters, participant_user_ids = participants
            banned_user_ids = banned_users
        else:
            results = await iterate_chat_participants(chat_id)
            if results is None:
                logging.warning(f"Room scan of {chat_id} returned a severe exception. Abandoning scan.")
                return {}
            batch_insert_parameters, participant_user_ids = results


        # Mark end of iter_participants section and log duration if TIMER_CHAT
        end_iter_time = time.time()
            
        batch_insert_or_update_chat_member(batch_insert_parameters)


        # Step 3: Identify users that have left or joined, or who were previously banned
        user_ids_not_in_iter_participants = member_ids_in_db.union(unknown_status_in_db) - participant_user_ids # Members, Admins or 'Not Available's in DB minus current chat occupants = Left since last scan 
        
        left_user_ids = await verify_user_left_chat(user_ids_not_in_iter_participants, chat_id)

        joined_user_ids = participant_user_ids - member_ids_in_db.union(unknown_status_in_db) # Current chat occupants minus Members/Admins/Not Available in DB = Net new + rejoins and unbanned
        unbanned_user_ids = joined_user_ids.intersection(banned_ids_in_db) # Currently banned in the DB but rejoined the group
        user_ids_to_ban = left_user_ids - admin_ids - let_leave_without_banning - shin_ids - whitelist_set
        results = {'chat_id': chat_id, 'joined_user_ids': joined_user_ids}
    

        # Step 4: Batch update the database for users that have left 
        batch_update_joined(joined_user_ids, chat_id)
        batch_update_left(left_user_ids, chat_id)
        # remove_unbanned_user_from_blacklist(unbanned_user_ids, chat_id)


        # Mark end of database update section and log duration if TIMER_CHAT
        end_db_update_time = time.time()
            

        # Step 5: If ban_leavers_mode is on, ban anyone with a status of "left"
        ban_leavers_mode = get_ban_leavers_status(chat_id)
        if ban_leavers_mode and ban_leavers_mode[0]==1:
            last_scan = lookup_last_scan(chat_id)
            suspend_banning = (not last_scan) or (last_scan and (datetime.now(timezone.utc) - last_scan) > timedelta(minutes=10))
            
            if len(user_ids_to_ban) > 0:
                if not suspend_banning:
                    logging.warning(f"BAN-LEAVERS MODE ON FOR {chat_id} - THE FOLLOWING {len(user_ids_to_ban)} USERS WILL BE BANNED:")
                    logging.warning(user_ids_to_ban)
                    await uniban_from_list(user_ids_to_ban, reason = f'SCAN - LEFT {chat.title} WHILE NO-LEAVERS MODE ON')
                else:
                    logging.warning(f"BAN-LEAVERS MODE ON FOR {chat_id} - NO SCAN IN LAST 10 MINUTES - BANNING SUSPENDED.")


            for joined_user_id in joined_user_ids:
                if (joined_user_id, chat_id) in let_leave_without_banning:
                    let_leave_without_banning.discard((joined_user_id, chat_id))

        # Mark end of ban leavers section and log duration if TIMER_CHAT
        end_ban_leavers_time = time.time()
            

        # Step 6: Get total list of banned users, including any that were just banned by activating banned_leavers mode.
        if is_supergroup:           

            # Remove the users already banned in the database, so as to only update those who were not banned before the scan.
            update_ban_status = banned_user_ids - banned_ids_in_db

            # Set status, last_banned, and times_banned fields for those just banned
            batch_update_banned(update_ban_status, chat_id)

            # Arrive at a list of manually-unbanned users by subtracting currently banned users from those marked as banned in the DB.
            manually_unbanned =  banned_ids_in_db - banned_user_ids
            # remove_unbanned_user_from_blacklist(manually_unbanned, chat_id)
            batch_update_left(manually_unbanned, chat_id)

        else:
            if ban_leavers_mode[0]==1 and context and len(left_user_ids) > 0:
                # Set status, last_banned, and times_banned fields for those just banned
                batch_update_banned(user_ids_to_ban, chat_id)   


        # Mark end of get banned list section and log duration if TIMER_CHAT
        end_get_banned_list_time = time.time()
            

        scanning_underway.remove(chat_id)

        # If no one has been kicked from this chat in the last 10 minutes, make sure the kick_started boolean is false
        # This variable is expected to be set back to false when a kick ends, but the following block will periodically make sure it is reset
        most_recent_kick = lookup_most_recent_kick_timestamp(chat_id)
        no_kick_in_last_ten = (not most_recent_kick ) or (most_recent_kick  and (datetime.now(timezone.utc) - most_recent_kick ) > timedelta(minutes=10))
        if no_kick_in_last_ten:
            kick_started == False

        # Final logging for the last part of the function if TIMER_CHAT
        end_time = time.time()
            

        print(f"SCAN: Update of {chat_id} ({chat.title}) completed. Scan found {len(joined_user_ids)} new users and {len(left_user_ids)} users who left. Scan time: {end_time - start_time:.2f} sec.")
    
    except (BadRequest, BadRequestError, Forbidden, ChannelPrivateError, NetworkError, RetryAfter, TimedOutError) as e:
        logging.warning(f"Bot does not seem to have Admin rights in {chat.title} Chat processessing not completed.\n")
        scanning_underway.remove(chat_id)
        return {}
    
    except (ConnectionRefusedError, ConnectionError) as e:
        logging.error(f"Connection to Telethon Bot interrupted. Chat processessing not completed.\n")
        scanning_underway.remove(chat_id)
        return {}
    
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
        logging.error(f"Error in the process_chat_member_updates() function: {e}")
        scanning_underway.remove(chat_id)
        return {}
    return results


async def iterate_chat_participants(chat_id):
    try:
        participant_user_ids = set()
        batch_insert_parameters = []

        async for participant in telethon.iter_participants(chat_id):
            user_id = participant.id
            member_record_dict = lookup_group_member(user_id, chat_id)
            member_record_dict = member_record_dict[0] if len(member_record_dict) > 0 else None
            if not hasattr(participant, 'participant'):
                user_status = 'Not Available'
            elif isinstance(participant.participant, ChannelParticipantAdmin):
                user_status = 'Admin'
            elif isinstance(participant.participant, ChannelParticipantCreator):
                user_status = 'Creator'
            elif isinstance(participant.participant, ChannelParticipant):
                user_status = 'Member'
            elif isinstance(participant.participant, ChatParticipantAdmin):
                user_status = 'Admin'
            elif isinstance(participant.participant, ChatParticipantCreator):
                user_status = 'Creator'
            elif isinstance(participant.participant, ChatParticipant):
                user_status = 'Member'
            elif isinstance(participant.participant, ChannelParticipantBanned):
                user_status = 'Banned'      
            else:
                user_status = 'Not Available'

            batch_insert_parameters.append((
                user_id,
                chat_id,
                " ".join(filter(None, [participant.first_name, participant.last_name])),
                participant.username,
                participant.premium,
                participant.verified,
                participant.bot,
                participant.fake,
                participant.scam,
                participant.restricted,
                participant.restriction_reason if participant.restriction_reason else None,
                user_status,
                user_id,
                chat_id,
                participant.participant.date.strftime("%Y-%m-%d %H:%M:%S.%f") if hasattr(participant.participant, 'date') else None,
                participant.participant.date.strftime("%Y-%m-%d %H:%M:%S.%f") if hasattr(participant.participant, 'date') else None,
                #last_left
                user_id,
                chat_id,
                #last_kicked
                user_id,
                chat_id,
                #last_posted
                user_id,
                chat_id,
                #last_banned
                user_id,
                chat_id,
                #times_joined
                user_id,
                chat_id,
                #times_posted
                user_id,
                chat_id, 
                #times_left
                user_id,
                chat_id, 
                #times_kicked
                user_id,
                chat_id,
                #times_banned
                user_id,
                chat_id
            ))
            if user_status != 'Banned':
                participant_user_ids.add(user_id)
        return batch_insert_parameters, participant_user_ids
    except Exception as e:
        logging.error(f" Error getting participant information during lookup: {e}")
        return None


async def iterate_banned_chat_participants(chat_id):
    banned_user_ids = set()
    if chat_id == -1002100074918:
        pass
    try:
        async for participant in telethon.iter_participants(chat_id, filter = ChannelParticipantsKicked):     
            participant_id = getattr(participant, 'id', None)
            if participant_id and isinstance(participant_id, int):
                banned_user_ids.add(participant_id)
    except (AttributeError, ValueError) as e:
        logging.error(f"Error getting banned participant information during lookup: {e}")
        # Capture the exception and the traceback
        exc_type, exc_value, exc_traceback = sys.exc_info()
        # Format the traceback as a string
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
        # Log the detailed traceback
        print("Error occurred:\n" + "".join(traceback_details))
        return set()
    except Exception as e:
        logging.error(f"Severe error getting banned participant information during lookup: {e}")
        return None

    return banned_user_ids


async def verify_user_left_chat(user_list, chat_id):
    # Under the right conditions, users who leave the chat may be subject to ban.
    # If the user is not found by telethon's iter_participants(), this function will veryify they have a chat status if 'left'
    left_user_ids = set()
    for user_id_to_be_verified in user_list:
        try:
            result = None
            result_user_id = None
            result = await kickbot.get_chat_member(chat_id, user_id_to_be_verified)
            result_user_id = result.user.id
        except Exception as e:
            logging.warning(f"SCAN: Error verifying {user_id_to_be_verified} left {chat_id} - {e}")
            batch_update_left([user_id_to_be_verified], chat_id)# UPDATE IN DB DIRECTLY WITHOUT FLAGGING AS A LEFT USER FOR BANNING PURPOSES
        if not result:
            continue # If the user is not found in the chat, it is unclear what is wrong but they shouldn't be banned
        if result.status in ["administrator", "creator"]:
            logging.warning(f"SCAN: {result_user_id} ({result.user.full_name}) left the chat {chat_id} but was an administrator.")
            pass
        elif result.status == 'left': # In this condition, the user has left the chat and is rightly subject to ban
            logging.warning(f"SCAN: {result_user_id} ({result.user.full_name}) is verified to have left {chat_id}.")
            left_user_ids.add(result_user_id)
        elif result.status == 'kicked': # In telethon vocabulary, 'kicked' means banned (i.e. on the 'removed' list).
            batch_update_banned([result_user_id], chat_id)
        else:
            logging.warning(f"SCAN: {result_user_id} ({result.user.full_name}) has a status of {result.status} in {chat_id}, and was therefore not considered a leaver.")
            pass

    return left_user_ids


async def process_scanner_obligation_kicks(results):
    # "results" is a dict with a chat_id int and a joined_user_ids list of ints
    try:
        results_chat_id = results.get('chat_id')
        if not results_chat_id:
            return
        # This shouldn't happen, but if for any reason there are no admins in the cache, abandon further processing
        admins = chat_admins_cache.get(results_chat_id)
        if not admins:
            return

        obligation_chat_id = lookup_obligation_chat(results_chat_id)
        last_scan = lookup_last_scan(results_chat_id)

        # If this is the first scan, or it's been over an hour since the last scan, we will not run obligation kicks
        # Most likely the bot was just switched on after being off, and we will avoid kicking the backlogged users
        suspend_obligation_kicks = (not last_scan) or (last_scan and (datetime.now(timezone.utc)) - last_scan) > timedelta(minutes=10)

        # If no obligation chat is assigned, or if obligation kicks are suspended, abandon further processing
        if not obligation_chat_id or suspend_obligation_kicks:
            return

        results_joined_user_ids = results.get('joined_user_ids')
        results_chat = await kickbot.get_chat(results_chat_id)  
        results_chat_type = results_chat.type
        chat_name_dict = get_chat_ids_and_names()
        whitelist_data = get_whitelist(results_chat_id)
        whitelist_set = {entry[0] for entry in whitelist_data}
        shin_ids = set(keyword_search_from_db('shinanygans'))

        #For every new user who just joined the chat...
        
        for joined_user_id in results_joined_user_ids:
            try:
                # If the newly joined user meets one of the following immunity conditions, move on to the next user
                if joined_user_id in AUTHORIZED_ADMINS:
                    continue
                elif joined_user_id in admins:
                    continue
                elif joined_user_id in whitelist_set:
                    continue
                elif joined_user_id in shin_ids:
                    continue

                # Look in our internal group_member database table to see if the newly joined user is a member of the proper obligation chat
                lookup = lookup_active_group_member(joined_user_id, obligation_chat_id)
                logging.warning(f"SCAN: Joining user {joined_user_id} {'DOES' if len(lookup)>0 else 'DOES NOT'} appear in our internal DB for obligation chat {obligation_chat_id}")

                # If user not found locally in obligation chat, verify with a get_chat_member lookup before kicking
                if len(lookup)==0:
                    joined_chat_obligation_member = await kickbot.get_chat_member(obligation_chat_id, joined_user_id)
                    joined_user_member_dict = {}

                    # If the newly joined member is veified to NOT be an active member of the obligation chat, get the required user_name and entity object and process the kick.
                    if joined_chat_obligation_member.status not in ["administrator", "creator", "member"]:
                        joined_user_member_dict = lookup_group_member(joined_user_id)

                        # Adjust joined_user_member_dict to be the first record returned by lookup_group_member(), or None
                        joined_user_member_dict = joined_user_member_dict[0] if len(joined_user_member_dict)>0 else None
                        joined_user_name = (f"{joined_user_member_dict.get('user_name') if joined_user_member_dict else ''}")

                        logging.warning(f"SCAN: {results_chat_id} OBLIGATION KICK: {joined_user_name} ({joined_user_id} - @{joined_user_member_dict.get('username') }) kicked from {chat_name_dict.get('results_chat_id')} for not belonging to {chat_name_dict.get(obligation_chat_id)}.")

                        joined_user_telethon = await telethon.get_entity(joined_user_id)
                        await obligation_kick(joined_user_id, results_chat_id, results_chat_type, joined_user_name, chat_name_dict.get(obligation_chat_id))

                        #Insert or update this group member in the satabase, with a status of "kicked"
                        update_or_insert_group_member(results_chat_id, joined_user_telethon, EventType.KICKED)
            except Exception as e:
                logging.warning(f"SCAN: Exception raised with joined user id {joined_user_id} - {e} while evaluating obligation kicks. Moving on to next joined user in list.")
                continue
    except (BadRequest, BadRequestError, Forbidden, ChannelPrivateError) as e:
        logging.warning(f"SCAN: While evaluating obligation kicks, bad request or no permissions error with chat.")

    except Exception as e:
        logging.warning(f"SCAN: Exception raised with {results_chat_id} - {e}.")
    return


async def suspend_scanning():
    if not tracking_chat_members:
        return
    global scanning_underway
    logging.warning("Suspending timed chat tracking.")
    schedule.clear()
    while kick_started:
        await asyncio.sleep(1)
    schedule.every(3).minutes.do(update_chat_members)

    logging.warning("Timed chat tracking re-started.")

    return


# Function to run the scheduled tasks
async def run_scheduled_tasks():
    while tracking_chat_members:
        await schedule.run_pending()
        await asyncio.sleep(1)


# ********* COMMAND HANDLING *********


async def start_chat_member_tracking(update: Update=None, context: CallbackContext=None):
    schedule.every(3).minutes.do(update_chat_members, update, context)
    global tracking_chat_members
    tracking_chat_members= True  
    print("Timed chat tracking started.")

    # Create and start the scheduled tasks task
    asyncio.create_task(run_scheduled_tasks())


async def stop_chat_member_tracking(update: Update, context: CallbackContext):
    global tracking_chat_members
    tracking_chat_members = False  
    schedule.clear()
    print("Timed chat tracking stopped.")


# Send the help message to the user who triggered the /help command.
@is_admin_of_authorized_chat_check
async def start_command(update: Update, context: CallbackContext) -> None:
    """Send a message with information about the bot's available commands."""
    chat_id = update.effective_chat.id
    message=("""
        💥 Welcome to KickBot, your partner in lurker-slaughter 💥\n\n
        Keep this chat open. Status messages and command responses from the kickbot will be sent here.\n
    """
    )
    await context.bot.send_message(chat_id=chat_id, text=message)
    return


# Send the help message to the user who triggered the /help command.
@is_admin_of_authorized_chat_check
async def help_command(update: Update, context: CallbackContext) -> None:
    """Send a message with information about the bot's available commands."""
    chat_id = update.effective_chat.id
    await context.bot.send_message(chat_id=chat_id, text=HELP_MESSAGE)
    return


async def three_strike_mode(update: Update, context: CallbackContext):
    chat_id = update.message.chat_id
    chat_name = update.effective_chat.title
    issuer_user_id = update.effective_user.id
    chat_type = update.effective_chat.type
    
    if chat_type == ChatType.PRIVATE:
        message = await context.bot.send_message(
            chat_id=chat_id,
            text="<i style='color:#808080;'> This command only works in the chat where you want to whitelist an individual. Status updates will come here..</i>",
            parse_mode=ParseMode.HTML
        )       
        # Schedule a task to delete the message after 5 seconds
        asyncio.create_task(delete_message_after_delay(context, message))
        return

    try:
        # Send the acknowledgement
        message = await context.bot.send_message(
            chat_id=chat_id,
            text="<i style='color:#808080;'> Response will be sent privately.</i>",
            parse_mode=ParseMode.HTML
        )
        # Schedule a task to delete the message after 5 seconds
        asyncio.create_task(delete_message_after_delay(context, message))

        ts_mode = update_three_strikes(chat_id)
        logging.warning(f"Three strikes mode in {chat_name} now set to {ts_mode}")
        three_strikes_message=" Any user with 2+ previous kicks will now be banned." if ts_mode else ""
        if chat_id in DEBUG_CHATS:
            await context.bot.send_message(chat_id=chat_id, text=f"DEBUG: Three strikes mode in {chat_name} now set to {ts_mode}. {three_strikes_message}")
        await context.bot.send_message(chat_id=issuer_user_id, text=f"Three strikes mode in {chat_name} now set to {ts_mode}. {three_strikes_message}")
        
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
        logging.error(f"Error in the three_strike_mode() function: {e}")
    return


async def ban_leavers_mode(update: Update, context: CallbackContext):
    chat_id = update.message.chat_id
    chat_name = update.effective_chat.title
    issuer_user_id = update.effective_user.id
    chat_type = update.effective_chat.type
    
    if chat_type == ChatType.PRIVATE:
        message = await context.bot.send_message(
            chat_id=chat_id,
            text="<i style='color:#808080;'> This command only works in the chat where you want to whitelist an individual. Status updates will come here..</i>",
            parse_mode=ParseMode.HTML
        )       
        # Schedule a task to delete the message after 5 seconds
        asyncio.create_task(delete_message_after_delay(context, message))
        return

    try:
        # Send the acknowledgement
        message = await context.bot.send_message(
            chat_id=chat_id,
            text="<i style='color:#808080;'> Response will be sent privately.</i>",
            parse_mode=ParseMode.HTML
        )
        # Schedule a task to delete the message after 5 seconds
        asyncio.create_task(delete_message_after_delay(context, message))

        bl_mode = update_ban_leavers_status(chat_id)
        logging.warning(f"Ban-leavers mode in {chat_name} now set to {bl_mode}")
        ban_leavers_message="Any users leaving the group will now be banned." if bl_mode else ""
        if chat_id in DEBUG_CHATS:
            await context.bot.send_message(chat_id=chat_id, text=f"DEBUG: Ban-leavers mode in {chat_name} now set to {bl_mode}. {ban_leavers_message}")
        await context.bot.send_message(chat_id=issuer_user_id, text=f"Ban-leavers mode in {chat_name} now set to {bl_mode}. {ban_leavers_message}")

    except Forbidden as e:
        logging.error(f"Error in the ban_leavers() function (perhaps issued command as anon): {e}")

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
        logging.error(f"Error in the ban_leavers() function: {e}")
    return


async def request_log(update: Update, context: CallbackContext):
    # Upload app.log file
    chat_id = update.effective_chat.id
    chat_type = update.effective_chat.type
    if chat_type != ChatType.PRIVATE:
        await context.bot.send_message(chat_id=chat_id, text="This command only works in a private chat with the bot")
        return
    log_file_path = "app.log"  
    try:
        if os.path.exists(log_file_path):
            with open(log_file_path, "rb") as log_file:
                await context.bot.send_document(chat_id=chat_id, document=log_file)
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
        logging.error(f"Error in the request_log() function: {e}")
    return


async def chat_status(update: Update, context: CallbackContext):
    chat_id = update.message.chat_id
    chat_name = update.effective_chat.title
    issuer_user_id = update.effective_user.id
    chat_type = update.effective_chat.type

    total_members = 0
    posted_in_last_12_hours = 0
    not_posted = 0

    if not context.args:
        # No arguments passed
        update.message.reply_text("No arguments provided.")
        return

    if not is_user_admin(context.bot.id, chat_id):
        return
    
    if chat_type == ChatType.PRIVATE:
        message = await context.bot.send_message(
            chat_id=chat_id,
            text="<i style='color:#808080;'> This command only works in the chat where you want to look up current status The responses will come here.</i>",
            parse_mode=ParseMode.HTML
        )       
        # Schedule a task to delete the message after a few seconds
        asyncio.create_task(delete_message_after_delay(context, message))
        return
    else:
        message = await context.bot.send_message(
            chat_id=chat_id,
            text="<i style='color:#808080;'> Response will be sent privately.</i>",
            parse_mode=ParseMode.HTML
        )
        asyncio.create_task(delete_message_after_delay(context, message))
    try:
        cutoff_date, readable_string_of_duration = calculate_cutoff_date(context.args[0])
        user_data = get_user_activity(chat_id)
        
        # Convert user_data and admin_ids into sets for faster lookups
        user_data_set = {entry['user_id'] for entry in user_data}

        three_strikes_mode = get_three_strikes(chat_id)
        ban_leavers_mode = get_ban_leavers_status(chat_id)
        obligation_chat = lookup_obligation_chat(chat_id)

        if API_ID and API_HASH:
            await check_telethon_connection()
            async for user in telethon.iter_participants(chat_id):
                user_id = user.id
                is_member = isinstance(user.participant, ChannelParticipant) or isinstance(user.participant, ChatParticipant)
                if is_member:
                    total_members += 1
                # Check if the user exists in the user_data set or has no last_activity
                if user_id in user_data_set:
                    matching_entry = next(entry for entry in user_data if entry['user_id'] == user_id)
                    last_activity = matching_entry['last_activity']
                else:
                    last_activity = None

                # Convert last_activity to datetime if it's not None
                last_activity_datetime = datetime.strptime(last_activity, '%Y-%m-%d %H:%M:%S.%f') if last_activity else None

                # If the user has a last_activity, and it is after the cutoff date, they are immune from kick
                if is_member and (last_activity_datetime is not None and cutoff_date < last_activity_datetime):
                    posted_in_last_12_hours += 1

                if is_member and last_activity_datetime is None:
                    not_posted +=1
        time_window_lurk_rate = round((total_members - posted_in_last_12_hours) / total_members * 100, 1) if total_members > 0 else "N/A"
        total_lurk_rate = round((not_posted) / total_members * 100, 1) if total_members > 0 else "N/A"
        lurker_message = f"KICKBOT GROUP CHAT STATS FOR {chat_name}.\n\n"
        lurker_message += f"❌ 3 STRIKES MODE is {'on' if three_strikes_mode[0]==1 else 'off'}.\n\n"
        lurker_message += f"🚫 BAN LEAVERS MODE is {'on' if ban_leavers_mode[0]==1 else 'off'}.\n\n"
        lurker_message += f"🚫 OBLIGATION BACKUP SET TO {obligation_chat if obligation_chat else 'NONE'}.\n\n"
        lurker_message += f"👤 There are {total_members} non-admin members in the group.\n\n"
        lurker_message += f"⏱ {total_members - posted_in_last_12_hours} have NOT posted in the last {readable_string_of_duration} ({time_window_lurk_rate}% recent lurker).\n\n"
        lurker_message += f"💥 {not_posted} users have not posted at all. ({total_lurk_rate}% total lurker)"
        if chat_id in DEBUG_CHATS:
            await context.bot.send_message(chat_id=chat_id, text=f"DEBUG: {lurker_message}")
        await context.bot.send_message(chat_id=issuer_user_id, text=lurker_message)
    except (IndexError, ValueError) as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
        await context.bot.send_message(chat_id=chat_id, text="Invalid command format. Use /gcstats <time> (e.g., /gcstats 1d).")
        logging.error(f"An error occurred in kick_inactive_users(), probably due to an invalid time argument.")
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
        logging.error(f"Error assembling chat stats: {e}")
    return


async def lookup(update: Update, context: CallbackContext):
    chat_id = update.message.chat_id
    issuer_user_id = update.effective_user.id
    chat_type = update.effective_chat.type

    id_or_username = context.args[0]
    try:
        if chat_type != ChatType.PRIVATE:
            message = await context.bot.send_message(
                chat_id=chat_id,
                text="<i style='color:#808080;'> Response will be sent privately.</i>",
                parse_mode=ParseMode.HTML
            )
        id_or_username = int(id_or_username)
    except:
        pass   
    try:
        await check_telethon_connection()
        kicked_user = await telethon.get_entity(id_or_username)
        kicked_user_id = kicked_user.id
        
    except Exception as e:
        logging.error(f" Error getting entity and chat member information during lookup: {e}")
        await context.bot.send_message(chat_id=issuer_user_id, text=f"Error looking up user {id_or_username} in Telegram.")
        return
    try:
        if chat_type != ChatType.PRIVATE:
            group_member_dict = lookup_group_member(kicked_user_id, chat_id)
        else:
            group_member_dict = lookup_group_member(kicked_user_id)

        kicked_user_data = lookup_user_in_kick_db(kicked_user_id)
        # blacklist_data = lookup_user_in_blacklist(kicked_user_id)

        if not group_member_dict or len(group_member_dict)==0:
            kicked_user_message = "User not found in kickbot database."
        else:
            group_member_first = group_member_dict[0]
            kicked_user_name = group_member_first['user_name']
            kicked_user_username = group_member_first['user_username']
            kicked_user_is_bot = 'True' if group_member_first['is_bot'] else 'False'
            kicked_user_is_premium = 'True' if group_member_first['is_premium'] else 'False'
            kicked_user_is_fake = 'True' if group_member_first['is_fake'] else 'False'
            kicked_user_is_restricted = 'True' if group_member_first['is_restricted'] else 'False'
            kicked_user_restriction_reason = group_member_first['restricted_reason']  if group_member_first['restricted_reason'] else ''
            kicked_user_is_scam = 'True' if group_member_first['is_scam'] else 'False'
            kicked_user_is_verified = 'True' if group_member_first['is_verified'] else 'False'
            
            kicked_user_message="KICKBOT USER RECORD\n"
            kicked_user_message+=f"NAME: {kicked_user_name}\n"
            kicked_user_message+=f"ID: {kicked_user_id}\n"
            kicked_user_message+=f"USERNAME: @{kicked_user_username}\n"
            kicked_user_message+=f"PREMIUM: {kicked_user_is_premium}\n"
            kicked_user_message+=f"VERIFIED: {kicked_user_is_verified}\n"
            kicked_user_message+=f"IS BOT: {kicked_user_is_bot}\n"
            kicked_user_message+=f"RESTRICTED: {kicked_user_is_restricted}\n"
            kicked_user_message+=f"{'RESTR REASON: ' if group_member_first['is_restricted'] else ''}{kicked_user_restriction_reason}\n\n"
        await context.bot.send_message(chat_id=issuer_user_id, text=kicked_user_message)

        i_am_admin = []
        for group_member_row in group_member_dict:
            group_member_chat_id = group_member_row['chat_id']
            admins = chat_admins_cache.get(group_member_chat_id)
            titles = get_chat_ids_and_names()
            group_member_chat_title = titles.get(group_member_chat_id)
            if not admins:
                await authorize_chat_and_update_cache(group_member_chat_id, group_member_chat_title)
            if await is_user_admin(kickbot.id, group_member_chat_id):
                i_am_admin.append(group_member_row)   
    

        for i_am_admin_row in i_am_admin:
            kicked_user_chat_id = i_am_admin_row['chat_id']
            chat = await telethon.get_entity(kicked_user_chat_id)       
            chat_name = chat.title
            kicked_user_row = next((row for row in kicked_user_data if row[1] == chat_id), None)
            # blacklist_row = next((row for row in blacklist_data if row[1] == chat_id), None)

            kicked_user_number_kicks = kicked_user_row[2] if kicked_user_row else None
            if i_am_admin_row['last_posted']:
                kicked_user_last_posted = datetime.strptime(i_am_admin_row['last_posted'], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=utc_timezone).strftime('%d %B, %Y - %H:%M:%S')
            else:
                kicked_user_last_posted = "Never"
            kicked_user_last_kicked = datetime.strptime(kicked_user_row[4], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=utc_timezone).strftime('%d %B, %Y - %H:%M:%S') if kicked_user_row else None
            try:
                chat_member = await context.bot.get_chat_member(kicked_user_chat_id, kicked_user_id)
            except (BadRequest, BadRequestError, Forbidden, ChannelPrivateError) as e:
                logging.warning(f"Bad lookup request error in {chat_name}: {e}")
                continue

            kicked_user_status = None
            
            if chat_member:
                if chat_member.status == ChatMember.ADMINISTRATOR:
                    kicked_user_status = "Admin"
                elif chat_member.status == ChatMember.BANNED:
                    kicked_user_status = "Banned"
                elif chat_member.status == ChatMember.LEFT:
                    kicked_user_status = 'Kicked' if i_am_admin_row['status']=='Kicked' else "Left" #Kicked if in DB as Kicked, else Left
                elif chat_member.status == ChatMember.MEMBER:
                    kicked_user_status = "Member"
                elif chat_member.status == ChatMember.OWNER:
                    kicked_user_status = "Owner"
                elif chat_member.status == ChatMember.RESTRICTED:
                    kicked_user_status = "Restricted"
                else:
                    kicked_user_status = "None"
            else:
                    kicked_user_status = "None"

            # Search for the user in the list of participants
            kicked_user_last_joined = datetime.strptime(group_member_row['last_joined'], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=utc_timezone).strftime('%d %B, %Y - %H:%M:%S') if group_member_row['last_joined'] else 'N/A'
            kicked_chat_message=""
            kicked_chat_message+=f"{kicked_user_name} - {chat_name}\n"
            # kicked_chat_message+=' - BLACKLISTED\n' if blacklist_row else '\n'
            kicked_chat_message+=f"KICKS from {chat_name}: {kicked_user_number_kicks}\n"
            kicked_chat_message+=f"BANS from {chat_name}: {group_member_row['times_banned'] if group_member_row['times_banned'] else 'N/A'}\n"
            kicked_chat_message+=f"MOST RECENTLY JOINED: {kicked_user_last_joined if kicked_user_last_joined else 'N/A'}\n"
            kicked_chat_message+=f"LAST POST: {kicked_user_last_posted}\n"
            kicked_chat_message+=f"LAST KICKED: {kicked_user_last_kicked}\n"
            kicked_chat_message+=f"STATUS: {kicked_user_status}\n\n"
            await context.bot.send_message(chat_id=issuer_user_id, text=kicked_chat_message)

    except (BadRequest, BadRequestError, Forbidden, ChannelPrivateError) as e:
        logging.error(f"Error in the processing user lookup for {chat_id}: Bot removed or group nuked.{e}")
        return
   
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
        logging.error(f"Error in the processing user lookup: {e}")
    return


# Function to remove data from chats that are no longer active
async def clean_database(update, context):
    chat_id = update.effective_message.chat_id
    issuer_chat_id = update.effective_chat.id
    chat_type = update.effective_chat.type
    if chat_type != ChatType.PRIVATE:
        await context.bot.send_message(chat_id=chat_id, text="This command only works in a private chat with the bot")
        return
    try:
        active_chats, inactive_chats, active_str, inactive_str = await find_inactive_chats()
        if len(inactive_chats)>0:
            await context.bot.send_message(chat_id=issuer_chat_id, text = inactive_str)
            logging.warning(inactive_str)
            await context.bot.send_message(chat_id=issuer_chat_id, text = "Purging...\n")
            logging.warning("Purging...\n")

        del_chats_from_db(inactive_chats)

        if len(inactive_chats)>0:
            logging.warning("Inactive channels deleted.\n")
            await context.bot.send_message(chat_id=issuer_chat_id, text = "Inactive channels deleted.\n")
        logging.warning(active_str)
        await context.bot.send_message(chat_id=issuer_chat_id, text = active_str)
        return
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
        logging.error(f"An error occured while cleaning the database: {e}")
    return


async def find_inactive_chats():
    active_chats = []
    inactive_chats = []
    try:
        chat_ids_in_database = list_chats_in_db()
        chat=None
        active_str = "CURRENT ACTIVE CHATS\n"
        inactive_str = "INACTIVE CHATS IN DATABASE\n"
        for chat_id in chat_ids_in_database:
            rt = 0
            while rt < max_retries:
                try:
                    chat = await kickbot.get_chat(chat_id)
                    if chat.type == ChatType.PRIVATE:
                        # if not a private bot chat:
                        inactive_chats.append(chat_id)
                        inactive_str = inactive_str + f"{chat_id}\n"
                        break
                    else:
                        active_chats.append([chat.id, chat.title])
                        active_str = active_str + f"{chat.id} - {chat.title}\n"
                        break
                except (BadRequest, Forbidden) as e:
                    # Expecting deleted chats to get this error
                    inactive_chats.append(chat_id)
                    inactive_str = inactive_str + f"{chat_id}\n"
                    logging.warning(f"Bad Request occurred in chat {chat_id}: Possible that {chat.title if chat else 'chat'} has nuked.")
                    break
                except (RetryAfter, TimedOut, NetworkError) as e:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    wait_seconds = e.retry_after if hasattr(e, 'retry_after') else 3
                    logging.warning(f"Error in find_inactive_chats() - {e}.  Line: {exc_traceback.tb_lineno} - Type: {exc_type}. Waiting for {wait_seconds} seconds...")
                    await asyncio.sleep(wait_seconds)
                    rt += 1
                    if rt == max_retries:
                        logging.warning(f"Max retry limit reached. Chat {chat.title if chat else 'chat'} not classified.")
                        break
                except Exception as e:
                    # Unknown exception, being conservative and not labeling as inactive.
                    active_chats.append(chat_id)
                    active_str = active_str + f"{chat_id}\n"
                    logging.warning(f"Unhandled exception occurred in chat {chat_id}: {e}")
                    break

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(exc_type, exc_value, exc_traceback)
        logging.error(f"An error occurred while cleaning the database: {e}")
    return active_chats, inactive_chats, active_str, inactive_str


async def show_wholeft(update: Update, context: CallbackContext):
    chat_id = update.message.chat_id
    issuer_user_id = update.effective_user.id
    chat_type = update.effective_chat.type

    try:
        if chat_type is not ChatType.PRIVATE:
            message = await context.bot.send_message(
                chat_id=chat_id,
                text="<i style='color:#808080;'> Response will be sent privately.</i>",
                parse_mode=ParseMode.HTML
            )
            asyncio.create_task(delete_message_after_delay(context, message))
            wholeft_data = get_wholeft(chat_id)
        else:
            wholeft_data = get_wholeft_from_private()

        # Group users by channel_id
        users_by_channel = {}
        for user_id, channel_id, _, _, time_in_group_str, user_name in wholeft_data:
            # Convert the string representation to a timedelta object
            time_in_group = str_to_timedelta(time_in_group_str)
            if channel_id not in users_by_channel:
                users_by_channel[channel_id] = []
            users_by_channel[channel_id].append((user_id, user_name, time_in_group))

        # Print the results
        await check_telethon_connection()

        csv_filename = "leavers.csv"

        with open(csv_filename, mode='w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(["CHAT ID", "CHAT NAME", "USER ID", "USER NAME", "TIMES LEFT", "AVG TIME IN GROUP"])

        for channel_id, user_data in users_by_channel.items():
            chat_entity = await telethon.get_entity(channel_id)
            title = chat_entity.title
            # wholeft_message += f"{title.upper()}\n"
            users_to_report = []
            users_info = []  # List to store user information for sorting
            for user_id, user_name, time_in_group in user_data:
                num_times_left = len([entry for entry in user_data if entry[0] == user_id])
                avg_time_in_group = sum((time_in_group for uid, _, time_in_group in user_data if uid == user_id), timedelta()) / num_times_left                

                users_info.append({
                    'user_name': user_name,
                    'user_id': user_id,
                    'num_times_left': num_times_left,
                    'avg_time_in_group': avg_time_in_group,
                })

            # Sort the list based on avg_time_in_group
            users_info.sort(key=lambda x: x['avg_time_in_group'])
            
            users_to_report = []
            for user_info in users_info:
                user_name = user_info['user_name']
                user_id = user_info['user_id']
                num_times_left = user_info['num_times_left']
                avg_time_in_group = user_info['avg_time_in_group']
                avg_time_str = format_timedelta(avg_time_in_group)


                
                if user_id not in users_to_report:
                    users_to_report.append(user_id)

                    # Write data to CSV file
                    
                    with open(csv_filename, mode='a', newline='') as csv_file:
                        csv_writer = csv.writer(csv_file)
                        # Write data for the current user
                        csv_writer.writerow([channel_id, title, user_id, user_name, num_times_left, avg_time_str])

        # Upload the CSV file
        with open(csv_filename, 'rb') as csv_file:
            await context.bot.send_document(chat_id=issuer_user_id, document=csv_file, filename="leavers.csv")


    except Forbidden as e:
        logging.error(f"Error in the wholeft printing process: {e}")
        message = await context.bot.send_message(
            chat_id=chat_id,
            text="<i style='color:#808080;'> Please open a chat with the bot to see responses.</i>",
            parse_mode=ParseMode.HTML
        )
        asyncio.create_task(delete_message_after_delay(context, message))
    
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
        logging.error(f"Error in the wholeft printing process: {e}")

    return


async def set_backup(update, context):
    user_id = update.effective_user.id
    issuer_chat_id = update.effective_chat.id
    issuer_chat_type = update.effective_chat.type
    issuer_chat_name = update.effective_chat.title
    try:
        await check_telethon_connection()

        if issuer_chat_type == ChatType.PRIVATE:
            message = await context.bot.send_message(
                chat_id=issuer_chat_id,
                text="<i style='color:#808080;'> The /setbackup command only works in the public chat you want to set up.</i>",
                parse_mode=ParseMode.HTML
            )
            asyncio.create_task(delete_message_after_delay(context, message))
            return
        else:
            message = await context.bot.send_message(
                chat_id=issuer_chat_id,
                text="<i style='color:#808080;'> Response will be sent privately.</i>",
                parse_mode=ParseMode.HTML
            )
            asyncio.create_task(delete_message_after_delay(context, message))

        i_am_admin = []
        active_ids = list_chats_in_db()
        for active_id in active_ids:
            admins = chat_admins_cache.get(active_id)
            titles = get_chat_ids_and_names()
            chat_title = titles.get(active_id)
            if not admins:
                await authorize_chat_and_update_cache(active_id, chat_title)
            if await is_user_admin(kickbot.id, active_id):
                i_am_admin.append(active_id)


        if len(i_am_admin)<2:
            message = await context.bot.send_message(
                chat_id=issuer_chat_id,
                text="<i style='color:#808080;'>In order to set a backup, Kickbot must be admin in at least two chats.</i>",
                parse_mode=ParseMode.HTML
        )
            return
        buttons = []
        button_names={}
        for chat_id in i_am_admin:
            if chat_id != issuer_chat_id:
                try:
                    entity = await context.bot.get_chat(chat_id)
                    button_text = entity.title
                except Exception as e:
                    logging.error(f" Error getting entity information for chat_id {chat_id}: {e}")
                    button_text = str(chat_id)

                buttons.append(InlineKeyboardButton(button_text, callback_data=f"setbackup_{chat_id}"))
                button_names[chat_id] = button_text

        buttons.append(InlineKeyboardButton("None", callback_data="setbackup_None"))


        # Create a two-column layout for the buttons
        keyboard = [buttons[i:i+2] for i in range(0, len(buttons), 2)]

        # Create an inline keyboard markup
        reply_markup = InlineKeyboardMarkup(keyboard)

        # Send a message with the inline keyboard
        menu_message = await context.bot.send_message(
            chat_id=user_id,
            text=f"Select the group that you want to set as the obligation group for <strong>{issuer_chat_name}</strong>:",
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup
        )
        # Save the message ID for later reference
        context.user_data['menu_message_id'] = menu_message.message_id
        context.user_data['chat_id_to_receive_obligation'] = issuer_chat_id
        context.user_data['chat_title_to_receive_obligation'] = issuer_chat_name
        context.user_data['button_names'] = button_names

    except (BadRequest, BadRequestError, Forbidden, ChannelPrivateError) as e:
        logging.error(f"Error in the processing user lookup for {chat_id}: Bot removed or group nuked.{e}")
        return
    
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
        logging.error(f" Error in set_backup(): {e}")
    return

# Callback function for handling button clicks
async def button_click(update, context):
    try:
        query = update.callback_query
        chat_id = query.message.chat_id
        user_id = query.from_user.id
        message_id = context.user_data['menu_message_id'] 
        issuer_chat_id = context.user_data['chat_id_to_receive_obligation']
        issuer_chat_title = context.user_data['chat_title_to_receive_obligation']
        button_names = context.user_data['button_names']

        # Extract the callback_data
        callback_data = query.data.split("_")
        action = callback_data[0]
        choice = callback_data[1]

        if action != "setbackup":
            return

        # Check the action and perform the corresponding operation
        if choice == "None":
            # Perform the operation to set the obligation_chat to None
            delete_obligation_chat(issuer_chat_id)
            message_text = f"Obligation group set to <strong>None</strong> for <strong>{issuer_chat_title}</strong>."
        else:
            try:
                choice_int = int(choice)
            except:
                message_text = "Invalid action."
            if choice == issuer_chat_id:
                delete_obligation_chat(issuer_chat_id)
                message_text = f"Chat cannot be its own obligation group. Obligation for <strong>{issuer_chat_title}</strong> set to <strong>None</strong>."
            else:
                # Perform the operation to set the obligation_chat
                insert_obligation_chat(issuer_chat_id, choice_int)
                message_text = f"Obligation group set to <strong>{button_names[choice_int]}</strong> for <strong>{issuer_chat_title}</strong>."

        try:
            # Send a confirmation message
            await context.bot.send_message(chat_id=chat_id, text=message_text, parse_mode=ParseMode.HTML)
                # Delete the original message
            await asyncio.sleep(3)
            await context.bot.delete_message(chat_id=user_id, message_id=message_id)
        except BadRequest as e:
            logging.error(f" Error with setbackup confirmation messages: {e}")


    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
        logging.error(f" Error processing response in set_backup(): {e}")
    return


async def obligation_kick(user_id, chat_id, chat_type, user_name, obligation_chat_name): 
    global let_leave_without_banning
    await check_telethon_connection()
    rt = 0
    while rt < max_retries:
        try:
            logging.warning(f"Kicking {user_name} from {chat_id} for not belonging to {obligation_chat_name}.")
            greeting = await telethon.send_message(
            entity=chat_id,
            message=f"{user_name}, This group needs you to first join the group <strong>{obligation_chat_name}</strong> before coming here. After that, you'll be free to rejoin.",
            parse_mode='html'
        ) 
            await asyncio.sleep(5)
            await telethon.delete_messages(chat_id, greeting)
            
            if (chat_type == ChatType.SUPERGROUP or chat_type == ChatType.CHANNEL):
                let_leave_without_banning.add((user_id, chat_id))
                logging.warning(f"User ID {user_id} added to LET LEAVE WITHOUT BAN.")
                await kickbot.unban_chat_member(chat_id, user_id)
            else:
                await kickbot.ban_chat_member(chat_id, user_id)
                # insert_kicked_user_in_blacklist(user_id, chat_id)      
            #logging.warning(let_leave_without_banning)
            break

        except UserAdminInvalidError as e:
            logging.warning(f"An error occured in obligation_kick() - Permission issue kicking {user_name} from {chat_id}. -  {e}")
            break  

        except (BadRequest, BadRequestError, Forbidden, ChannelPrivateError) as e:
            logging.warning(f"An error occured in obligation_kick() - Closed topic, nuke, or bot missing. -  {e}")
            break  

        except (ValueError) as e:
            logging.warning(f"A value error occured in obligation_kick(): {e}")
            break

        except RetryAfter as e:
                wait_seconds = e.retry_after
                logging.warning(f"Got a RetryAfter error. Waiting for {wait_seconds} seconds...")
                await asyncio.sleep(wait_seconds)
                rt += 1
                if rt == max_retries:
                    logging.warning(f"Max retry limit reached. Message not sent.")
                    raise e
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            await debug_to_chat(exc_type, exc_value, exc_traceback)
            logging.error(f"An error occured in obligation_kick(): {e}")    
            break    
    return



# ********* REALTIME CHAT EVENT HANDLING *********

async def process_realtime_obligation_kick(context, chat_id, chat_type, chat_name_dict, chat_member):
    obligation_chat_id = lookup_obligation_chat(chat_id)
    if not obligation_chat_id:
        return
    
    user_id = chat_member.user.id
    user_name = " ".join(filter(None, [chat_member.user.first_name, chat_member.user.last_name]))
    username = chat_member.user.username
    chat_name = chat_name_dict.get(chat_id) 
    obligation_chat_name = chat_name_dict.get(obligation_chat_id) if obligation_chat_id else ''
    try:
        # Is user a member of the required obligation chat?
        obligation_member = await context.bot.get_chat_member(obligation_chat_id, user_id)
        if obligation_member.status not in ["member", "administrator", "creator"]:
            await obligation_kick(user_id, chat_id, chat_type, user_name, obligation_chat_name)
            update_or_insert_group_member(chat_id, chat_member, EventType.KICKED)
            logging.warning(f"REALTIME: Obligation kick for {user_name} ({user_id}, @{username}) from {chat_name} for not being in {obligation_chat_name}.")
    except Exception as e:
        logging.error(f"Error checking obligation chat membership for {user_id}: {e}")
    return


async def process_realtime_ban_leavers(new_chat_member, new_status, chat_id, user_id, user_name, username, chat_name_dict, ban_leavers_mode):
    # This function executes if ban_leavers_mode is on, and will ban anyone with a status of "left"
    global let_leave_without_banning
    try:   
        excused = any([user_id == user and chat_id == chat for user, chat in let_leave_without_banning])
        chat_name = chat_name_dict.get(chat_id) 
        member = lookup_group_member(user_id, chat_id)
        status = member[0]['status'] if member else None # Current database status of leaver
        logging.warning(f"REALTIME: {chat_id} -- {user_name} (@{username}, {user_id}) leaving {chat_name}. Chat status is: {new_status}. "
            f"Ban Leavers mode is {'ON' if ban_leavers_mode[0]==1 else 'OFF'}. "
            f"Obligation kick hallpass list: {let_leave_without_banning}"
        )
        # If user has been kicked due to an obligation kick, excuse then from being banned
        if not excused:
            left_user_ids = [user_id]
            if status == "Kicked":
                logging.warning(f"REALTIME: BAN-LEAVERS MODE ON FOR {chat_id}, BUT {user_name} ALREADY KICKED. DOING NOTHING.")
            else:
                logging.warning(f"REALTIME: BAN-LEAVERS MODE ON FOR {chat_id} - UNI-BANNING {user_name}")
                await uniban_from_list(left_user_ids, reason = f'REALTIME - LEFT {chat_name} WHILE NO-LEAVERS MODE ON')
            update_or_insert_group_member(chat_id, new_chat_member, EventType.BANNED)

        # If the user is on the excused list, log that they were kicked because of an obligation chat.
        else:
            logging.warning(f"REALTIME: {user_name} KICKED FROM {chat_name} FOR NOT BELONGING TO OBLIGATION CHAT.")       
    except Exception as e:
        logging.error(f"Error processing leaver-bans for {user_id}: {e}")
    let_leave_without_banning.discard((user_id, chat_id))
    return


async def handle_new_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ Thank you to python-telegram-bot example chatmemberbot.py for the model of this function
        https://github.com/python-telegram-bot/python-telegram-bot/blob/master/examples/chatmemberbot.py
    """

    def is_admin_or_whitelist(user_id, new_status, whitelist_set, shin_ids):
        if new_status in ["administrator", "creator"] or \
            user_id in AUTHORIZED_ADMINS or \
            user_id in whitelist_set or \
            user_id in shin_ids:
            logging.info(f"REALTIME: Admin, whitelisted, or special user {user_name} ({user_id}) joined {chat_name}. Ignoring.")
            return True
        else:
            return False

    global chat_admins_cache

    # Early return if the update doesn't concern a status change
    status_change = update.chat_member.difference().get("status")
    if status_change is None:
        return
    
    # Determine if the user has transitioned from non-member to member
    old_status, new_status = status_change
    was_member = old_status in ["member", "administrator", "creator"]
    is_member = new_status in ["member", "administrator", "creator"]

    # If the status change is not someone joining or leaving the chat, return early
    if not (not was_member and is_member) and not (not is_member and was_member):
        return

    chat_id, chat_name, chat_type = update.effective_chat.id, update.effective_chat.title, update.effective_chat.type
    chat_type = update.effective_chat.type
    new_chat_member = update.chat_member.new_chat_member
    
    user_id = new_chat_member.user.id
    user_name = " ".join(filter(None, [new_chat_member.user.first_name, new_chat_member.user.last_name]))
    username = new_chat_member.user.username

    shin_ids = set(keyword_search_from_db('shinanygans'))
    whitelist_set = {entry[0] for entry in get_whitelist(chat_id)}
    chat_name_dict = get_chat_ids_and_names()

    try:
        # Proceed with this block if a user who was already in the group changes status
        if was_member and is_member:

            # If user just became an admin, add to cached admins
            if old_status not in ["administrator"] and new_status in ["administrator"]:
                chat_admins_cache[chat_id].add(user_id)


            # If user is no longer an admin, remove from cached admins
            if old_status in ["administrator", "creator"] and new_status not in ["administrator"]:
                chat_admins_cache[chat_id].discard(user_id)

        # Proceed with this block if there has been a transition to being a member (joined group)
        elif not was_member and is_member:

            # No matter the admin status, log the joining of the group in the 
            update_or_insert_group_member(chat_id, new_chat_member, EventType.JOINED)
            
            # Ignore admins, whitelisted users, or special IDs

            if is_admin_or_whitelist(user_id, new_status, whitelist_set, shin_ids):
                return

            # Record the user in the user_activity database
            insert_user_in_db(user_id, chat_id, "user_activity")
            
            # Process obligation kick for non-admin members of supergroups
            if chat_type in [ChatType.SUPERGROUP, ChatType.CHANNEL]:
                await process_realtime_obligation_kick(context, chat_id, chat_type, chat_name_dict, new_chat_member)

        # Proceed with this block if there has been a transition to no longer being a member (left group)
        elif (not is_member and was_member): 

            # If a kick is started, ignore. The kick functionality will do the database updates
            if user_id == context.bot.id or kick_started:
                return
            
            if new_status == 'kicked':  #User was banned manually by an admin
                logging.warning(f"REALTIME: {chat_id} -- {user_name} (@{username}, {user_id}) removed from {chat_name}."
            )
                # Because we're not sure if the user was kicked or banned, we will rely on the next room scan to set the database status
                return 

            #  No matter the admin status, log the leaving of the group 
            update_or_insert_group_member(chat_id, new_chat_member, EventType.LEFT)
            update_left_groups()

            # No further processing is needed for admins, whitelisted users, or special IDs

            if is_admin_or_whitelist(user_id, new_status, whitelist_set, shin_ids):
                return

            # Remove the user from the user_activity database (media posting info erased when user leaves)
            delete_user_from_db(user_id, chat_id, "user_activity")

            # If ban_leavers_mode is on, ban anyone with a status of "left"
            ban_leavers_mode = get_ban_leavers_status(chat_id)

            if ban_leavers_mode[0]==1:
                await process_realtime_ban_leavers(new_chat_member, new_status, chat_id, user_id, user_name, username, chat_name_dict, ban_leavers_mode)

    except (BadRequest, Forbidden) as e:
        logging.warning(f"PRIVATE ERROR in handle_new_member() - {chat_name} may no longer be active")

    except (TimeoutError, NetworkError) as e:
        logging.error(f"Timeout error in the handle_new_member() function: {e}")

    except ValueError:
        logging.error(f"Error in the handle_new_member() function, likely from a bad get_entity lookup on a user_id.")

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
        logging.error(f"Error in the handle_new_member() function: {e}")
    return


async def handle_message(update: Update, context: CallbackContext): 
    rt = 0
    await check_telethon_connection()
    if not update.effective_chat or not update.effective_message or not update.effective_user:
        return
    while rt < max_retries:
        try:          
            chat_type = update.effective_chat.type
            # Kickbot private chats do not process ordinary messages.
            #if chat_type == ChatType.PRIVATE:
            #    if update.message.from_user.id in AUTHORIZED_ADMINS and update.message.document and update.message.document.file_name == "blacklist.csv":
            #        await handle_blacklist_import(update, context)
            #        return

            chat_id = update.effective_message.chat_id
            chat_name = update.effective_chat.title
            user = update.effective_user
            user_id = user.id
            user_name = " ".join(filter(None, [user.first_name, user.last_name]))

            # No matter what the message contains, capture the sender in the DB 
            insert_user_in_db(user_id, chat_id, "user_activity")

            # If the message contained acceptable media, process further
            if update.effective_message.document or update.effective_message.photo or update.effective_message.video:
                date = update.effective_message.date

                chat_member = None
                try:
                    chat_member = await context.bot.get_chat_member(chat_id, user_id)
                    update_or_insert_group_member(chat_id, chat_member, EventType.POSTED)
                except Exception as e:
                    logging.error(f"Exception in handle_message() looking up chat member: {e}. Proceeding as if not an admin.")
                # If the sender was not an admin, update the last_activity in the database
                if not chat_member or chat_member.status not in ["administrator", "creator"]:
                    logging.warning(f"User ID {user_id} '{user_name}' in chat {chat_id} '{chat_name}' *POSTED MEDIA*")
                    update_user_activity(user_id, chat_id, date)

                else:
                    logging.info(f"User ID {user_id} '{user_name}' in chat {chat_id} '{chat_name}' contains acceptable media but is an admin. Ignoring.")
            break

        except (BadRequest, BadRequestError, Forbidden, ChannelPrivateError) as e:
            logging.warning(f"PRIVATE ERROR - {chat_name} may no longer be active")
            break

        except (RetryAfter, TimedOut, TimeoutError, NetworkError) as e:
            wait_seconds = e.retry_after if hasattr(e, 'retry_after') else 3
            logging.warning(f"Got a RetryAfter error. Waiting for {wait_seconds} seconds...")
            await asyncio.sleep(wait_seconds)
            rt += 1
            if rt == max_retries:
                logging.warning(f"Max retry limit reached. Message not sent.")
                raise e
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
            logging.error(f"An error occured in handle_message() parsing a post for db entry: {e}")   
            break     
    return


# ********* WHITELISTING *********


async def whitelist_user(update: Update, context: CallbackContext):
    chat_id = update.message.chat_id
    chat_name = update.effective_chat.title
    issuer_user_id = update.effective_user.id
    chat_type = update.effective_chat.type
    
    if chat_type == ChatType.PRIVATE:
        message = await context.bot.send_message(
            chat_id=chat_id,
            text="<i style='color:#808080;'> This command only works in the chat where you want to whitelist an individual. Status updates will come here..</i>",
            parse_mode=ParseMode.HTML
        )       
        # Schedule a task to delete the message after 5 seconds
        asyncio.create_task(delete_message_after_delay(context, message))
        return

    try:
        # Send the acknowledgement
        message = await context.bot.send_message(
            chat_id=chat_id,
            text="<i style='color:#808080;'> Response will be sent privately.</i>",
            parse_mode=ParseMode.HTML
        )
        # Schedule a task to delete the message after 5 seconds
        asyncio.create_task(delete_message_after_delay(context, message))
        await check_telethon_connection()
        try:
            lookup_id = int(context.args[0])
        except:
            lookup_id = context.args[0]

        user = await telethon.get_entity(lookup_id)
        user_id = user.id
        user_name = " ".join(filter(None, [user.first_name, user.last_name]))
        insert_user_in_db(user_id, chat_id, "whitelist")
        logging.warning(f"{user_name} has been whitelisted in {chat_name}")
        if chat_id in DEBUG_CHATS:
            await context.bot.send_message(chat_id=chat_id, text=f"DEBUG: {user_name} has been whitelisted in {chat_name}")
        await context.bot.send_message(chat_id=issuer_user_id, text=f"{user_name} has been whitelisted in {chat_name}")
        
    except (IndexError, ValueError):
        await context.bot.send_message(chat_id=issuer_user_id, text="Invalid command format. Use /whitelist <user id or @ name> (e.g., /whitelist @username).")
        logging.error(f"An error occurred in whitelist_user(), probably due to an invalid time argument.")
        
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
        logging.error(f"Error in the whitelisting process: {e}")
    return


async def dewhitelist_user(update: Update, context: CallbackContext):
    chat_id = update.message.chat_id
    chat_name = update.effective_chat.title
    issuer_user_id = update.effective_user.id
    chat_type = update.effective_chat.type
    
    if chat_type == ChatType.PRIVATE:
        message = await context.bot.send_message(
            chat_id=chat_id,
            text="<i style='color:#808080;'> This command only works in the chat where you want to de-whitelist an individual. Status updates will come here..</i>",
            parse_mode=ParseMode.HTML
        )       
        # Schedule a task to delete the message after 5 seconds
        asyncio.create_task(delete_message_after_delay(context, message))
        return
    try:
        # Send the acknowledgement
        message = await context.bot.send_message(
            chat_id=chat_id,
            text="<i style='color:#808080;'> Response will be sent privately.</i>",
            parse_mode=ParseMode.HTML
        )
        # Schedule a task to delete the message after 5 seconds
        asyncio.create_task(delete_message_after_delay(context, message))
        await check_telethon_connection()
        try:
            lookup_id = int(context.args[0])
        except:
            lookup_id = context.args[0]
        user = await telethon.get_entity(lookup_id)
        user_id = user.id
        user_name = " ".join(filter(None, [user.first_name, user.last_name]))
        delete_user_from_db(user_id, chat_id, "whitelist")
        logging.warning(f"{user_name} has been de-whitelisted from {chat_name}")
        if chat_id in DEBUG_CHATS:
            await context.bot.send_message(chat_id=chat_id, text=f"DEBUG: {user_name} has been de-whitelisted from {chat_name}")
        await context.bot.send_message(chat_id=issuer_user_id, text=f"{user_name} has been de-whitelisted from {chat_name}")
        

    except (IndexError, ValueError):
        await context.bot.send_message(chat_id=issuer_user_id, text="Invalid command format. Use /dewhitelist <user id or @ name> (e.g., /dewhitelist @username).")
        logging.error(f"An error occurred in dewhitelist_user(), probably due to an invalid argument.")
        
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
        logging.error(f"Error in the dewhitelisting process: {e}")
    return


async def show_whitelist(update: Update, context: CallbackContext):
    chat_id = update.message.chat_id
    issuer_user_id = update.effective_user.id
    chat_type = update.effective_chat.type
    try:
        if chat_type is not ChatType.PRIVATE:
            try:
                message = await context.bot.send_message(
                    chat_id=chat_id,
                    text="<i style='color:#808080;'> Response will be sent privately.</i>",
                    parse_mode=ParseMode.HTML
                )
                asyncio.create_task(delete_message_after_delay(context, message))
            except Exception as e:
                logging.error(f"Couldn't send message back to {chat_id} - {e}")
            whitelist_data = get_whitelist(chat_id)
        else:
            whitelist_data = get_whitelist_from_private()

        
        whitelist_message=f"WHITELISTED USERS\n\n"

        # Group users by channel_id
        users_by_channel = {}
        for user_id, channel_id in whitelist_data:
            if channel_id not in users_by_channel:
                users_by_channel[channel_id] = []
            users_by_channel[channel_id].append(user_id)

        # Print the results
        await check_telethon_connection()
        for channel_id, user_ids in users_by_channel.items():
            try:
                chat_entity = await context.bot.get_chat(channel_id)
            except BadRequest:
                logging.error(f"Error in show_whitelist() getting chat {channel_id} - Bad Request error. Abandoning, and deleting from DB.")
                del_chats_from_db([channel_id])
            except Exception as e:
                logging.error(f"Error in show_whitelist() getting chat {channel_id} ({e}) - Skipping.") #BUG delete chat if bad request/not found
                continue
            title=chat_entity.title
            whitelist_message += f"{title.upper()}\n"
            for user_id in user_ids:
                user_entity = await telethon.get_entity(user_id)
                user_name = " ".join(filter(None, [user_entity.first_name, user_entity.last_name]))
                whitelist_message += f"{user_name} ({user_id})\n"
            whitelist_message += "\n"
        if chat_id in DEBUG_CHATS:
            await context.bot.send_message(chat_id=chat_id, text=f"DEBUG: {whitelist_message}")
        await context.bot.send_message(chat_id=issuer_user_id, text=whitelist_message)
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
        logging.error(f"Error in the whitelist printing process: {e}")
    return


# ********* KICK PROCESSING *********


# Function to accept a batch of users and kick them. Used with asyncio.gather() in kick_inactive_users()
async def process_user_batch(batch, context, issuer_chat_id, issuer_chat_type, issuer_chat_name, pretend, ban, pbar):
    
    # Kick the inactive users and count the number of users kicked
    banned_count = 0
    banned_uids = []
    for user_info in batch:
        user_id = user_info[0]
        last_activity_str = user_info[1]
        last_activity_readable = datetime.strptime(last_activity_str, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=utc_timezone).strftime('%d %B, %Y - %H:%M:%S') if last_activity_str else None
        rt = 0
        while rt < max_retries:
            try:
                # Decide whether to ban or kick based on the kick count
                kick_count = lookup_kick_count_in_kick_db(user_id, issuer_chat_id)
                three_strikes = False if kick_count < 2 else True
                three_strikes_mode = get_three_strikes(issuer_chat_id)
                three_strikes_ban = three_strikes_mode[0]==1 and three_strikes
                action = 'ALLOWED TO REMAIN'
                if not pretend:
                    # Increment the user's kick count in the database
                    insert_kicked_user_in_kick_db(user_id, issuer_chat_id, last_activity_str)
                
                    # If supergroup, use 'unban' for kick. Otherwise just ban.               
                    if (issuer_chat_type == ChatType.SUPERGROUP or issuer_chat_type == ChatType.CHANNEL) and not ban and not three_strikes_ban:
                        await context.bot.unban_chat_member(issuer_chat_id, user_id)
                        action = 'KICKED'
                    else:
                        await context.bot.ban_chat_member(issuer_chat_id, user_id)
                        # insert_kicked_user_in_blacklist(user_id, issuer_chat_id)
                        banned_uids.append(user_id)
                        if three_strikes_ban:
                            action = 'BANNED (THIRD STRIKE)'
                        elif ban:
                            action = 'BANNED (BAN PURGE)'
                        else:
                            action = 'BANNED (GROUP NOT SUPERGROUP OR CHANNEL)'
                else:
                    action = 'PRETEND-KICKED'
                    
                banned_count += 1
                
                # Update the progress bar
                pbar.update(1)    

                logging.warning(f"User ID {user_id} {action} from {issuer_chat_id} '{issuer_chat_name}' (kick # {kick_count+1}).")
                
                with open('kick.log', 'a') as log_file:
                    log_file.write(f"User ID: {user_id}, Last Activity: {last_activity_readable}, Kick # {kick_count+1}{' (BANNED)' if three_strikes_ban else ''}.\n")
                break
            except (RetryAfter, TimedOut, NetworkError) as e:
                wait_seconds = e.retry_after if hasattr(e, 'retry_after') else 3
                logging.warning(f"Got a RetryAfter error. Waiting for {wait_seconds} seconds...")
                await asyncio.sleep(wait_seconds)
                rt += 1
                if rt == max_retries:
                    logging.warning(f"Max retry limit reached. Error: {e}. User {user_id} not kicked.")
                    break
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                await debug_to_chat(exc_type, exc_value, exc_traceback)
                logging.warning(f"Got an error while processing: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
                rt += 1
                if rt == max_retries:
                    logging.warning(f"Max retry limit reached. Error: {e}. User {user_id} not kicked.")
                    break
    batch_update_banned(banned_uids, issuer_chat_id)
    return banned_count


async def assemble_banned_list(chat_id, admin_ids, cutoff_date):
    users_to_ban = []
    banned_name_lookup = {}
    rt = 0
    while rt < max_retries:
        try:
            logging.warning(f"STARTING DB QUERIES.")
            #with sqlite3.connect(DATABASE_PATH) as conn:

            user_data = get_user_activity(chat_id)

            # Convert user_data and admin_ids into sets for faster lookups
            user_data_set = {entry['user_id'] for entry in user_data}
            whitelist_data = get_whitelist(chat_id)

            # Convert user_data and admin_ids into sets for faster lookups
            whitelist_set = {entry[0] for entry in whitelist_data}

            if API_ID and API_HASH:
                logging.warning(f"QUERYING ROOM MEMBERS.")
                await check_telethon_connection()
                async for user in telethon.iter_participants(chat_id):
                    user_id = user.id
                    user_name =  " ".join(filter(None, [user.first_name, user.last_name]))
                    username = user.username
                    is_member = isinstance(user.participant, ChannelParticipant) or isinstance(user.participant, ChatParticipant)

                    # Check if the user exists in the user_data set or has no last_activity
                    if user_id in user_data_set:
                        matching_entry = next(entry for entry in user_data if entry['user_id'] == user_id)
                        last_activity = matching_entry['last_activity']
                    else:
                        last_activity = None

                    # Convert last_activity to datetime if it's not None
                    last_activity_datetime = datetime.strptime(last_activity, '%Y-%m-%d %H:%M:%S.%f') if last_activity else None
                    
                    immune=False

                    # If the user is not a member (e.g. they are an admin), they are immune from kick
                    if not is_member:
                        immune=True

                    # If the user has a last_activity, and it is after the cutoff date, they are immune from kick
                    if last_activity_datetime is not None and  cutoff_date < last_activity_datetime:
                        immune = True    

                    if username and 'shinanygans' in username:
                        immune = True

                    # If whitelisted, immune from kick  
                    if user_id in whitelist_set:
                        immune=True  

                    if not immune:
                        users_to_ban.append((user_id, last_activity)) 
                        banned_name_lookup[user_id] = user_name
            else:
                for user_id, last_activity in tqdm(user_data, desc="Assembling Banned List", unit=" user"):
                    if user_id not in admin_ids:
                        users_to_ban.append((user_id, last_activity)) 
            return users_to_ban, banned_name_lookup
        
        except RetryAfter as e:
                    wait_seconds = e.retry_after
                    logging.warning(f"Got a RetryAfter error. Waiting for {wait_seconds} seconds...")
                    await asyncio.sleep(wait_seconds)
                    rt += 1
                    if rt == max_retries:
                        logging.warning(f"Max retry limit reached. Room member list not created.")
                        raise e
                
        except Exception as e:
                # Handle exceptions as needed
                logging.exception(f"An error occurred while gathering users to ban: {e}")
                raise e
                

# Function to kick inactive users
async def kick_inactive_users(update: Update, context: CallbackContext, pretend=False, ban=False, quiet=False):
    global kick_started

    if not update.message or kick_started == True:
        logging.warning("Kick already started. Abandoning.")
        return

    kick_started = True
    try:
        issuer_user_id = update.effective_user.id
        issuer_user_name = update._effective_user.full_name
        issuer_chat_id = update.message.chat_id
        issuer_chat_name = update.effective_chat.title
        issuer_chat_type = update.effective_chat.type
        pretend_str = "PRETEND " if pretend == True else ""

        try:
            admins = await update.effective_chat.get_administrators()
            admin_ids = [admin.user.id for admin in admins]
        except (BadRequest, BadRequestError, Forbidden, ChannelPrivateError) as e:
            logging.warning(f"Bad Request/Forbidden in chat_status() - Bot no longer in group.")
            return

        if issuer_user_id not in admin_ids:
            if not quiet:
                await context.bot.send_message(chat_id=issuer_chat_id, text="You are not an admin in this channel.")
            kick_started = False
            return

        logging.warning(f"\n\nHEADS UP! A {pretend_str if pretend else 'LIVE '}inactivity purge has been started in {issuer_chat_name} ({issuer_chat_id})\n\n")

        cutoff_date, readable_string_of_duration = calculate_cutoff_date(context.args[0])
        #logging.warning(f"Requested duration is {readable_string_of_duration}. Cutoff date is {cutoff_date}.\n")

        with open('kick.log', 'w') as log_file:
            log_file.write(f"\n\nA {pretend_str}kick has been started in {issuer_chat_name} ({issuer_chat_id}) at {datetime.utcnow().strftime('%d %B, %Y - %H:%M:%S')} UTC by {issuer_user_name}.\n")
            log_file.write(f"Requested duration is {readable_string_of_duration}. Cutoff date is {cutoff_date.strftime('%d %B, %Y - %H:%M:%S')}.\n\n")
            prefix = "BANNED USERS:\n" if ban else "KICKED USERS:\n"
            log_file.write(prefix)

    except (IndexError, ValueError):
        if not quiet:
            await context.bot.send_message(chat_id=issuer_chat_id, text="Invalid command format. Use /inactivekick <time> (e.g., /inactivekick 1d).")
        logging.error(f"An error occurred in kick_inactive_users(), probably due to an invalid time argument.")
        kick_started = False
        return
    
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
        logging.exception(f"An error occurred in kick_inactive_users() during the argument formatting process: {e}")
        kick_started = False
        return
    
    try:
        if len(scanning_underway) > 0 and not quiet:
            await context.bot.send_message(chat_id=issuer_chat_id, text="Waiting for room scanning to complete...")
        while len(scanning_underway) > 0:
            await asyncio.sleep(1)
        asyncio.create_task(suspend_scanning()) 
        if not quiet:
            await context.bot.send_message(chat_id=issuer_chat_id, text=START_PURGE)
        users_to_ban, banned_name_lookup = await assemble_banned_list(issuer_chat_id, admin_ids, cutoff_date)
        # Count the ban list, and announce to the group.
        count_of_users_to_ban = len(users_to_ban)
        admin_message = f" The KickBot is about to purge {count_of_users_to_ban} users from {issuer_chat_name} who have not posted media in the last {readable_string_of_duration}."
        if not quiet:
            await context.bot.send_message(chat_id=issuer_chat_id, text=admin_message)
        else:
            try:
                await context.bot.send_message(chat_id=issuer_user_id, text=admin_message)
            except Forbidden as e:
                pass

    except (IndexError, ValueError) as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
        logging.error(f"An error occurred in kick_inactive_users(), while assembling the kick list. {e}")
        kick_started = False
        return
    
    # Define a shared variable to keep track of the total banned count
    total_banned_count = 0
    pbar = tqdm(total=len(users_to_ban), desc="KICKBOT: Kicking Users", unit=" user")

    # Split the lust of users to ban into sub-lists for asynchronous processing
    try:
        # Calculate the number of sub-lists based on set number of lists
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
            task = process_user_batch(user_batch, context, issuer_chat_id, issuer_chat_type, issuer_chat_name, pretend, ban, pbar)
            tasks.append(task)

        # Execute tasks concurrently using asyncio.gather
        banned_counts = await asyncio.gather(*tasks)

        # Sum up banned counts from all batches
        total_banned_count = sum(banned_counts)

        # Extract user_ids from the users_to_ban list
        user_ids = [user_info[0] for user_info in users_to_ban]

        # Create a tuple of (user_id, chat_id) for each user to be deleted
        delete_params = [(user_id, issuer_chat_id) for user_id in user_ids]

        if not pretend:
            deleted_kicks_from_user_activity(delete_params)
            batch_update_kicked(user_ids, issuer_chat_id)

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
        logging.exception(f"An error occurred in kick_inactive_users() during the ban process: {e}")
        kick_started = False

    try:   
        kicked_or_banned = "Banned" if ban else "Kicked"
        final_message = f"{kicked_or_banned} {total_banned_count} users for inactivity."
        if not quiet:
            await context.bot.send_message(chat_id=issuer_chat_id, text=final_message)

        # If the kick happened in the debug chat, report out the users who were kicked.
        if issuer_chat_id in DEBUG_CHATS and API_ID and API_HASH:
            text = "DEBUG: BANNED USERS:\n\n" if ban else "DEBUG: KICKED USERS:\n\n"
            for user in users_to_ban:
                text = text + f"{banned_name_lookup[user[0]]} - Last activity: {user[1]}\n"
            await context.bot.send_message(chat_id=issuer_chat_id, text=text)
        tqdm.close(pbar)
        kick_started = False

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(exc_type, exc_value, exc_traceback, update=update)
        logging.exception(f"An error occurred in kick_inactive_users() during the ban process: {e}")
        kick_started = False
    return


# ********* ASYNCIO TASK CREATION FUNCTIONS *********


# Command to begin the kick inactive users process. Starts a separate async event loop.
@authorized_admin_check
async def inactive_kick_loop(update: Update, context: CallbackContext):
    asyncio.create_task(kick_inactive_users(update, context, pretend=False, ban=False))
    return


# Command to begin the kick inactive users process. Starts a separate async event loop.
@authorized_admin_check
async def inactive_ban_loop(update: Update, context: CallbackContext):
    asyncio.create_task(kick_inactive_users(update, context, pretend=False, ban=True))
    return


# Command to simulate kick purge without really doing it. Starts a separate async event loop.
@authorized_admin_check
async def pretend_kick_loop(update: Update, context: CallbackContext):
    asyncio.create_task(kick_inactive_users(update, context, pretend=True, ban=False))


# Command to begin the kick inactive users process. Starts a separate async event loop.
@authorized_admin_check
async def quiet_kick_loop(update: Update, context: CallbackContext):
    asyncio.create_task(kick_inactive_users(update, context, pretend=False, ban=False, quiet=True))
    return


#Command to purch the database of data from chats that are no longer active
@authorized_admin_check
async def clean_database_loop(update: Update, context: CallbackContext):
    asyncio.create_task(clean_database(update, context))
    return


#Command to purch the database of data from chats that are no longer active
@authorized_admin_check
async def request_log_loop(update: Update, context: CallbackContext):
    asyncio.create_task(request_log(update, context))
    return


#Command to purch the database of data from chats that are no longer active
@authorized_chat_check
async def chat_status_loop(update: Update, context: CallbackContext):
    asyncio.create_task(chat_status(update, context))
    return


#Command to purch the database of data from chats that are no longer active
# @is_admin_of_authorized_chat_check
@authorized_admin_check
async def lookup_loop(update: Update, context: CallbackContext):
    asyncio.create_task(lookup(update, context))
    return


#Command to purch the database of data from chats that are no longer active
@authorized_chat_check
async def three_strikes_mode_loop(update: Update, context: CallbackContext):
    asyncio.create_task(three_strike_mode(update, context))
    return


#Command to purch the database of data from chats that are no longer active
@authorized_chat_check
async def ban_leavers_mode_loop(update: Update, context: CallbackContext):
    asyncio.create_task(ban_leavers_mode(update, context))
    return


#Command to purch the database of data from chats that are no longer active
@authorized_chat_check
async def handle_new_member_loop(update: Update, context: CallbackContext):
    asyncio.create_task(handle_new_member(update, context))
    return


#Command to purch the database of data from chats that are no longer active
@authorized_admin_check
async def whitelist_user_loop(update: Update, context: CallbackContext):
    asyncio.create_task(whitelist_user(update, context))
    return


#Command to purch the database of data from chats that are no longer active
@authorized_admin_check
async def dewhitelist_user_loop(update: Update, context: CallbackContext):
    asyncio.create_task(dewhitelist_user(update, context))
    return


#Command to purch the database of data from chats that are no longer active
@authorized_admin_check
async def show_whitelist_loop(update: Update, context: CallbackContext):
    asyncio.create_task(show_whitelist(update, context))
    return


#Command to purch the database of data from chats that are no longer active
@authorized_admin_check
async def show_wholeft_loop(update: Update, context: CallbackContext):
    asyncio.create_task(show_wholeft(update, context))
    return


#Command to purch the database of data from chats that are no longer active
# @is_admin_of_authorized_chat_check
@authorized_admin_check
async def unban_loop(update: Update, context: CallbackContext):
    asyncio.create_task(unban(update, context))
    return


@authorized_admin_check
async def set_backup_loop(update: Update, context: CallbackContext):
    asyncio.create_task(set_backup(update, context))
    return


@authorized_chat_check
async def handle_message_loop(update: Update, context: CallbackContext):
    asyncio.create_task(handle_message(update, context))
    return


# ********* STOP AND RESTART **********

async def stop_and_restart():
    """Gracefully stop the Updater and replace the current process with a new one"""
    await app.stop()
    os.execl(sys.executable, sys.executable, *sys.argv)

@authorized_admin_check
async def restart(update, context):
    try:
        if update.effective_chat.type != ChatType.PRIVATE:
            return
        if update.effective_user.id not in AUTHORIZED_ADMINS:
            return
        await update.message.reply_text('Bot is restarting...')
        asyncio.create_task(stop_and_restart())
    except Exception as e:
        logging.error(f"Failed to restart the bot: {e}")




# ********* MAIN *********


async def post_init(application: Application):
    await start_chat_member_tracking()  
    asyncio.create_task(cache_admins_on_startup())
    # asyncio.get_event_loop().set_debug(True)

async def cache_admins_on_startup():
    db_chats = list_chats_in_db()
    for db_chat in db_chats:
        await update_chat_admins_cache(db_chat)
    return

def main() -> None:
    """Run bot."""
    global kickbot
    global app

    # Create the Application and pass it your bot's token.
    application = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    application.add_handler(CommandHandler("inactivekick", inactive_kick_loop))
    application.add_handler(CommandHandler("pretendkick", pretend_kick_loop))
    application.add_handler(CommandHandler("quietkick", quiet_kick_loop))
    application.add_handler(CommandHandler("inactiveban", inactive_ban_loop))
    application.add_handler(CommandHandler("wl_add", whitelist_user_loop))
    application.add_handler(CommandHandler("wl", show_whitelist_loop))
    application.add_handler(CommandHandler("wl_del", dewhitelist_user_loop))
    application.add_handler(CommandHandler("3strikes", three_strikes_mode_loop)) 

    application.add_handler(CommandHandler("lurkinfo", lookup_loop))  

    application.add_handler(CommandHandler("log", request_log_loop))     
    application.add_handler(CommandHandler("gcstats", chat_status_loop)) 
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("cleandb", clean_database_loop)) 
    application.add_handler(CommandHandler("test", update_chat_members))
    application.add_handler(CommandHandler("track", start_chat_member_tracking))
    application.add_handler(CommandHandler("stop", stop_chat_member_tracking))
    application.add_handler(CommandHandler("wholeft", show_wholeft_loop))
    application.add_handler(CommandHandler("noleavers", ban_leavers_mode_loop))
    application.add_handler(CommandHandler("forgive", unban_loop))
    application.add_handler(CommandHandler("setbackup", set_backup_loop)) 
    application.add_handler(CommandHandler("restart", restart)) 

    application.add_handler(CallbackQueryHandler(button_click, pattern='^setbackup_.*'))

    application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_message_loop))
    application.add_handler(ChatMemberHandler(handle_new_member_loop, ChatMemberHandler.CHAT_MEMBER))
    application.add_error_handler(error)
    

    # Run the bot until the user presses Ctrl-C
    try:
        kickbot = application.bot
        app = application
        if API_ID and API_HASH:
            global telethon
            telethon = TelegramClient('memberlist_bot', API_ID, API_HASH).start(bot_token=BOT_TOKEN)

        application.run_polling(allowed_updates=Update.ALL_TYPES, close_loop=False)
  
        
    except Exception as e:
            print(e)
        
    finally:
        try:
            schedule.clear()
            if telethon.is_connected():
                telethon.disconnect()
        except Exception as e:
            print(e)



if __name__ == "__main__":
    main()