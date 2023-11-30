#! /usr/bin/python

"""
KickBot - Your Partner in Lurker-Slaughter
Authored by Shinanygans (shinanygans@proton.me)

This bot will gather data over time about which users are in your group and the last time they posted media.
You can command this bot to eject from your group those who have not posted in a given time span or have never posted.
"""

import logging
from logging.handlers import TimedRotatingFileHandler
import asyncio
import pytz

from config import (
    BOT_TOKEN,
    API_ID,
    API_HASH,
    DEBUG_CHATS,
    DEBUG_ADMIN_MESSAGE,
    DEBUG_UPDATE_MESSAGE,
    START_PURGE,
    HELP_MESSAGE,
    AUTHORIZED_ADMINS,
    NUM_BATCHES,
)
from db_utils import (
    initialize_db,
    insert_user_in_db,
    delete_user_from_db,
    list_chats_in_db,
    del_chats_from_db,
    get_user_activity,
    update_user_activity,
    deleted_kicks_from_user_activity,
    lookup_user_in_kick_db,
    lookup_kick_count_in_kick_db,
    insert_kicked_user_in_kick_db,
    get_whitelist,
    get_whitelist_from_private,
    is_chat_authorized,
    insert_authorized_chat,
    delete_authorized_chat,
    get_three_strikes,
    update_three_strikes
)

from datetime import datetime, timedelta
from functools import wraps
from tqdm import tqdm
from telethon.sync import TelegramClient
from telethon.tl.types import (
    ChannelParticipantAdmin, 
    ChannelParticipant, 
    ChannelParticipantCreator,
    ChatParticipant, 
    ChatParticipantAdmin, 
    ChatParticipantCreator
)
from telegram import Update, ChatMember, Message
from telegram.constants import ChatType, ParseMode
from telegram.error import RetryAfter, Forbidden
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
when = 'midnight'  # Rotate logs at midnight (other options include 'H', 'D', 'W0' - 'W6', 'MIDNIGHT', or a custom time)
interval = 1  # Rotate daily
backup_count = 7  # Retain logs for 7 days
log_handler = TimedRotatingFileHandler('app.log', when=when, interval=interval, backupCount=backup_count)
log_handler.suffix = "%Y-%m-%d"  # Suffix for log files (e.g., 'my_log.log.2023-10-22')

logging.basicConfig(
    level=logging.INFO,
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
max_retries = 3
authorized_chats = set()
utc_timezone = pytz.utc
kick_started = False
admin_participant_types = (ChannelParticipantAdmin, ChannelParticipantCreator, ChatParticipantAdmin, ChatParticipantCreator)

# Initialize the SQLite database
initialize_db()


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
                await asyncio.sleep(wait_seconds)
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
        if is_chat_authorized(chat_id):
            return await handler_function(update, context, *args, **kwargs)
        
        # If there are admins and this chat is not pre-approved, get the chat admins and see if there's a match.
        rt = 0
        while rt < max_retries:
            try:      
                admins = await update.effective_chat.get_administrators()
            
            except Forbidden as e:
                delete_authorized_chat(chat_id)
                logging.error(f"An access error occured in authorized_chat_check() for {chat_id} - {chat_title}: Deauthorizing")
            
            except Exception:
                return

            try:
                admin_ids = {admin.user.id for admin in admins}
                set_admin_ids = set(admin_ids)
                set_auth_admins = set(AUTHORIZED_ADMINS)
                if set_auth_admins.intersection(set_admin_ids):
                    insert_authorized_chat(chat_id)
                    return await handler_function(update, context, *args, **kwargs)
                else:
                    return 
            except RetryAfter as e:
                wait_seconds = e.retry_after
                logging.warning(f"Got a RetryAfter error. Waiting for {wait_seconds} seconds...")
                await asyncio.sleep(wait_seconds)
                rt += 1
                if rt == max_retries:
                    logging.warning(f"Max retry limit reached. Message not sent.")
                    return
            except Exception as e:
                logging.warning(f"An error occured in authorized_chat_check(): {e}")
                return
    return wrapper


# Registered error handler for the app
async def error(update, context):
    err = f"Update: {update}\nError: {context.error}"
    logging.error(err, exc_info=context.error)
    return


async def delete_message_after_delay(context: CallbackContext, message:Message):
    await asyncio.sleep(3)  # Wait for 3 seconds
    await context.bot.delete_message(chat_id=message.chat_id, message_id=message.message_id)
    return


# Send the help message to the user who triggered the /help command.
@authorized_admin_check
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
            await insert_user_in_db(user_id, chat_id, "user_activity")
        else:
            logging.info(f"User ID {user_id} '{user_name}' entered chat {chat_id} '{chat_name}'. Admin. Ignoring.")
    if (not is_member and was_member) and user_id != context.bot.id and not kick_started:
        if not member.new_chat_member.status in ["administrator", "creator"]:
            delete_user_from_db(user_id, chat_id, "user_activity")
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
            await insert_user_in_db(user_id, chat_id, "user_activity")

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


@authorized_chat_check
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
        logging.error(f"Error in the three_strike_mode() function: {e}")
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


@authorized_chat_check
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

        try:
            lookup_id = int(context.args[0])
        except:
            lookup_id = context.args[0]

        user = await telethon.get_entity(lookup_id)
        user_id = user.id
        user_name = user.first_name + (" " + user.last_name if user.last_name else "")
        await insert_user_in_db(user_id, chat_id, "whitelist")
        logging.warning(f"{user_name} has been whitelisted in {chat_name}")
        if chat_id in DEBUG_CHATS:
            await context.bot.send_message(chat_id=chat_id, text=f"DEBUG: {user_name} has been whitelisted in {chat_name}")
        await context.bot.send_message(chat_id=issuer_user_id, text=f"{user_name} has been whitelisted in {chat_name}")
        
    except (IndexError, ValueError):
        await context.bot.send_message(chat_id=issuer_user_id, text="Invalid command format. Use /whitelist <user id or @ name> (e.g., /whitelist @username).")
        logging.error(f"An error occurred in whitelist_user(), probably due to an invalid time argument.")
        
    except Exception as e:
        logging.error(f"Error in the whitelisting process: {e}")
    return


@authorized_admin_check
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

        user = await telethon.get_entity(context.args[0])
        user_id = user.id
        user_name = user.first_name + (" " + user.last_name if user.last_name else "")
        delete_user_from_db(user_id, chat_id, "whitelist")
        logging.warning(f"{user_name} has been de-whitelisted from {chat_name}")
        if chat_id in DEBUG_CHATS:
            await context.bot.send_message(chat_id=chat_id, text=f"DEBUG: {user_name} has been de-whitelisted from {chat_name}")
        await context.bot.send_message(chat_id=issuer_user_id, text=f"{user_name} has been de-whitelisted from {chat_name}")
        

    except (IndexError, ValueError):
        await context.bot.send_message(chat_id=issuer_user_id, text="Invalid command format. Use /dewhitelist <user id or @ name> (e.g., /dewhitelist @username).")
        logging.error(f"An error occurred in dewhitelist_user(), probably due to an invalid time argument.")
        
    except Exception as e:
        logging.error(f"Error in the dewhitelisting process: {e}")
    return


@authorized_admin_check
async def show_whitelist(update: Update, context: CallbackContext):
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
            whitelist_data = get_whitelist(chat_id)
        else:
            whitelist_data = get_whitelist_from_private(chat_id)

        
        whitelist_message=f"WHITELISTED USERS\n\n"

        # Group users by channel_id
        users_by_channel = {}
        for user_id, channel_id in whitelist_data:
            if channel_id not in users_by_channel:
                users_by_channel[channel_id] = []
            users_by_channel[channel_id].append(user_id)

        # Print the results
        for channel_id, user_ids in users_by_channel.items():
            chat_entity = await telethon.get_entity(channel_id)
            title=chat_entity.title
            whitelist_message += f"{title.upper()}\n"
            for user_id in user_ids:
                user_entity = await telethon.get_entity(user_id)
                user_name = user_entity.first_name + (" " + user_entity.last_name if user_entity.last_name else "")
                whitelist_message += f"{user_name} ({user_id})\n"
            whitelist_message += "\n"
        if chat_id in DEBUG_CHATS:
            await context.bot.send_message(chat_id=chat_id, text=f"DEBUG: {whitelist_message}")
        await context.bot.send_message(chat_id=issuer_user_id, text=whitelist_message)
    except Exception as e:
        logging.error(f"Error in the whitelist printing process: {e}")
    return


@authorized_chat_check
async def chat_status(update: Update, context: CallbackContext):
    chat_id = update.message.chat_id
    chat_name = update.effective_chat.title
    issuer_user_id = update.effective_user.id
    chat_type = update.effective_chat.type
    total_members = 0
    posted_in_last_12_hours = 0
    not_posted = 0

    
    if chat_type == ChatType.PRIVATE:
        message = await context.bot.send_message(
            chat_id=chat_id,
            text="<i style='color:#808080;'> This command only works in the chat where you want to look up current status The responses will come here..</i>",
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

        if API_ID and API_HASH:
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
        time_window_lurk_rate = round((total_members - posted_in_last_12_hours) / total_members * 100, 1)
        total_lurk_rate = round((not_posted) / total_members * 100, 1)
        lurker_message = f"KICKBOT GROUP CHAT STATS FOR {chat_name}.\n\n"
        lurker_message += f"❌ 3 STRIKES MODE is {'on' if three_strikes_mode[0]==1 else 'off'}.\n\n"
        lurker_message += f"👤 There are {total_members} non-admin members in the group.\n\n"
        lurker_message += f"⏱ {total_members - posted_in_last_12_hours} have NOT posted in the last {readable_string_of_duration} ({time_window_lurk_rate}% recent lurker).\n\n"
        lurker_message += f"💥 {not_posted} users have not posted at all. ({total_lurk_rate}% total lurker)"
        if chat_id in DEBUG_CHATS:
            await context.bot.send_message(chat_id=chat_id, text=f"DEBUG: {lurker_message}")
        await context.bot.send_message(chat_id=issuer_user_id, text=lurker_message)
    except (IndexError, ValueError):
        await context.bot.send_message(chat_id=chat_id, text="Invalid command format. Use /gcstats <time> (e.g., /gcstats 1d).")
        logging.error(f"An error occurred in kick_inactive_users(), probably due to an invalid time argument.")
    except Exception as e:
        logging.error(f"Error assembling chat stats: {e}")
    return


@authorized_admin_check
async def lookup(update: Update, context: CallbackContext):
    chat_id = update.message.chat_id
    chat_name = update.effective_chat.title
    issuer_user_id = update.effective_user.id
    chat_type = update.effective_chat.type

    id_or_username = context.args[0]
    try:
        id_or_username = int(id_or_username)
    except:
        pass   
    try:
        user = await telethon.get_entity(id_or_username)
        user_id = user.id
        chat_member = await context.bot.get_chat_member(chat_id, user_id)
    except Exception as e:
        logging.error(f" Error getting entity and chat member information during lookup: {e}")
        chat_member = None

    kicked_user_status = None
    if chat_member:
        if chat_member.status == ChatMember.ADMINISTRATOR:
            kicked_user_status = "Admin"
        elif chat_member.status == ChatMember.BANNED:
            kicked_user_status = "Banned"
        elif chat_member.status == ChatMember.LEFT:
            kicked_user_status = "Left"
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
    
    try:
        # Search for the user in the list of participants
        async for participant in telethon.iter_participants(chat_id):
            if participant.id == user_id:
                kicked_user_is_bot = participant.bot
                kicked_user_is_fake = participant.fake
                kicked_user_is_premium = participant.premium
                kicked_user_is_restricted = participant.restricted
                kicked_user_restriction_reason = participant.restriction_reason if participant.restriction_reason else ""
                kicked_user_is_scam = participant.scam
                kicked_user_is_verified = participant.verified
                kicked_user_username = participant.username
                kicked_user_first_joined = participant.participant.date
                break
        
        if chat_type == ChatType.PRIVATE:
            message = await context.bot.send_message(
                chat_id=chat_id,
                text="<i style='color:#808080;'> This command only works in the chat where you want to look up the user's kick history. The responses will come here..</i>",
                parse_mode=ParseMode.HTML
            )       
            # Schedule a task to delete the message after 5 seconds
            asyncio.create_task(delete_message_after_delay(context, message))
            return
        else:
            message = await context.bot.send_message(
                chat_id=chat_id,
                text="<i style='color:#808080;'> Response will be sent privately.</i>",
                parse_mode=ParseMode.HTML
            )
            asyncio.create_task(delete_message_after_delay(context, message))
        
        kicked_user_data = lookup_user_in_kick_db(user_id, chat_id)

        if not kicked_user_data:
            kicked_user_message = "User not found in kickbot database."
        else:
            kicked_user_id = kicked_user_data[0]
            kicked_user = await telethon.get_entity(kicked_user_id)
            kicked_user_name = f"{kicked_user.first_name}{' ' + kicked_user.last_name if kicked_user.last_name else ''}"
            kicked_user_number_kicks = kicked_user_data[2]
            if kicked_user_data[3]:
                kicked_user_last_posted = datetime.strptime(kicked_user_data[3], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=utc_timezone).strftime('%d %B, %Y - %H:%M:%S')
            else:
                kicked_user_last_posted = "Never"
            kicked_user_last_kicked = datetime.strptime(kicked_user_data[4], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=utc_timezone).strftime('%d %B, %Y - %H:%M:%S')
            kicked_user_first_joined = kicked_user_first_joined.strftime('%d %B, %Y - %H:%M:%S')

            kicked_user_message="KICKED USER RECORD\n"
            kicked_user_message+=f"NAME: {kicked_user_name}\n"
            kicked_user_message+=f"ID: {user_id}\n"
            kicked_user_message+=f"USERNAME: @{kicked_user_username}\n"
            kicked_user_message+=f"KICKS from {chat_name}: {kicked_user_number_kicks}\n"
            kicked_user_message+=f"FIRST JOINED: {kicked_user_first_joined}\n"
            kicked_user_message+=f"LAST POST: {kicked_user_last_posted}\n"
            kicked_user_message+=f"LAST KICKED: {kicked_user_last_kicked}\n"
            kicked_user_message+=f"STATUS: {kicked_user_status}\n\n"

            kicked_user_message+="OTHER INFO\n"
            kicked_user_message+=f"PREMIUM: {kicked_user_is_premium}\n"
            kicked_user_message+=f"VERIFIED: {kicked_user_is_verified}\n"
            kicked_user_message+=f"IS BOT: {kicked_user_is_bot}\n"
            kicked_user_message+=f"IS FAKE: {kicked_user_is_fake}\n"
            kicked_user_message+=f"IS SCAM: {kicked_user_is_scam}\n"
            kicked_user_message+=f"RESTRICTED: {kicked_user_is_restricted}\n"
            kicked_user_message+=f"{'RESTR REASON: ' if kicked_user_is_restricted else ''}{kicked_user_restriction_reason}\n"

        if chat_id in DEBUG_CHATS:
            await context.bot.send_message(chat_id=chat_id, text=f"DEBUG: {kicked_user_message}")      
        await context.bot.send_message(chat_id=issuer_user_id, text=kicked_user_message)
    except Exception as e:
        logging.error(f"Error in the processing user lookup: {e}")
    return


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



#Command to purch the database of data from chats that are no longer active
@authorized_admin_check
async def clean_database_loop(update: Update, context: CallbackContext):
    asyncio.create_task(clean_database(update, context))
    return


# Function to remove data from chats that are no longer active
async def clean_database(update, context):
    chat_id = update.effective_message.chat_id
    issuer_chat_id = update.effective_chat.id
    chat_type = update.effective_chat.type
    if chat_type != ChatType.PRIVATE:
        await context.bot.send_message(chat_id=chat_id, text="This command only works in a private chat with the bot")
        return
    active_chats = []
    inactive_chats = []
    try:
        chat_ids_in_database = list_chats_in_db()
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

        del_chats_from_db(inactive_chats)

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
async def process_user_batch(batch, context, issuer_chat_id, issuer_chat_type, issuer_chat_name, pretend, ban, pbar):
    
    # Kick the inactive users and count the number of users kicked
    banned_count = 0
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

                if not pretend:
                    # Increment the user's kick count in the database
                    insert_kicked_user_in_kick_db(user_id, issuer_chat_id, last_activity_str)
                
                    # If supergroup, use 'unban' for kick. Otherwise just ban.               
                    if (issuer_chat_type == ChatType.SUPERGROUP or issuer_chat_type == ChatType.CHANNEL) and not ban and not three_strikes_ban:
                        await context.bot.unban_chat_member(issuer_chat_id, user_id)
                    else:
                        await context.bot.ban_chat_member(issuer_chat_id, user_id)
                    
                banned_count += 1
                
                # Update the progress bar
                pbar.update(1)    

                logging.info(f"User ID {user_id} {'BANNED' if three_strikes_ban else 'KICKED'} from {issuer_chat_id} '{issuer_chat_name}' (kick # {kick_count+1}).")
                
                with open('kick.log', 'a') as log_file:
                    log_file.write(f"User ID: {user_id}, Last Activity: {last_activity_readable}, Kick # {kick_count+1}{' (BANNED)' if three_strikes_ban else ''}.\n")
                break
            except RetryAfter as e:
                wait_seconds = e.retry_after
                logging.warning(f"Got a RetryAfter error. Waiting for {wait_seconds} seconds...")
                await asyncio.sleep(wait_seconds)
                rt += 1
                if rt == max_retries:
                    logging.warning(f"Max retry limit reached. Error: {e}. User {user_id} not kicked.")
                    break
            except Exception as e:
                logging.warning(f"Got an error while processing: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
                rt += 1
                if rt == max_retries:
                    logging.warning(f"Max retry limit reached. Error: {e}. User {user_id} not kicked.")
                    break
    return banned_count


async def assemble_banned_list(chat_id, admin_ids, cutoff_date) -> [tuple]:
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
                async for user in telethon.iter_participants(chat_id):
                    user_id = user.id
                    user_name = f"{user.first_name}{' ' + user.last_name if user.last_name else ''}"
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
async def kick_inactive_users(update: Update, context: CallbackContext, pretend=False, ban=False):
    global kick_started

    if not update.message or kick_started == True:
        return

    kick_started = True
    try:
        issuer_user_id = update.effective_user.id
        issuer_user_name = update._effective_user.full_name
        issuer_chat_id = update.message.chat_id
        issuer_chat_name = update.effective_chat.title
        issuer_chat_type = update.effective_chat.type
        pretend_str = "PRETEND " if pretend == True else ""
        admins = await update.effective_chat.get_administrators()
        admin_ids = {admin.user.id for admin in admins}

        if issuer_user_id not in admin_ids:
            await context.bot.send_message(chat_id=issuer_chat_id, text="You are not an admin in this channel.")
            return

        logging.warning(f"\n\nHEADS UP! A {pretend_str if pretend else 'LIVE '}inactivity purge has been started in {issuer_chat_name} ({issuer_chat_id})\n\n")

        cutoff_date, readable_string_of_duration = calculate_cutoff_date(context.args[0])
        logging.warning(f"Requested duration is {readable_string_of_duration}. Cutoff date is {cutoff_date}.\n")

        with open('kick.log', 'w') as log_file:
            log_file.write(f"\n\nA {pretend_str}kick has been started in {issuer_chat_name} ({issuer_chat_id}) at {datetime.utcnow().strftime('%d %B, %Y - %H:%M:%S')} UTC by {issuer_user_name}.\n")
            log_file.write(f"Requested duration is {readable_string_of_duration}. Cutoff date is {cutoff_date.strftime('%d %B, %Y - %H:%M:%S')}.\n\n")
            prefix = "BANNED USERS:\n" if ban else "KICKED USERS:\n"
            log_file.write(prefix)
    except (IndexError, ValueError):
        await context.bot.send_message(chat_id=issuer_chat_id, text="Invalid command format. Use /inactivekick <time> (e.g., /inactivekick 1d).")
        logging.error(f"An error occurred in kick_inactive_users(), probably due to an invalid time argument.")
        kick_started = False
        return
    
    try:
        await context.bot.send_message(chat_id=issuer_chat_id, text=START_PURGE)
        users_to_ban, banned_name_lookup = await assemble_banned_list(issuer_chat_id, admin_ids, cutoff_date)
        # Count the ban list, and announce to the group.
        count_of_users_to_ban = len(users_to_ban)
        admin_message = f" The KickBot is about to purge {count_of_users_to_ban} users from {issuer_chat_name} who have not posted media in the last {readable_string_of_duration}."
        await context.bot.send_message(chat_id=issuer_chat_id, text=admin_message)

    except (IndexError, ValueError) as e:
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

    except Exception as e:
        logging.exception(f"An error occurred in kick_inactive_users() during the ban process: {e}")
    kicked_or_banned = "Banned" if ban else "Kicked"
    final_message = f"{kicked_or_banned} {total_banned_count} users for inactivity."
    await context.bot.send_message(chat_id=issuer_chat_id, text=final_message)

    # If the kick happened in the debug chat, report out the users who were kicked.
    if issuer_chat_id in DEBUG_CHATS and API_ID and API_HASH:
        text = "DEBUG: BANNED USERS:\n\n" if ban else "DEBUG: KICKED USERS:\n\n"
        for user in users_to_ban:
            text = text + f"{banned_name_lookup[user[0]]} - Last activity: {user[1]}\n"
        await context.bot.send_message(chat_id=issuer_chat_id, text=text)

    kick_started = False
    return


def main() -> None:
    """Run bot."""
    # Create the Application and pass it your bot's token.
    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("inactivekick", inactive_kick_loop))
    application.add_handler(CommandHandler("pretendkick", pretend_kick_loop))
    application.add_handler(CommandHandler("inactiveban", inactive_ban_loop))
    application.add_handler(CommandHandler("wl_add", whitelist_user))
    application.add_handler(CommandHandler("wl", show_whitelist))
    application.add_handler(CommandHandler("wl_del", dewhitelist_user))
    application.add_handler(CommandHandler("3strikes", three_strike_mode))
    application.add_handler(CommandHandler("lurkinfo", lookup))  
    application.add_handler(CommandHandler("gcstats", chat_status))  
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("cleandb", clean_database_loop))
    application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_message))
    application.add_handler(ChatMemberHandler(handle_new_member, ChatMemberHandler.CHAT_MEMBER))
    application.add_error_handler(error)

    # Run the bot until the user presses Ctrl-C
    if API_ID and API_HASH:
        global telethon
        telethon = TelegramClient('memberlist_bot', API_ID, API_HASH).start(bot_token=BOT_TOKEN)
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    finally:
        if telethon:
            telethon.disconnect()



if __name__ == "__main__":
    main()