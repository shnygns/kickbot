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
import re

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
    lookup_group_member,
    lookup_active_group_member,
    lookup_admin_ids,
    lookup_user_in_kick_db,
    lookup_kick_count_in_kick_db,
    insert_kicked_user_in_kick_db,
    get_whitelist,
    get_whitelist_from_private,
    is_chat_authorized,
    insert_authorized_chat,
    delete_authorized_chat,
    get_three_strikes,
    update_three_strikes,
    get_ban_leavers_status,
    update_ban_leavers_status,
    insert_chat_member,
    batch_update_db,
    batch_update_joined,
    batch_update_left,
    batch_update_kicked,
    batch_update_banned,
    list_member_ids_in_db,
    list_kicked_users_in_db,
    list_banned_users_in_db,
    keyword_search_from_db,
    return_blacklist,
    get_wholeft,
    get_wholeft_from_private,
    update_left_groups,
    update_or_insert_chat_member,
    insert_kicked_user_in_blacklist,
    insert_userlist_into_blacklist,
    remove_unbanned_user_from_blacklist,
    lookup_user_in_blacklist,
    batch_insert_or_update_chat_member,
    str_to_timedelta,
    format_timedelta,
    insert_obligation_chat,
    delete_obligation_chat,
    lookup_obligation_chat
)
import time
from datetime import datetime, timedelta
from functools import wraps
from tqdm import tqdm
import aioschedule as schedule
from telethon.sync import TelegramClient
from telethon.errors import TakeoutInitDelayError, ChannelPrivateError, BadRequestError, UserAdminInvalidError
from telethon.tl.types import (
    ChannelParticipantAdmin, 
    ChannelParticipantsAdmins,
    ChannelParticipant, 
    ChannelParticipantCreator,
    ChatParticipant, 
    ChatParticipantAdmin, 
    ChatParticipantCreator,
    ChannelParticipantsKicked
)
from telegram import Update, ChatMember, Message, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatType, ParseMode
from telegram.error import RetryAfter, Forbidden
from telegram.ext import (
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
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
tracking_chat_members = True
max_retries = 3
authorized_chats = set()
utc_timezone = pytz.utc
kick_started = False
scanning_underway = []
let_leave_without_banning = set()
admin_participant_types = (ChannelParticipantAdmin, ChannelParticipantCreator, ChatParticipantAdmin, ChatParticipantCreator)

# Initialize the SQLite database
initialize_db()


# ********* WRAPPERS *********


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
                exc_type, exc_value, exc_traceback = sys.exc_info()
                await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
                logging.warning(f"Got a RetryAfter error. Waiting for {wait_seconds} seconds...")
                await asyncio.sleep(wait_seconds)
                rt += 1
                if rt == max_retries:
                    logging.warning(f"Max retry limit reached. Message not sent.")
                    raise e
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
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
                exc_type, exc_value, exc_traceback = sys.exc_info()
                await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
                logging.warning(f"Got a RetryAfter error. Waiting for {wait_seconds} seconds...")
                await asyncio.sleep(wait_seconds)
                rt += 1
                if rt == max_retries:
                    logging.warning(f"Max retry limit reached. Message not sent.")
                    return
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
                logging.warning(f"An error occured in authorized_chat_check(): {e}")
                return
    return wrapper


# Custom decorator function to check if a chat is authorized (use for room data collection).
def is_admin_of_authorized_chat_check(handler_function):
    @wraps(handler_function)
    async def wrapper(update: Update, context: CallbackContext, *args, **kwargs):

        # If the chat is private, it is authorized. Check passed.
        chat_type = update.effective_chat.type
        if chat_type == ChatType.PRIVATE:
            return await handler_function(update, context, *args, **kwargs)
        
        # If no authorized admins are in the list, the bot is open. Check passed.
        if not AUTHORIZED_ADMINS:
            return await handler_function(update, context, *args, **kwargs)
        
        # If the command issuer is a named admin on the bot, they are authorized. Check passed.
        user_id = update.effective_user.id
        if user_id in AUTHORIZED_ADMINS:
            return await handler_function(update, context)
        
        # Check if the chat is authorized based on the SQLite table
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        chat_title = update.effective_chat.title
        
        # Get the chat admins and see if there's a match to the issuing user id.
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
                    if user_id in admin_ids:
                        return await handler_function(update, context, *args, **kwargs)                  
                    else:
                        return 
                else:
                    return 
            except RetryAfter as e:
                wait_seconds = e.retry_after
                exc_type, exc_value, exc_traceback = sys.exc_info()
                await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
                logging.warning(f"Got a RetryAfter error. Waiting for {wait_seconds} seconds...")
                await asyncio.sleep(wait_seconds)
                rt += 1
                if rt == max_retries:
                    logging.warning(f"Max retry limit reached. Message not sent.")
                    return
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
                logging.warning(f"An error occured in authorized_chat_check(): {e}")
                return
    return wrapper


# ********* UTILITIES *********


# Registered error handler for the app
async def error(update, context):
    err = f"Update: {update}\nError: {context.error}"
    logging.error(err, exc_info=context.error)
    return


async def suspend_scanning():
    if not tracking_chat_members:
        return
    global scanning_underway
    logging.warning("Suspending timed chat tracking.")
    schedule.clear()
    while kick_started:
        await asyncio.sleep(1)
    schedule.every(2).minutes.do(update_chat_members)

    logging.warning("Timed chat tracking re-started.")

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


async def debug_to_chat(update: Update, context: CallbackContext, exc_type, exc_value, exc_traceback):
    chat_title = update.effective_chat.title if update else "N/A"
    bot_name = context.bot.name if context else "KICKBOT"
    
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
    await asyncio.sleep(3)  # Wait for 3 seconds
    await context.bot.delete_message(chat_id=message.chat_id, message_id=message.message_id)
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


async def process_chat_member_updates(chat_id, update: Update=None, context: CallbackContext=None):
    try:
        global telethon
        if not telethon.is_connected():
            telethon.connect()
        global scanning_underway
        scanning_underway.append(chat_id)
        chat = await telethon.get_entity(chat_id)
        # admins = await telethon.get_participants(chat, filter=ChannelParticipantsAdmins)
        admin_ids = lookup_admin_ids(chat_id)
        # admin_ids = set([admin.id for admin in admins])
        shin_ids = set(keyword_search_from_db('shinanygans'))
        # Step 1: Fetch the list of user_ids from the chat_member table for the given chat_id
        member_ids_in_db = set(list_member_ids_in_db(chat_id)) # Users who are currently 'Member' or 'Admin' of 'Creator' status
        banned_ids_in_db = set(list_banned_users_in_db(chat_id)) # Users who are currently 'Banned' status
        is_megagroup = hasattr(chat, 'megagroup') and chat.megagroup
        is_channel = hasattr(chat, 'broadcast') and chat.broadcast

        # Step 2: Initialize set for tracking current participant user_ids
        participant_user_ids = set()
        participant_dict = {}

        # Step 3: Iterate through the participants returned by iter_participants
        logging.warning(f"Cataloging members of {chat_id}")
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
            else:
                user_status = 'Not Available'

            # Step 4: Update the chat_member data
            batch_insert_parameters.append((
                user_id,
                chat_id,
                f"{participant.first_name}{' ' + participant.last_name if participant.last_name else ''}",
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
            participant_user_ids.add(user_id)
            participant_dict[user_id] = user_status
        batch_insert_or_update_chat_member(batch_insert_parameters)

        # Step 5: Identify users that have left or joined, or who were previously banned
        left_user_ids = member_ids_in_db - participant_user_ids # Members or Admins in DB minus current Members/Admins = Left since last scan
        joined_user_ids = participant_user_ids - member_ids_in_db # Current Members/Admins minus Members/Admins in DB = Net new + rejoins and unbanned
        unbanned_user_ids = joined_user_ids.intersection(banned_ids_in_db) # Currently banned in the DB but rejoined the group
        user_ids_to_ban = left_user_ids - admin_ids - let_leave_without_banning - shin_ids
        results = {'chat_id': chat_id, 'joined_user_ids': joined_user_ids}
        # Step 5.5: If obligations is on, joiners who meet certain conditions must be analyzed and possibly kicked.
        obligation_chat_id = lookup_obligation_chat(chat_id)
        supergroup = is_megagroup or is_channel
        
        logging.warning(f"SCAN: {chat_id} -- {chat.title} Kickbot found {len(joined_user_ids)} users who showed in the scan of {chat_id} but were not listed as members in the DB.")
        if supergroup and len(joined_user_ids) > 0:
            obligation_chat = None
            if obligation_chat_id:
                obligation_chat = await telethon.get_entity(obligation_chat_id) 
            #    start_time = time.time()
            #    logging.warning(f"Getting obligation chat members from {obligation_chat.title}")
            #    obligation_chat_members = await telethon.get_participants(obligation_chat_id) if obligation_chat_id else None
            #    end_time = time.time()
            #    logging.warning(f"Obligation list-building complete. Elapsed time: {end_time - start_time} sec.")

            for joined_user_id in joined_user_ids:
                #shinfree = joined_user_id not in shin_ids
                #is_member_of_obligation_chat = False
                joined_member = await telethon.get_entity(joined_user_id)
                joined_user_name = (f"{joined_member.first_name if joined_member.first_name else ''} {joined_member.last_name if joined_member.last_name else ''}")
                #joined_username = joined_member.username if joined_member.username else None
                logging.warning(f"SCAN: {chat_id} -- {joined_user_name} - {joined_member.username}"
                                f"{' -ADMIN' if joined_user_id in AUTHORIZED_ADMINS or joined_user_id in admin_ids else ''} "
                                f"joining {chat.title}. Prior Status: {member_record_dict.get(user_id) if member_record_dict and member_record_dict.get(user_id) else 'N/A'} - New Status: {participant_dict[joined_user_id]} "
                                f"{'(OBLIGATION: '+ obligation_chat.title + ')' if obligation_chat else ''}"
                                )
                #if obligation_chat_id:
                #    start_time = time.time()
                #    async for obligation_participant in telethon.iter_participants(obligation_chat_id, search=joined_user_name):
                #        if obligation_participant.id == joined_user_id:
                #            is_member_of_obligation_chat == True
                #            break
                #    end_time = time.time()
                #    logging.warning(f"SCAN: {chat_id} -- {joined_user_name} {'WAS' if is_member_of_obligation_chat else 'WAS NOT'} found in {obligation_chat.title} via full name search. Elapsed time: {end_time - start_time} sec.")

                #    if not is_member_of_obligation_chat and joined_username:
                #        start_time = time.time()
                #        async for obligation_participant in telethon.iter_participants(obligation_chat_id, search=joined_user_name):
                #            if obligation_participant.id == joined_user_id:
                #                is_member_of_obligation_chat == True
                #                break
                #        end_time = time.time()
                #        logging.warning(f"SCAN: {chat_id} -- {joined_user_name} {'WAS' if is_member_of_obligation_chat else 'WAS NOT'} found in {obligation_chat.title} via full name search. Elapsed time: {end_time - start_time} sec.")

                #    if not is_member_of_obligation_chat:
                #        start_time = time.time()
                #        is_member_of_obligation_chat = any(joined_user_id == participant.id for participant in obligation_chat_members)
                #        end_time = time.time()
                #        logging.warning(f"SCAN: {chat_id} -- {joined_user_name} {'WAS' if is_member_of_obligation_chat else 'WAS NOT'} found in {obligation_chat.title} via get_participants() list. Elapsed time: {end_time - start_time} sec.")
                
                #if obligation_chat and not is_member_of_obligation_chat and joined_user_id not in AUTHORIZED_ADMINS and joined_user_id not in admin_ids and shinfree:
                #    logging.warning(f"SCAN: {chat_id} -- Calling obligation kick.")
                #    await obligation_kick(joined_user_id, chat_id, joined_user_name, obligation_chat.title)
                #    update_or_insert_chat_member(participant, chat_id, "last_kicked", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))


        # Step 6: Batch update the database for users that have left
        
        batch_update_joined(joined_user_ids, chat_id)
        batch_update_left(left_user_ids, chat_id)
        remove_unbanned_user_from_blacklist(unbanned_user_ids, chat_id)

        ban_leavers_mode = get_ban_leavers_status(chat_id)
        #If ban_leavers_mode is on, ban anyone with a status of "left"
        if ban_leavers_mode[0]==1:
            logging.warning(f"BAN-LEAVERS MODE ON FOR {chat_id} - BANNING {len(user_ids_to_ban)} USERS WHO LEFT")

            if len(user_ids_to_ban) > 0:
                logging.warning("USERS TO BE BANNED:")
                logging.warning(user_ids_to_ban)
                await uniban_from_list(user_ids_to_ban)

            for joined_user_id in joined_user_ids:
                if (joined_user_id, chat_id) in let_leave_without_banning:
                    let_leave_without_banning.discard((joined_user_id, chat_id))

            

        
        if (is_megagroup or is_channel):

            # Get total list of banned users, including any that were just banned by activating banned_leavers mode.
            banned_user_ids = set()
            async for participant in telethon.iter_participants(chat_id, filter = ChannelParticipantsKicked):     
                if hasattr(participant, 'id') and isinstance(participant.id, int):
                    banned_user_ids.add(participant.id)

            # banned_user_ids = set([participant.id async for participant in telethon.iter_participants(chat_id, filter = ChannelParticipantsKicked)])

            # Remove the users already banned in the database, so as to only update those who were not banned before the scan.
            update_ban_status = banned_user_ids - banned_ids_in_db

            # Set status, last_banned, and times_banned fields for those just banned
            batch_update_banned(update_ban_status, chat_id)

            # Arrive at a list of manually-unbanned users by subtracting currently banned users from those marked as banned in the DB.
            manually_unbanned =  banned_ids_in_db - banned_user_ids
            remove_unbanned_user_from_blacklist(manually_unbanned, chat_id)
            batch_update_left(manually_unbanned, chat_id)

        else:
            if ban_leavers_mode[0]==1 and context and len(left_user_ids) > 0:
                # Set status, last_banned, and times_banned fields for those just banned
                batch_update_banned(user_ids_to_ban, chat_id)   
        scanning_underway.remove(chat_id)
        print(f"Update of {chat_id} completed.")
    
    except BadRequestError as e:
        active_chats, inactive_chats, active_str, inactive_str = await find_inactive_chats(context)
        if len(inactive_chats) > 0:
            logging.warning(f"Bot does not seem to have Admin rights in {chat.title} Chat processessing not completed.\n")
            scanning_underway.remove(chat_id)
    
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
        logging.error(f"Error in the process_chat_member_updates() function: {e}")
        scanning_underway.remove(chat_id)
    return results


async def uniban_from_list(left_user_ids):
    async def ban_user(user_id):
        await telethon.edit_permissions(chat_id, user_id, view_messages=False)
    chat_ids_in_database = list_chats_in_db()
    for chat_id in chat_ids_in_database:
        try:
            chat = await telethon.get_entity(chat_id)
            if hasattr(chat, 'username') and chat.username:
                logging.warning(f"Can't ban from {chat.title} - PRIVATE")
            else:
                logging.warning(f"Banning left users from {chat.title}")
                insert_userlist_into_blacklist(left_user_ids, chat_id)
                await asyncio.gather(*(ban_user(user_id) for user_id in left_user_ids))
        except ChannelPrivateError as e:
            logging.warning(f"Can't ban from {chat.title} - PRIVATE ERROR - May no longer be active")
        except Exception as e:
            logging.warning(f"Uniban error: {e}")

    return


async def unban(update: Update=None, context: CallbackContext=None):
    @authorized_admin_check
    async def universal_unban(update: Update=None, context: CallbackContext=None):
        chat_ids_in_database = list_chats_in_db()
        for chat_id in chat_ids_in_database:
            try:
                remove_unbanned_user_from_blacklist(unban_user_list, chat_id)
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
            remove_unbanned_user_from_blacklist(unban_user_list, issuer_chat_id)
            chat = await telethon.get_entity(issuer_chat_id)
            if hasattr(chat, 'username') and chat.username:
                logging.warning(f"Can't unban from {chat.title} - PRIVATE")
            else:
                logging.warning(f"Unbanning left users from {chat.title}")
                await telethon.edit_permissions(issuer_chat_id, unban_user_id)
                batch_update_left(unban_user_list, issuer_chat_id)
        except ChannelPrivateError as e:
            logging.warning(f"Can't unban from {chat.title} - PRIVATE ERROR - Chat may no longer be active")
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
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
        logging.warning(f"Unban argument error: {e}")  

    try:
        id_or_username = int(id_or_username)
    except:
        pass

    try:
        global telethon
        if telethon.is_connected() == False:
            telethon.connect()
        unban_user_entity = await telethon.get_entity(id_or_username)
        unban_user_id = unban_user_entity.id
        unban_user_list.append(unban_user_id)
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
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
            await context.bot.send_message(
                chat_id=issuer_chat_id,
                text="Processing unbans..."
            )
            await universal_unban(update, context)
            await context.bot.send_message(
                chat_id=issuer_chat_id,
                text=f"{id_or_username} has been unbanned from all active groups."
            )
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
        logging.error(f"{e}")
    return


async def update_chat_members(update: Update=None, context: CallbackContext=None):
    try:

        active_chats, inactive_chats, active_str, inactive_str = await find_inactive_chats(context)
        if len(inactive_chats) > 0:
            logging.warning("Found inactive chats. Cleaning database.\n")
            logging.warning(inactive_str)
            del_chats_from_db(inactive_chats)
            logging.warning("Purging...\n")
            logging.warning("Inactive channels deleted.\n")
            logging.warning(active_str)  

        me = await telethon.get_me()
        i_am_admin = []
        active_ids = list_chats_in_db()
        for chat_id in active_ids:
            async for user in telethon.iter_participants(chat_id, filter=ChannelParticipantsAdmins):
                if user.id == me.id:
                    i_am_admin.append(chat_id)
                    break
        if len(i_am_admin) > 0:
            # Create a list of tasks
            results_list = []
            tasks = [process_chat_member_updates(chat_id, update, context) for chat_id in i_am_admin]

            # Execute tasks concurrently using asyncio.gather()
            results_list = await asyncio.gather(*tasks)
            shin_ids = set(keyword_search_from_db('shinanygans'))
            # Process obligation kicks in batch
            for results in results_list:
                results_chat_id = results['chat_id']
                results_joined_user_ids = results['joined_user_ids']
                admins = lookup_admin_ids(results_chat_id)
                obligation_chat_id = lookup_obligation_chat(results_chat_id)
                if obligation_chat_id:
                    for joined_user_id in results_joined_user_ids:
                        if joined_user_id not in AUTHORIZED_ADMINS and joined_user_id not in admins and joined_user_id not in shin_ids:
                            lookup = lookup_active_group_member(joined_user_id, obligation_chat_id)
                            logging.warning(f"SCAN: **ALT LOOKUP** Joining user {joined_user_id} {'DOES' if len(lookup)>0 else 'DOES NOT'} appear in our internal DB for obligation chat {obligation_chat_id}")
                            if len(lookup)==0:
                                obligation_chat = await telethon.get_entity(obligation_chat_id)
                                joined_user = await telethon.get_entity(joined_user_id)
                                joined_user_name = (f"{joined_user.first_name if joined_user.first_name else ''} {joined_user.last_name if joined_user.last_name else ''}")
                                logging.warning(f"SCAN: {results_chat_id} -- ***ALT LOOKUP** OBLIGATION KICK: {joined_user_name} kicked from {results_chat_id}.")
                                await obligation_kick(joined_user_id, chat_id, joined_user_name, obligation_chat.title)
                                update_or_insert_chat_member(joined_user, chat_id, "last_kicked", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))

            update_left_groups()
        print("Update completed.")
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
        logging.error(f"Error in the update_chat_members() function: {e}")


# Function to run the scheduled tasks
async def run_scheduled_tasks():
    while tracking_chat_members:
        await schedule.run_pending()
        await asyncio.sleep(1)


# ********* COMMAND HANDLING *********


async def start_chat_member_tracking(update: Update=None, context: CallbackContext=None):
    schedule.every(2).minutes.do(update_chat_members, update, context)
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
        await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
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
        
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
        logging.error(f"Error in the three_strike_mode() function: {e}")
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
        await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
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

        if API_ID and API_HASH:
            global telethon
            if telethon.is_connected() == False:
                telethon.connect()
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
        lurker_message += f"👤 There are {total_members} non-admin members in the group.\n\n"
        lurker_message += f"⏱ {total_members - posted_in_last_12_hours} have NOT posted in the last {readable_string_of_duration} ({time_window_lurk_rate}% recent lurker).\n\n"
        lurker_message += f"💥 {not_posted} users have not posted at all. ({total_lurk_rate}% total lurker)"
        if chat_id in DEBUG_CHATS:
            await context.bot.send_message(chat_id=chat_id, text=f"DEBUG: {lurker_message}")
        await context.bot.send_message(chat_id=issuer_user_id, text=lurker_message)
    except (IndexError, ValueError) as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
        await context.bot.send_message(chat_id=chat_id, text="Invalid command format. Use /gcstats <time> (e.g., /gcstats 1d).")
        logging.error(f"An error occurred in kick_inactive_users(), probably due to an invalid time argument.")
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
        logging.error(f"Error assembling chat stats: {e}")
    return


async def ban_from_blacklist(update: Update, context: CallbackContext):
    chat_id = update.message.chat_id
    chat_name = update.effective_chat.title
    issuer_user_id = update.effective_user.id
    chat_type = update.effective_chat.type
    if chat_type == ChatType.PRIVATE:
        message = await context.bot.send_message(
            chat_id=chat_id,
            text="<i style='color:#808080;'> This command only works in the chat where you want to ban everyone on Kickbot's blacklist. The responses will come here.</i>",
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
            blacklist = return_blacklist()
            if blacklist is None:
                return
            logging.warning(f"BANNING BLACKLISTED USERS FROM {chat_name}")
            await context.bot.send_message(
            chat_id=issuer_user_id,
            text=f"BANNING BLACKLISTED USERS FROM {chat_name}"
        )       
            blacklisted_uids = []
            for blacklisted_user in blacklist:
                uid = blacklisted_user[0]
                try:
                    await context.bot.ban_chat_member(chat_id, uid)
                    blacklisted_uids.append(uid)
                except Exception as e:
                    logging.warning(f"Could not ban {uid} from {chat_id} - Ban error or deleted account.")
                insert_kicked_user_in_blacklist(uid, chat_id)
            batch_update_banned(blacklisted_uids, chat_id)
            logging.warning(f"BANNING BLACKLIST FROM {chat_name} COMPLETE. BANNED {len(blacklisted_uids)} USERS.")
            await context.bot.send_message(
            chat_id=issuer_user_id,
            text=f"BANNING BLACKLIST FROM {chat_name} COMPLETE. BANNED {len(blacklisted_uids)} USERS."
        )    


        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
            logging.error(f"Error assembling chat stats: {e}")
    return


async def lookup(update: Update, context: CallbackContext):
    chat_id = update.message.chat_id
    issuer_user_id = update.effective_user.id
    chat_type = update.effective_chat.type

    id_or_username = context.args[0]
    try:
        id_or_username = int(id_or_username)
    except:
        pass   
    try:
        global telethon
        if telethon.is_connected() == False:
            telethon.connect()
        kicked_user = await telethon.get_entity(id_or_username)
        kicked_user_id = kicked_user.id
        
    except Exception as e:
        logging.error(f" Error getting entity and chat member information during lookup: {e}")
        chat_member = None
    try:
        if chat_type != ChatType.PRIVATE:
            message = await context.bot.send_message(
                chat_id=chat_id,
                text="<i style='color:#808080;'> Response will be sent privately.</i>",
                parse_mode=ParseMode.HTML
            )
            asyncio.create_task(delete_message_after_delay(context, message))
            group_member_dict = lookup_group_member(kicked_user_id, chat_id)
        else:
            group_member_dict = lookup_group_member(kicked_user_id)

        kicked_user_data = lookup_user_in_kick_db(kicked_user_id)
        blacklist_data = lookup_user_in_blacklist(kicked_user_id)

        if not group_member_dict or len(group_member_dict)==0:
            kicked_user_message = "User not found in kickbot database."
        else:
            group_member_first = group_member_dict[0]
            # kicked_user_name = f"{kicked_user.first_name}{' ' + kicked_user.last_name if kicked_user.last_name else ''}"
            kicked_user_name = group_member_first['user_name']
            # kicked_user_username = kicked_user.username
            kicked_user_username = group_member_first['user_username']
            # kicked_user_is_bot = kicked_user.bot
            kicked_user_is_bot = 'True' if group_member_first['is_bot'] else 'False'
            #kicked_user_is_premium = kicked_user.premium
            kicked_user_is_premium = 'True' if group_member_first['is_premium'] else 'False'
            #kicked_user_is_fake = kicked_user.fake
            kicked_user_is_fake = 'True' if group_member_first['is_fake'] else 'False'
            #kicked_user_is_restricted = kicked_user.restricted
            kicked_user_is_restricted = 'True' if group_member_first['is_restricted'] else 'False'
            #kicked_user_restriction_reason = kicked_user.restriction_reason if kicked_user.restriction_reason else ""
            kicked_user_restriction_reason = group_member_first['restricted_reason']  if group_member_first['restricted_reason'] else ''
            #kicked_user_is_scam = kicked_user.scam
            kicked_user_is_scam = 'True' if group_member_first['is_scam'] else 'False'
            #kicked_user_is_verified = kicked_user.verified
            kicked_user_is_verified = 'True' if group_member_first['is_verified'] else 'False'
            
            kicked_user_message="KICKBOT USER RECORD\n"
            kicked_user_message+=f"NAME: {kicked_user_name}\n"
            kicked_user_message+=f"ID: {kicked_user_id}\n"
            kicked_user_message+=f"USERNAME: @{kicked_user_username}\n"
            kicked_user_message+=f"PREMIUM: {kicked_user_is_premium}\n"
            kicked_user_message+=f"VERIFIED: {kicked_user_is_verified}\n"
            kicked_user_message+=f"IS BOT: {kicked_user_is_bot}\n"
            kicked_user_message+=f"IS FAKE: {kicked_user_is_fake}\n"
            kicked_user_message+=f"IS SCAM: {kicked_user_is_scam}\n"
            kicked_user_message+=f"RESTRICTED: {kicked_user_is_restricted}\n"
            kicked_user_message+=f"{'RESTR REASON: ' if kicked_user_is_restricted else ''}{kicked_user_restriction_reason}\n\n"
            await context.bot.send_message(chat_id=issuer_user_id, text=kicked_user_message)

            for group_member_row in group_member_dict:
                kicked_user_chat_id = group_member_row['chat_id']
                chat = await telethon.get_entity(kicked_user_chat_id)
                chat_name = chat.title
                kicked_user_row = next((row for row in kicked_user_data if row[1] == chat_id), None)
                blacklist_row = next((row for row in blacklist_data if row[1] == chat_id), None)

                kicked_user_number_kicks = kicked_user_row[2] if kicked_user_row else None
                if kicked_user_row and kicked_user_row[3]:
                    kicked_user_last_posted = datetime.strptime(kicked_user_data[3], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=utc_timezone).strftime('%d %B, %Y - %H:%M:%S')
                else:
                    kicked_user_last_posted = "Never"
                kicked_user_last_kicked = datetime.strptime(kicked_user_row[4], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=utc_timezone).strftime('%d %B, %Y - %H:%M:%S') if kicked_user_row else None
                
                chat_member = await context.bot.get_chat_member(kicked_user_chat_id, kicked_user_id)
                kicked_user_status = None
                
                if chat_member:
                    if chat_member.status == ChatMember.ADMINISTRATOR:
                        kicked_user_status = "Admin"
                    elif chat_member.status == ChatMember.BANNED:
                        kicked_user_status = "Banned"
                    elif chat_member.status == ChatMember.LEFT:
                        kicked_user_status = group_member_row['status'] if group_member_row['status'] else "Left"
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
                kicked_user_last_joined = datetime.strptime(group_member_row['last_joined'], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=utc_timezone).strftime('%d %B, %Y - %H:%M:%S') if group_member_row else 'N/A'
                #kicked_user_last_joined = None
                #async for participant in telethon.iter_participants(kicked_user_chat_id, search=kicked_user.first_name):
                #    if participant.id == kicked_user_id:
                #        kicked_user_last_joined = participant.participant.date.strftime('%d %B, %Y - %H:%M:%S') if chat_member.status == ChatMember.MEMBER else None
                #        break
                kicked_chat_message=""
                kicked_chat_message+=f"{kicked_user_name} - {chat_name}{' - BLACKLISTED' if blacklist_row else ''}\n"
                kicked_chat_message+=f"KICKS from {chat_name}: {kicked_user_number_kicks}\n"
                kicked_chat_message+=f"BANS from {chat_name}: {group_member_row['times_banned'] if group_member_row['times_banned'] else 'N/A'}\n"
                kicked_chat_message+=f"MOST RECENTLY JOINED: {kicked_user_last_joined if kicked_user_last_joined else 'N/A'}\n"
                kicked_chat_message+=f"LAST POST: {kicked_user_last_posted}\n"
                kicked_chat_message+=f"LAST KICKED: {kicked_user_last_kicked}\n"
                kicked_chat_message+=f"STATUS: {kicked_user_status}\n\n"
                await context.bot.send_message(chat_id=issuer_user_id, text=kicked_chat_message)
   
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
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
        active_chats, inactive_chats, active_str, inactive_str = await find_inactive_chats(context)
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
        await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
        logging.error(f"An error occured while cleaning the database: {e}")
    return


async def find_inactive_chats(context):
    active_chats = []
    inactive_chats = []
    try:
        chat_ids_in_database = list_chats_in_db()
        active_str = "CURRENT ACTIVE CHATS\n"
        inactive_str = "INACTIVE CHATS IN DATABASE\n"
        for chat_id in chat_ids_in_database:
            try:
                chat = await telethon.get_entity(chat_id)
                #if chat.type == ChatType.PRIVATE:
                if hasattr(chat, 'username') and chat.username:
                    inactive_chats.append(chat_id)
                    inactive_str = inactive_str + f"{chat_id}\n"
                else:
                    active_chats.append([chat.id, chat.title])
                    active_str = active_str + f"{chat.id} - {chat.title}\n"
            except ChannelPrivateError as e:
                inactive_chats.append(chat_id)
                inactive_str = inactive_str + f"{chat_id}\n"
            except Exception as e:
                inactive_chats.append(chat_id)
                inactive_str = inactive_str + f"{chat_id}\n"
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(None, context, exc_type, exc_value, exc_traceback)
        logging.error(f"An error occured while cleaning the database: {e}")
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

        # wholeft_message = "USERS WHO LEFT\n"
        # wholeft_message += "Users leaving in -10min listed individually\n\n"

        # Group users by channel_id
        users_by_channel = {}
        for user_id, channel_id, _, _, time_in_group_str, user_name in wholeft_data:
            # Convert the string representation to a timedelta object
            time_in_group = str_to_timedelta(time_in_group_str)
            if channel_id not in users_by_channel:
                users_by_channel[channel_id] = []
            users_by_channel[channel_id].append((user_id, user_name, time_in_group))

        # Print the results
        global telethon
        if telethon.is_connected() == False:
            telethon.connect()

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
                    # if avg_time_in_group < timedelta(minutes=10):
                    #    wholeft_message += f"{user_name} ({user_id}) - Times Left: {num_times_left}, Avg Time in Group: {avg_time_str}\n"
                    users_to_report.append(user_id)

                    # Write data to CSV file
                    
                    with open(csv_filename, mode='a', newline='') as csv_file:
                        csv_writer = csv.writer(csv_file)
                        # Write data for the current user
                        csv_writer.writerow([channel_id, title, user_id, user_name, num_times_left, avg_time_str])


            # wholeft_message += "\n"

        # if chat_id in DEBUG_CHATS:
           # await context.bot.send_message(chat_id=chat_id, text=f"DEBUG: {wholeft_message}")

        # await context.bot.send_message(chat_id=issuer_user_id, text=wholeft_message)
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
        await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
        logging.error(f"Error in the wholeft printing process: {e}")

    return


async def get_blacklist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_type = update.effective_chat.type
    chat_id = update.effective_chat.id
    issuer_user_id = update.effective_user.id
    try:
        if chat_type is not ChatType.PRIVATE:
            message = await context.bot.send_message(
                chat_id=chat_id,
                text="<i style='color:#808080;'> Response will be sent privately.</i>",
                parse_mode=ParseMode.HTML
            )
            asyncio.create_task(delete_message_after_delay(context, message))

        
        blacklist = return_blacklist() # bl.user_id, bl.channel_id, bl.ban_count, bl.last_banned, gm.user_name
        csv_filename = "blacklist.csv"

        # Sort the blacklist by chat_id and then by last_banned (newest to oldest)
        sorted_blacklist = sorted(
            blacklist,
            key=lambda x: (
                x[1],
                datetime.strptime(x[3], "%Y-%m-%d %H:%M:%S.%f") if x[3] else datetime.min,
            ),
            reverse=True
        )


        with open(csv_filename, mode='w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(["CHAT ID", "USER ID", "USER NAME", "BAN COUNT", "MOST RECENT BAN"])


        for row in sorted_blacklist:
            with open(csv_filename, mode='a', newline='') as csv_file:
                csv_writer = csv.writer(csv_file)
                csv_writer.writerow([row[1], row[0], row[4], row[2], row[3]])

        with open(csv_filename, 'rb') as csv_file:
            await context.bot.send_document(chat_id=issuer_user_id, document=csv_file, filename="blacklist.csv")

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
        await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
        logging.error(f"Error in the blacklist printing process: {e}")
    return


async def set_backup(update, context):
    user_id = update.effective_user.id
    issuer_chat_id = update.effective_chat.id
    issuer_chat_type = update.effective_chat.type
    issuer_chat_name = update.effective_chat.title
    try:
        global telethon
        if telethon.is_connected() == False:
            telethon.connect()

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

        # Get the distinct chat_ids from the database
        chat_ids = list_chats_in_db()

        buttons = []
        button_names={}
        for chat_id in chat_ids:
            if chat_id != issuer_chat_id:
                try:
                    entity = await telethon.get_entity(chat_id)
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
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
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
            # Perform the operation to set the obligation_chat
            insert_obligation_chat(issuer_chat_id, choice_int)
            message_text = f"Obligation group set to <strong>{button_names[choice_int]}</strong> for <strong>{issuer_chat_title}</strong>."


        # Send a confirmation message
        reply = await context.bot.send_message(chat_id=chat_id, text=message_text, parse_mode=ParseMode.HTML)
            # Delete the original message
        await asyncio.sleep(3)
        await context.bot.delete_message(chat_id=user_id, message_id=message_id)
        #await context.bot.delete_message(chat_id=user_id, message_id=reply.id)
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
        logging.error(f" Error processing response in set_backup(): {e}")


async def obligation_kick(user_id, chat_id, user_name, obligation_chat_name): 
    global telethon
    global let_leave_without_banning
    if telethon.is_connected() == False:
        telethon.connect()
    rt = 0
    while rt < max_retries:
        try:
            logging.warning(f"Kicking {user_name} from {chat_id} for not belonging to {obligation_chat_name}.")
            greeting = await telethon.send_message(
            entity=chat_id,
            message=f"{user_name}, This group needs you to first join the group <strong>{obligation_chat_name}</strong> before coming here. After that, you'll be free to rejoin.",
            parse_mode='html'
        )
            await asyncio.sleep(3)
            await telethon.delete_messages(chat_id, greeting)
            await telethon.edit_permissions(chat_id, user_id, view_messages=False)
            await telethon.edit_permissions(chat_id, user_id)
            let_leave_without_banning.add((user_id, chat_id))
            logging.warning(f"User ID {user_id} added to LET LEAVE WITHOUT BAN. Printing full list contents:")
            logging.warning(let_leave_without_banning)
            break

        except UserAdminInvalidError:
            logging.warning(f"An error occured in obligation_kick() - Permission issue kicking {user_name} from {chat_id}. -  {e}")
            break  

        except BadRequestError as e:
            logging.warning(f"An error occured in obligation_kick() - Obligation kicks don't work in closed topics. -  {e}")
            break  

        except Forbidden as e:
            logging.warning(f"An error occured in obligation_kick(): {e}")
            break

        except ValueError as e:
            logging.warning(f"An error occured in obligation_kick(): {e}")
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
            await debug_to_chat(None, None, exc_type, exc_value, exc_traceback)
            logging.error(f"An error occured in obligation_kick(): {e}")    
            break    
    return



# ********* CHAT EVENT HANDLING *********


# Add a user_id to the database whenever the bot sees them enter a chat.
async def handle_new_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ Thank you to python-telegram-bot example chatmemberbot.py for the model of this function
        https://github.com/python-telegram-bot/python-telegram-bot/blob/master/examples/chatmemberbot.py
    """
    global telethon
    global let_leave_without_banning
    if telethon.is_connected() == False:
        telethon.connect()   
   
    chat_id = update.effective_chat.id
    chat_name = update.effective_chat.title
    chat_type = update.effective_chat.type
    #admins = await update.effective_chat.get_administrators()
    #admin_ids = {admin.user.id for admin in admins}
    admin_ids = lookup_admin_ids(chat_id)
    member = update.chat_member
    user = member.new_chat_member.user
    user_name = user.first_name + (" " + user.last_name if user.last_name else "")
    username = user.username
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

    try:
        participant = await telethon.get_entity(user_id)
        if not was_member and is_member:
            member_record_dict = lookup_group_member(user_id, chat_id)
            member_record_dict = member_record_dict[0] if len(member_record_dict) > 0 else None
            update_or_insert_chat_member(participant, chat_id, status='Member')

            if not member.old_chat_member.status in ["administrator", "creator"]:
                obligation_chat_id = lookup_obligation_chat(chat_id)
                insert_user_in_db(user_id, chat_id, "user_activity")
                shinfree = False if username and 'shinanygans' in username else True
                supergroup = True if chat_type == ChatType.SUPERGROUP or chat_type == ChatType.CHANNEL else False
                obligation_member = None
                obligation_chat = None
                if obligation_chat_id and supergroup and user_id not in AUTHORIZED_ADMINS and shinfree:
                    obligation_chat = await context.bot.get_chat(obligation_chat_id)
                    obligation_member = await context.bot.get_chat_member(obligation_chat_id, user_id)

                    if not obligation_member or not hasattr(obligation_member, 'status') or obligation_member.status not in [ ChatMember.MEMBER,
                                                                                                                             ChatMember.ADMINISTRATOR,
                                                                                                                             ChatMember.OWNER
                                                                                                                             ]:       
                        await obligation_kick(user_id, chat_id, user_name, obligation_chat.title)
                        update_or_insert_chat_member(participant, chat_id, "last_kicked", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))
                        pass
                if obligation_chat_id:
                    if obligation_member:
                        obligation_found = ' - FOUND)'
                    else:
                        obligation_found = ' - NOT FOUND)'
                else:
                    obligation_found = ''
                logging.warning(f"REALTIME: {chat_id} -- {user_name} - {username} "
                f"joining {chat_name}. Previous DB Status: {member_record_dict['status'] if member_record_dict else 'NOT FOUND'} "
                f"{'(OBLIGATION: '+ obligation_chat.title if obligation_chat else ''} {obligation_found if obligation_found else ''}"
                )
            else:
                logging.info(f"REALTIME: User ID {user_id} '{user_name}' entered chat {chat_id} '{chat_name}'. Admin. Ignoring.")
        if (not is_member and was_member) and user_id != context.bot.id and not kick_started:
            update_or_insert_chat_member(participant, chat_id, "last_left", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))
            update_left_groups()

            if not member.new_chat_member.status in ["administrator", "creator"]:
                delete_user_from_db(user_id, chat_id, "user_activity")
                ban_leavers_mode = get_ban_leavers_status(chat_id)
                #If ban_leavers_mode is on, ban anyone with a status of "left"
                excused = any([user_id == user and chat_id == chat for user, chat in let_leave_without_banning])
                shinfree = False if username and 'shinanygans' in username else True
                member = lookup_group_member(user_id, chat_id)
                status = member[0]['status'] if member else None
                logging.warning(f"REALTIME: {chat_id} -- {user_name} - {username} leaving {chat_name}. "
                    f"Ban Leavers mode for {chat_name} is {'ON' if ban_leavers_mode[0]==1 else 'OFF'}. Current DB Status: {status}. "
                    f"Excuse list contains: {let_leave_without_banning}"
                )
                if ban_leavers_mode[0]==1 and user_id not in AUTHORIZED_ADMINS and user_id not in admin_ids and not excused and shinfree:
                    left_user_ids = [user_id]
                    if status == "Kicked":
                        logging.warning(f"REALTIME: BAN-LEAVERS MODE ON FOR {chat_id}, BUT {user_name} ALREADY KICKED. DOING NOTHING.")
                    else:
                        logging.warning(f"REALTIME: BAN-LEAVERS MODE ON FOR {chat_id} - UNI-BANNING {user_name}")
                        await uniban_from_list(left_user_ids)
                    update_or_insert_chat_member(participant, chat_id, "last_banned", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))
                elif ban_leavers_mode[0]==1 and excused:
                    logging.warning(f"REALTIME: {user_name} KICKED FROM {chat_name} FOR NOT BELONGING TO OBLIGATION CHAT.")
                let_leave_without_banning.discard((user_id, chat_id))

    except TimeoutError as e:
        logging.error(f"Timeout error in the handle_new_member() function: {e}")
    
    except Forbidden as e:
        logging.error(f"Forbidden error in the handle_new_member() function: {e}")

    except ValueError as e:
        logging.error(f"Error in the handle_new_member() function, likely from a bad get_entity lookup on a user_id.: {e}")

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
        logging.error(f"Error in the handle_new_member() function: {e}")

    return


# Check if it's a media message (photo, video, etc.) and update user activity
@authorized_chat_check
async def handle_message(update: Update, context: CallbackContext): 
    global telethon
    if telethon.is_connected() == False:
        telethon.connect()
    chat_type = update.effective_chat.type
    # Kickbot private chats do not process ordinary messages.
    if chat_type == ChatType.PRIVATE:
        return
    rt = 0
    while rt < max_retries:
        try:
            chat_id = update.effective_message.chat_id
            chat_name = update.effective_chat.title
            user = update.effective_user
            user_id = user.id
            user_name = user.first_name + (" " + user.last_name if user.last_name else "")

            # No matter what the message contains, capture the sender in the DB 
            insert_user_in_db(user_id, chat_id, "user_activity")

            # If the message contained acceptable media, process further
            if update.effective_message.document or update.effective_message.photo or update.effective_message.video:
                date = update.effective_message.date
                chat_member = await context.bot.get_chat_member(chat_id, user_id)

                # If the sender was not an admin, update the last_activity in the database
                if not chat_member.status in ["administrator", "creator"]:
                    logging.warning(f"User ID {user_id} '{user_name}' in chat {chat_id} '{chat_name}' *POSTED MEDIA*")
                    await debug_message(context, chat_id, user_name, DEBUG_UPDATE_MESSAGE)
                    update_user_activity(user_id, chat_id, date)
                    async for participant in telethon.iter_participants(chat_id, search=user.first_name):
                        if participant.id == user_id:
                            update_or_insert_chat_member(participant, chat_id, "last_posted", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))
                else:
                    logging.info(f"User ID {user_id} '{user_name}' in chat {chat_id} '{chat_name}' contains acceptable media but is an admin. Ignoring.")
                    await debug_message(context, chat_id, user_name, DEBUG_ADMIN_MESSAGE)
            break

        except Forbidden as e:
            logging.warning(f"{e}")

        except TimeoutError as e:
            logging.warning(f"{e}")

        except RetryAfter as e:
                wait_seconds = e.retry_after
                exc_type, exc_value, exc_traceback = sys.exc_info()
                await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
                logging.warning(f"Got a RetryAfter error. Waiting for {wait_seconds} seconds...")
                await asyncio.sleep(wait_seconds)
                rt += 1
                if rt == max_retries:
                    logging.warning(f"Max retry limit reached. Message not sent.")
                    raise e
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
            logging.error(f"An error occured in handle_message() parsing a post for db entry: {e}")        
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
        global telethon
        if telethon.is_connected() == False:
            telethon.connect()
        try:
            lookup_id = int(context.args[0])
        except:
            lookup_id = context.args[0]

        user = await telethon.get_entity(lookup_id)
        user_id = user.id
        user_name = user.first_name + (" " + user.last_name if user.last_name else "")
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
        await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
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
        global telethon
        if telethon.is_connected() == False:
            telethon.connect()
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
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
        logging.error(f"Error in the dewhitelisting process: {e}")
    return


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
            whitelist_data = get_whitelist_from_private()

        
        whitelist_message=f"WHITELISTED USERS\n\n"

        # Group users by channel_id
        users_by_channel = {}
        for user_id, channel_id in whitelist_data:
            if channel_id not in users_by_channel:
                users_by_channel[channel_id] = []
            users_by_channel[channel_id].append(user_id)

        # Print the results
        global telethon
        if telethon.is_connected() == False:
            telethon.connect()
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
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
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

                if not pretend:
                    # Increment the user's kick count in the database
                    insert_kicked_user_in_kick_db(user_id, issuer_chat_id, last_activity_str)
                
                    # If supergroup, use 'unban' for kick. Otherwise just ban.               
                    if (issuer_chat_type == ChatType.SUPERGROUP or issuer_chat_type == ChatType.CHANNEL) and not ban and not three_strikes_ban:
                        await context.bot.unban_chat_member(issuer_chat_id, user_id)
                    else:
                        await context.bot.ban_chat_member(issuer_chat_id, user_id)
                        insert_kicked_user_in_blacklist(user_id, issuer_chat_id)
                        banned_uids.append(user_id)
                    
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
                exc_type, exc_value, exc_traceback = sys.exc_info()
                await debug_to_chat(None, context, exc_type, exc_value, exc_traceback)
                logging.warning(f"Got an error while processing: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
                rt += 1
                if rt == max_retries:
                    logging.warning(f"Max retry limit reached. Error: {e}. User {user_id} not kicked.")
                    break
    batch_update_banned(banned_uids, issuer_chat_id)
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
                global telethon
                if telethon.is_connected() == False:
                    telethon.connect()
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
        if len(scanning_underway) > 0:
            await context.bot.send_message(chat_id=issuer_chat_id, text="Waiting for room scanning to complete...")
        while len(scanning_underway) > 0:
            await asyncio.sleep(1)
        asyncio.create_task(suspend_scanning()) 
        await context.bot.send_message(chat_id=issuer_chat_id, text=START_PURGE)
        users_to_ban, banned_name_lookup = await assemble_banned_list(issuer_chat_id, admin_ids, cutoff_date)
        # Count the ban list, and announce to the group.
        count_of_users_to_ban = len(users_to_ban)
        admin_message = f" The KickBot is about to purge {count_of_users_to_ban} users from {issuer_chat_name} who have not posted media in the last {readable_string_of_duration}."
        await context.bot.send_message(chat_id=issuer_chat_id, text=admin_message)

    except (IndexError, ValueError) as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
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
        await debug_to_chat(update, context, exc_type, exc_value, exc_traceback)
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
    tqdm.close(pbar)
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
@is_admin_of_authorized_chat_check
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
@authorized_admin_check
async def get_blacklist_loop(update: Update, context: CallbackContext):
    asyncio.create_task(get_blacklist(update, context))
    return


#Command to purch the database of data from chats that are no longer active
@authorized_admin_check
async def ban_from_blacklist_loop(update: Update, context: CallbackContext):
    asyncio.create_task(ban_from_blacklist(update, context))
    return


#Command to purch the database of data from chats that are no longer active
@is_admin_of_authorized_chat_check
async def unban_loop(update: Update, context: CallbackContext):
    asyncio.create_task(unban(update, context))
    return


@authorized_admin_check
async def set_backup_loop(update: Update, context: CallbackContext):
    asyncio.create_task(set_backup(update, context))
    return


# ********* MAIN *********


async def post_init(application: Application):
    await start_chat_member_tracking()  
    # asyncio.get_event_loop().set_debug(True)

def main() -> None:
    """Run bot."""
    # Create the Application and pass it your bot's token.
    application = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    application.add_handler(CommandHandler("inactivekick", inactive_kick_loop))
    application.add_handler(CommandHandler("pretendkick", pretend_kick_loop))
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
    application.add_handler(CommandHandler("blacklist", get_blacklist_loop))
    application.add_handler(CommandHandler("importbl", ban_from_blacklist_loop))
    application.add_handler(CommandHandler("forgive", unban_loop))
    application.add_handler(CommandHandler("setbackup", set_backup_loop))
    application.add_handler(CallbackQueryHandler(button_click, pattern='^setbackup_.*'))
    application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_message))
    application.add_handler(ChatMemberHandler(handle_new_member_loop, ChatMemberHandler.CHAT_MEMBER))
    application.add_error_handler(error)


    # Run the bot until the user presses Ctrl-C
    try:
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