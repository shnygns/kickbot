import sqlite3
import pytz
import csv
import logging
from datetime import datetime, timedelta, timezone
from config import DATABASE_PATH
from telegram.constants import ChatType
from telegram import ChatMember

from telethon.tl.types import (
    ChannelParticipantAdmin, 
    ChannelParticipant, 
    ChannelParticipantCreator,
    ChatParticipant, 
    ChatParticipantAdmin, 
    ChatParticipantCreator
)
from enum import Enum

class EventType(Enum):
    JOINED = "joined"
    LEFT = "left"
    KICKED = "kicked"
    BANNED = "banned"
    POSTED = "posted"

utc_timezone = pytz.utc

# ********* INITIALIZE DATABASE *********

def initialize_db():
    with sqlite3.connect(DATABASE_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

    # Create the table with a composite unique constraint
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_activity (
                user_id INTEGER,
                channel_id INTEGER,
                last_activity TIMESTAMP,
                PRIMARY KEY (user_id, channel_id)
            )
        """
        )


        cursor.execute("""
            CREATE TABLE IF NOT EXISTS whitelist (
                user_id INTEGER,
                channel_id INTEGER,
                PRIMARY KEY (user_id, channel_id)
            )
        """
        )


        cursor.execute("""
            CREATE TABLE IF NOT EXISTS kicked_users (
                user_id INTEGER,
                channel_id INTEGER,
                kick_count INTEGER DEFAULT 0,
                last_posted TIMESTAMP,
                last_kicked TIMESTAMP,
                PRIMARY KEY (user_id, channel_id)
            )
        """
        )


        # Create the authorized_chats table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS authorized_chats (
                chat_id INTEGER PRIMARY KEY,
                chat_name STRING,
                three_strikes_mode BOOLEAN DEFAULT FALSE,
                ban_leavers_mode BOOLEAN DEFAULT FALSE,
                obligation_chat INTEGER,
                last_scan TIMESTAMP,
                last_admin_update TIMESTAMP
            )
        """
        )

        cursor.execute(f"PRAGMA table_info(authorized_chats)")
        columns = [column[1] for column in cursor.fetchall()]

        if 'ban_leavers_mode' not in columns:
            # If the column doesn't exist, add it to the table
            cursor.execute(f"ALTER TABLE authorized_chats ADD COLUMN ban_leavers_mode BOOLEAN DEFAULT FALSE")
        if 'obligation_chat' not in columns:
            # If the column doesn't exist, add it to the table
            cursor.execute(f"ALTER TABLE authorized_chats ADD COLUMN obligation_chat INTEGER")
        if 'last_scan' not in columns:
            # If the column doesn't exist, add it to the table
            cursor.execute(f"ALTER TABLE authorized_chats ADD COLUMN last_scan TIMESTAMP")
        if 'chat_name' not in columns:
            # If the column doesn't exist, add it to the table
            cursor.execute(f"ALTER TABLE authorized_chats ADD COLUMN chat_name STRING")
        if 'last_admin_update' not in columns:
            # If the column doesn't exist, add it to the table
            cursor.execute(f"ALTER TABLE authorized_chats ADD COLUMN last_admin_update TIMESTAMP")


        # Create the index on user_id and channel_id
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS user_activity_index ON user_activity (user_id, channel_id);
            """
        )


    # Create the events table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS blacklist (
                user_id INTEGER,
                channel_id INTEGER,
                ban_count INTEGER DEFAULT 0,
                last_banned TIMESTAMP,
                PRIMARY KEY (user_id, channel_id)
            )
        ''')

    # Create the left_group table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS  left_group (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                chat_id INTEGER,
                last_joined TIMESTAMP,
                last_left TIMESTAMP,
                time_in_group TIMESTAMP,
                FOREIGN KEY (user_id, chat_id) REFERENCES group_member (user_id, chat_id)
            )
        ''')
                       
    # Create the group_member table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS group_member (
                user_id INTEGER,
                chat_id INTEGER,
                user_name TEXT,
                user_username TEXT,
                is_premium INTEGER,
                is_verified INTEGER,
                is_bot INTEGER,
                is_fake INTEGER,
                is_scam INTEGER,
                is_restricted INTEGER,
                restricted_reason TEXT,
                status TEXT,
                first_joined TIMESTAMP,
                last_joined TIMESTAMP,
                last_left TIMESTAMP,
                last_kicked TIMESTAMP,
                last_banned TIMESTAMP,
                last_posted TIMESTAMP,
                times_joined INTEGER,
                times_posted INTEGER,
                times_left INTEGER,
                times_kicked INTEGER,
                times_banned INTEGER,
                PRIMARY KEY (user_id, chat_id)
            )
        ''')
        cursor.execute(f"PRAGMA table_info(group_member)")
        columns = [column[1] for column in cursor.fetchall()]
        if 'last_banned' not in columns:
            # If the column doesn't exist, add it to the table
            cursor.execute(f"ALTER TABLE group_member ADD COLUMN last_banned BOOLEAN DEFAULT FALSE")
        if 'times_banned' not in columns:
            # If the column doesn't exist, add it to the table
            cursor.execute(f"ALTER TABLE group_member ADD COLUMN times_banned BOOLEAN DEFAULT FALSE")

        conn.commit()
    return



# ********* CHAT AUTHORIZATION COMMANDS *********

def is_chat_authorized(chat_id, chat_name):
    try:
        with sqlite3.connect(DATABASE_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT chat_name FROM authorized_chats WHERE chat_id = ?", (chat_id,))
            response = cursor.fetchone()
            if chat_name and response and chat_name not in response:
                cursor.execute("UPDATE authorized_chats SET chat_name = ? WHERE chat_id = ?", (chat_name, chat_id))
                conn.commit()
            return response is not None
    except Exception as e:
        print(f"Error checking if chat is authorized: {e}")
        return False
    

def insert_authorized_chat(chat_id, chat_name):
    try:
        with sqlite3.connect(DATABASE_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO authorized_chats (chat_id, chat_name) VALUES (?, ?)", (chat_id, chat_name))
            conn.commit()
    except Exception as e:
        print(f"Error inserting authorized chat: {e}")
    return


def get_chat_ids_and_names():
    try:
        chat_info = {}
        with sqlite3.connect(DATABASE_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT chat_id, chat_name FROM authorized_chats")
            rows = cursor.fetchall()
            for row in rows:
                chat_id, chat_name = row
                chat_info[chat_id] = chat_name
        return chat_info
    except Exception as e:
        print(f"Error fetching chat IDs and names: {e}")
        return {}


# ********* USER ACTIVITY COMMANDS *********

def insert_user_in_db(user_id, chat_id, table):
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()

        # Using string formatting for table name, ensure table is a trusted value
        cursor.execute(f"SELECT 1 FROM {table} WHERE user_id = ? AND channel_id = ? LIMIT 1", (user_id, chat_id))
        row = cursor.fetchone()

        if not row:
            # The combination doesn't exist, so insert it
            cursor.execute(f"INSERT INTO {table} (user_id, channel_id) VALUES (?, ?)", (user_id, chat_id))
            conn.commit()
    return


#Delete user from database when he leaves or is kicked
def delete_user_from_db(user_id, chat_id, table):
        with sqlite3.connect(DATABASE_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Get users who haven't posted media since the cutoff date or have never posted
            cursor.execute(
                f"""
                DELETE FROM {table}
                WHERE user_id = {user_id} AND channel_id = {chat_id}
                """
            )
            conn.commit()
        return


def list_chats_in_db():
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()

        # Get a list of unique chat_ids from the database
        cursor.execute("SELECT DISTINCT chat_id FROM authorized_chats")
        chat_ids_in_database = {row[0] for row in cursor.fetchall()}
    return chat_ids_in_database


def del_chats_from_db(chat_list):
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()

        # Get a list of unique chat_ids from the database
        for chat_id in chat_list:
            cursor.execute("DELETE FROM user_activity WHERE channel_id = ?", (chat_id,))
            cursor.execute("DELETE FROM group_member WHERE chat_id = ?", (chat_id,))
            cursor.execute("DELETE FROM left_group WHERE chat_id = ?", (chat_id,))
            cursor.execute("DELETE FROM authorized_chats WHERE chat_id = ?", (chat_id,))
            cursor.execute("DELETE FROM kicked_users WHERE channel_id = ?", (chat_id,))
        conn.commit()
    return


def get_user_activity(chat_id):
    with sqlite3.connect(DATABASE_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        # Get users who haven't posted media since the cutoff date or have never posted
        cursor.execute(
            """
            SELECT user_id, last_activity FROM user_activity
            WHERE channel_id = ?
            """,
            (chat_id,),
        )
        user_data = cursor.fetchall()
    return user_data


# Function to update the last_activity timestamp for a user_id / chat_id combination when someone posts media
def update_user_activity(user_id, channel_id, date):
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
    return


def deleted_kicks_from_user_activity(delete_params):
 # Construct the SQL query with placeholders
    delete_query = """
        DELETE FROM user_activity
        WHERE user_id = ? AND channel_id = ?
    """

    # Connect to the database and execute the delete query
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.executemany(delete_query, delete_params)
        conn.commit()
    return


# ********* BLACKLIST DB COMMANDS *********


def lookup_user_in_blacklist(user_id):
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute( "SELECT * FROM blacklist WHERE user_id = ?", (user_id,))
        kicked_user_data = cursor.fetchall()
    return kicked_user_data



def return_blacklist():
    query = f"""
    SELECT bl.user_id, bl.channel_id, bl.ban_count, bl.last_banned, gm.user_name
    FROM blacklist bl
    LEFT JOIN group_member gm ON bl.user_id = gm.user_id AND bl.channel_id = gm.chat_id
    """
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
    return result



def insert_kicked_user_in_blacklist(user_id, chat_id):
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO blacklist (user_id, channel_id, ban_count, last_banned)
            VALUES (?, ?, COALESCE((SELECT ban_count FROM blacklist WHERE user_id = ? AND channel_id = ?) + 1, 1), ?)
        """, (user_id, chat_id, user_id, chat_id, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")))
        conn.commit()
    return


def insert_userlist_into_blacklist(user_id_list, chat_id):
    if not user_id_list:
            return
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        update_query = f'''
            INSERT OR REPLACE INTO blacklist (user_id, channel_id, ban_count, last_banned)
            VALUES (?, ?, COALESCE((SELECT ban_count FROM blacklist WHERE user_id = ? AND channel_id = ?) + 1, 1), ?)
        '''

        update_params = [
            (user_id, chat_id, user_id, chat_id, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))
            for user_id in user_id_list
        ]                      
        cursor.executemany(update_query, update_params)
        conn.commit()
    conn.close
    return


def remove_unbanned_user_from_blacklist(user_list, chat_id):
    delete_query = """
        DELETE FROM blacklist
        WHERE user_id = ? AND channel_id = ?
    """

    # Connect to the database and execute the delete query
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.executemany(delete_query, ((user_id, chat_id) for user_id in user_list))
        conn.commit()
    return



# ********* KICKED USER DB COMMANDS *********


def lookup_user_in_kick_db(user_id):
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute( "SELECT * FROM kicked_users WHERE user_id = ?", (user_id,))
        kicked_user_data = cursor.fetchall()
    return kicked_user_data


def lookup_kick_count_in_kick_db(user_id, chat_id):
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT kick_count FROM kicked_users WHERE user_id = ? AND channel_id = ?", (user_id, chat_id))
        result = cursor.fetchone()
        kick_count = result[0] if result else 0
    return kick_count


def insert_kicked_user_in_kick_db(user_id, chat_id, last_activity_str):
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO kicked_users (user_id, channel_id, kick_count, last_posted, last_kicked)
            VALUES (?, ?, COALESCE((SELECT kick_count FROM kicked_users WHERE user_id = ? AND channel_id = ?) + 1, 1), ?, ?)
        """, (user_id, chat_id, user_id, chat_id, last_activity_str, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")))
        conn.commit()
    return


def lookup_most_recent_kick_timestamp(chat_id):
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT MAX(last_kicked)
            FROM kicked_users
            WHERE channel_id = ?
        """, (chat_id,))
        result = cursor.fetchone()
    if result and result[0]:
        return datetime.strptime(result[0], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=utc_timezone)
    else:
        return None


# ********* WHITELIST COMMANDS *********

def get_whitelist(chat_id):

    query = f"SELECT user_id, channel_id FROM whitelist WHERE channel_id = {chat_id}"
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        whitelist_data = cursor.fetchall()
    return whitelist_data


def get_whitelist_from_private():

    query = "SELECT user_id, channel_id FROM whitelist"
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        whitelist_data = cursor.fetchall()
    return whitelist_data




# ********* THREE STRIKES COMMANDS *********
def get_three_strikes(chat_id):
    try:
        with sqlite3.connect(DATABASE_PATH) as conn:
            cursor = conn.cursor()
            
            # Fetch the current value of the 3_strikes_mode column
            cursor.execute("SELECT three_strikes_mode FROM authorized_chats WHERE chat_id = ?", (chat_id,))
            current_value = cursor.fetchone()
            return current_value
    except Exception as e:
        print(f"Error retreiving three_strikes_mode: {e}")
        return None


def update_three_strikes(chat_id):
    try:
        with sqlite3.connect(DATABASE_PATH) as conn:
            cursor = conn.cursor()
            
            # Fetch the current value of the 3_strikes_mode column
            cursor.execute("SELECT three_strikes_mode FROM authorized_chats WHERE chat_id = ?", (chat_id,))
            current_value = cursor.fetchone()

            # If the entry exists, update the value
            if current_value:
                new_value = not current_value[0]  # Toggle the boolean value
                cursor.execute("UPDATE authorized_chats SET three_strikes_mode = ? WHERE chat_id = ?", (new_value, chat_id))
            else:
                # If the entry does not exist, insert a new row with True
                new_value = True
                cursor.execute("INSERT INTO authorized_chats (chat_id, three_strikes_mode) VALUES (?, ?)", (chat_id, new_value))
            
            conn.commit()
            return new_value
    except Exception as e:
        print(f"Error updating three_strikes_mode: {e}")
        return None




# ********* BAN LEAVERS MODE COMMANDS *********

def get_ban_leavers_status(chat_id):
    try:
        with sqlite3.connect(DATABASE_PATH) as conn:
            cursor = conn.cursor()
            
            # Fetch the current value of the ban_leavers_mode column
            cursor.execute("SELECT ban_leavers_mode FROM authorized_chats WHERE chat_id = ?", (chat_id,))
            current_value = cursor.fetchone()
            return current_value
    except Exception as e:
        print(f"Error retreiving ban_leavers_mode: {e}")
        return None


def update_ban_leavers_status(chat_id):
    try:
        with sqlite3.connect(DATABASE_PATH) as conn:
            cursor = conn.cursor()
            
            # Fetch the current value of the ban_leavers_mode column
            cursor.execute("SELECT ban_leavers_mode FROM authorized_chats WHERE chat_id = ?", (chat_id,))
            current_value = cursor.fetchone()

            # If the entry exists, update the value
            if current_value:
                new_value = not current_value[0]  # Toggle the boolean value
                cursor.execute("UPDATE authorized_chats SET ban_leavers_mode = ? WHERE chat_id = ?", (new_value, chat_id))
            else:
                # If the entry does not exist, insert a new row with True
                new_value = True
                cursor.execute("INSERT INTO authorized_chats (chat_id, ban_leavers_mode) VALUES (?, ?)", (chat_id, new_value))
            
            conn.commit()
            return new_value
    except Exception as e:
        print(f"Error updating ban_leavers_mode: {e}")
        return None




# ********* CHAT MEMBER AND EVENTS COMMANDS *********

def lookup_group_member(user_id, chat_id=None):
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()

        if chat_id:
            query = ('''
            SELECT * FROM group_member
            WHERE user_id = ? AND chat_id = ?
        ''' )
            params = (user_id, chat_id)
        else:
            query = ('''
            SELECT * FROM group_member
            WHERE user_id = ?
        ''' )           
            params = (user_id,)
        cursor.execute(query, params)

    result = cursor.fetchall()
    if result:
        column_names = [description[0] for description in cursor.description]

        #if len(result) == 1:
        #    result_dict = dict(zip(column_names, result[0]))
        #    return result_dict
        #else:
        result_list = [dict(zip(column_names, row)) for row in result]
        return result_list

    return []


def lookup_active_group_member(user_id, chat_id=None):
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()

        if chat_id:
            query = ('''
            SELECT * FROM group_member
            WHERE user_id = ? AND chat_id = ? AND (status != 'Left' OR status != 'Kicked' OR status != 'Banned')
        ''' )
            params = (user_id, chat_id)
        else:
            query = ('''
            SELECT * FROM group_member 
            WHERE user_id = ? AND (status != 'Left' OR status != 'Kicked' OR status != 'Banned')
        ''' )           
            params = (user_id,)
        cursor.execute(query, params)

    result = cursor.fetchall()
    if result:
        column_names = [description[0] for description in cursor.description]
        result_list = [dict(zip(column_names, row)) for row in result]
        return result_list

    return []



def lookup_admin_ids(chat_id=None):
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()

        if chat_id:
            query = ('''
            SELECT user_id FROM group_member
            WHERE chat_id = ? AND status = 'Admin'
        ''' )
            params = (chat_id, )
            cursor.execute(query, params)
        else:
            query = ('''
            SELECT user_id FROM group_member
            WHERE status = 'Admin'
        ''' )    
            cursor.execute(query)

    result = cursor.fetchall()
    return set(user_id for (user_id,) in result)




def list_member_ids_in_db(chat_id):
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT user_id FROM group_member
            WHERE chat_id = ? AND (status = 'Member' OR status = 'Admin' OR status = 'Creator')
        ''', (chat_id,))

        user_ids = [row[0] for row in cursor.fetchall()]

    return user_ids


def list_unkonwn_status_in_db(chat_id):
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT user_id FROM group_member
            WHERE chat_id = ? AND (status = 'Not Available')
        ''', (chat_id,))

        user_ids = [row[0] for row in cursor.fetchall()]

    return user_ids


def list_kicked_users_in_db(chat_id):
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT user_id FROM group_member
            WHERE chat_id = ? AND status = 'Kicked'
        ''', (chat_id,))

        user_ids = [row[0] for row in cursor.fetchall()]

    return user_ids


def list_banned_users_in_db(chat_id):
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT user_id FROM group_member
            WHERE chat_id = ? AND status = 'Banned'
        ''', (chat_id,))

        user_ids = [row[0] for row in cursor.fetchall()]

    return user_ids


def keyword_search_from_db(keyword):
    query = '''
        SELECT user_id
        FROM group_member
        WHERE user_name LIKE ? OR user_username LIKE ?
    '''

    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()

        # Use parameterized queries to prevent SQL injection
        cursor.execute(query, ('%' + keyword + '%', '%' + keyword + '%'))

        # Fetch the results
        result = cursor.fetchall()

    # Extract user_ids from the result
    user_ids = [row[0] for row in result]

    return user_ids


def batch_insert_or_update_chat_member(params):
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()

        # Use a prepared statement for better performance
        update_query = f'''
            INSERT OR REPLACE INTO group_member (
                user_id, 
                chat_id, 
                user_name, 
                user_username, 
                is_premium, 
                is_verified,
                is_bot,
                is_fake,
                is_scam,
                is_restricted,
                restricted_reason,
                status,
                first_joined,
                last_joined,
                last_left,
                last_kicked,
                last_posted,
                last_banned,
                times_joined,
                times_posted,
                times_left,
                times_kicked,
                times_banned
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                COALESCE((SELECT first_joined FROM group_member WHERE user_id = ? AND chat_id = ?), ?), 
                ?,
                (SELECT last_left FROM group_member WHERE user_id = ? AND chat_id = ?),
                (SELECT last_kicked FROM group_member WHERE user_id = ? AND chat_id = ?),
                (SELECT last_posted FROM group_member WHERE user_id = ? AND chat_id = ?),
                (SELECT last_banned FROM group_member WHERE user_id = ? AND chat_id = ?),
                (SELECT times_joined FROM group_member WHERE user_id = ? AND chat_id = ?),
                (SELECT times_posted FROM group_member WHERE user_id = ? AND chat_id = ?),  
                (SELECT times_left FROM group_member WHERE user_id = ? AND chat_id = ?),  
                (SELECT times_kicked FROM group_member WHERE user_id = ? AND chat_id = ?),
                (SELECT times_banned FROM group_member WHERE user_id = ? AND chat_id = ?)
            )

        '''
        cursor.executemany(update_query, params)
        conn.commit()
    return





def batch_update_joined(user_ids, chat_id):
    if not user_ids:
        return
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        update_query = f'''
            UPDATE group_member
            SET times_joined = COALESCE((SELECT times_joined FROM group_member WHERE user_id = ? AND chat_id = ?) + 1, 1)
            WHERE user_id = ? AND chat_id = ?
        '''

        update_params = [
            (user_id, chat_id, user_id, chat_id)
            for user_id in user_ids
        ]            
        cursor.executemany(update_query, update_params)
        conn.commit()
    conn.close
    return


def batch_update_left(user_ids, chat_id):
    if not user_ids:
        return
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        update_query = f'''
            UPDATE group_member
            SET status = 'Left', last_left = ?, 
            times_left = COALESCE((SELECT times_left FROM group_member WHERE user_id = ? AND chat_id = ?) + 1, 1)
            WHERE user_id = ? AND chat_id = ?
        '''

        update_params = [
            (datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), user_id, chat_id, user_id, chat_id)
            for user_id in user_ids
        ]            
        cursor.executemany(update_query, update_params)
        conn.commit()
    conn.close
    return


def batch_update_kicked(user_ids, chat_id):
    if not user_ids:
        return
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        update_query = f'''
            UPDATE group_member
            SET status = 'Kicked', last_kicked = ?, 
            times_kicked = COALESCE((SELECT times_kicked FROM group_member WHERE user_id = ? AND chat_id = ?) + 1, 1)
            WHERE user_id = ? AND chat_id = ?
        '''

        update_params = [
            (datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), user_id, chat_id, user_id, chat_id)
            for user_id in user_ids
        ]            
        cursor.executemany(update_query, update_params)
        conn.commit()
    conn.close
    return


def batch_update_banned(user_ids, chat_id):
    if not user_ids:
        return
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        update_query = f'''
            UPDATE group_member
            SET
                last_banned = CASE
                    WHEN status = 'Banned' THEN COALESCE((SELECT last_banned FROM group_member WHERE user_id = ? AND chat_id = ?), NULL)
                    ELSE ?
                END,
                times_banned = CASE
                    WHEN status = 'Banned' THEN COALESCE((SELECT times_banned FROM group_member WHERE user_id = ? AND chat_id = ?), NULL)
                    ELSE COALESCE((SELECT times_banned FROM group_member WHERE user_id = ? AND chat_id = ?) + 1, 1)
                END,
                status = 'Banned'
            WHERE
                user_id = ? AND chat_id = ?
        '''

        update_params = [
            (user_id, chat_id, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), user_id, chat_id, user_id, chat_id, user_id, chat_id)
            for user_id in user_ids
        ]                      
        cursor.executemany(update_query, update_params)
        conn.commit()
    conn.close
    return


def update_left_groups():
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()

        # Get the most recent last_left timestamp from the left_group table
        cursor.execute("SELECT MAX(last_left) FROM left_group")
        most_recent_left_time = cursor.fetchone()[0]
        

        if most_recent_left_time is None:
            # Select records from group_member that meet the conditions
            cursor.execute(f'''
                SELECT
                    user_id,
                    chat_id,
                    last_joined,
                    last_left
                FROM
                    group_member
                WHERE
                    status = 'Left'
                    AND last_joined IS NOT NULL
                    AND last_left IS NOT NULL
            ''')

            records_to_insert = []
            for row in cursor.fetchall():
                user_id, chat_id, last_joined, last_left = row

                # Convert last_joined and last_left from string to datetime
                last_joined = datetime.strptime(last_joined, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=utc_timezone)
                last_left = datetime.strptime(last_left, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=utc_timezone)

                # Calculate time_in_group as HH:MM:SS
                time_in_group = str(last_left - last_joined)
                # print(f"INSERTING INTO LEFT GROUP DB: {user_id} - {time_in_group}")


                # Append the data to the list
                records_to_insert.append((user_id, chat_id, last_joined, last_left, time_in_group))
        else:

            # Select records from group_member that meet the conditions
            cursor.execute(f'''
                SELECT
                    user_id,
                    chat_id,
                    last_joined,
                    last_left
                FROM
                    group_member
                WHERE
                    status = 'Left'
                    AND last_joined IS NOT NULL
                    AND last_left IS NOT NULL
                    AND last_left > ?
                    
            
    ''', (most_recent_left_time, ))
            
            # Append to SQL query to specify a certain time in group under which to note
            #AND strftime('%s', last_left) - strftime('%s', last_joined) <= 300

            records_to_insert = []
            for row in cursor.fetchall():
                user_id, chat_id, last_joined, last_left = row

                # Convert last_joined and last_left from string to datetime
                last_joined = datetime.strptime(last_joined, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=utc_timezone)
                last_left = datetime.strptime(last_left, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=utc_timezone)

                # Calculate time_in_group as HH:MM:SS
                time_in_group = str(last_left - last_joined)
                #logging.warning(f"INSERTING INTO LEFT GROUP DB: {user_id} - {time_in_group}")


                # Append the data to the list
                records_to_insert.append((user_id, chat_id, last_joined, last_left, time_in_group))

        # Insert the selected records into the left_group table
        if records_to_insert:
            cursor.executemany('''
                INSERT INTO left_group (
                    user_id,
                    chat_id,
                    last_joined,
                    last_left,
                    time_in_group
                )
                VALUES (?, ?, ?, ?, ?)
            ''', records_to_insert)

        # Commit the changes
        conn.commit()
    return records_to_insert


def batch_update_blacklist():
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()

        # Get the most recent last_left timestamp from the left_group table
        cursor.execute("SELECT MAX(last_banned) FROM blacklist")
        most_recent_banned_time = cursor.fetchone()[0]
        

        if most_recent_banned_time is None:
            # Select records from group_member that meet the conditions
            cursor.execute(f'''
                SELECT
                    user_id,
                    chat_id,
                    last_banned,
                    times_banned
                FROM
                    group_member
                WHERE
                    status = 'Banned'
                    AND last_joined IS NOT NULL
                    AND last_left IS NOT NULL
            ''')

            records_to_insert = []
            for row in cursor.fetchall():
                user_id, chat_id, last_banned, times_banned = row

                # Convert last_joined and last_left from string to datetime
                last_banned = datetime.strptime(last_banned, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=utc_timezone)
                print(f"INSERTING INTO BLACKLIST: {user_id}")


                # Append the data to the list
                records_to_insert.append((user_id, chat_id, last_banned.strftime("%Y-%m-%d %H:%M:%S.%f"), times_banned))
        else:

            # Select records from group_member that meet the conditions
            cursor.execute(f'''
                SELECT
                    user_id,
                    chat_id,
                    last_banned,
                    times_banned
                FROM
                    group_member
                WHERE
                    status = 'Banned'
                    AND last_banned IS NOT NULL
                    AND times_banned IS NOT NULL
                    AND last_banned > ?
                    
            
    ''', (most_recent_banned_time, ))
            
            # Append to SQL query to specify a certain time in group under which to note
            #AND strftime('%s', last_left) - strftime('%s', last_joined) <= 300

            records_to_insert = []
            for row in cursor.fetchall():
                user_id, chat_id, last_banned, times_banned = row

                # Convert last_joined and last_left from string to datetime
                last_banned= datetime.strptime(last_banned, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=utc_timezone)

                print(f"INSERTING INTO BLACKLIST: {user_id}")


                # Append the data to the list
                records_to_insert.append((user_id, chat_id, last_banned.strftime("%Y-%m-%d %H:%M:%S.%f"), times_banned))

        # Insert the selected records into the left_group table
        if records_to_insert:
            cursor.executemany('''
                INSERT OR REPLACE INTO blacklist (
                    user_id,
                    channel_id,
                    last_banned,
                    ban_count
                )
                VALUES (?, ?, ?, ?)
            ''', records_to_insert)

        # Commit the changes
        conn.commit()
    return records_to_insert


def get_wholeft(chat_id):

    #query = f"SELECT user_id, chat_id, last_joined, last_left, time_in_group FROM left_group WHERE chat_id = {chat_id}"
    query = f"""
    SELECT lg.user_id, lg.chat_id, lg.last_joined, lg.last_left, lg.time_in_group, gm.user_name
    FROM left_group lg
    JOIN group_member gm ON lg.user_id = gm.user_id AND lg.chat_id = gm.chat_id
    WHERE lg.chat_id = {chat_id}
    """
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        wholeft_data = cursor.fetchall()
    return wholeft_data


def get_wholeft_from_private():

    #query = "SELECT user_id, chat_id, last_joined, last_left, time_in_group FROM left_group"
    query = f"""
    SELECT lg.user_id, lg.chat_id, lg.last_joined, lg.last_left, lg.time_in_group, gm.user_name
    FROM left_group lg
    JOIN group_member gm ON lg.user_id = gm.user_id AND lg.chat_id = gm.chat_id
    """
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        wholeft_data = cursor.fetchall()
    return wholeft_data


# Function to convert string to timedelta (when retrieving from SQLite)
def str_to_timedelta(duration_str):
    # Implement logic to convert your standardized format to timedelta
    # For example, you can check if the string contains 'days' and parse accordingly
    # This is a simplified example, and you may need to adapt it to your actual data
    if 'day' in duration_str:
        days, time_part = duration_str.split(', ')
        days = int(days.split()[0])
        time_part = time_part.split(':')
        hours, minutes = map(int, time_part[:-1])  # Extracting seconds separately
        seconds_with_fraction = float(time_part[-1])
        return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds_with_fraction)
    else:
        time_part = duration_str.split(':')
        hours, minutes = map(int, time_part[:-1])  # Extracting seconds separately
        seconds_with_fraction = float(time_part[-1])
        return timedelta(hours=hours, minutes=minutes, seconds=seconds_with_fraction)


def format_timedelta(timedelta_obj):
    total_seconds = int(timedelta_obj.total_seconds())
    
    # Display as seconds if less than a minute
    if total_seconds < 60:
        return f"{total_seconds} sec"
    
    # Otherwise, display as a formatted time string
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    # Display in the format HH:MM:SS
    return f"{hours:02}:{minutes:02}:{seconds:02}"


def insert_obligation_chat(chat_id, obligation_chat_id):
    # Update the authorized_chats table with the obligation_chat_id
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE authorized_chats
            SET obligation_chat = ?
            WHERE chat_id = ?
        ''', (obligation_chat_id, chat_id))
        conn.commit()
    return


def delete_obligation_chat(chat_id):
    # Update the authorized_chats table, setting obligation_chat to NULL
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE authorized_chats
            SET obligation_chat = NULL
            WHERE chat_id = ?
        ''', (chat_id, ))
        conn.commit()
    return


def lookup_obligation_chat(chat_id):
    # Update the authorized_chats table with the obligation_chat_id
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT obligation_chat 
            FROM authorized_chats
            WHERE chat_id = ?
        ''', (chat_id, ))
        obligation_chat_id = cursor.fetchone()
    return obligation_chat_id[0]


def insert_last_scan(chat_id, last_scan_date=None):
    if last_scan_date is None:
        last_scan_date = datetime.utcnow()
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE authorized_chats SET last_scan = ? WHERE chat_id = ?
        ''', (last_scan_date.strftime("%Y-%m-%d %H:%M:%S.%f"), chat_id))
        conn.commit()
    return


def lookup_last_scan(chat_id):
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT last_scan
            FROM authorized_chats
            WHERE chat_id = ?
        ''', (chat_id, ))
        last_scan = cursor.fetchone()
        last_scan_datetime = None
        if last_scan and last_scan[0]:
            last_scan_datetime = datetime.strptime(last_scan[0], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=utc_timezone)
    return last_scan_datetime


def insert_last_admin_update(chat_id, last_update=None):
    if last_update is None:
        last_update = datetime.now(timezone.utc)
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE authorized_chats SET last_admin_update = ? WHERE chat_id = ?
        ''', (last_update.strftime("%Y-%m-%d %H:%M:%S.%f"), chat_id))
        conn.commit()
    return


def lookup_last_admin_update(chat_id):
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT last_admin_update
            FROM authorized_chats
            WHERE chat_id = ?
        ''', (chat_id, ))
        last_admin_update = cursor.fetchone()
        last_admin_update_datetime = None
        if last_admin_update and last_admin_update[0]:
            last_admin_update_datetime = datetime.strptime(last_admin_update[0], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=utc_timezone)
    return last_admin_update_datetime


def import_blacklist_from_csv(csv_filename):
    try:
        with open(csv_filename, 'r', newline='') as csv_file:
            csv_reader = csv.reader(csv_file)
            next(csv_reader)  # Skip header row
            blacklist_data = [[row[0], row[1], row[3], row[4]] for row in csv_reader]
            # expected headers -- "CHAT ID", "USER ID", "USER NAME", "BAN COUNT", "MOST RECENT BAN"

        with sqlite3.connect(DATABASE_PATH) as conn:
            cursor = conn.cursor()
            cursor.executemany('''
                INSERT OR IGNORE INTO blacklist (channel_id, user_id, ban_count, last_banned)
                VALUES (?, ?, ?, ?)
            ''', blacklist_data)
            conn.commit()
        logging.info("Blacklist data imported successfully.")
        return blacklist_data
    except Exception as e:
        logging.error(f"Error importing blacklist data: {e}")
        return []
    

def extract_user_data(user_obj):
    user_data = {
        'status': 'Not Available'
    }
    if hasattr(user_obj, 'id'):  # Telethon User object, produced by either get_entity() or iter_participants()
        user_data['id'] = user_obj.id
        user_data['first_name'] = user_obj.first_name
        user_data['last_name'] = user_obj.last_name
        user_data['full_name'] = f"{user_obj.first_name}{' ' + user_obj.last_name if user_obj.last_name else ''}"
        user_data['username'] = user_obj.username
        user_data['premium'] = user_obj.premium
        user_data['verified'] = user_obj.verified
        user_data['deleted'] = user_obj.deleted
        user_data['bot'] = user_obj.bot
        user_data['fake'] = user_obj.fake
        user_data['scam'] = user_obj.scam
        user_data['restricted'] = user_obj.restricted
        user_data['restricted_reason'] = user_obj.restriction_reason if user_obj.restriction_reason else None

        if hasattr(user_obj, 'participant'): #Produced by iter_participants()
            participant = user_obj.participant
            user_data['join_date'] = participant.date if  hasattr(participant, 'date') else None
            user_data['join_date_string'] = participant.date.strftime("%Y-%m-%d %H:%M:%S.%f") if hasattr(participant, 'date') else None
            if isinstance(participant, ChannelParticipantAdmin) or isinstance(participant, ChatParticipantAdmin):
                user_data['status'] = "Admin"
            elif isinstance(participant, ChannelParticipantCreator) or isinstance(participant, ChatParticipantCreator):
                user_data['status'] = "Creator"
            elif isinstance(participant, ChannelParticipant) or isinstance(participant, ChatParticipant):
                user_data['status'] = "Member"


    elif hasattr(user_obj, 'status'):  #Python-telegram-bot ChatMember Object
        user = user_obj.user
        user_data['id'] = user.id
        user_data['first_name'] = user.first_name
        user_data['last_name'] = user.last_name
        user_data['full_name'] = f"{user.first_name}{' ' + user.last_name if user.last_name else ''}"
        user_data['username'] = user.username
        user_data['premium'] = user.is_premium
        user_data['bot'] = user.is_bot

        if user_obj.status in ["administrator"]:
            user_data['status'] = "Admin"
        elif user_obj.status in ["kicked"]:
            user_data['status'] = "Banned"
        elif user_obj.status in ["left"]:
            user_data['status'] = "Left"
        elif user_obj.status in ["member"]:
            user_data['status'] = "Member"
        elif user_obj.status in ["creator"]:
            user_data['status'] = "Creator"
        else:
            user_data['status'] = "Not Available"
    else:
        raise ValueError("Unsupported user object type")
    return user_data



def update_or_insert_group_member(chat_id, tg_object, event:EventType=None, join_date=None):
    user_data = extract_user_data(tg_object)
    user_id = user_data.get('id')
    user_status = user_data.get('status')
    now_string = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")

    # Discover whether user has an existing record with a first_joined or last_joined datestamp
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT first_joined, last_joined FROM group_member
            WHERE user_id = ? AND chat_id = ?
            LIMIT 1
            """, (user_id, chat_id)
        )
        row=cursor.fetchone()
        preexisting_last_joined_date = None
        if row:
            if row[1]:
                 preexisting_last_joined_date = row[1] # last_joined field from existing record
            elif row[0]:
                 preexisting_last_joined_date = row[0] # first_joined field from existing record

        # If a last_joined date is present in user_data, use that. Otherwise, use pre-existing database timestamps, otherwise use now
        user_data_joined_date = user_data.get('join_date_string')
        if join_date:
            last_joined = join_date.strftime("%Y-%m-%d %H:%M:%S.%f")
        elif user_data_joined_date:
            last_joined = user_data_joined_date
        elif preexisting_last_joined_date:
            last_joined = preexisting_last_joined_date
        else:
            last_joined = now_string


        # Check if the specific combination of user_id and chat_id already exists
        cursor.execute(
            f"""
            SELECT status FROM group_member
            WHERE user_id = {user_id} AND chat_id = {chat_id}
            LIMIT 1
            """
        )
        row = cursor.fetchone()

        # If chat status could not be determined fromt he telegram object, retain the status that currently exists in the database
        if row and user_status == 'Not Available':
            user_status = row[0]

        # If no record for the user_id/chat_id, create one and update status 
        if not row:
            insert_or_replace_group_member_with_user_data(chat_id, user_data) 

        elif event == EventType.JOINED:  #Rejoining group, execute only if record currently exists
                cursor.execute(
                    f"""
                    UPDATE group_member
                    SET last_joined = ?, status = ?, times_joined = COALESCE((SELECT times_joined FROM group_member WHERE user_id = ? AND chat_id = ?) + 1, 1)
                    WHERE user_id = ? AND chat_id = ?
                    """,
                    (last_joined, user_status, user_id, chat_id, user_id, chat_id)
                )       

        # Depending on the event, update dates and counts in database
        # If the event is something other than JOINED, it's block needs to run whether the record previously existed or was just created
        if event == EventType.POSTED:
            cursor.execute(
                f"""
                UPDATE group_member
                SET last_posted = ?, status = ? , times_posted = COALESCE((SELECT times_posted FROM group_member WHERE user_id = ? AND chat_id = ?) + 1, 1)
                WHERE user_id = ? AND chat_id = ?
                """,
                (now_string, user_status, user_id, chat_id, user_id, chat_id)
            )  
                
        elif event == EventType.LEFT:
            cursor.execute(
                f"""
                UPDATE group_member
                SET last_left = ?, status = ? , times_left = COALESCE((SELECT times_left FROM group_member WHERE user_id = ? AND chat_id = ?) + 1, 1)
                WHERE user_id = ? AND chat_id = ?
                """,
                (now_string, user_status, user_id, chat_id, user_id, chat_id)
            )  
                            
        elif event == EventType.KICKED:
            cursor.execute(
                f"""
                UPDATE group_member
                SET last_kicked = ?, status = ? , times_kicked = COALESCE((SELECT times_kicked FROM group_member WHERE user_id = ? AND chat_id = ?) + 1, 1)
                WHERE user_id = ? AND chat_id = ?
                """,
                (now_string, user_status, user_id, chat_id, user_id, chat_id)
            )    
            
        elif event == EventType.BANNED:
            cursor.execute(
                f"""
                UPDATE group_member
                SET last_banned = ?, status = ? , times_banned = COALESCE((SELECT times_banned FROM group_member WHERE user_id = ? AND chat_id = ?) + 1, 1)
                WHERE user_id = ? AND chat_id = ?
                """,
                (now_string, user_status, user_id, chat_id, user_id, chat_id)
            )    

        conn.commit()
    return

def insert_or_replace_group_member_with_user_data(chat_id, user_data):
    # Prepare variables from user_data dict
    user_id = user_data.get('id')
    user_name = user_data.get('full_name')
    user_username = user_data.get('username')
    is_premium = user_data.get('premium')
    is_verified = user_data.get('verified')
    is_bot = user_data.get('bot')
    is_fake = user_data.get('fake')
    is_scam = user_data.get('scam')
    is_restricted = user_data.get('restricted')
    restriction_reason = user_data.get('restriction_reason')
    user_status = user_data.get('status')
    join_date_string = user_data.get('join_date_string')

    # Insert or replace record into db
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO group_member (
                       user_id, 
                       chat_id, 
                       user_name, 
                       user_username, 
                       is_premium, 
                       is_verified,
                       is_bot,
                       is_fake,
                       is_scam,
                       is_restricted,
                       restricted_reason,
                       status,
                       first_joined,
                       last_joined,
                       times_joined
                      )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1 )
        ''', (user_id, 
              chat_id, 
              user_name,
              user_username,
              is_premium,
              is_verified,
              is_bot,
              is_fake,
              is_scam,
              is_restricted,
              restriction_reason,
              user_status,
              join_date_string,
              join_date_string,
              ))
        conn.commit()
    return

