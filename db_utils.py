import sqlite3
import pytz
import csv
import logging
from datetime import datetime, timedelta
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
                last_scan TIMESTAMP
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


def delete_authorized_chat(chat_id):
    try:
        with sqlite3.connect(DATABASE_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM authorized_chats WHERE chat_id = ?", (chat_id,))
            conn.commit()
    except Exception as e:
        print(f"Error deleting authorized chat: {e}")
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

# Add user_id / chat_id combination to the database
def insert_user_in_db(user_id, chat_id, table):
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        conn.execute('BEGIN')

        # Check if the specific combination of user_id and chat_id already exists
        cursor.execute(
            f"""
            SELECT 1 FROM {table}
            WHERE user_id = {user_id} AND channel_id = {chat_id}
            LIMIT 1
            """
        )
        row = cursor.fetchone()

        if not row:
            # The combination doesn't exist, so insert it
            cursor.execute(
                f"""
                INSERT INTO {table} (user_id, channel_id)
                VALUES ({user_id}, {chat_id})
                """
            )
            conn.commit()
            
        else:
            # The combination already exists
            conn.rollback()  # Roll back the transaction to discard the changes
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


def batch_update_db(user_ids, chat_id, status):
    if not user_ids:
        return
    additional_query_string = ""
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
            
        if status == "Left":
            field = "last_left"
            additional_query_string = (
                """
                , times_left = COALESCE((SELECT times_left FROM group_member WHERE user_id = ? AND chat_id = ?) + 1, 1)
                """
            )
        elif status == "Kicked":
            field = "last_kicked"
            additional_query_string = (
                """
                , times_kicked = COALESCE((SELECT times_kicked FROM group_member WHERE user_id = ? AND chat_id = ?) + 1, 1)
                """
            )

        # Use a prepared statement for better performance
        update_query = f'''
            UPDATE group_member
            SET status = ?, {field} = ? {additional_query_string}
            WHERE user_id = ? AND chat_id = ?
        '''

        # Batch execute the prepared statement
        update_params = [
            (status, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), user_id, chat_id, user_id, chat_id)
            for user_id in user_ids
        ]            
        cursor.executemany(update_query, update_params)
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


def insert_chat_member(telethon_chat_member, chat_id, status=None):
    user_id = telethon_chat_member.id
    user_name = f"{telethon_chat_member.first_name}{' ' + telethon_chat_member.last_name if telethon_chat_member.last_name else ''}"
    user_username = telethon_chat_member.username
    is_premium = telethon_chat_member.premium
    is_verified = telethon_chat_member.verified
    is_bot = telethon_chat_member.bot
    is_fake = telethon_chat_member.fake
    is_scam = telethon_chat_member.scam
    is_restricted = telethon_chat_member.restricted
    restriction_reason = telethon_chat_member.restriction_reason if telethon_chat_member.restriction_reason else None

    if status:
        user_status = status
    elif not hasattr(telethon_chat_member, 'participant'):
        user_status = 'Not Available'
    elif isinstance(telethon_chat_member.participant, ChannelParticipantAdmin):
        user_status = 'Admin'
    elif isinstance(telethon_chat_member.participant, ChannelParticipantCreator):
        user_status = 'Creator'
    elif isinstance(telethon_chat_member.participant, ChannelParticipant):
        user_status = 'Member'
    elif isinstance(telethon_chat_member.participant, ChatParticipantAdmin):
        user_status = 'Admin'
    elif isinstance(telethon_chat_member.participant, ChatParticipantCreator):
        user_status = 'Creator'
    elif isinstance(telethon_chat_member.participant, ChatParticipant):
        user_status = 'Member'
    else:
        user_status = 'Not Available'

    join_date = telethon_chat_member.participant.date.strftime("%Y-%m-%d %H:%M:%S.%f") if (hasattr(telethon_chat_member, 'participant') and hasattr(telethon_chat_member.participant, 'date')) else None

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
              join_date,
              join_date,
              ))
        conn.commit()
    return


def update_or_insert_chat_member(telethon_chat_member, chat_id, field=None, value=None, status=None):
    user_id = telethon_chat_member.id
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT first_joined, last_joined FROM group_member
            WHERE user_id = {user_id} AND chat_id = {chat_id}
            LIMIT 1
            """
        )
        row=cursor.fetchone()
        preexisting_last_joined_date = None
        if row:
            if row[1]:
                 preexisting_last_joined_date = row[1]
            elif row[0]:
                 preexisting_last_joined_date = row[0]
                
        if hasattr(telethon_chat_member, 'participant') and hasattr(telethon_chat_member.participant, 'date'):
            last_joined = telethon_chat_member.participant.date
        else:
            last_joined = preexisting_last_joined_date

        if field == "last_posted":
            additional_query_string = (
                f"""
                , times_posted = COALESCE((SELECT times_posted FROM group_member WHERE user_id = {user_id} AND chat_id = {chat_id}) + 1, 1)
                """
            )
        if field == "last_left":
            additional_query_string = (
                f"""
                , times_left = COALESCE((SELECT times_left FROM group_member WHERE user_id = {user_id} AND chat_id = {chat_id}) + 1, 1)
                """
            )
        if field == "last_banned":
            additional_query_string = (
                f"""
                , times_banned = COALESCE((SELECT times_banned FROM group_member WHERE user_id = {user_id} AND chat_id = {chat_id}) + 1, 1)
                """
            )
        if field == "last_kicked":
            additional_query_string = (
                f"""
                , times_kicked = COALESCE((SELECT times_kicked FROM group_member WHERE user_id = {user_id} AND chat_id = {chat_id}) + 1, 1)
                """
            )
        # last_joined = None
        if field == 'last_left':
            user_status = 'Left'
        elif field == 'last_banned':
            user_status = 'Banned'
        elif field == 'last_kicked':
            user_status = 'Kicked'
        elif status=='Member':
            user_status=status
            last_joined = datetime.utcnow()
        elif status:
            user_status=status
        elif not hasattr(telethon_chat_member, 'participant'):
            user_status = 'Not Available'
        elif isinstance(telethon_chat_member.participant, ChannelParticipantAdmin):
            user_status = 'Admin'
           # last_joined = telethon_chat_member.participant.date if hasattr(telethon_chat_member.participant, 'date') else None
        elif isinstance(telethon_chat_member.participant, ChannelParticipantCreator):
            user_status = 'Creator'
           # last_joined = telethon_chat_member.participant.date if hasattr(telethon_chat_member.participant, 'date') else None
        elif isinstance(telethon_chat_member.participant, ChannelParticipant):
            user_status = 'Member'
           # last_joined = telethon_chat_member.participant.date if hasattr(telethon_chat_member.participant, 'date') else None
        elif isinstance(telethon_chat_member.participant, ChatParticipantAdmin):
            user_status = 'Admin'
           # last_joined = telethon_chat_member.participant.date if hasattr(telethon_chat_member.participant, 'date') else None
        elif isinstance(telethon_chat_member.participant, ChatParticipantCreator):
            user_status = 'Creator'
           # last_joined = telethon_chat_member.participant.date if hasattr(telethon_chat_member.participant, 'date') else None
        elif isinstance(telethon_chat_member.participant, ChatParticipant):
            user_status = 'Member'
           # last_joined = telethon_chat_member.participant.date if hasattr(telethon_chat_member.participant, 'date') else None
        else:
            user_status = 'Not Available'
            #last_joined = telethon_chat_member.participant.date if hasattr(telethon_chat_member.participant, 'date') else None

        if isinstance(last_joined, datetime):
            last_joined = last_joined.strftime('%Y-%m-%d %H:%M:%S.%f') 
        

        # Check if the specific combination of user_id and chat_id already exists
        cursor.execute(
            f"""
            SELECT 1 FROM group_member
            WHERE user_id = {user_id} AND chat_id = {chat_id}
            LIMIT 1
            """
        )
        row = cursor.fetchone()

        if not row and status:
            insert_chat_member(telethon_chat_member, chat_id)
            cursor.execute(
            f"""
            UPDATE group_member
            SET status = ?
            WHERE user_id = ? AND chat_id = ?
            """,
            (user_status, user_id, chat_id),
        )               
        elif not row:
            insert_chat_member(telethon_chat_member, chat_id)
        elif field is None:
            cursor.execute(
                f"""
                UPDATE group_member
                SET last_joined = ?, status = ?, times_joined = COALESCE((SELECT times_joined FROM group_member WHERE user_id = ? AND chat_id = ?) + 1, 1)
                WHERE user_id = ? AND chat_id = ?
                """,
                (last_joined, user_status, user_id, chat_id, user_id, chat_id),
            )  
        else:
             cursor.execute(
                f"""
                UPDATE group_member
                SET {field} = ?, status = ? {additional_query_string}     
                WHERE user_id = ? AND chat_id = ?
                """,
                (value, user_status, user_id, chat_id),
            )      
        conn.commit()
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
                logging.warning(f"INSERTING INTO LEFT GROUP DB: {user_id} - {time_in_group}")


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