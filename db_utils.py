import sqlite3
import pytz
from datetime import datetime
from config import DATABASE_PATH
from telegram.constants import ChatType
from telegram import ChatMember

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
                three_strikes_mode BOOLEAN DEFAULT FALSE
            )
        """
        )


        # Create the index on user_id and channel_id
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS user_activity_index ON user_activity (user_id, channel_id);
            """
        )


    # Create the events table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                chat_id INTEGER,
                is_admin INTEGER,
                timestamp TIMESTAMP,
                event_type TEXT,
                FOREIGN KEY (user_id, chat_id) REFERENCES chat_member (user_id, chat_id)
            )
        ''')


    # Create the chat_member table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chat_member (
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
                last_posted TIMESTAMP,
                number_kicks INTEGER,
                PRIMARY KEY (user_id, chat_id)
            )
        ''')
        conn.commit()
    return


# ********* CHAT AUTHORIZATION COMMANDS *********

def is_chat_authorized(chat_id):
    try:
        with sqlite3.connect(DATABASE_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM authorized_chats WHERE chat_id = ?", (chat_id,))
            return cursor.fetchone() is not None
    except Exception as e:
        print(f"Error checking if chat is authorized: {e}")
        return False
    

def insert_authorized_chat(chat_id):
    try:
        with sqlite3.connect(DATABASE_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO authorized_chats (chat_id) VALUES (?)", (chat_id,))
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





# ********* USER ACTIVITY COMMANDS *********

# Add user_id / chat_id combination to the database
async def insert_user_in_db(user_id, chat_id, table):
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
        cursor.execute("SELECT DISTINCT channel_id FROM user_activity")
        chat_ids_in_database = {row[0] for row in cursor.fetchall()}
    return chat_ids_in_database


def del_chats_from_db(chat_list):
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()

        # Get a list of unique chat_ids from the database
        for chat_id in chat_list:
            cursor.execute("DELETE FROM user_activity WHERE channel_id = ?", (chat_id,))
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

# ********* KICKED USER DB COMMANDS *********


def lookup_user_in_kick_db(user_id, chat_id):
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute( "SELECT * FROM kicked_users WHERE user_id = ? AND channel_id = ?", (user_id, chat_id))
        kicked_user_data = cursor.fetchone()
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
        """, (user_id, chat_id, user_id, chat_id, last_activity_str, datetime.utcnow()))
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


# ********* CHAT MEMBER AND EVENTS COMMANDS *********

def insert_chat_member(context_chat_member, telethon_chat_member, chat_id):
    user_id = telethon_chat_member.id
    user_name = f"{telethon_chat_member.first_name}{' ' + telethon_chat_member.last_name if telethon_chat_member.last_name else ''}"
    user_username = telethon_chat_member.username
    is_premium = telethon_chat_member.premium
    is_verified = telethon_chat_member.verified
    is_bot = telethon_chat_member.bot
    is_fake = telethon_chat_member.fake
    is_scam = telethon_chat_member.scam
    is_premium = telethon_chat_member.premium
    is_restricted = telethon_chat_member.restricted
    restricted_reason = telethon_chat_member.restricted_reason if telethon_chat_member.restricted_reason else None
    first_joined = telethon_chat_member.participant.date.strftime("%Y-%m-%d %H:%M:%S.%f")

    if context_chat_member:
        if context_chat_member.status == ChatMember.ADMINISTRATOR:
            user_status = "Admin"
        elif context_chat_member.status == ChatMember.BANNED:
            user_status = "Banned"
        elif context_chat_member.status == ChatMember.LEFT:
            user_status = "Left"
        elif context_chat_member.status == ChatMember.MEMBER:
            user_status = "Member"
        elif context_chat_member.status == ChatMember.OWNER:
            user_status = "Owner"
        elif context_chat_member.status == ChatMember.RESTRICTED:
            user_status = "Restricted"
        else:
            user_status = "None"
    else:
            user_status = "None"


    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT last_joined, last_left FROM chat_member
            WHERE user_id = ? AND chat_id = ?
            """,
            (user_id, chat_id),
        )
        row = cursor.fetchone()
        
        if row:
            last_joined = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=utc_timezone) if row[0] else None #Last joined date defaults to reviously recorded value.
            if row[1] and datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=utc_timezone) > last_joined: #Most recent action = LEFT
                last_joined = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f") #Therefore user is rejoining, last_joined updates to now
        else:
            last_joined = telethon_chat_member.participant.date.strftime("%Y-%m-%d %H:%M:%S.%f") #User not in DB, last joined date = original join date

        cursor.execute('''
            INSERT OR REPLACE INTO chat_member (
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
                       last_joined
                      )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
              restricted_reason,
              user_status,
              first_joined,
              last_joined
              ))
        conn.commit()
    return




def insert_event(user_id, chat_id, is_admin, timestamp, event_type):
    # Insert a new event into the database
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO events (user_id, chat_id, is_admin, timestamp, event_type)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, chat_id, is_admin, timestamp, event_type))
        conn.commit()


def get_user_last_event(user_id, chat_id):
    # Retrieve the last event for a user in a specific chat
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM events
            WHERE user_id = ? AND chat_id = ?
            ORDER BY timestamp DESC
            LIMIT 1
        ''', (user_id, chat_id))
        return cursor.fetchone()