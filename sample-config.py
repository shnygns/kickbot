#! /usr/bin/python
"""
CONFIG.PY

This file stores all Telegram API session information and config settings.
"""

""" CREDENTIALS """
BOT_TOKEN = ""

""" SETTINGS """

# If this list is empty, your bot may be used by anyone who is an admin in a chat where the bot is operating. You can fill this 
# list with authorized User IDs (separated by commas, no quotes) that are acceptable for use. 
# Only those users listed will be able to command the bot, and the bot will only work in rooms where someone on the list is an admin.
AUTHORIZED_ADMINS = []


# Chats in this list will receive special debug messages in the chat when certain events are triggered.
# These debug messages are visible only in the chats listed.
DEBUG_CHATS = []


""" DEBUG MESSAGES """
DEBUG_CAPTURE_MESSAGE = f"I have taken notice of your insignificant presence and have added you to the Doomsday Database."
DEBUG_ADMIN_MESSAGE = f"your offering would acceptable to me. But you are an admin so you don't count."
DEBUG_UPDATE_MESSAGE = f"your offering was acceptable to me. I am updating your most recent activity."
DEBUG_INACTIVE_PURGE = f"you are a lazy dipshit who doesn't post often enough. KICKED!"
DEBUG_NEVER_POSTED = f"you are a lurking asshole who doesn't post shit. KICKED!"

""" ADMIN MESSAGES """
START_PURGE = "You ungrateful fuckwits have angered the gods with your lazy content sharing."

HELP_MESSAGE =  (f"💥 Welcome to KickBot, your partner in lurker-slaughter 💥\n\n"
                "Here are some of the commands you can use:\n\n"
                "/help - Show this help message.\n\n"
                "/inactivekick <time> - Kick inactive users who haven't posted media in the specified time.\n\n"
                "/pretendkick <time> - Simulate kicking inactive users without actually kicking them.\n\n"
                "For example, the command /inactivekick 1d would kick all who have not posted in the last day, or who have never posted.\n\n"
                "<time> units use (s)econds, (m)inutes, (h)ours, (d)ays, (M)onths, or (y)ears."
)
