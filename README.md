# KickBot - Your Partner in Lurker-Slaughter
### Authored by Shinanygans (shinanygans@proton.me)

This bot will gather data over time about which users are in your group and the last time they posted media.
You can command this bot to eject from your group those who have not posted in a given time span or have never posted.

## Prerequisites

Before you begin, ensure you have met the following requirements:

- **Python 3.10 or higher**: If you don't have Python installed, you can download it from the [official Python website](https://www.python.org/downloads/).

## Installation

Follow these steps to set up your project:

1. Clone this repository to your local machine:

    ```shell
    git clone https://github.com/shnygns/kickbot.git
    ```

2. Navigate to the project directory:

    ```shell
    cd kickbot
    ```

3. If you prefer using Pipenv:

    - Install Pipenv (if not already installed):

        ```shell
        pip3 install pipenv
        ```

    - Create a virtual environment and install dependencies:

        ```shell
        pipenv install 
        ```
        ...or, so specify the python version overtly:

        ```shell
        pipenv install --python 3.10
        ```


    - Activate the virtual environment:

        ```shell
        pipenv shell
        ```

4. If you prefer using venv and requirements.txt:

    - Create a virtual environment:

        ```shell
        python3 -m venv venv
        ```

    - Activate the virtual environment:

        - On Windows:

            ```shell
            .\venv\Scripts\activate
            ```

        - On macOS and Linux:

            ```shell
            source venv/bin/activate
            ```

    - Install dependencies:

        ```shell
        pip install -r requirements.txt
        ```

5. Make a copy of the template file `sample-config.py` and rename it to `config.py`:

    ```shell
    cp sample-config.py config.py
    ```

6. Open `config.py` and configure the bot token and any other necessary settings.


7. IMPORTANT - Configure named admins in config.py:
This bot works by vacuuming up information on users and posts that it sees in your groups. It stories these data in a
SQLite flat file. If other people find your KickBot through a Telegram search and run it in their rooms, THEIR DATA WILL BE
STORED IN YOUR DATABASE! This is no bueno.

To stop this from happening, put your Telegram user_id in the AUTHORIZED_ADMINS list in config.py. If there are user_ids in 
this list, then only these user_ids will be able to issue bot commands, and the bot will only vacuum up information from chats
in which at least one user_id on this list is an admin. Problem solved.

AUTHORIZED_ADMINS = [XXXXXXXXXX, XXXXXXXXXX]


8. Run the script from your virtual environment shell:

    ```shell
    python kickbot.py
    ```

## Getting a Bot Token

To run your Telegram bot, you'll need a Bot Token from the Telegram BotFather. Follow these steps to obtain one:

1. Open the Telegram app and search for the "BotFather" bot.

2. Start a chat with BotFather and use the `/newbot` command to create a new bot.

3. Follow the instructions to choose a name and username for your bot.

4. Once your bot is created, BotFather will provide you with a Bot Token. Copy this token.

5. In the `config.py` file, set the `BOT_TOKEN` variable to your Bot Token.

## Usage

Invite this bot to your group as an admin. Make sure the bot's settings in Bot Father give it full privilidges.
The bot will store user information in its database as people interact in your group.

When people upload media (pics, videos and other documents), kickbot will track the most recent timestamp.

When a group admin issues the /inactivekick <time> command (like "/inactivekick 1d"), the bot search its database for known
users who have not posted media within that timeframe or have never posted media. 

They will be kicked from the group, and you will be given a summary of the number of people kicked. If you kick fewer than 10 users,
you will see them named individually.

Here are some of the commands you can use:

/help - Show this help message.\n\n"
/inactivekick <time> - Kick inactive users who haven't posted media in the specified time.
/pretendkick <time> - Simulate kicking inactive users without actually kicking them.

For example, the command /inactivekick 1d would kick all who have not posted in the last day, or who have never posted.

<time> units use (s)econds, (m)inutes, (h)ours, (d)ays, (M)onths, or (y)ears.


## Configuration and Features

Do NOT forget to make a copy of sample-config.py and call it config.py. The program will error out if you don't. 
The config file is where you store your bot token from Bot Father. But you can also configure other special features!


### Authorized Admins

AUTHORIZED_ADMINS = [XXXXXXXXXX, XXXXXXXXXX]

If this list is empty, your bot may be used by anyone who is an admin in a chat where the bot is operating. You can fill this 
list with authorized User IDs (separated by commas, no quotes) that are acceptable for use. 

Only those users listed will be able to command the bot, and the bot will only work in rooms where someone on the list is an admin.


### Debug Chats

DEBUG_CHATS = [-XXXXXXXXXX, -100XXXXXXXXXX]

Sometimes you may want to test KickBot in a small group before rolling it out to a bigger group.

Chats in this list will receive special debug messages in the chat when certain events are triggered.
These debug messages are visible only in the chats listed.


### Message Configuration

You have total control over the messages that Kickbot issues. I've included some sample messaging for fun. Those messages
can be changed in the config.py file.


## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.


## Acknowledgments

This bot was built using the python-telegram-bot library and API wrapper. It also uses tqdm for its progress bars.


## Support
This script is provided AS-IS without warranties of any kind. I am exceptionally lazy, and fixes/improvements will proceed in
direct proportion to how much I like you.

"Son...you're on your own." --Blazing Saddles



