####################
# BTC TELEGRAM BOT #
####################
import time
import configparser
import logging
import asyncio
import numpy
import aiohttp
import mysql.connector
import bybit

# Get database config
config = configparser.RawConfigParser()
config.read("./database.ini")
dbhost = config.get("Database", "dbhost")
database = config.get("Database", "database")
dbuser = config.get("Database", "dbuser")
dbpass = config.get("Database", "dbpass")

# Get the database running
db = mysql.connector.connect(host=dbhost,
                             database=database,
                             user=dbuser,
                             password=dbpass)

cursor = db.cursor(dictionary=True, buffered=True)

# Get configs from database
# Telegram token
sqlquery = "SELECT config_value FROM configs WHERE config_name = 'telegram_token'"
cursor.execute(sqlquery)
records = cursor.fetchone()
tgbot_token = records["config_value"]

# Initiate
price_index = None
price_bitmex = 0
price_deribit = 0
price_bybit = 0
price_bitstamp = 0
price_coinbase = 0
price_binance = 0
last_time_exec = time.time()

##############
# user class #
##############
class User:
    def __init__(self, user_id, telegram_id, user_name, enabled,
                 price_steps, announced_price, price_level, last_position_size, client):

        self.id = user_id
        self.telegram_id = telegram_id
        self.name = user_name
        self.is_enabled = enabled
        self.price_steps = price_steps
        self.announced_price = announced_price
        self.price_level = price_level
        self.ask_price_steps = False
        self.client = client
        self.last_position_size = last_position_size
        self.history = []

        # Fill history
        while len(self.history) < 600:
            self.history.append(price_steps)

# Get enabled users from database
sqlquery = "select * from users"
cursor.execute(sqlquery)
records = cursor.fetchall()

# Create list of active users
user_list = []
for user in records:
    # Get trade_client from database
    sqlquery = "select * from api_keys where user_id = {}".format(user["user_id"])
    cursor.execute(sqlquery)
    key_pairs = cursor.fetchall()

    client = None
    for pair in key_pairs:
        if pair["name"] == "bybit":
            api_key = pair["api_key"]
            api_secret = pair["api_secret"]
            client = bybit.bybit(test=False, api_key=api_key, api_secret=api_secret)

    user_object = User(user["user_id"],
                       user["telegram_id"],
                       user["user_name"],
                       user["enabled"],
                       user["price_steps"],
                       user["announced_price"],
                       user["price_level"],
                       user["last_position_size"],
                       client)

    user_list.append(user_object)

############
# Database #
############
def execute_query(sqlquery):
    try:
        db = mysql.connector.connect(host=dbhost,
                                     database=database,
                                     user=dbuser,
                                     password=dbpass)

        cursor = db.cursor(dictionary=True, buffered=True)
        cursor.execute(sqlquery)
        db.commit()
        db.close()
    except mysql.connector.Error as error:
        logger.info("DB Error: {}".format(error))

############
# Telegram #
############
async def get_messages(offset):
    try:
        async with aiohttp.ClientSession() as session:
            offset_url = "https://api.telegram.org/bot{}/getUpdates?offset={}".format(tgbot_token, offset)
            async with session.get(offset_url) as response:
                bot_messages = await response.json()
                return bot_messages
    except Exception as error:
        logger.info("Error: Telegram API failed!")
        logger.debug(error)
        return None


# Send message to a chat
async def send_message(telegram_id, message):
    try:
        async with aiohttp.ClientSession() as session:
            message_url = "https://api.telegram.org/bot{}/sendMessage?chat_id={}&text={}".format(tgbot_token,
                                                                                                 telegram_id, message)
            async with session.get(message_url) as response:
                for user in user_list:
                    if telegram_id == user.telegram_id:
                        logger.info("Send message to {}: {}".format(user.name, message))
                        await response.json()
                        return True

    except Exception as error:
        logger.info("Error: Could not set message!")
        logger.debug(error)
        return False


def setup_logger():
    # Prints logger info to terminal
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)  # Change this to DEBUG if you want a lot more info
    ch = logging.StreamHandler()
    # create formatter
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


# Setup logger
logger = setup_logger()

##############
# Price APIs #
##############
async def get_latest_bitcoin_price(source):
    global price_bitmex
    global price_deribit
    global price_bybit
    global price_bitstamp
    global price_coinbase
    global price_binance

    while True:
        # Price source Bitmex
        if source == "bitmex":
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                            'https://www.bitmex.com/api/v1/trade?symbol=XBT&count=1&reverse=true') as response:
                        response_json = await response.json()
                        price_bitmex = int(response_json[0]["price"])
                        logger.debug(("Bitmex Price:", price_bitmex))
                        await asyncio.sleep(10)
            except Exception as error:
                price_bitmex = None
                logger.debug("Bitmex went shit!")
                logger.debug(error)
                await asyncio.sleep(10)

        # Price source Deribit
        if source == "deribit":
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                            'https://www.deribit.com/api/v2/public/ticker?instrument_name=BTC-PERPETUAL') as response:
                        response_json = await response.json()
                        price_deribit = int(response_json["result"]["last_price"])
                        logger.debug(("Deribit Price:", price_deribit))
                        await asyncio.sleep(3)
            except Exception as error:
                price_deribit = None
                logger.debug("Deribit went shit!")
                logger.debug(error)
                await asyncio.sleep(10)

        # Price source bybit
        elif source == "bybit":
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get('https://api.bybit.com/v2/public/tickers') as response:
                        response_json = await response.json()
                        price_bybit = int(float((response_json["result"][0]["last_price"])))
                        logger.debug(("Bybit Price:", price_bybit))
                        await asyncio.sleep(3)
            except Exception as error:
                price_deribit = None
                logger.debug("Bybit went shit!")
                logger.debug(error)
                await asyncio.sleep(10)

        # Price source bitstamp
        elif source == "bitstamp":
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get('https://www.bitstamp.net/api/v2/ticker/BTCUSD/') as response:
                        response_json = await response.json()
                        price_bitstamp = int(float((response_json["last"])))
                        logger.debug(("Bitstamp Price:", price_bitstamp))
                        await asyncio.sleep(3)
            except Exception as error:
                price_deribit = None
                logger.debug("Bitstamp went shit!")
                logger.debug(error)
                await asyncio.sleep(10)

        # Price source coinbase
        elif source == "coinbase":
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get('https://api.pro.coinbase.com/products/BTC-USD/ticker') as response:
                        response_json = await response.json()
                        price_coinbase = int(float((response_json["price"])))
                        logger.debug(("Coinbase Price:", price_coinbase))
                        await asyncio.sleep(3)
            except Exception as error:
                price_deribit = None
                logger.debug("Coinbase went shit!")
                logger.debug(error)
                await asyncio.sleep(10)

        # Price source binance futures
        elif source == "binance":
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get('https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT') as response:
                        response_json = await response.json()
                        price_binance = int(float((response_json["price"])))
                        logger.debug(("Binance Price:", price_binance))
                        await asyncio.sleep(3)
            except Exception as error:
                price_deribit = None
                logger.debug("Binance went shit!")
                logger.debug(error)
                await asyncio.sleep(10)


###############
# Price Index #
###############
async def price_index_calc():
    global price_index, last_time_exec
    last_index = 0
    while True:
        try:
            price_index_np = numpy.array([price_bitmex,
                                          price_deribit,
                                          price_bybit,
                                          price_bitstamp,
                                          price_coinbase,
                                          price_binance])

            price_index = int(price_index_np[numpy.nonzero(price_index_np)].mean())

            if last_index != price_index:
                last_index = price_index
                logger.debug(("Price Index:", price_index))

        except Exception as error:
            logger.info("Error: Index could not be build!")
            logger.debug(error)

        # Check performance
        exec_time = (time.time() - last_time_exec)
        last_time_exec = time.time()
        if exec_time > 0.5:
            logger.info(("Longer exec times: ", exec_time))

        await asyncio.sleep(0.1)


###################
# Chat monitoring #
###################
tg_offset = "-0"
bot_restarted = True

async def chat_monitor():
    global tg_offset, bot_restarted, user_list
    while True:
        # Get updates from bot
        bot_messages_json = await get_messages(tg_offset)
        # Check the amount of messages received
        try:
            message_amount = len(bot_messages_json["result"])
        except KeyError and TypeError:
            message_amount = 0

        # Check messages if exists
        if message_amount != 0:
            # Suppress old actions if bot restarted
            if bot_restarted:
                bot_restarted = False
                logger.info("Telegram API Ready!")
            else:
                # Go through all new messages
                for bot_message in bot_messages_json["result"]:
                    # Catch key error due to other updates than message
                    try:
                        bot_messages_text = str(bot_message["message"]["text"])
                        telegram_id = bot_message["message"]["from"]["id"]

                        logger.info(("New Message: " + bot_messages_text))
                        # Check who wrote the message
                        for user in user_list:
                            if int(telegram_id) == int(user.telegram_id):
                                logger.info(("From user: ", user.name))

                                ############
                                # Commands #
                                ############
                                try:
                                    # Split message by " " to be able to parse it easier
                                    splitted = bot_messages_text.split(' ')

                                    # The user wants to change the price steps
                                    if splitted[0] == "/set_price_steps":
                                        user.ask_price_steps = True
                                        if len(splitted) > 1:
                                            try:
                                                # Look for a valid value for the new price steps
                                                price_steps = int(splitted[1])
                                                # Update user
                                                user.price_steps = price_steps
                                                user.ask_price_steps = False
                                                # Update database
                                                sqlquery = "UPDATE users SET price_steps = '{}' " \
                                                           "WHERE telegram_id = '{}'".format(user.price_steps,
                                                                                             user.telegram_id)
                                                execute_query(sqlquery)
                                                # Confirm to user
                                                message = "The price stepping is set to {} now.".format(
                                                    str(splitted[1]))
                                                await send_message(telegram_id, message)
                                            except ValueError as error:
                                                # The user did not give a valid value for the price steps
                                                logger.info("The user did not give a valid value for the price steps")
                                                logger.debug(error)

                                    if user.ask_price_steps:
                                        try:
                                            # Look for a valid value for the new price steps
                                            price_steps = int(splitted[0])
                                            # Update user
                                            user.price_steps = price_steps
                                            user.ask_price_steps = False
                                            # Update database
                                            sqlquery = "UPDATE users SET price_steps = '{}' " \
                                                       "WHERE telegram_id = '{}'".format(user.price_steps,
                                                                                         user.telegram_id)
                                            execute_query(sqlquery)
                                            # Confirm to user
                                            message = "The price stepping is set to {} now.".format(str(splitted[0]))
                                            await send_message(telegram_id, message)
                                        # The user did not give a valid value for the price steps so ask him
                                        except ValueError as error:
                                            logger.info("The user did not give a valid value for the price steps")
                                            logger.debug(error)
                                            message = "Tell me your desired price steps in USD as integer"
                                            await send_message(telegram_id, message)

                                    elif splitted[0] == "/position_show":
                                        try:
                                            if user.client:
                                                position = await get_position(user.client)
                                                if position:
                                                    await send_message(telegram_id, position)
                                            else:
                                                message = "No client connected!"
                                                await send_message(telegram_id, message)

                                        except Exception as error:
                                            logger.info("Error: Fetching user position failed!")
                                            logger.debug(error)

                                except Exception as error:
                                    logger.info("Command execution failed!")
                                    logger.info(error)
                                break

                    # Discard all other messages
                    except KeyError:
                        logger.info("Another type of message received")

                    # Set new offset to acknowledge messages on the telegram api
                    tg_offset = str(bot_message["update_id"] + 1)

        if bot_restarted:
            bot_restarted = False
            logger.info("Telegram API Ready!")
        await asyncio.sleep(3)

#############
# Announcer #
#############
async def announcer():
    while True:
        if price_index:
            for user in user_list:
                if user.is_enabled:
                    # Get new price level for the user
                    new_price_level = int(price_index / user.price_steps)
                    # Do nothing if no new price level
                    if user.price_level != new_price_level:
                        # Price has to move more then xx% of the divider to avoid spam
                        if (price_index < (user.announced_price - (user.price_steps / 5))) or (
                                price_index > (user.announced_price + (user.price_steps / 5))):
                            # Check if new price level is not in history
                            # Or if the price stable over time
                            if new_price_level not in user.history or \
                                    (sum(user.history) / len(user.history)) == new_price_level:

                                if user.announced_price > price_index:
                                    priceIs = "Lower"
                                    message = priceIs + " price level: " + str(
                                        new_price_level * user.price_steps + user.price_steps) + " - " + str(
                                        new_price_level * user.price_steps)
                                else:
                                    priceIs = "Higher"
                                    message = priceIs + " price level: " + str(
                                        new_price_level * user.price_steps) + " - " + str(
                                        new_price_level * user.price_steps + user.price_steps)
                                # debug
                                logger.debug(("Price Index: ", price_index))
                                logger.debug(("Announced_price : ", user.announced_price))

                                # Construct message to the user
                                if user.client:
                                    position_message = await get_position(user.client)
                                    if position_message and not position_message == "No open position!":
                                        message = message + "\n" + position_message

                                await send_message(user.telegram_id, message)

                                # Update user
                                user.announced_price = price_index
                                user.price_level = new_price_level
                                # Update database
                                sqlquery = "UPDATE users SET announced_price = '{}', price_level = '{}' " \
                                           "WHERE telegram_id = '{}'".format(user.announced_price, user.price_level,
                                                                             user.telegram_id)
                                execute_query(sqlquery)

                    # Make history
                    user.history.append(new_price_level)
                    del user.history[0]

        await asyncio.sleep(1)

#############
# Positions #
#############
async def get_position(client):
    try:
        open_position = client.Positions.Positions_myPosition().result()[0]["result"][0]

        if open_position["size"] == 0:
            return "No open position!"

        entry_distance = price_index - int(open_position["entry_price"])
        stop_distance = price_index - int(open_position["stop_loss"])

        side = "Long"
        if open_position["side"] == "Sell":
            side = "Short"

        message = side + " Position: " + str(open_position["size"]) + "\n" + \
                  "Entry: " + str(int(open_position["entry_price"])) + " (" + str(entry_distance) + ") " + " | " + \
                  "Stop: " + str(open_position["stop_loss"]) + " (" + str(stop_distance) + ")\n" + \
                  "Open PNL: " + str(round(open_position["unrealised_pnl"], 4)) + " | " + \
                  "Daily PNL: " + str(round(open_position["realised_pnl"], 4))
        return message
    except Exception as error:
        logger.info("Error: get_position failed!")
        logger.debug(error)
        return False

####################
# Position Tracker #
####################
async def position_tracker():
    while True:
        try:
            if price_index:
                for user in user_list:
                    if user.is_enabled and user.client:
                        open_position = user.client.Positions.Positions_myPosition().result()[0]["result"][0]
                        if open_position["size"] == 0 and open_position["size"] != user.last_position_size:
                            user.last_position_size = 0
                            # Update database
                            sqlquery = "UPDATE users SET last_position_size = '{}' " \
                                       "WHERE telegram_id = '{}'".format(user.last_position_size, user.telegram_id)
                            execute_query(sqlquery)

                            message = "Position closed! Daily PNL: {}".format(round(open_position["realised_pnl"], 4))
                            await send_message(user.telegram_id, message)

                        elif user.last_position_size != open_position["size"]:
                            entry_distance = price_index - int(open_position["entry_price"])
                            stop_distance = price_index - int(open_position["stop_loss"])
                            side = "Long"

                            if open_position["side"] == "Sell":
                                side = "Short"

                            message = side + " Position: " + str(open_position["size"]) + "\n" + \
                                      "Entry: " + str(int(open_position["entry_price"])) + " (" + str(entry_distance) + ") " + " | " + \
                                      "Stop: " + str(open_position["stop_loss"]) + " (" + str(stop_distance) + ")\n" + \
                                      "Open PNL: " + str(round(open_position["unrealised_pnl"], 4)) + " | " + \
                                      "Daily PNL: " + str(round(open_position["realised_pnl"], 4))

                            await send_message(user.telegram_id, message)
                            user.last_position_size = open_position["size"]

                            # Update database
                            sqlquery = "UPDATE users SET last_position_size = '{}' " \
                                       "WHERE telegram_id = '{}'".format(user.last_position_size, user.telegram_id)
                            execute_query(sqlquery)

            await asyncio.sleep(60)

        except Exception as error:
            logger.info("Position tracker failed!")
            logger.debug(error)
            await asyncio.sleep(60)

#########
# Loop  #
#########
loop = asyncio.get_event_loop()
try:
    exchanges = ["bitmex", "deribit",
                 "bybit", "bitstamp",
                 "coinbase", "binance"]

    for exchange in exchanges:
        asyncio.ensure_future(get_latest_bitcoin_price(exchange))

    asyncio.ensure_future(price_index_calc())
    asyncio.ensure_future(chat_monitor())
    asyncio.ensure_future(announcer())
    asyncio.ensure_future(position_tracker())

    loop.run_forever()

except KeyboardInterrupt:
    pass
finally:
    print("Closing Loop")
    loop.close()
