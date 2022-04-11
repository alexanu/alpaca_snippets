# Source: https://github.com/itsjafer/swing-trader


import requests
import collections
import string
import random
import os
import alpaca_trade_api as tradeapi
import json
import time
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timezone, date, timedelta
from urllib.request import urlopen

env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

def request_response(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    # Set CORS headers for the preflight request
    if request.method == 'OPTIONS':
        # Allows GET requests from any origin with the Content-Type
        # header and caches preflight response for an 3600s
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)
    # Set CORS headers for the main request
    headers = {
        'Access-Control-Allow-Origin': '*'
    }

    # Default responses
    responseFail = {
        "success": "false" 
    }
    response = {
        "success": "true" 
    }
    # Get the tweet from the post request
    request_json = request.get_json()
    tweet = request_json['tweet'].lower()
    print(tweet) # for logging
    
    success = parse_tweet(tweet)

    if success:
        return (json.dumps(response, default=str), 200, headers)
    return (json.dumps(responseFail, default=str), 200, headers)

def parse_tweet(tweet):
    
    alpaca = tradeapi.REST(
        os.getenv("ACCESS_KEY_ID"),
        os.getenv("SECRET_ACCESS_KEY"),
        base_url=os.getenv("BASE_URL")
    )

    # Add trailing stops to some of our orders
    addTrailingStops(alpaca)

    if "added" not in tweet and "swing" not in tweet:
        return False

    # Get the tweet
    # Get the stock tickers from the tweet
    tickers = getStockTicker(tweet)
    print(tickers)

    # Go through each ticker and position size them
    # Position size based on:
    # account risk = 1 %
    # trade risk = 15%
    # position size = account risk / (price * traderisk)
    purchases = collections.defaultdict(int)
    if len(tickers) <= 0:
        return False
    
    # Go through our open positions and sell any positions that are too old
    sellStaleOrders(alpaca)

    for ticker in tickers:
        qty, price = getPositionSize(ticker, alpaca)
        if (qty > 0):
            purchases[ticker] = (qty, price)
        break # testing only considering the first ticker mentioned 

    if len(purchases) <= 0:
        print("No purchases to be made")
        return False

    # Create a unique ID for this order
    trimmed_tweet = tweet.lower()[0:min(len(tweet), 20)]
    unique_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))

    # Buy shares to cover our position sizes
    for ticker in purchases:
        quantity, price = purchases[ticker]
        if not purchaseTicker(alpaca, ticker, quantity, price, unique_id):
            print(f"Error purchasing {ticker} with potential ID: {ticker}+{unique_id}")
        else:
            print(f"Purchased {quantity} of {ticker} for {price}")

    account = alpaca.get_account()

    if len(purchases) > 0:
        return False

    return True

def trailingStopTicker(alpaca, ticker, quantity, price, unique_id):
    order = alpaca.get_order_by_client_order_id(
        f'{ticker}+{unique_id}'
    )

    if order.status == "filled":
        # Set a trailing stop loss 
        alpaca.submit_order(
            symbol=ticker,
            qty=quantity,
            side='sell',
            type='trailing_stop',
            trail_percent=10,  # stop price will be hwm*0.90
            time_in_force='gtc',
        )

        return True
    
    return False

def purchaseTicker(alpaca, ticker, quantity, price, unique_id):
    if quantity <= 0:
        return False
    account = alpaca.get_account()

    try:
        # We buy now, and we'll set a sell order later to avoid PDT
        alpaca.submit_order(
            symbol=ticker,
            qty=quantity,
            side='buy',
            type='market',
            time_in_force='gtc',
            client_order_id = f'{ticker}+{unique_id}'
        )
    except Exception as e:
        print(e)
        return False
    
    return True

def getPositionSize(ticker, alpaca):
    account = alpaca.get_account()

    # Get account equity
    equity = float(account.equity)
    cash = float(account.cash)

    # Get price of the ticker
    try:
        bars = alpaca.get_barset(ticker, "minute", 1)
        price = float(bars[ticker][0].c)
    except:
        print("couldn't get info for ticker " + ticker)
        return 0, 0

    accountRisk = equity * 0.05 # the max we're willing to lose overall
    tradeRisk = 0.5 # How much we're willing to lose on one trade
    positionSize = accountRisk / (tradeRisk * price) # number of shares we can buy
    
    try:
        currentPosition = alpaca.get_position(ticker).qty
    except:
        currentPosition = 0

    # Check if we can even buy this many 
    if (int(positionSize) - int(currentPosition)) * price > cash:
        print(f"Can't afford to buy {(positionSize - currentPosition)} shares. Only have {cash} in cash.")
        return 0,0

    # Unfortunately, Alpaca doesn't support fractional shares yet (but should soon)
    if (int(positionSize) - int(currentPosition) > 0):
        print(f"Planning to to buy {int(positionSize - currentPosition)} shares of {ticker}")
        return int(positionSize) - int(currentPosition), price

    return 0,0

def getAllTickers():
    r = urlopen("https://www.sec.gov/include/ticker.txt")

    tickers = {line.decode('UTF-8').split("\t")[0].upper() for line in r}
    return tickers

def getStockTicker(tweet):
    allTickers = getAllTickers()
    tickers = set()
    for word in tweet.split(" "):
        if '$' not in word:
            continue
        word = word.replace("$", "")
        word = word.translate(str.maketrans('', '', string.punctuation))
        if word.upper() not in allTickers:
            continue 
        tickers.add(word.upper())
    return tickers

def sellStaleOrders(alpaca):
    # Check if we need to sell something
    positions = {position.symbol for position in alpaca.list_positions()}
    orders = alpaca.list_orders(status='all')
    heldOrders = collections.defaultdict(list)
    for order in orders:
        if order.status == "held" and order.symbol in positions and order.filled_at == None:
            heldOrders[order.symbol].append((order.qty, order.submitted_at, order.id))

    for heldOrder in heldOrders:
        submissionTime = str(heldOrders[heldOrder][0][1])
        dateObject = datetime.fromisoformat(submissionTime)
        today = datetime.now(timezone.utc)
        if (today - dateObject).days > 14:
            print(heldOrder + " has been held for more than 14 days")
            # time to get rid of this stock
            for quantity, submitted, id in heldOrders[heldOrder]:
                alpaca.cancel_order(id)
            # sell whatever we have of that stock
            quantityToSell = alpaca.get_position(heldOrder).qty
            try:
                alpaca.submit_order(
                    symbol=heldOrder,
                    qty=quantityToSell,
                    side="sell",
                    type="market",
                    time_in_force="gtc"
                )
            except:
                print("failed to sell " + heldOrder)


def addTrailingStops(alpaca):
    # Go through all orders up until yesterday
    closed_orders = alpaca.list_orders(status='closed',until=date.today())
    open_orders = alpaca.list_orders(status='open',until=date.today())
    
    open_sell_symbols = {order.symbol for order in open_orders if order.side == "sell" and order.filled_at == None}
    positions = {position.symbol for position in alpaca.list_positions()}

    for order in closed_orders:
        # We only care about stocks we hold, or stocks that don't already have sell orders
        if order.symbol not in positions or order.symbol in open_sell_symbols:
            continue        
        quantity = order.filled_qty
        try:
            # Set a trailing stop for it
            alpaca.submit_order(symbol=order.symbol,qty=quantity,side='sell',type='trailing_stop',trail_percent=10, time_in_force='gtc')
            print(f"Placed a trailing stop order for {order.symbol}")
        except Exception as e:
            print(f"Unable to place sell order for {order.symbol}")
            print(e)



if __name__ == "__main__":
    parse_tweet("$GHSI to the moon")

