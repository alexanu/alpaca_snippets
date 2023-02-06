from Alpaca_config import *
import Universes

import logging
logging.basicConfig(level=logging.INFO)

import random # standard python library
import time # standard python library

from twilio.rest import Client
twilio_client = Client(Twilio_account_SID, Twilio_token)#  Send the message

from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import MarketOrderRequest, StopLossRequest
from alpaca.data.requests import StockLatestQuoteRequest
from alpaca.trading.client import TradingClient
from alpaca.data import StockHistoricalDataClient
trading_client = TradingClient(API_KEY_PAPER, API_SECRET_PAPER) # dir(trading_client)
stock_client = StockHistoricalDataClient(API_KEY_PAPER,API_SECRET_PAPER)


focus = Universes.Spiders
max_investment_per_position = 1000
num_to_buy = 1
strategy_weight = 0.2 # 20% from all buying power of my account will be dedicated to this strategy
max_items = 10 # I don't want to have more than 10 stocks in pf
stop_loss = -0.05
invoked_via = "Azu_Func_TimeTrigger"
strategy_name = "Rnd_Spider"+"_"+str(max_investment_per_position)+"_"+str(max_items)+"_invoked_"+invoked_via


def inform(message_to_send):
    message = twilio_client.messages.create(
                              body=message_to_send,
                              from_=Twilio_phone,
                              to=Reciep_Phone
                          )
    logging.info(message.sid)
    logging.info(message_to_send)

def close_position(ticker):
    print(f'Closing position in {ticker}')
    trading_client.close_position(ticker)
    time.sleep(5)
    return ticker

def buy_position(ticker):
    investment = min(max_investment_per_position,(strategy_weight * float(trading_client.get_account().buying_power)/max_items)) # either max allowed either rest of cash
    latest_ask_price = stock_client.get_stock_latest_quote(StockLatestQuoteRequest(symbol_or_symbols=[ticker]))[ticker].ask_price
    quantity_to_buy = int(investment//latest_ask_price)
    coid = strategy_name + "_" + str(int(time.mktime(trading_client.get_clock().timestamp.timetuple())))
    try:
        market_order_data = MarketOrderRequest(
            symbol=ticker,
            qty=quantity_to_buy, # any unfilled orders after the open will be cancelled
            side=OrderSide.BUY,
            client_order_id = coid,
            stop_loss = StopLossRequest(stop_price=latest_ask_price*(1-stop_loss/100)),
            time_in_force=TimeInForce.GTC) # dir(TimeInForce)
        inform(f"Strategy {strategy_name} is buying {quantity_to_buy} of {ticker} at market")
        time.sleep(5)
        return trading_client.submit_order(order_data=market_order_data)
    except Exception as _e:
        logging.error(_e)
        return None


def buy_random():
    clock = trading_client.get_clock()
    # if market not open, exit
    if not clock.is_open:
        time_to_open = (clock.next_open - clock.timestamp).total_seconds()//3600
        inform(f'Market is currently closed. Will open in {time_to_open} hours')
        exit()
    else:
        positions = trading_client.get_all_positions()
        positions_dict_closed = [close_position(position.symbol) for position in positions if float(position.unrealized_plpc)<stop_loss]
        if positions_dict_closed:
            inform(f'Strategy {strategy_name} closed unprofitable positions in {positions_dict_closed}')

        isins_buy = random.choices(focus,k=num_to_buy)
        [buy_position(stock) for stock in isins_buy]

        positions = trading_client.get_all_positions()
        [print(f"{p.symbol} (value {p.market_value} with profit of {p.unrealized_pl}",end="; ") for p in positions]

if __name__ == "__main__":
    buy_random()