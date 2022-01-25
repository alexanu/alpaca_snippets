
# Source: https://github.com/nickrr7001/AlpacaTradingBot

import datetime
import alpaca_trade_api as tradeapi
from alpaca_trade_api.rest import TimeFrame
from Alpaca_config import *
alpaca = tradeapi.REST(API_KEY_PAPER, API_SECRET_PAPER, API_BASE_URL_PAPER, 'v2')

from Universes import Famous
def TradingBot():
    while True:
        hour = datetime.datetime.now().hour
        if (alpaca.get_clock().is_open and hour >= 8):
            stockList = Famous
            stocksToBuy = []
            account = alpaca.get_account()
            if account.trading_blocked:
                print('Account is currently restricted from trading.')
                return
            buyingpower = float(account.buying_power)
            ownedStocks = alpaca.list_positions()
            for i in ownedStocks:
                ticker = i.symbol
                pos = alpaca.get_position(ticker)
                Profit = float(pos.unrealized_plpc)
                if (Profit >= 3.0 or Profit <= -3.0):
                    print ("Selling stock: " + i.symbol + " @ profit of " + Profit + "%")
                    alpaca.submit_order(ticker,pos.qty,side='sell',type='market',time_in_force='opg')
            if (buyingpower > 0 and stockList != None and stockList[0] != None):
                for i in stockList:
                    owned = False
                    for j in ownedStocks:
                        if (i.name == j.symbol):
                            owned = True
                            break
                    if (owned == False):
                        stocksToBuy.append(i.name)
                budget = buyingpower / len(stocksToBuy)
                for i in stocksToBuy:
                    price = float(alpaca.get_position(i).current_price)
                    if (price > budget):
                        continue
                    else:
                        print ("Buying Stock: " + i)
                        shares = int(budget/price)
                        print("Buying {} shares of {}".format(shares,i))
                        alpaca.submit_order(symbol=i,qty=shares,side='buy',type='market',time_in_force='gtc')
                stockList = [None]
        print("Restarting...")

TradingBot()