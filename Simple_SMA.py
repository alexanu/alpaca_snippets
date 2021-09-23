

import threading
import alpaca_trade_api as tradeapi

from .Universes import Famous

skey = "Insert Secret Key"
apiKey = "Insert API Key"
apiEndpoint = "Insert Endpoint"
api = tradeapi.REST(apiKey,skey)

def positionExists(symbol,portfolio):
    for position in portfolio:
        if (symbol == position.symbol):
            return True
    return False


def autoTrade():
    while (api.get_clock().is_open):
        account = api.get_account()
        if account.trading_blocked:
            print('Account is currently restricted from trading.')
            return
        buyingpower = account.buying_power
        #start by opening new positions
        limitPerStock = buyingpower/len(Famous)
        print("Buying Power {}".format(buyingpower))
        print("Limit Per Stock {}".format(limitPerStock))
        portfolio = api.list_positions()
        for i in Famous:
            if (positionExists(i,portfolio)):
                continue
            else:
                ... # Here code to get sma20 for every ticker
                if (sma20 < 0): #we buy the stock
                    price = .... # get price
                    quanitity = 0
                    while ((quanitity+1) * price < limitPerStock and (quanitity+1) * price < buyingpower):
                        quanitity += 1
                    if (quanitity == 0):
                        continue
                    print("Buying {} shares of {}".format(limitPerStock,i))
                    api.submit_order(symbol=i,qty=limitPerStock,side='buy',type='market',time_in_force='gtc')


        for position in portfolio:
            profit = position.unrealized_pl
            percentChange = (profit/position.cost_basis) * 100
            if (percentChange > 5):
                print("Selling {} shares of {}".format(position.qty,position.symbol))
                api.submit_order(symbol=position.symbol,qty=position.qty,side='sell',type='market',time_in_force='opg')
       
    print("Market is Closed")
autoTrade()