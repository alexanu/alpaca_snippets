
# Source: https://github.com/nickrr7001/AlpacaTradingBot

from .Universes import Famous
def TradingBot():
    while True:
        hour = datetime.datetime.now().hour
        if (isMarketOpen() and hour >= 8):
            stockList = Famous
            stocksToBuy = []
            account = alpaca.get_account()
            buyingpower = float(account.buying_power)
            ownedStocks = alpaca.list_positions()
            for i in ownedStocks:
                ticker = i.symbol
                pos = alpaca.get_position(ticker)
                Profit = float(pos.unrealized_plpc)
                if (Profit >= 3.0 or Profit <= -3.0):
                    print ("Selling stock: " + i.symbol + " @ profit of " + Profit + "%")
                    sellStock(ticker,pos.qty)
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
                        api.submit_order(symbol=i,qty=shares,side='buy',type='market',time_in_force='gtc')
                stockList = [None]
        print("Restarting...")

TradingBot()