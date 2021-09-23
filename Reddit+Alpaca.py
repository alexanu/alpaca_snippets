# Source: 

# Idea: 
'''
top 10 most commonly mentioned stocks on reddit as often as possible, 
and rebalances its portfolio accordingly. 
Whenever a stock leaves the top 10, 
AlpacaTrader closes its position on that stock. 
Whenever a stock enters the top 10, 
AlpacaTrader opens a position for that stock.
'''

#ToDos
'''
Add filters for types of stock to avoid purchasing (i.e. specific countries/industries)
Add a non-daytrade strategy of buying and holding once a day
'''


import praw
from pmaw import PushshiftAPI

import collections
import string
import requests
import os
import sys
import time
import pandas as pd
import threading

from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path 
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from reddit_lingo import reddit_lingo, blacklist

import alpaca_trade_api as tradeapi



# looks through a subreddit for the most commonly mentioned stocks

class StockAnalysis:

    def __init__(self, limit, sentiment):
        env_path = Path('.') / '.env'
        load_dotenv(dotenv_path=env_path)
        self.UPVOTES = 1
        self.BLACKLIST = blacklist
        self.UPVOTE_RATIO = 0.70 
        self.reddit = praw.Reddit(
            user_agent = "Comment Extraction",
            client_id=os.getenv("CLIENT_ID"),
            client_secret=os.getenv("CLIENT_SECRET"),
            username=os.getenv("USERNAME"),
            password=os.getenv("PASSWORD")
        )
        self.limit = limit
        self.sentiment = sentiment
        self.vader = SentimentIntensityAnalyzer()
        self.vader.lexicon.update(reddit_lingo)
        
    def getAllTickers(self):
        # URL = "https://dumbstockapi.com/stock"
        # param = dict(
        #     format="tickers-only",
        #     exchanges="NYSE,NASDAQ,AMEX"
        # )

        # response = requests.get(url=URL, params=param)
        # data = response.json()
        # return set(data)
        from urllib.request import urlopen
        r = urlopen("https://www.sec.gov/include/ticker.txt")
        tickers = {line.decode('UTF-8').split("\t")[0].upper() for line in r}
        return tickers

    def getTickersFromSubreddit(self, sub):
        subreddit = self.reddit.subreddit(sub)
        sortedByHot = subreddit.hot(limit=self.limit)
        allTickers = self.getAllTickers()
        numPosts = 0 
        tickersSentiment = collections.defaultdict(set)
        tickers = collections.defaultdict(int)
        for submission in sortedByHot:
            if submission.upvote_ratio < self.UPVOTE_RATIO or submission.ups < self.UPVOTES:
                continue 
        
            authors = set()
            submission.comment_sort = "new"
            comments = submission.comments
            if self.sentiment:
                submission.comments.replace_more(limit=None)
            numPosts += 1
            for comment in comments:
                try:
                    commentAuthor = comment.author.name
                    if comment.score < self.UPVOTES or commentAuthor in authors:
                        continue
                except: # if the author wasn't found, or no score available 
                    continue
                
                if comment.body.isupper():
                    continue

                numPosts += 1
                for word in comment.body.split(" "):
                    word = word.replace("$", "")
                    word = word.translate(str.maketrans('', '', string.punctuation))
                    if (not word.isupper()) or len(word) > 5 or word in self.BLACKLIST or word not in allTickers:
                        continue
                    authors.add(commentAuthor)
                    if (self.sentiment):
                        tickersSentiment[word].add(self.getSentimentScore(comment.body))
                    tickers[word] += 1

        return tickers, tickersSentiment, numPosts

    def getSentimentScore(self, comment):
        score = self.vader.polarity_scores(comment)
        return score['compound']


'''
if __name__ == "__main__":

    sentiment = True
    stockAnalysis = StockAnalysis(10, sentiment)

    subreddits = [
        # "wallstreetbets",
        "robinhoodpennystocks+pennystocks",
        # "stocks"
    ]

    for subreddit in subreddits:
        startTime = time.time()
        scraped_tickers, scraped_sentiment, numPosts = stockAnalysis.getTickersFromSubreddit(subreddit)
        if (sentiment):
            for ticker in scraped_sentiment:
                if len(scraped_sentiment[ticker]) <= 2:
                    scraped_sentiment[ticker] = 0
                    continue
                scraped_sentiment[ticker] = sum(scraped_sentiment[ticker])/len(scraped_sentiment[ticker])
            # normalization
            top_tickers = collections.defaultdict(int)
            factor=1.0/sum(scraped_sentiment.values())
            for k in scraped_sentiment:
                scraped_sentiment[k] = scraped_sentiment[k]*factor
                top_tickers[k] += scraped_sentiment[k]
            factor=1.0/sum(scraped_tickers.values())
            for k in scraped_tickers:
                scraped_tickers[k] = scraped_tickers[k]*factor
                top_tickers[k] += scraped_tickers[k]
            top_tickers = dict(sorted(top_tickers.items(), key=lambda x: x[1], reverse = True))
            print(f"This took {(time.time() - startTime)/60} minutes")
            print(f"Scraped {numPosts} posts in {subreddit}")
            print("Ticker: score")
            ticker_list = list(top_tickers)[0:10]
            for ticker in ticker_list:
                print(f"{ticker}: {top_tickers[ticker]}")
        else:
            top_tickers = dict(sorted(scraped_tickers.items(), key=lambda x: x[1], reverse = True))
            print(f"This took {(time.time() - startTime)/60} minutes")
            print(f"Scraped {numPosts} posts in {subreddit}")
            print("Ticker: score")
            ticker_list = list(top_tickers)[0:10]
            for ticker in ticker_list:
                print(f"{ticker}: {top_tickers[ticker]}")
'''


# 1. We start by waiting for market open
# 2. We close all open orders
# 3. We rebalance our portfolio based on reddit
# 4. Rinse and repeat every minute or so 


class AlpacaTrader:
    def __init__(self, subreddit, limit, sentiment):
        env_path = Path('.') / '.env'
        load_dotenv(dotenv_path=env_path)
        self.limit = limit
        self.subreddit = subreddit
        self.sentiment = sentiment
        self.init()

    def init(self):
        self.alpaca = tradeapi.REST(
            os.getenv("ACCESS_KEY_ID"),
            os.getenv("SECRET_ACCESS_KEY"),
            base_url="https://paper-api.alpaca.markets"
        )
        self.equity = None
        self.blacklist = set()
        self.qBuying = None
        self.positions = None 
        self.adjustedQBuying = None

    def run(self):
        # First, cancel any existing orders so they don't impact our buying power.
        orders = self.alpaca.list_orders(status="open")
        for order in orders:
            self.alpaca.cancel_order(order.id)

        # Wait for market to open.
        print("Waiting for market to open...")
        tAMO = threading.Thread(target=self.awaitMarketOpen)
        tAMO.start()
        tAMO.join()
        print("Market opened.")

        # Rebalance the portfolio as much as we can
        while True:
            # Figure out when the market will close so we can prepare to sell beforehand.
            clock = self.alpaca.get_clock()
            closingTime = clock.next_close.replace(tzinfo=datetime.timezone.utc).timestamp()
            currTime = clock.timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()
            self.timeToClose = closingTime - currTime

            if(self.timeToClose < (60 * 15)):
                print("Market closing soon. Exiting.")
                exit()
                # Close all positions when 15 minutes til market close.
                print("Market closing soon.  Closing positions.")

                positions = self.alpaca.list_positions()
                if (len(positions) <= 0):
                    exit()
                for position in positions:
                    orderSide = 'sell'
                    qty = abs(int(float(position.qty)))
                    respSO = []
                    tSubmitOrder = threading.Thread(target=self.submitOrder(qty, position.symbol, orderSide, respSO))
                    tSubmitOrder.start()
                    tSubmitOrder.join()

                # Run script again after market close for next trading day.
                print("Selling all positions.")
            else:
                # Rebalance the portfolio.
                tRebalance = threading.Thread(target=self.rebalance)
                tRebalance.start()
                tRebalance.join()

    def rebalance(self):
        tRerank = threading.Thread(target=self.rerank)
        tRerank.start()
        tRerank.join()

        # Clear existing orders again.
        orders = self.alpaca.list_orders(status="open")
        for order in orders:
            self.alpaca.cancel_order(order.id)
        
        print("We are going to keep the following positions: " + str(self.positions))

        executed = []
        positions = self.alpaca.list_positions()

        self.blacklist.clear()
        for position in positions:
            # Remove positions that are no longer in the positions set
            if position.symbol not in self.positions:
                # Sell the position
                respSO = []
                tSO = threading.Thread(target=self.submitOrder, args=[int(position.qty), position.symbol, "sell", respSO])
                tSO.start()
                tSO.join()
            else:
                # print(f"We are keeping {position.symbol}")
                if (int(position.qty) == int(self.qBuying[position.symbol])):
                    # print(f"We already have the correct amount of shares for {position.symbol}. Skipping.")
                    pass
                else:
                    # Need to adjust position amount
                    diff = int(self.qBuying[position.symbol]) - int(float(position.qty))
                    print(f"Adjusting position amount for {position.symbol} from {position.qty} to {self.qBuying[position.symbol]}.")
                    if (diff > 0):
                        side = "buy"
                    else:
                        side = "sell"
                    respSO = []
                    tSO = threading.Thread(target=self.submitOrder, args=[abs(diff), position.symbol, side, respSO])
                    tSO.start()
                    tSO.join()
                executed.append(position.symbol)
                self.blacklist.add(position.symbol)
            
        # submit the orders to be executed
        respSendBO = []
        tSendBO = threading.Thread(target=self.sendBatchOrder, args=[self.qBuying, self.positions, "buy", respSendBO])
        tSendBO.start()
        tSendBO.join()
        respSendBO[0][0] += executed

        # find out which orders didn't get completed
        respGetTP = collections.defaultdict(int)
        if (len(respSendBO[0][1]) > 0):
            print("Some orders were not completed successfully. Retrying.")
            tGetTP = threading.Thread(target=self.getTotalPrice, args=[respSendBO[0][0], respGetTP])
            tGetTP.start()
            tGetTP.join()
        
        # resubmit orders that were not completed
        for stock in respSendBO[0][1]:
            qty = self.qBuying[stock]
            respResendBO = []
            tResendBO = threading.Thread(target=self.submitOrder, args=[qty, stock, "buy", respResendBO])
            tResendBO.start()
            tResendBO.join()

        self.qBuying.clear()
            
    def rerank(self):
        tRank = threading.Thread(target=self.rank)
        tRank.start()
        tRank.join()

        # figure out how many shares to buy of each stock
        self.equity = int(float(self.alpaca.get_account().equity))
        self.equityPerStock = int(self.equity // len(self.positions))

        respGetTP = collections.defaultdict(int)
        tGetTP = threading.Thread(target=self.getTotalPrice, args=[self.positions, respGetTP])
        tGetTP.start()
        tGetTP.join()

        self.qBuying = respGetTP

    def getTotalPrice(self, positions, resp):
        for stock in positions:
            bars = self.alpaca.get_barset(stock, "minute", 1)
            resp[stock] = int(self.equityPerStock // bars[stock][0].c)

    def rank(self):
        tGetPC = threading.Thread(target=self.getTickers)
        tGetPC.start()
        tGetPC.join()

    def getTickers(self):
        # the core ranking mechanism, reddit popularity
        stockAnalysis = StockAnalysis(self.limit, self.sentiment)
        scraped_tickers, scraped_sentiment, numPosts = stockAnalysis.getTickersFromSubreddit(self.subreddit)
        top_tickers = collections.defaultdict(int)
        if self.sentiment:
            for ticker in scraped_sentiment:
                if len(scraped_sentiment[ticker]) <= 2:
                    scraped_sentiment[ticker] = 0
                    continue
                scraped_sentiment[ticker] = sum(scraped_sentiment[ticker])/len(scraped_sentiment[ticker])
            factor=1.0/sum(scraped_sentiment.values())
            for k in scraped_sentiment:
                scraped_sentiment[k] = scraped_sentiment[k]*factor
                top_tickers[k] += scraped_sentiment[k]
            factor=1.0/sum(scraped_tickers.values())
            for k in scraped_tickers:
                scraped_tickers[k] = scraped_tickers[k]*factor
                top_tickers[k] += scraped_tickers[k]
        top_tickers = dict(sorted(scraped_tickers.items(), key=lambda x: x[1], reverse = True))
        ticker_set = set(list(top_tickers)[0:10])
        self.positions = ticker_set

    def sendBatchOrder(self, qty, stocks, side, resp):
        executed = []
        incomplete = []
        for stock in stocks:
            if(self.blacklist.isdisjoint({stock})):
                respSO = []
                tSubmitOrder = threading.Thread(target=self.submitOrder, args=[qty[stock], stock, side, respSO])
                tSubmitOrder.start()
                tSubmitOrder.join()
                if(not respSO[0]):
                    # Stock order did not go through, add it to incomplete.
                    incomplete.append(stock)
                else:
                    executed.append(stock)
                respSO.clear()
        resp.append([executed, incomplete])

    # Submit an order if quantity is above 0.
    def submitOrder(self, qty, stock, side, resp):
        if(qty > 0):
            try:
                self.alpaca.submit_order(stock, qty, side, "market", "day")
                print("Market order of | " + str(qty) + " " + stock + " " + side + " | completed.")
                resp.append(True)
            except:
                print("Market Order of | " + str(qty) + " " + stock + " " + side + " | did not go through.")
                resp.append(False)
        else:
            print("Market Order of | " + str(qty) + " " + stock + " " + side + " | not completed.")
            resp.append(True) 

    def awaitMarketOpen(self):
        isOpen = self.alpaca.get_clock().is_open
        while(not isOpen):
            clock = self.alpaca.get_clock()
            openingTime = clock.next_open.replace(tzinfo=datetime.timezone.utc).timestamp()
            currTime = clock.timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()
            timeToOpen = int((openingTime - currTime) / 60)
            print(str(timeToOpen) + " minutes til market open.")
            time.sleep(60)
            isOpen = self.alpaca.get_clock().is_open

if __name__ == "__main__":
    sentiment = len(sys.argv) > 1
    alpacaTrader = AlpacaTrader("robinhoodpennystocks+pennystocks", 1000, sentiment)
    alpacaTrader.run()

