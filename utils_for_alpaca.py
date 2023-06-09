import pandas as pd
import logging as log
import math
from time import sleep

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from Alpaca_config import *

from alpaca.trading.enums import OrderSide, TimeInForce, AssetClass, AssetStatus, AssetExchange, OrderStatus, CorporateActionType, CorporateActionSubType
from alpaca.trading.requests import GetCalendarRequest, GetAssetsRequest, GetOrdersRequest, MarketOrderRequest, LimitOrderRequest, StopLossRequest, TrailingStopOrderRequest, GetPortfolioHistoryRequest, GetCorporateAnnouncementsRequest
from alpaca.data.requests import StockLatestQuoteRequest, StockTradesRequest, StockQuotesRequest, StockBarsRequest, StockSnapshotRequest, StockLatestBarRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import Adjustment, DataFeed, Exchange

from alpaca.trading.client import TradingClient
from alpaca.data import StockHistoricalDataClient
from alpaca.broker.client import BrokerClient


class Alpaca:
    def _auth_trading(self):
        trading_client = TradingClient(API_KEY_PAPER, API_SECRET_PAPER) # dir(trading_client)
        return trading_client

    def _auth_hist(self):
        stock_client = StockHistoricalDataClient(API_KEY_PAPER,API_SECRET_PAPER)
        return stock_client

    def _auth_brok(self):
        broker_client = BrokerClient(API_KEY_PAPER,API_SECRET_PAPER,sandbox=False,api_version="v2")
        return broker_client


    def read_strategy_params(self, strategy):
        sheet_id = '1jg-IXE4GcEpnT8BR7fUZKW7PzFF6eSN9OgGeGmQnU38'
        sheet_name = 'Sheet1'
        url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}'
        df = pd.read_csv(url)
        strategy_allocated_capital = df[df.Strategy_ID == strategy]['Allocated_Capital'].values[0]
        return strategy_allocated_capital


    def sent_alpaca_email(self, mail_subject, mail_content):

        email_sent = False
        message = MIMEMultipart() # setup the MIME
        message['From'] = 'Alpaca Paper'
        message['To'] = receiver_address
        message['Subject'] = mail_subject   #The subject line
        message.attach(MIMEText(mail_content, 'plain')) # body and the attachments for the mail

        #Create SMTP session for sending the mail
        session = smtplib.SMTP('smtp.gmail.com', 587) # use gmail with port
        session.starttls() # enable security
        session.login(sender_address, sender_pass) # login with mail_id and password
        text = message.as_string()
        session.sendmail(sender_address, receiver_address, text)
        session.quit()
        email_sent = True
        return email_sent


    def get_tkrs_snapshot_df(self,tickers):
        stock_client = self._auth_hist()
        snap = stock_client.get_stock_snapshot(StockSnapshotRequest(symbol_or_symbols=tickers, feed = DataFeed.SIP))
        snapshot_data = {stock: [snapshot.latest_trade.price, 
                                snapshot.previous_daily_bar.close,
                                snapshot.daily_bar.close,
                                (snapshot.daily_bar.close/snapshot.previous_daily_bar.close)-1,
                                ]
                        for stock, snapshot in snap.items() if snapshot and snapshot.daily_bar and snapshot.previous_daily_bar
                        }
        snapshot_columns=['price', 'prev_close', 'last_close', 'gain']
        snapshot_df = pd.DataFrame(snapshot_data.values(), snapshot_data.keys(), columns=snapshot_columns)
        return snapshot_df


    def get_market_snapshot_df(self):
        trading_client = self._auth_trading()
        stock_client = self._auth_hist()
        assets = trading_client.get_all_assets(GetAssetsRequest(asset_class=AssetClass.US_EQUITY,status= AssetStatus.ACTIVE))
        exclude_strings = ['Etf', 'ETF', 'Lp', 'L.P', 'Fund', 'Trust', 'Depositary', 'Depository', 'Note', 'Reit', 'REIT']
        assets_in_scope = [asset for asset in assets
                            if asset.exchange != 'OTC' # OTC stocks play by different rules than Exchange Traded stocks (often referred to as NMS). 
                            and asset.shortable
                            and asset.tradable
                            and asset.marginable # if a stock is not marginable that means it cannot be used as collateral for margin. 
                            and asset.fractionable # indirectly filters out a lot of small volatile stocks:  
                            and asset.easy_to_borrow 
                            and asset.maintenance_margin_requirement == 30
                            and not (any(ex_string in asset.name for ex_string in exclude_strings))]
        snapshots_dict = {}
        CHUNK_SIZE = 1000 # There is a maximum length a URI can be => so get the snapshots in 'chunks'
        for chunk_start in range(0, len(assets_in_scope), CHUNK_SIZE):
            chunk_end = chunk_start + CHUNK_SIZE
            chunk = assets_in_scope[chunk_start:chunk_end]
            snapshots_chunk = stock_client.get_stock_snapshot(StockSnapshotRequest(symbol_or_symbols=chunk, feed = DataFeed.SIP))
            snapshots_dict.update(snapshots_chunk)

        snapshot_data = {stock: [snapshot.latest_trade.price, 
                                snapshot.previous_daily_bar.close,
                                snapshot.daily_bar.close,
                                (snapshot.daily_bar.close/snapshot.previous_daily_bar.close)-1,
                                ]
                        for stock, snapshot in snapshots_dict.items() if snapshot and snapshot.daily_bar and snapshot.previous_daily_bar
                        }
        snapshot_columns=['price', 'prev_close', 'last_close', 'gain']
        snapshot_df = pd.DataFrame(snapshot_data.values(), snapshot_data.keys(), columns=snapshot_columns)
        return snapshot_df


