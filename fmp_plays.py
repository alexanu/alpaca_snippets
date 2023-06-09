
from urllib.request import urlopen
import json
import pandas as pd
import numpy as np
import datetime as dt


from Alpaca_config import fmp_api_key
from Universes import TOP10_US_SECTOR

base_fmp_url = 'https://financialmodelingprep.com/api/'

# Senators transactions ----------------------------------------------------------------------------------------

def get_senators_for_ticker(symbol):
    specific_url = f'v4/senate-disclosure?symbol={symbol}&apikey={fmp_api_key}#'
    response = urlopen(base_fmp_url+specific_url)
    data = response.read().decode("utf-8")
    data_df = pd.json_normalize(json.loads(data))
    return data_df

fmp_senators_deals = pd.concat((get_senators_for_ticker(ticker) for ticker in TOP10_US_SECTOR)).reset_index()
fmp_senators_deals_backup = fmp_senators_deals.copy()
# fmp_senators_deals = fmp_senators_deals_backup.copy()
fmp_senators_deals['amount'] = fmp_senators_deals['amount'].str.replace('[\$, ]', '', regex=True)
fmp_senators_deals[['From', 'To']] = fmp_senators_deals['amount'].str.split('-', expand=True)
fmp_senators_deals['From'] = fmp_senators_deals['From'].astype(int)

bins = [0, 100000, 250000, 500000, 1000000, np.inf]
labels = ['small','normal', 'big', 'very big', 'huge']
fmp_senators_deals['amount'] = pd.cut(fmp_senators_deals['From'], bins=bins, labels=labels)
needed_cols = ['disclosureDate', 'transactionDate', 'owner','ticker', 'assetDescription', 'type', 'amount', 'representative', 'capitalGainsOver200USD']
fmp_senators_deals = fmp_senators_deals[needed_cols]

fmp_senators_deals['disclosureDate'] = pd.to_datetime(fmp_senators_deals['disclosureDate'], errors='coerce')
fmp_senators_deals['transactionDate'] = pd.to_datetime(fmp_senators_deals['transactionDate'], errors='coerce')
fmp_senators_deals = fmp_senators_deals.dropna(subset=['transactionDate','disclosureDate'])
fmp_senators_deals['ReportingLag'] = (fmp_senators_deals['disclosureDate'] - fmp_senators_deals['transactionDate']).dt.days

fmp_senators_deals.to_excel('senators.xlsx',index=False)


# -------------------------------------------------------------------------------------------------------------------


specific_url = f'v4/senate-disclosure-rss-feed?page=0&apikey={fmp_api_key}#'
response = urlopen(base_fmp_url+specific_url)
data = response.read().decode("utf-8")
fmp_senators_deals = pd.json_normalize(json.loads(data))
fmp_senators_deals['amount'] = fmp_senators_deals['amount'].str.replace('[\$, ]', '', regex=True)
fmp_senators_deals[['From', 'To']] = fmp_senators_deals['amount'].str.split('-', expand=True)
fmp_senators_deals['From'] = fmp_senators_deals['From'].astype(int)

bins = [0, 100000, 250000, 500000, 1000000, np.inf]
labels = ['small','normal', 'big', 'very big', 'huge']
fmp_senators_deals['amount'] = pd.cut(fmp_senators_deals['From'], bins=bins, labels=labels)
needed_cols = ['disclosureDate', 'transactionDate', 'owner','ticker', 'assetDescription', 'type', 'amount', 'representative']
fmp_senators_deals = fmp_senators_deals[needed_cols]

fmp_senators_deals['disclosureDate'] = pd.to_datetime(fmp_senators_deals['disclosureDate'], errors='coerce')
fmp_senators_deals['transactionDate'] = pd.to_datetime(fmp_senators_deals['transactionDate'], errors='coerce')
fmp_senators_deals = fmp_senators_deals.dropna(subset=['transactionDate','disclosureDate'])
fmp_senators_deals['ReportingLag'] = (fmp_senators_deals['disclosureDate'] - fmp_senators_deals['transactionDate']).dt.days
# filter out small transactions & transactions which were reported much later than occured
not_small_transcations = fmp_senators_deals[(fmp_senators_deals['amount'] != 'small') & (fmp_senators_deals['ReportingLag']<30)].reset_index(drop=True)

#--------------------------------------------------------------------------------

today = dt.datetime.now().strftime("%Y-%m-%d")
n_days_future = (dt.datetime.now() + dt.timedelta(days=50)).strftime("%Y-%m-%d")
specific_url = f'v4/earning-calendar-confirmed?from={today}&to={n_days_future}&apikey={fmp_api_key}'
response = urlopen(base_fmp_url+specific_url)
data = response.read().decode("utf-8")
earn_calls = pd.json_normalize(json.loads(data))
earn_calls['date'] = pd.to_datetime(earn_calls['date'], format='%Y-%m-%d')
earn_calls['time US'] = pd.to_datetime(earn_calls['time'], format='%H:%M')
earn_calls['datetime'] = pd.to_datetime(earn_calls['date'].dt.date.astype(str) + ' ' + earn_calls['time US'].dt.time.astype(str))
earn_calls['datetime'] = earn_calls['datetime'].dt.tz_localize('US/Eastern')
earn_calls['time MUC'] = earn_calls['datetime'].dt.tz_convert('Europe/Berlin')
earn_calls.sort_values(by='time MUC',inplace=True)
today_earnings = len(earn_calls[earn_calls['time MUC'].dt.date == dt.date.today()])
earn_calls['time MUC'] = earn_calls['time MUC'].dt.strftime('%a, %d %b, %H:%M')

earn_calls['time MUC'] = earn_calls['time MUC'].dt.time
earn_calls = earn_calls[['symbol', 'exchange', 'time', 'when', 'date', 'publicationDate','time MUC']]

#---------------------------------------------------------------------------------------------------------------

#https://financialmodelingprep.com/api/v3/earning_calendar?apikey=YOUR_API_KEY

specific_url = f'v3/earning_calendar?apikey={fmp_api_key}'
response = urlopen(base_fmp_url+specific_url)
data = response.read().decode("utf-8")
earn_calls = pd.json_normalize(json.loads(data))
earn_calls = earn_calls[['date','symbol', 'time', 'fiscalDateEnding','updatedFromDate']]


# Economic calendar ---------------------------------------------------------------------------------------------
#https://financialmodelingprep.com/api/v3/economic_calendar?from=2021-09-05&to=2021-10-19&apikey=YOUR_API_KEY

today = dt.datetime.now().strftime("%Y-%m-%d")
n_days_future = (dt.datetime.now() + dt.timedelta(days=10)).strftime("%Y-%m-%d")
specific_url = f'v3/economic_calendar?from={today}&to={n_days_future}&apikey={fmp_api_key}'
response = urlopen(base_fmp_url+specific_url)
data = response.read().decode("utf-8")
econ_events = pd.json_normalize(json.loads(data))
econ_events = econ_events[(econ_events['impact'] == 'High')].reset_index(drop=True)
econ_events['date'] = pd.to_datetime(econ_events['date'])
econ_events['date'] = econ_events['date'].dt.tz_localize('UTC')
econ_events['munich_time'] = econ_events['date'].dt.tz_convert('Europe/Berlin')
econ_events['munich_time'] = econ_events['munich_time'].dt.tz_localize(None)
current_time = dt.datetime.now()
econ_events = econ_events[econ_events['munich_time'] > current_time]
econ_events = econ_events[['munich_time','country', 'event']]
econ_events.sort_values(by='munich_time',inplace=True)
econ_events['munich_time'] = econ_events['munich_time'].dt.strftime('%a, %d %b, %H:%M')

dt.datetime.today()


#---   Insiders trading ----------------------------------------------

# https://financialmodelingprep.com/api/v4/insider-trading-transaction-type?apikey=YOUR_API_KEY

specific_url = f'v4/insider-trading-transaction-type?apikey={fmp_api_key}'
response = urlopen(base_fmp_url+specific_url)
insider_trans_types = response.read().decode("utf-8")

tra_type = 'P-Purchase,S-Sale'
specific_url = f'v4/insider-trading?transactionType={tra_type}&page=0&apikey={fmp_api_key}'
response = urlopen(base_fmp_url+specific_url)
data = response.read().decode("utf-8")
insider_trans = pd.json_normalize(json.loads(data))
insider_trans.columns

insider_trans.drop(['securityName','link','reportingCik','formType'], axis=1, inplace=True)
insider_trans['price'] = round(insider_trans['price'].astype(float),2)
insider_trans['amount'] = insider_trans['price'] * insider_trans['securitiesTransacted']

insider_trans.groupby('amount').count()

# https://financialmodelingprep.com/api/v4/insider-trading?symbol=AAPL&page=0&apikey=YOUR_API_KEY

def get_insiders_for_ticker(symbol):
    specific_url = f'v4/insider-trading?symbol={symbol}&page=0&apikey={fmp_api_key}'
    response = urlopen(base_fmp_url+specific_url)
    data = response.read().decode("utf-8")
    data_df = pd.json_normalize(json.loads(data))
    return data_df

insider_trans_stock = pd.concat((get_insiders_for_ticker(ticker) for ticker in TOP10_US_SECTOR)).reset_index()
insider_trans_stock_backup = insider_trans_stock.copy()
# insider_trans_stock.drop(['securityName','link','reportingCik','formType'], axis=1, inplace=True)
insider_trans_stock.drop(['link'], axis=1, inplace=True)
insider_trans_stock = insider_trans_stock.dropna(subset=['transactionType'])
insider_trans_stock['securitiesTransacted'] = round(insider_trans_stock['securitiesTransacted'].fillna(0).astype(int),0)
insider_trans_stock = insider_trans_stock[insider_trans_stock['securitiesTransacted'] != 0]
insider_trans_stock['securitiesOwned'] = round(insider_trans_stock['securitiesOwned'].fillna(0).astype(int),0)
insider_trans_stock['price'] = round(insider_trans_stock['price'].fillna(0).astype(float),2)
insider_trans_stock['amount'] = insider_trans_stock['price'] * insider_trans_stock['securitiesTransacted']

insider_trans_stock['filingDate'] = pd.to_datetime(insider_trans_stock['filingDate'])
insider_trans_stock['filingDate'] = insider_trans_stock['filingDate'].dt.tz_localize('US/Eastern')
insider_trans_stock['filingDate CET'] = insider_trans_stock['filingDate'].dt.tz_convert('Europe/Berlin')
insider_trans_stock['filingDate CET'] = insider_trans_stock['filingDate CET'].dt.tz_localize(None)
insider_trans_stock['filingDate'] = insider_trans_stock['filingDate'].dt.tz_localize(None)
insider_trans_stock['transactionDate'] = pd.to_datetime(insider_trans_stock['transactionDate'], errors='coerce')
insider_trans_stock['ReportingLag'] = (insider_trans_stock['filingDate'] - insider_trans_stock['transactionDate']).dt.days

from collections import OrderedDict # needed as the order is important

mapping_insiders = OrderedDict({
    '10':'10 perc owner',
    'CEO':'CEO',
    'chief ex':'CEO',
    'CFO':'CFO',
    'COO':'COO',
    })

mapping_SecType = OrderedDict({
    'phan':'Phantom Stock',
    'pay':'Payout on Restr Defer Perf Stock Unit',
    'restri':'Restr Defer Perf Stock Unit',
    'RSU':'Restr Defer Perf Stock Unit',
    'PSU':'Restr Defer Perf Stock Unit',
    'perf':'Restr Defer Perf Stock Unit',
    'defe':'Restr Defer Perf Stock Unit',
    'MSU':'Restr Defer Perf Stock Unit',
    'DSU':'Restr Defer Perf Stock Unit',
    'divi':'Dividend Equivalent Units',
    'unit':'Restr Defer Perf Stock Unit',
    'conv':'Convertibles',
    'pref':'Preffered Stock',
    'appre':'Stock Appreciation Rights',
    'buy':'Right to Buy',
    'purch':'Right to Buy',
    'acq':'Right to Buy',
    'rights to':'Right to Buy',
    'option':'Right to Buy',
    'warr':'Right to Buy',
    'obli':'Obligation to Sell',
    'sell':'Right to Sell',
    'comm':'Common Stock',
    'ordi':'Common Stock',
})

insider_trans_stock['InsiderType'] = np.nan  # Initialize new column with NaNs
for key, value in mapping_insiders.items():
    insider_trans_stock.loc[insider_trans_stock['typeOfOwner'].str.contains(key, case=False, na=False) & insider_trans_stock['InsiderType'].isna(), 'InsiderType'] = value
insider_trans_stock['InsiderType'].fillna('Other', inplace=True)

insider_trans_stock['SecurityType'] = np.nan  # Initialize new column with NaNs
for key, value in mapping_SecType.items():
    insider_trans_stock.loc[insider_trans_stock['securityName'].str.contains(key, case=False, na=False) & insider_trans_stock['SecurityType'].isna(), 'SecurityType'] = value
insider_trans_stock['SecurityType'].fillna('Other', inplace=True)



insider_trans_stock.to_excel('test.xlsx')

insider_trans_stock.drop(['InsiderType'], axis=1, inplace=True)

