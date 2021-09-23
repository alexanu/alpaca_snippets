# Source: https://github.com/proinvwin/My_5min_Strategy/blob/master/Divergence_5min.py



import alpaca_trade_api as tradeapi
import pandas as pd
import time
import datetime
from datetime import timedelta
import os
import smtplib
from email.message import EmailMessage
import requests
from bs4 import BeautifulSoup



hot_surge = 30
strong_surge = 20
min_price = 5
max_price = 330
avgVol_min = 500000
rsi_minimum = 25
track_qty = 50

hotVol_min = 120000
strongVol_min = 90000

market_value = 1.5


# First, open the API connection LIVE API CONNECTION
api = tradeapi.REST(
    'XXXX',
    'XXXX',
    'https://api.alpaca.markets'
)

""" # First, open the API connection PAPER API CONNECTION
api = tradeapi.REST(
    'XXXX',
    'XXX'
    'XXXX',
    'https://paper-api.alpaca.markets'
) """

account = api.get_account()
print(account)
import logging
logging.basicConfig(filename='./new_5min_ema.log', format='%(name)s - %(levelname)s - %(message)s')
logging.warning('{} logging started'.format(datetime.datetime.now().strftime("%x %X")))

def myEmail_Notify(my_content):
    msg = EmailMessage()
    msg['Subject'] = 'Testing Followed Stocks 5-min'
    msg['From'] = ' x@gmail.com'
    msg['To'] = ' x@gmail.com'
    msg.set_content(my_content) 
    
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login('x@gmail.com', 'xxxxx') 
        
        smtp.send_message(msg)

def yahoo_url(stock):
    yahoo_1 = 'https://finance.yahoo.com/quote/'
    yahoo_2 = '?p='
    yahoo_3 = '&.tsrc=fin-tre-srch'
    ystock_path = yahoo_1+stock+yahoo_2+stock+yahoo_3
    return ystock_path 

def signal_num():
    market = yahoo_indexQuote() #================[float(sp_change[:-1]), float(dji_change[:-1]), float(nasdaq_change[:-1])]
    if market[0] >= market_value:
        Lmax = 0.005
        Lmin = 0.0015
        Smax = -0.002
        Smin = -0.0015
        Ldiv = 0.95
        Sdiv = 1.25
    elif 0 < market[0] < market_value:
        Lmax = 0.005
        Lmin = 0.0015
        Smax = -0.002
        Smin = -0.0015
        Ldiv = 0.95
        Sdiv = 1.1
    else:
        Lmax = 0.005
        Lmin = 0.0015
        Smax = -0.002
        Smin = -0.0015
        Ldiv = 1.25
        Sdiv = 0.95
    return [Lmax, Lmin, Smax, Smin, Ldiv, Sdiv]

   
def get_min_bars(symbols):
    data = api.get_barset(symbols, '1Min', limit=1000).df
    data.dropna(axis = 0, how ='any', inplace = True)
    #print(data)
    return data

def get_data_bars(symbols, rate, slow, fifty, mid, fast):
    data = api.get_barset(symbols, rate, limit=1000).df
    data.dropna(axis = 0, how ='any', inplace = True)
    #print(data)
    for x in symbols:
        data.loc[:, (x, 'slow_sma')] = data[x]['close'].rolling(window=slow).mean()

    #data.dropna(axis = 0, how ='any', inplace = True)    
    for x in symbols:      
        data.loc[:, (x, 'cl_divv')] = data[x]['low'] - data[x]['slow_sma'] 

    data.dropna(axis = 0, how ='any', inplace = True) 
    print(data)
    return data

def get_signal_bars(symbol_list, rate, sma_slow, sma_fifty, sma_mid, ema_fast):
    data = get_data_bars(symbol_list, rate, sma_slow, sma_fifty, sma_mid, ema_fast)
    #data.dropna(axis = 0, how ='any', inplace = True)
    print(data)
    signals = {}
    for x in symbol_list:
        mean_divv = data[x]['cl_divv'].mean()
        print(f'This is the MEAN DIVV for {x}: {mean_divv}')
        min_divv = data[x]['cl_divv'].min()
        print(f'This is the MIN DIVV for {x}: {min_divv}')
        max_divv = data[x]['cl_divv'].max()
        print(f'This is the MAX DIVV for {x}: {max_divv}')
        std_divv = data[x]['cl_divv'].std()
        print(f'This is the STD DIVV for {x}: {std_divv}') 

        print('\n \n')

        len_data = len(data[x]['cl_divv'])
        print(f'This is the current Length of Data: {len_data}')    

        datum = get_min_bars(x)
        print(datum)


        #price_Datum = datum.iloc[-1]['close']
        price_Datum = datum[x].iloc[-1]['close']
        print(f'This is the current Datum_PRICE: {price_Datum}')


        current_slowSMA = data[x].iloc[-1]['slow_sma']
        print(f'This is the current_slowSMA is: {current_slowSMA}')

        current_divv = price_Datum - data[x].iloc[-1]['slow_sma']
        print(f'This is the current DIVV is: {current_divv}')

######################################################################################################
        

        if (current_divv <= (min_divv*1.1)): signal = 6
        elif (current_divv <= (min_divv*1.05)): signal = 5
        elif (current_divv <= (min_divv)): signal = 4
        elif (current_divv <= (min_divv*0.95)): signal = 3
        elif (current_divv <= (min_divv*0.9)): signal = 2
        elif (current_divv <= (min_divv*0.85)): signal = 1


        elif (current_divv >= (max_divv*1.5)): signal = -7
        elif (current_divv >= (max_divv*1.375)): signal = -6
        elif (current_divv >= (max_divv*1.25)): signal = -5
        elif (current_divv >= (max_divv*1.15)): signal = -4
        #elif (current_divv >= (max_divv)): signal = -3
        #elif (current_divv >= (max_divv*0.90)): signal = -2
        #elif (current_divv >= (max_divv*0.8)): signal = -1
     
#####################################################################################################
 
            
        else: signal = 0
        signals[x] = signal
    return signals


def securities():
    df = pd.read_html('https://topforeignstocks.com/indices/components-of-the-sp-500-index/')
    print(type(df))
    print(len(df))
    #print(df)
    print(df[0])
    df2 = df[0].iloc[:, 0:3]
    print(df2)
    stocks = df2['Ticker'].tolist()          

    return stocks

def analyst_Ratings(symbol):
    url = 'https://www.benzinga.com/stock/' + symbol + '/ratings'
    df = pd.read_html(url)
    print(type(df))
    print(len(df))
    print(df)
    print(df[0])
    df2 = df[0].iloc[:, 0:4]
    print(df2)
    print(df2['Action'].tolist())
    print(df2['Current'].tolist())
    print([df2['Action'].tolist()[0], df2['Current'].tolist()[0]])

    return [df2['Action'].tolist()[0], df2['Current'].tolist()[0]]

def main_list():
    num = len(stocks)
    print(f' lenght of list is: {num}')

    x = 0
    y = num
    main_list = []
    for i in range(x, y, 1):
        try:
            x = i
            chunks = stocks[x:x+1]
            print(chunks)
            main_list.append(chunks)
        except:
            pass

    print(main_list)
    return main_list

stocks = ['AAPL', 'MSFT', 'AMZN', 'FB', 'TSLA', 'GOOGL', 'GOOG', 'BRK.B', 'JNJ', 'JPM', 'V', 'UNH', 'PG', 'NVDA', 'DIS', 'MA', 'HD', 'PYPL', 'BAC', 
          'VZ', 'CMCSA', 'ADBE', 'NFLX', 'INTC', 'T', 'MRK', 'PFE', 'WMT', 'CRM', 'TMO', 'ABT', 'PEP', 'KO', 'XOM', 'CSCO', 'ABBV', 'NKE', 'AVGO', 
          'QCOM', 'CVX', 'ACN', 'COST', 'MDT', 'MCD', 'NEE', 'TXN', 'DHR', 'HON', 'UNP', 'LIN', 'BMY', 'WFC', 'C', 'AMGN', 'LLY', 'PM', 'SBUX', 'LOW', 
          'ORCL', 'IBM', 'AMD', 'UPS', 'BA', 'MS', 'BLK', 'RTX', 'CAT', 'GS', 'NOW', 'GE', 'MMM', 'INTU', 'CVS', 'AMT', 'TGT', 'ISRG', 'DE', 'CHTR', 
          'BKNG', 'SCHW', 'MU', 'AMAT', 'LMT', 'FIS', 'TJX', 'ANTM', 'MDLZ', 'SYK', 'CI', 'ZTS', 'AXP', 'SPGI', 'GILD', 'TMUS', 'MO', 'LRCX', 'BDX', 
          'ADP', 'CSX', 'CME', 'PLD', 'CB', 'CL', 'TFC', 'ADSK', 'ATVI', 'USB', 'PNC', 'DUK', 'FISV', 'CCI', 'ICE', 'SO', 'NSC', 'APD', 'GPN', 'VRTX', 
          'EQIX', 'ITW', 'SHW', 'D', 'FDX', 'DD', 'HUM', 'EL', 'ADI', 'MMC', 'ECL', 'ILMN', 'EW', 'PGR', 'GM', 'DG', 'BSX', 'NEM', 'ETN', 'COF', 'REGN', 
          'EMR', 'COP', 'AON', 'WM', 'HCA', 'MCO', 'NOC', 'FCX', 'ROP', 'KMB', 'ROST', 'DOW', 'CTSH', 'KLAC', 'TEL', 'IDXX', 'BAX', 'TWTR', 'EXC', 'EA', 
          'APH', 'CNC', 'ALGN', 'AEP', 'SNPS', 'APTV', 'STZ', 'MCHP', 'A', 'BIIB', 'SYY', 'CMG', 'CDNS', 'LHX', 'MET', 'DLR', 'DXCM', 'JCI', 'TT', 'BK', 
          'MSCI', 'XLNX', 'PH', 'IQV', 'PPG', 'GIS', 'CMI', 'F', 'HPQ', 'GD', 'TRV', 'AIG', 'TROW', 'EBAY', 'MAR', 'SLB', 'SRE', 'MNST', 'XEL', 'EOG', 
          'ALXN', 'ORLY', 'INFO', 'CARR', 'ALL', 'PSA', 'ZBH', 'TDG', 'VRSK', 'WBA', 'PRU', 'YUM', 'HLT', 'PSX', 'ANSS', 'CTAS', 'RMD', 'CTVA', 'PCAR', 
          'ES', 'ROK', 'DFS', 'BLL', 'SBAC', 'MCK', 'PAYX', 'AFL', 'ADM', 'MTD', 'MSI', 'AZO', 'MPC', 'AME', 'FAST', 'SWK', 'KMI', 'PEG', 'GLW', 'VFC', 
          'LUV', 'SPG', 'FRC', 'WEC', 'OTIS', 'AWK', 'STT', 'SWKS', 'DLTR', 'ENPH', 'WLTW', 'WELL', 'WMB', 'KEYS', 'DAL', 'CPRT', 'MXIM', 'WY', 'LYB', 
          'BBY', 'CLX', 'KR', 'FTV', 'CERN', 'VLO', 'TTWO', 'ED', 'AMP', 'MKC', 'AJG', 'EIX', 'FLT', 'DTE', 'DHI', 'VIAC', 'WST', 'FITB', 'VTRS', 'SIVB', 
          'HSY', 'EFX', 'AVB', 'KHC', 'ZBRA', 'PXD', 'TER', 'VMC', 'PPL', 'LH', 'PAYC', 'ETSY', 'CHD', 'MKTX', 'LEN', 'O', 'CBRE', 'IP', 'QRVO', 'RSG', 
          'NTRS', 'KSU', 'ARE', 'VRSN', 'HOLX', 'SYF', 'EQR', 'ALB', 'XYL', 'ODFL', 'EXPE', 'FTNT', 'MLM', 'URI', 'LVS', 'TSN', 'ETR', 'MTB', 'CDW', 'TFX', 
          'DOV', 'AEE', 'AMCR', 'GRMN', 'OKE', 'HIG', 'KEY', 'GWW', 'BR', 'HAL', 'PKI', 'COO', 'CTLT', 'VTR', 'TYL', 'IR', 'OXY', 'CFG', 'TSCO', 'STE', 
          'NUE', 'RF', 'INCY', 'AKAM', 'HES', 'DGX', 'WDC', 'CMS', 'CAH', 'CAG', 'ULTA', 'KMX', 'AES', 'CE', 'ABC', 'WAT', 'DRI', 'ANET', 'FE', 'VAR', 
          'EXPD', 'CTXS', 'FMC', 'IEX', 'NDAQ', 'POOL', 'K', 'CCL', 'HPE', 'PEAK', 'BKR', 'DPZ', 'ESS', 'GPC', 'J', 'IT', 'HBAN', 'WAB', 'ABMD', 'EMN', 
          'NTAP', 'MAS', 'DRE', 'MAA', 'BF.B', 'EXR', 'NVR', 'LDOS', 'OMC', 'PKG', 'RCL', 'AVY', 'BIO', 'STX', 'SJM', 'PFG', 'TDY', 'CINF', 'CHRW', 'HRL', 
          'CXO', 'BXP', 'UAL', 'IFF', 'XRAY', 'JKHY', 'MGM', 'NLOK', 'JBHT', 'RJF', 'FBHS', 'LNT', 'HAS', 'EVRG', 'WRK', 'WHR', 'PHM', 'AAP', 'CNP', 'ATO', 
          'TXT', 'FFIV', 'LW', 'ALLE', 'UHS', 'UDR', 'DVN', 'L', 'HWM', 'LB', 'LKQ', 'WYNN', 'PWR', 'CBOE', 'FOXA', 'LYV', 'LUMN', 'HST', 'BWA', 'HSIC', 
          'TPR', 'RE', 'CPB', 'LNC', 'IPG', 'SNA', 'WU', 'AAL', 'GL', 'WRB', 'MOS', 'TAP', 'PNR', 'CF', 'NRG', 'DVA', 'FANG', 'ROL', 'DISCK', 'PNW', 'CMA', 
          'MHK', 'NWL', 'NI', 'IPGP', 'AIZ', 'IRM', 'ZION', 'DISH', 'JNPR', 'NCLH', 'AOS', 'PVH', 'NLSN', 'RHI', 'DXC', 'SEE', 'NWSA', 'REG', 'COG', 'BEN', 
          'IVZ', 'HII', 'FLIR', 'KIM', 'APA', 'ALK', 'PRGO', 'MRO', 'PBCT', 'LEG', 'NOV', 'FRT', 'VNO', 'DISCA', 'RL', 'HBI', 'FLS', 'FTI', 'UNM', 'FOX', 
          'VNT', 'GPS', 'SLG', 'XRX', 'HFC', 'UAA', 'UA', 'NWS',
          'HOOD', 'SAVA', 'GEM', 'NIO', 'RIOT', 'SPCE', 'XPEV', 'WISH', 'AFRM', 'ON', 'COIN', 'PDD', 'LRCX', 'BB', 'NVAX', 'BIDU', 'CLOV', 'RBLX', 'SDC',
          ]
          
firstClass_Options = ['AAPL', 'MSFT', 'AMZN', 'FB', 'TSLA', 'GOOGL', 'GOOG', 'BRK.B', 'JNJ', 'JPM', 'V', 'UNH', 'PG', 'NVDA', 'DIS', 'MA', 'HD', 'PYPL', 'BAC', 
          'VZ', 'CMCSA', 'ADBE', 'NFLX', 'INTC', 'T', 'MRK', 'PFE', 'WMT', 'CRM', 'TMO', 'ABT', 'PEP', 'KO', 'XOM', 'CSCO', 'ABBV', 'NKE', 'AVGO', 
          'QCOM', 'CVX', 'ACN', 'COST', 'MDT', 'MCD', 'NEE', 'TXN', 'DHR', 'UNP', 'BMY', 'WFC', 'C', 'LOW', 'FCX', 'ROST', 'DOW', 'TWTR', 'MU', 'AMAT', 'MDLZ', 'LRCX',
          'ORCL', 'IBM', 'AMD', 'UPS', 'BA', 'MS', 'CAT', 'CVS', 'TGT', 'ATVI', 'GPN', 'VRTX', 'FDX', 'GM',
          'HOOD', 'SAVA', 'GEM', 'NIO', 'RIOT', 'SPCE', 'XPEV', 'WISH', 'AFRM', 'ON', 'COIN', 'PDD', 'LRCX', 'BB', 'NVAX', 'BIDU', 'CLOV', 'RBLX',
          ]


n = 0
long_list = []
short_list = []
buy_dict = {}
sell_dict = {}
while True:
    #t = list(time.localtime())
    #if ((t[3])*100 +t[4]) >= 431 and int((t[3])*100 +t[4]) < 2100 and 0 <= datetime.datetime.today().weekday() <=4: 
    #my_message = f'Started' 
    #myEmail_Notify(my_message)
    mainlist = main_list()
    for i in range(len(mainlist)):
        try:      
            signals = get_signal_bars(mainlist[i], '5Min', 200, 50, 20, 8)
            print(signals)

            #time.sleep(1)

            for signal in signals:
                if signals[signal] > 0: 
                    if analyst_Ratings(signal)[0] != 'Downgrades' or analyst_Ratings(signal)[1] != 'Sell' or analyst_Ratings(signal)[1] != 'Underweight':
                        if signal in buy_dict.keys():
                            if signals[signal] > buy_dict[signal]: 
                                if signal in firstClass_Options:   
                                    my_message = f'({signals[signal]}) : *** BUY {signal}; \n \n {yahoo_url(signal)}'
                                    myEmail_Notify(my_message)
                                    buy_dict.update(signals)  
                                else:                                   
                                    my_message = f'({signals[signal]}) : BUY {signal}; \n \n {yahoo_url(signal)}'
                                    myEmail_Notify(my_message)
                                    buy_dict.update(signals)
                        else:
                            if signal in firstClass_Options:   
                                my_message = f'({signals[signal]}) : *** BUY {signal}; \n \n {yahoo_url(signal)}'
                                myEmail_Notify(my_message)
                                buy_dict.update(signals)  
                            else:
                                my_message = f'({signals[signal]}) : BUY {signal}; \n \n {yahoo_url(signal)}'
                                myEmail_Notify(my_message)
                                buy_dict.update(signals)                      

                elif signals[signal] < 0:
                    #if analyst_Ratings(signal)[0] != 'Downgrades' or analyst_Ratings(signal)[1] != 'Sell' or analyst_Ratings(signal)[1] != 'Underweight':
                    if signal in sell_dict.keys():
                        if signals[signal] < sell_dict[signal]:  
                            if signal in firstClass_Options:            
                                my_message = f'({signals[signal]}) : *** SELL {signal}; \n \n {yahoo_url(signal)}'
                                myEmail_Notify(my_message)
                                sell_dict.update(signals)
                            else:
                                my_message = f'({signals[signal]}) : SELL {signal}; \n \n {yahoo_url(signal)}'
                                myEmail_Notify(my_message)
                                sell_dict.update(signals)                                
                    else:
                        if signal in firstClass_Options:          
                            my_message = f'({signals[signal]}) : *** SELL {signal}; \n \n {yahoo_url(signal)}'
                            myEmail_Notify(my_message)
                            sell_dict.update(signals)
                        else:
                            my_message = f'({signals[signal]}) : SELL {signal}; \n \n {yahoo_url(signal)}'
                            myEmail_Notify(my_message)
                            sell_dict.update(signals)          

        except:
            pass
    print(buy_dict)
    print(sell_dict)
    #time.sleep(160)