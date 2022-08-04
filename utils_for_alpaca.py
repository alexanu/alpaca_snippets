import pandas as pd
import logging as log
import math
from time import sleep

def get_allocated_capital(strategy):
    sheet_id = '1jg-IXE4GcEpnT8BR7fUZKW7PzFF6eSN9OgGeGmQnU38'
    sheet_name = 'Sheet1'
    url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}'
    df = pd.read_csv(url)
    strategy_allocated_capital = df[df.Strategy_ID == strategy]['Allocated_Capital'].values[0]
    return strategy_allocated_capital


def sent_alpaca_email(mail_subject, mail_content):
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    from Alpaca_config import sender_address, sender_pass, receiver_address

    #Setup the MIME
    message = MIMEMultipart()
    message['From'] = 'Alpaca Paper'
    message['To'] = receiver_address
    message['Subject'] = mail_subject   #The subject line

    #The body and the attachments for the mail
    message.attach(MIMEText(mail_content, 'plain'))
    #Create SMTP session for sending the mail
    session = smtplib.SMTP('smtp.gmail.com', 587) #use gmail with port
    session.starttls() #enable security
    session.login(sender_address, sender_pass) #login with mail_id and password
    text = message.as_string()
    session.sendmail(sender_address, receiver_address, text)
    session.quit()

def get_tkrs_snapshot(api,tickers):
    snapshots_dict = api.get_snapshots(tickers)
    snapshot_data = {stock: [snapshot.latest_trade.price, 
                            snapshot.prev_daily_bar.close,
                            snapshot.daily_bar.close,
                            (snapshot.daily_bar.close/snapshot.prev_daily_bar.close)-1,
                            ]
                    for stock, snapshot in snapshots_dict.items() if snapshot and snapshot.daily_bar and snapshot.prev_daily_bar
                    }
    snapshot_columns=['price', 'prev_close', 'last_close', 'gain']
    snapshot_df = pd.DataFrame(snapshot_data.values(), snapshot_data.keys(), columns=snapshot_columns)
    return snapshot_df

def get_market_snapshot(api):
    assets_list = api.list_assets() # all stocks tradable on Aplaca
    active_asset_list = [asset.symbol for asset in assets_list if asset.status=='active'] # list of active symbols
    snapshots_dict = {}
    CHUNK_SIZE = 1000 # There is a maximum length a URI can be => so get the snapshots in 'chunks'
    for chunk_start in range(0, len(active_asset_list), CHUNK_SIZE):
        chunk_end = chunk_start + CHUNK_SIZE
        chunk = active_asset_list[chunk_start:chunk_end]
        snapshots_chunk = api.get_snapshots(chunk)
        snapshots_dict.update(snapshots_chunk)

    snapshot_data = {stock: [snapshot.latest_trade.price, 
                            snapshot.prev_daily_bar.close,
                            snapshot.daily_bar.close,
                            (snapshot.daily_bar.close/snapshot.prev_daily_bar.close)-1,
                            ]
                    for stock, snapshot in snapshots_dict.items() if snapshot and snapshot.daily_bar and snapshot.prev_daily_bar
                    }

    snapshot_columns=['price', 'prev_close', 'last_close', 'gain']
    snapshot_df = pd.DataFrame(snapshot_data.values(), snapshot_data.keys(), columns=snapshot_columns)
    return snapshot_df

def get_all_trades(api):
    count = 0
    search = True
    while search:
        if count < 1:
            data = api.get_activities() # get most recent activities
            data = pd.DataFrame([activity._raw for activity in data])
            split_id = data.id.iloc[-1] # get the last order id for pagination
            trades = data
        else:
            data = api.get_activities(direction='desc', page_token=split_id)
            data = pd.DataFrame([activity._raw for activity in data])
            if data.empty:
                search = False
            else:
                split_id = data.id.iloc[-1]
                trades = trades.append(data)
        count += 1
    trades = trades[trades.order_status == 'filled']
    trades = trades.reset_index(drop=True)
    trades = trades.sort_index(ascending=False).reset_index(drop=True)
    trades['transaction_time'] = pd.to_datetime(trades['transaction_time'], format="%Y-%m-%d")
    return(trades)

# Calculates overnight gain (last close to current price)
def overnight_gain(api,stk):
    snapshot_data = api.get_snapshot(stk)

    clock = api.get_clock()
    current_time = clock.timestamp
    current_date = clock.timestamp.normalize()

    minute_bar_is_old = snapshot_data.minute_bar.timestamp < current_time - pd.Timedelta(15, 'minutes')
    if minute_bar_is_old:
        log.warning('minute data is more than 15 minutes old. timestamp. is: {}'.format(snapshot_data.minute_bar.timestamp))

    daily_bar_is_old = snapshot_data.daily_bar.timestamp < current_date
    if daily_bar_is_old:
        log.warning('daily data isnt current. timestamp. is: {}'.format(snapshot_data.daily_bar.timestamp))

    # Calculate gain (even if old data)
    price_current = snapshot_data.minute_bar.close
    price_prev_close = snapshot_data.prev_daily_bar.close

    gain_close_to_current = math.log(price_current / price_prev_close)
    log.debug('overnight gain calc. current price: {}  prev close price: {}  gain: {}'.format(price_current, price_prev_close, gain_close_to_current ))

    return gain_close_to_current

def daytrade_sell_check(api, symbol, DAYTRADE_PATTERN_PROTECTION=True):
    today= datetime.date.today().isoformat()
    activities= api.get_activities(after=today)
    activities= pd.DataFrame([a._raw for a in activities])
    if activities.size > 0:
        not_active_today= symbol not in list(activities['symbol'])
    else:
        not_active_today= True
    
    if not DAYTRADE_PATTERN_PROTECTION:
        return True
    elif DAYTRADE_PATTERN_PROTECTION and not_active_today :
        return True
    else:
        return False

def time_to_market_close(api):
    clock = api.get_clock()
    closing = clock.next_close - clock.timestamp
    return round(closing.total_seconds() / 60)

def wait_for_market_open(api):
	clock = api.get_clock()
	if not clock.is_open:
		time_to_open = clock.next_open - clock.timestamp
		sleep_time = round(time_to_open.total_seconds())
		sleep(sleep_time)
	return clock


