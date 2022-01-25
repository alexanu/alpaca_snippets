
import pytz
import pandas as pd
from alpaca_trade_api.rest import TimeFrame
import datetime


def get_stock_data(api, symbol, start, end, timeframe=timeframe, limit=limit, tz='America/New_York', adjustment='raw'):

	bars_df = api.get_bars(symbol, timeframe, start, end, limit=limit, adjustment=adjustment).df
	bars_df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
	bars_df.index.name = 'Datetime'
	accumulated_df = bars_df

	if len(accumulated_df) < limit:
		accumulated_df.index = pd.to_datetime(accumulated_df.index)		# convert to pd.datetime64 dtype
		accumulated_df.index = accumulated_df.index.tz_convert(tz)		# convert timzone
		return accumulated_df
	else:
		while True:
			last_dt = bars_df.index[-1].isoformat()
			bars_df = api.get_bars(symbol, timeframe, start=last_dt, end=end, limit=limit, adjustment=adjustment).df[1:]
			bars_df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
			bars_df.index.name = 'Datetime'

			accumulated_df = pd.concat([accumulated_df, bars_df])

			if len(bars_df) < (limit - 1): break
	
	accumulated_df.index = pd.to_datetime(accumulated_df.index)		# convert to pd.datetime64 dtype
	accumulated_df.index = accumulated_df.index.tz_convert(tz)		# convert timzone to EDT

	return accumulated_df

def get_all_stock_data(api, symbols, start, end = 'today', timeframe=TimeFrame.Minute, limit=10000, tz='America/New_York', fmt='csv', adjustment='raw',save_csv=True):

	start_dt = datetime.strptime(start, '%Y-%m-%dT%H:%M:%S%z')     # Alpaca provides data from 2016
	if(end == 'today'): 
        utc_now = pytz.utc.localize(datetime.utcnow())
    	end_dt=utc_now.astimezone(pytz.timezone(tz)).replace(microsecond=0).isoformat()
	else: end_dt = datetime.strptime(end, '%Y-%m-%dT%H:%M:%S%z')

    i = 1
    for sym in symbols:
        try:
            # Get barset data of indicated stock in snapshots of 10,000 minutes at a time
            bars_df = get_stock_data(api, sym, start_dt, end_dt, timeframe, limit, tz, adjustment)
            bars_df.index = df.index.tz_convert(None)
            if(save_csv == True): bars_df.to_csv(sym + '.csv')
            print('Progress: (%d / %d)\r' % (i, len(symbols)), end='')
            i += 1
        except Exception as e:
            print('Error: %s' % str(e))
            print('Stock: %s' % sym)
            print('------------------------')

    print('')
    print('Done!')
    