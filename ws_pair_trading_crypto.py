

def place_trades(spread, doge_entry_level, doge_loss_exit_level, shib_entry_level, shib_loss_exit_level):
    # there is an active position if there is 1 position
    active_position = len(rest_api.list_positions()) == 1
    
    if spread < doge_entry_level and not active_position:
        if rest_api.list_positions()[0].symbol == "SHIBUSD":
            shib_qty = rest_api.list_positions()[0].qty
        else:
            shib_qty = rest_api.list_positions()[1].qty

        # Place sell order for SHIB
        rest_api.submit_order(symbol="SHIBUSD", type="market", qty=shib_qty/2, side="sell", time_in_force="day")

        # place long order on DOGEUSD
        doge_notional_size = float(rest_api.get_account().cash)
        rest_api.submit_order(symbol="DOGEUSD", notional=doge_notional_size, type="market", side="buy", time_in_force="day")

    if spread > shib_entry_level and not active_position:
        if rest_api.list_positions()[0].symbol == "DOGEUSD":
            doge_qty = rest_api.list_positions()[0].qty
        else:
            doge_qty = rest_api.list_positions()[1].qty

        # Place sell order for DOGE
        rest_api.submit_order(symbol="DOGEUSD", type="market", qty=doge_qty/2, side="sell", time_in_force="day")

        # place long order on SHIBUSD
        shib_notional_size = float(rest_api.get_account().cash)
        rest_api.submit_order(symbol="SHIBUSD", notional=shib_notional_size, type="market", side="buy", time_in_force="day")

    elif spread < doge_loss_exit_level and active_position and (rest_api.list_positions()[0].symbol == "DOGEUSD"):
        # liquidate if loss exit level is breached
        if rest_api.list_positions()[0].symbol == "DOGEUSD":
            doge_qty = rest_api.list_positions()[0].qty
        else:
            doge_qty = rest_api.list_positions()[1].qty

        # Place sell order for DOGE
        rest_api.submit_order(symbol="DOGEUSD", type="market", qty=doge_qty/2, side="sell", time_in_force="day")

        # place long order on SHIBUSD
        shib_notional_size = float(rest_api.get_account().cash)
        rest_api.submit_order(symbol="SHIBUSD", notional=shib_notional_size, type="market", side="buy", time_in_force="day")

    elif spread > 0 and active_position and (rest_api.list_positions()[0].symbol == "DOGEUSD"):
        if rest_api.list_positions()[0].symbol == "DOGEUSD":
            doge_qty = rest_api.list_positions()[0].qty
        else:
            doge_qty = rest_api.list_positions()[1].qty

        # Place sell order for DOGE
        rest_api.submit_order(symbol="DOGEUSD", type="market", qty=doge_qty/2, side="sell", time_in_force="day")

        # place long order on SHIBUSD
        shib_notional_size = float(rest_api.get_account().cash)
        rest_api.submit_order(symbol="SHIBUSD", notional=shib_notional_size, type="market", side="buy", time_in_force="day")

    elif spread > shib_loss_exit_level and active_position and (rest_api.list_positions()[0].symbol == "SHIBUSD"):
        # liquidate if loss exit level is breached

        if rest_api.list_positions()[0].symbol == "SHIBUSD":
            shib_qty = rest_api.list_positions()[0].qty
        else:
            shib_qty = rest_api.list_positions()[1].qty

        # Place sell order for SHIBUSD
        rest_api.submit_order(symbol="SHIBUSD", type="market", qty=shib_qty/2, side="sell", time_in_force="day")

        # place long order on DOGEUSD
        doge_notional_size = float(rest_api.get_account().cash)
        rest_api.submit_order(symbol="DOGEUSD", notional=doge_notional_size, type="market", side="buy", time_in_force="day")

    elif spread < 0 and active_position and (rest_api.list_positions()[0].symbol == "SHIBUSD"):
        # liquidate if 0 spread is crossed with an active position

        if rest_api.list_positions()[0].symbol == "SHIBUSD":
            shib_qty = rest_api.list_positions()[0].qty
        else:
            shib_qty = rest_api.list_positions()[1].qty

        # Place sell order for SHIB
        rest_api.submit_order(symbol="SHIBUSD", type="market", qty=shib_qty/2, side="sell", time_in_force="day")

        # place long order on DOGEUSD
        doge_notional_size = float(rest_api.get_account().cash)
        rest_api.submit_order(symbol="DOGEUSD", notional=doge_notional_size, type="market", side="buy", time_in_force="day")


def calculate_live_spread(bar):
    data = {}
    time = datetime.datetime.fromtimestamp(bar.timestamp / 1000000000) # Convert bar timestamp to human readable form
    symbol = bar.symbol

    data[time][symbol] = bar  # If we've reached this point, then we have data for both symbols for the current timestamp
    timeslice = data[time]  # retrieve dictionary containing bar data from a single timestamp
    doge_data = timeslice["DOGEUSD"]
    shib_data = timeslice["SHIBUSD"]
    doge_close = doge_data.close
    shib_close = shib_data.close

    spread = doge_close - shib_close
    spread_std = timeslice["spread_std"] # we will use the historical STD
    
    # calculate entry and exit levels for standard deviation (dogecoin long, shiba inu short)
    doge_entry_level = -1 * spread_std
    doge_loss_exit_level = -3 * spread_std

    # calculate entry and exit levels for standard deviation (shiba inu long, dogecoin short)
    shib_entry_level = 1 * spread_std
    shib_loss_exit_level = 3 * spread_std

    place_trades(spread, doge_entry_level, doge_loss_exit_level, shib_entry_level, shib_loss_exit_level)



alpaca_stream = tradeapi.Stream(API_KEY, SECRET_KEY)

async def on_crypto_bar(bar):
    print(bar)
    calculate_live_spread(bar)

alpaca_stream.subscribe_crypto_bars(on_crypto_bar, "DOGEUSD", "SHIBUSD")

alpaca_stream.run()
