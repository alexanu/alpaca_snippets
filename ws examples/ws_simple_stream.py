from Alpaca_config import *
import websocket, json

endpoint_stream = "wss://stream.data.alpaca.markets/v2/sip" # I have a paid subscription

trades_stream = ["AAPL"]
quotes_stream = ["AMD","CLDR"]
bars_stream = ["AAPL","VOO","SPY"]

def on_open(ws):
    auth = {
            "action": "auth",
            "key": API_KEY_PAPER,
            "secret": API_SECRET_PAPER
        }
    
    ws.send(json.dumps(auth)) # json.dumps convert json to string
    
    message = {
                "action": "subscribe",
                # "trades": trades_stream,
                # "quotes": quotes_stream,
                "bars": bars_stream
            }
                
    ws.send(json.dumps(message))

def on_message(ws, message):
    print(message)

def on_close(ws):
    print("closed connection")

ws = websocket.WebSocketApp(endpoint_stream, on_open=on_open, on_message=on_message, on_close=on_close)
ws.run_forever()