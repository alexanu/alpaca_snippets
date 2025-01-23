# Instructions for Deployment:
'''
- Sign in to the AWS console, go to the CloudFormation console and deploy s3_infra.yml 
    ***(you will have to change the BucketName in the .yml file to something unique before deployment)***
- Add your API keys to the corresponding lines in infra.yml
  - Your API Key ID should go on line 10 of infra.yml
  - Your API Secret Key should go on line 21 of infra.yml
- Go to the S3 console and upload the src code to your S3 Bucket
- In AWS console go to the CloudFormation console and deploy infra.yml
'''

import numpy as np
import talib
import websocket, json
import boto3
import alpaca_trade_api as tradeapi


closing_prices = []
in_position = False


class Strategy:
    def __init__(self):
        self.api = tradeapi.REST(
            key_id=bot.api_key,
            secret_key=bot.api_secret,
            base_url=bot.base_url,
            api_version="v2",
        )

    def order(self, side, qty, type, time_in_force, limit_price=None):
        """
        The order method to implment the buy/sell functionality of Afk Trader
        :param side: buy or sell
        :param qty: amount of the stock you would like to buy
        :param type: type of trade to be made usually 'market'
        :param time_in_force: how long an order will remain active before it is executed or expired
        :param limit_price: limit price of the side if available
        :return: True or False
        """
        try:
            print("Sending order")
            order_val = self.api.submit_order(
                side=side,
                qty=qty,
                symbol=bot.symbol,
                type=type,
                time_in_force=time_in_force,
                limit_price=limit_price,
            )
            print(order_val)
            return True
        except Exception as e:
            print("An exception occured - {}".format(e))
            return False

    def rsi_indicator(self, RSI_PERIOD, RSI_OVERBOUGHT, RSI_OVERSOLD, message):
        """
        Basic trading stretegy implemented using a RSI value to determine if the stock is overbought or oversold. And
        sends buy/sell orders accordingly to the Alpaca API
        :param RSI_PERIOD: The period used to calculate the RSI value
        :param RSI_OVERBOUGHT: The RSI value indicating when a stock should be considered overbought
        :param RSI_OVERSOLD: The RSI value indicating when a stock should be considered oversold
        :param message: websocket message
        :return: None
        """

        global in_position

        self.message = message
        self.RSI_PERIOD = RSI_PERIOD
        self.RSI_OVERBOUGHT = RSI_OVERBOUGHT
        self.RSI_OVERSOLD = RSI_OVERSOLD

        print(self.message)
        open_price = self.message[0]["o"]
        close_price = self.message[0]["c"]
        closing_prices.append(float(close_price))

        print(
            "The close price is {}".format(close_price),
            "The open price is {}".format(open_price),
        )
        print("Closing prices list: {} ".format(closing_prices))

        if len(closing_prices) > self.RSI_PERIOD:
            np_closing_prices = np.array(closing_prices)
            rsi_values = talib.RSI(np_closing_prices, self.RSI_PERIOD)

            print("All rsi's calculated so far: {}".format(rsi_values))

            last_rsi_value = rsi_values[-1]

            print("The current rsi value is {}".format(last_rsi_value))

            if last_rsi_value > self.RSI_OVERBOUGHT:
                if in_position:
                    print("The Stock Is Overbought: SELL NOW!!!")
                    # put alpaca sell logic here
                    order_succeeded = self.order(
                        side="sell", qty=1, type="market", time_in_force="day"
                    )

                    if order_succeeded:
                        in_position = False
                else:
                    print(
                        "The stock is overbought but, you do not own any. So, you are unable to sell."
                    )

            if last_rsi_value < self.RSI_OVERSOLD:
                if in_position:
                    print(
                        "The Stock Is oversold but, you already own it. So, you are unable to buy."
                    )
                else:
                    print("The Stock Is Oversold: BUY NOW!!!")
                    # put alpaca buy order logic here
                    order_succeeded = self.order(
                        side="buy", qty=1, type="market", time_in_force="day"
                    )
                    if order_succeeded:
                        in_position = True


def get_secret_api_token(token_name):
    """
    Accesses AWS Secrets Manager and retrieves the secret token names for the API
    :return: secret token values
    """
    client = boto3.client("secretsmanager", region_name="us-east-1")
    response = client.get_secret_value(SecretId=token_name)
    return response["SecretString"]


class AfkTrader:
    def __init__(self, base_url, socket, api_key, api_secret, symbol):
        self.base_url = base_url
        self.socket = socket
        self.api_key = api_key
        self.api_secret = api_secret
        self.symbol = symbol
        self.api = tradeapi.REST(
            key_id=self.api_key,
            secret_key=self.api_secret,
            base_url=self.base_url,
            api_version="v2",
        )

    def on_open(self, ws):
        """
        The on_open method to implement when websocket is opened. Subscribes to the stream feed of a particular stock
        :param ws: websocket
        :return: None
        """
        print("Connection Opened")
        auth_data = {"action": "auth", "key": self.api_key, "secret": self.api_secret}
        ws.send(json.dumps(auth_data))
        channel_data = {
            "action": "subscribe",
            # 'trades': [self.symbol],
            # 'quotes': [self.symbol],
            "bars": [self.symbol],
        }
        ws.send(json.dumps(channel_data))

    def on_close(self, ws):
        """
        The on_close method to implement when the websocket connection is closed
        :param ws: websocket
        :return: 'Connection Closed'
        """
        return "Connection Closed"

    def on_message(self, ws, message):
        """
        The on_message method to implement the trading strategy to be executed
        :param ws: websocket
        :param message: websocket message response
        :return: None
        """
        print("Received Message")
        self.mess = json.loads(message)

        Strategy().rsi_indicator(
            RSI_PERIOD=14, RSI_OVERBOUGHT=70, RSI_OVERSOLD=30, message=self.mess
        )


bot = AfkTrader(
    base_url="https://paper-api.alpaca.markets",  # paper API
    socket="wss://stream.data.alpaca.markets/v1beta1/crypto",  # crypto endpoint
    api_key=get_secret_api_token(token_name="API_KEY_ID"),
    api_secret=get_secret_api_token(token_name="API_SECRET"),
    symbol="BTCUSD",
)


if __name__ == "__main__":

    ws = websocket.WebSocketApp(
        bot.socket,
        on_open=bot.on_open,
        on_close=bot.on_close,
        on_message=bot.on_message,
    )
    ws.run_forever()

