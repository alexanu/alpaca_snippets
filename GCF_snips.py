
# requirements.txt
    alpaca-trade-api
    google-cloud-storage==1.30.0


# Data from external API to csv and store to bucket

    import requests
    import pandas as pd
    from google.cloud import storage

    def api_to_gcs(url, endpoint, filename):
        data = requests.get(url)
        json = data.json()
        df = pd.DataFrame(json[endpoint])
        client = storage.Client(project='example-project-123')
        bucket = client.get_bucket('example-storage-bucket')    blob = bucket.blob(filename)
        blob.upload_from_string(df.to_csv(index = False),content_type = 'csv')


# Schedule Email sending

    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.application import MIMEApplication

    def schedule_email(event, context):
        msg = MIMEMultipart()
        fromaddr = 'sender@hotmail.com'
        msg['From'] = fromaddr
        msg["To"] = "receiver@gmail.com"
        msg['Subject'] = "Test Report"

        htmlEmail = """
        <p> Dear June, <br/><br/>
            Please find the attached monthly Report below.<br/><br/>
        <br/></p>
            Thank you! <br/><br/>
            Best Regards, <br/>
            June </p>
        <br/><br/>
        <font color="red">Please do not reply to this email as it is auto-generated via GCP. </font>
        """


        msg.attach(MIMEText(htmlEmail, 'html'))
        server = smtplib.SMTP('smtp.office365.com', 587)
        server.starttls()
        server.login(fromaddr, "password")
        text = msg.as_string()
        server.sendmail(fromaddr, ['receiver@gmail.com'], text)
        server.quit()

        print("Email are sent successfully!")


# Functions Framework https://cloud.google.com/functions/docs/functions-framework
    # Video from PTL: https://www.youtube.com/watch?v=j1_lqxsdJ8E
    # FF for Python: https://github.com/GoogleCloudPlatform/functions-framework-python
        # pip install functions-framework
        # create function "def submit_custom_order(request):" in main.py
        # functions-framework --target=submit_custom_order
            # functions-framework --target submit_custom_order --debug
            # the result will appear on localhost:8080
        # Install Insomnia for testing API requests => Create Request Collection => Create New Request (POST, JSON)
        # Put "localhost:8080" in URL field near "Send Button"
        # Enter JSON which will be send to localhost and envoce function Hello => output will appear on the right


# Submit custom order

    import alpaca_trade_api as tradeapi
    import Alpaca_config
    # from os import environ


    def submit_custom_order(request):

        alpaca = tradeapi.REST(Alpaca_config.API_KEY_PAPER, Alpaca_config.API_SECRET_PAPER, Alpaca_config.API_BASE_URL_PAPER, 'v2')
        # alpaca = tradeapi.REST(environ["API_KEY_PAPER"], environ["API_SECRET_PAPER"], environ["API_BASE_URL_PAPER"], 'v2')

        data = request.get_json()

        try:
            order = alpaca.submit_order(data['symbol'],data['quantity'],data['side'],data['order_type'],data['time_in_force'])
            print(order)
        except Exception as e:
            return{
                "code": "error",
                "message": str(e)
            }

        account = alpaca.get_account()

        return {
            'Order ID': order.id,
            'Order Time': str(order.created_at),
            'Order Status': order.status,
            'Buying Power': float(account.buying_power)
        }


# Schedule => PubSub => Function => Storage
    # https://towardsdatascience.com/how-to-schedule-a-serverless-google-cloud-function-to-run-periodically-249acf3a652e
    # Make sure to enable APIs for Google Cloud Storage, Functions, Pub/Sub, and Scheduler, ...
    # ... in your GCP project using the API console.
        # name of function: alpaca_pubsub_schedule_to_bucket
        # pubsub topic name: projects/alpaca-traiding/topics/alpaca_schedule_to_bucket

    from google.cloud import storage
    from datetime import datetime
    import alpaca_trade_api as tradeapi
    from os import environ
    # import Alpaca_config

    def day_chg():
        # alpaca = tradeapi.REST(Alpaca_config.API_KEY_PAPER, Alpaca_config.API_SECRET_PAPER, Alpaca_config.API_BASE_URL_PAPER, 'v2')
        alpaca = tradeapi.REST(environ["API_KEY_PAPER"], environ["API_SECRET_PAPER"], environ["API_BASE_URL_PAPER"], 'v2')
        snapshots_dict = alpaca.get_snapshot('SPY')
        day_change = round((snapshots_dict.daily_bar.close/snapshots_dict.prev_daily_bar.close-1)*100,2)
        return day_change

    def upload_blob(bucket_name, source_file_name, destination_blob_name):
        """Uploads a file to the bucket."""
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)

    def create_file_name():
        '''Generate a .txt filename using the current timestamp'''    
        date = datetime.now().strftime("%Y_%m_%d-%H_%M_%S_%P")
        file_name = 'SPY_' + date + '.txt'
        return file_name

    def write_to_file(file_name):
        file_name = '/tmp/' + file_name     # in cloud function environment we can only write to the /tmp directory.
        with open(file_name, 'w') as f:
            f.write("SPY chg is {}%".format(day_chg()))
        f.close()
    
    def hello_pubsub(event, context): # name of this function goes as 
        """Triggered from a message on a Cloud Pub/Sub topic.
        Args:
            event (dict): Event payload.
            context (google.cloud.functions.Context): Metadata for the event.
        """
        file_name = create_file_name() # The name of the file in GCS bucket once uploaded
        write_to_file(file_name)
        bucket_name = 'alpaca-traiding-algos' # name of storage bucket
        local_file_location = '/tmp/' + file_name # in cloud function environment we can only write to the /tmp directory.
        upload_blob(bucket_name, local_file_location, file_name)


# msg to Slack

    # Video about: https://www.youtube.com/watch?v=DfSQ2Qf4DR

    # On Slack: 1) create a channel (or use existing); 
    #           2) go to "Add App" and search for "Incoming Webhook"; 
    #           3) in the setting look for "Webhook URL" field and copy URL from there

    {
        "slack_webhook": "https://hooks.slack.com/services/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",
        "channel": "#alpaca-trading",
        "username": "manual-lambda-webhook",
        "message": "Hellow world"
    }

    #!/usr/bin/python3.9
    import urllib3
    import json

    http = urllib3.PoolManager()

    def send_to_Slack(event, context):
        
        url = event['slack_webhook'] # "<slack-webhook-url>"
        
        msg = {
            "channel": event['channel'], # "<slack-channel>"
            "username": event['username'], # "AWS-LAMBDA",
            "text": event['message']
        }
        
        encoded_msg = json.dumps(msg).encode('utf-8')
        
        response = http.request('POST',url, body=encoded_msg)
        
        print({
            "message": event['message'], 
            "status_code": response.status, 
            "response": response.data
        })
