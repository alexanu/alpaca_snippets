    
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
