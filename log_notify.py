import sys
import os
#import win32com.client as win32
from datetime import datetime
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError



class Logger(object):

    """
    Class to log output of the command line to a log file

    Usage:
    log = Logger('logfile.log')
    print("inside file")
    log.stop()
    print("outside file")
    log.start()
    print("inside again")
    log.stop()
    """

    def __init__(self, filename):
        self.filename = filename

    class Transcript:
        def __init__(self, filename):
            self.terminal = sys.stdout
            self.log = open(filename, "a")
        
        def write(self, message):
            date : str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')          
            self.log.write(f"{date}   {message}   ")
            self.terminal.write(message)
            
        def flush(self):
            pass

    def start(self):
        sys.stdout = self.Transcript(self.filename)

    def stop(self):
        sys.stdout.log.close()
        sys.stdout = sys.stdout.terminal


class slack_bot:
    
    def __init__(self):
        self.bot_slack_token : str = '######'
        self.channel : str = '######'
        self.client = WebClient(token=self.bot_slack_token)

# Function to send a message to a specific channel
    def send_message(self, error : str, file : str = ''):
        if file != '':
            message : str = f'{file} has stopped running due to the following error : {error}' 
        else:
            message : str = error
        try:
            response = self.client.chat_postMessage(
                channel=self.channel,
                text=message
            )
            print("Message sent successfully: ", response["ts"])
        except SlackApiError as e:
            print("Error sending message:", e.response["error"])
            
    
class notify(slack_bot):
    
    def __init__(self):
        super().__init__()
        self.mail_to : str = '######'
        
    # def send_mail(self,error,script):
    #     outlook = win32.Dispatch('outlook.application')
    #     mail = outlook.CreateItem(0)
    #     mail.To = self.mail_to
    #     mail.Subject = f'Error occured while running {script}'
    #     mail.Body = error
    #     #mail.HTMLBody = '<h2>HTML Message body</h2>' #this field is optional

    #     mail.Send()
  
        