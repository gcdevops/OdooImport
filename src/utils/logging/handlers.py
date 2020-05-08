import os 
import requests
import json
from logging import Handler, Formatter, ERROR

class SlackHandler(Handler):
    def __init__(self, level=ERROR, slack_url=None):
        super().__init__()

        if slack_url is None:
            self.slack_url = os.environ.get("SLACK_URL")
        else:
            self.slack_url = slack_url
    
    def emit(self, record):
        if(self.slack_url):
            formatted_log = self.format(record)
            requests.post(
                self.slack_url, 
                json.dumps({"text": formatted_log }), 
                headers={"Content-Type": "application/json"}
            )