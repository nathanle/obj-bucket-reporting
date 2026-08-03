#!/root/.venv/bin/python3
#!/usr/bin/python3
import os
from slack_sdk import WebClient
from slack_sdk.webhook import WebhookClient
from slack_sdk.errors import SlackApiError


def slack_send(filename):
    slack_token = os.environ.get("SLACK_BOT_TOKEN")
    url = os.environ.get("WEBHOOK_URL")
    message = os.environ.get("SLACK_MESSAGE")
    webhook = WebhookClient(url)
    response = webhook.send(text=message.format(filename))
    assert response.status_code == 200
    assert response.body == "ok"
