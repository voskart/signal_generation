import requests
import os
import polars as pl
from dotenv import load_dotenv


def send_msg(content: pl.DataFrame):
    load_dotenv()
    url = os.environ.get('DISCORD_WEBHOOK')
    embed = {
        "description": "Last found signals",
        "title": "Signal"
    }

    json_dump = content.write_json()

    data = {
        "content": json_dump,
        "username": "custom username",
        "embeds": [
            embed
        ],
    }

    headers = {
        "Content-Type": "application/json"
    }
    
    print(url)
    result = requests.post(url, json=data, headers=headers)
    if 200 <= result.status_code < 300:
        print(f"Webhook sent {result.status_code}")
    else:
        print(f"Not sent with {result.status_code}, response:\n{result.json()}")