import requests
import os
import datetime as dt
import polars as pl
import json
from dotenv import load_dotenv


def send_msg(content: pl.DataFrame):
    load_dotenv()
    url = os.environ.get('DISCORD_WEBHOOK')
    embed = {
        "description": "Last found signals",
        "title": "Signal"
    }

    entries = []
    for sig in content.rows():
        for i, item in enumerate(sig):
            entry = {}
            entry["name"] = content.columns[i]
            if type(item) == dt.datetime:
                entry["value"] = item.strftime("%m/%d/%Y %H:%M:%S")
            else:
                entry["value"] = str(item)
            if i != 0:
                entry["inline"] = "true"
            entries.append(entry)

    data = {
        "username": "custom username",
        "embeds": [
            {
                "title": "Signals",
                "fields": entries
            }
        ]
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