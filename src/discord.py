import requests
import os
import datetime as dt
import polars as pl
import json
from dotenv import load_dotenv


def send_msg(signal):
    load_dotenv()
    url = os.environ.get('DISCORD_WEBHOOK')
    embed = {
        "description": "Last found signals",
        "title": "Signal"
    }

    entries = []
    for i, item in enumerate(signal):
        entry = {}
        entry["name"] = item
        if type(signal[item]) == dt.datetime:
            entry["value"] = signal[item].strftime("%m/%d/%Y %H:%M:%S")
        else:
            entry["value"] = str(signal[item])
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
    
    result = requests.post(url, json=data, headers=headers)
    if 200 <= result.status_code < 300:
        print(f"Webhook sent {result.status_code}")
    else:
        print(f"Not sent with {result.status_code}, response:\n{result.json()}")


if __name__ == "__main__":
    print('main')