from db_connection import MongoConnection
from pymongo.errors import ConnectionFailure, CollectionInvalid
import datetime as dt
import pandas as pd
import os
from dotenv import load_dotenv
from velodata import lib as velo

class Signal():
    
    db = None

    def __init__(self) -> None:
        try:
            load_dotenv()
            mc = MongoConnection()
            self.db = mc.client['velodata']
        except ConnectionFailure as e:
            raise e

    def get_futures_data(self, product='ETHUSDT', exchange='binance-futures', start_date=dt.datetime(2024, 1, 1), end_date=dt.datetime.now()):
        try:
            return self.db.futures.find({
                'product': product,
                'exchange': exchange,
            })
        except CollectionInvalid as e:
            raise e
        
    def get_options_data(self):
        client = velo.client(os.environ.get('VELO_API'))
        print(client.get_term_structure(coins=['BTC']))

    
def main():
    sig = Signal()
    futures_data = list(sig.get_futures_data(product='BTCUSDT'))
    df = pd.DataFrame(futures_data)
    df.to_csv('./data/jan_data_btc.csv')
    # options_data = list(sig.get_options_data())
    # df = pd.DataFrame(options_data)
    # print(df)

if __name__ == '__main__':
    main()