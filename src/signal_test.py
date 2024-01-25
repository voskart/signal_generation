from db_connection import MongoConnection
from pymongo.errors import ConnectionFailure, CollectionInvalid
import datetime as dt
import pandas as pd

class Signal():
    
    db = None

    def __init__(self) -> None:
        try:
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
    
def main():
    sig = Signal()
    futures_data = list(sig.get_futures_data())
    df = pd.DataFrame(futures_data)
    df.to_csv('./data/jan_data.csv')

if __name__ == '__main__':
    main()