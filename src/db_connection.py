import pymongo
import os
import polars as pl
from dotenv import load_dotenv

class MongoConnection():
    load_dotenv()
    client = None

    def __init__(self, remote=True) -> None:
        URI, PASS, USER = "", "", ""
        if remote:
            URI = os.environ.get("MONGO_FT")
            USER = os.environ.get("USER_NAME_FT")
            PASS = os.environ.get("PASSWORD_FT")
        else:            
            URI = os.environ.get("MONGO_AV")
            USER = os.environ.get("USER_NAME_AV")
            PASS = os.environ.get("PASSWORD_AV")

        self.client = pymongo.MongoClient(URI, username=USER, password=PASS)

    def get_database(self, database="velodata"):
        try:
            return self.client.get_database(database)
        except:
            raise(NameError('Not Found'))
        
    def insert_signals(self, signals=pl.DataFrame):
        try:
            db = self.client.get_database("data")
            for signal in signals.to_dicts():
                db.signals.update_one(signal, signal, upsert=True)
        except:
            raise(NameError('Not Found'))


def main():
    mongoConnection = MongoConnection()
    db = mongoConnection.get_database("data")
    # print(db.futures.find_one())
    # print(db.options.find_one())



if __name__ == '__main__':
    main()