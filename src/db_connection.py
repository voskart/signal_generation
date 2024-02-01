import pymongo
import os
from dotenv import load_dotenv

class MongoConnection():
    URI = 'mongodb+srv://cluster0.2zoytm3.mongodb.net'
    client = None

    def __init__(self) -> None:
        load_dotenv()
        self.client = pymongo.MongoClient(MongoConnection.URI, username=os.environ.get('USER_NAME'), password=os.environ.get('PASSWORD'))

    def get_database(self):
        try:
            return self.client.get_database('velodata')
        except:
            raise(NameError('Not Found'))


def main():
    mongoConnection = MongoConnection()
    db = mongoConnection.client['velodata']
    print(f'Collections: {db.list_collection_names()}')
    # print(db.futures.find_one())
    # print(db.options.find_one())



if __name__ == '__main__':
    main()