from pymongo import MongoClient


class MongoDB():
    mongo_client = None
    mongo_db = None

    def __init__(self, connection_string, db):
        """Creates db objects and connects to db

        Args:
            connection_string (String): 
            db (mongo db)

        Raises:
            e: error connecting to db
        """
        super().__init__()

        try:
            self.mongo_client = MongoClient(connection_string)

            self.mongo_db = self.mongo_client[db]
            print("Successfully connected to MongoDB!")
        except Exception as e:
            print(e)
            raise e

    def upsert_to_mongodb(self, col, _id = None, data = None):
        """Writes or updates data for a document with a given id
        Searches for the id or the combination of a string and the data from the dict

        Args:
            col (mongodb collection):
            id (dict or String): key value pairs or String with field name
            data (dict)
        """
        try:
            for key, value in data.items():
                if type(_id) is dict:
                    result = col.update_one(
                        _id, {"$set":  {key: value}}, upsert=True)
                else:
                    col.update_one(
                        {_id : key}, {"$set":  {"data": value}}, upsert=True)
            return True

        except Exception as e:
            print(e)
            return False

    def get_collection(self, name):
        try:
            return self.mongo_db[name]
        except Exception as e:
            print(e)
            return None
