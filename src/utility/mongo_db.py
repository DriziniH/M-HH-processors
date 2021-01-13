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

    def upsert_to_mongodb(self, col, _id, _id_value = None, data = None):
        """Writes or updates data for document with given id

        Args:
            col (mongodb collection):
            _id (String): id field
            _id_value (Scalar type): id value
            data (dict)
        """
        try:
            for key, value in data.items():
                if _id_value:
                    col.update_one(
                        {_id: _id_value}, {"$set":  {key: value}}, upsert=True)
                else:
                    col.update_one(
                        {_id: key}, {"$set":  {"data": value}}, upsert=True)

        except Exception as e:
            print(e)

    def get_collection(self, name):
        try:
            return self.mongo_db[name]
        except Exception as e:
            print(e)
            return None
