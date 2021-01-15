from pymongo import MongoClient
from src.utility.logger import logger


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
            logger.info("Successfully connected to MongoDB!")
        except Exception as e:
            logger.error(f'Error connecting to MongoDB : {e}')
            raise e

    def upsert_to_mongodb(self, col, _id=None, data=None, mode="$set"):
        """Writes or updates data for a nested document with a given id

        Args:
            col (mongodb collection):
            id (dict): key value pairs
            data (dict): nested data
            mode (String): operator

        Returns bool: Operation successfully
        """

        def upsert_nested_data(key, value):
            """Recursive call if value is dict. Add dot notation for nested objects. Upsert single value if not

            Args:
                key (String)
                value (dict or scalar type)
            """
            if type(value) is dict:
                for key_sub, value_sub in value.items():
                    new_key = f'{key}.{key_sub}'  # add dot notation
                    upsert_nested_data(new_key, value_sub)
            else:
                col.update_one(
                    _id, {mode:  {key: value}}, upsert=True)

        try:
            for key, value in data.items():
                upsert_nested_data(key, value)

            return True

        except Exception as e:
            logger.error(
                f'Error updating data to MongoDB collection <{col}> : {e}')
            return False

    def get_collection(self, name):
        try:
            return self.mongo_db[name]
        except Exception as e:
            logger.error(f'Error getting collection <{name}> : {e}')
            return None
