from pymongo import MongoClient


class MongoHelper:
    def __init__(self, host: str, port: int, dbName: str):
        self.__client: MongoClient = MongoClient(host=host, port=port)
        self.__db = self.__client[dbName]

    def add_to_collection(self, collection_name: str, element: dict):
        self.__db[collection_name].insert_one(element)

    def add_many_to_collection(self, documents, collection_name="Papers"):
        self.__db[collection_name].insert_many(documents=documents)