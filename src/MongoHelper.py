from pymongo import MongoClient


class MongoHelper:
    def __init__(self, host: str, port: int, dbName: str):
        self.__client: MongoClient = MongoClient(host=host, port=port)
        self.__db = self.__client[dbName]

    def add_to_collection(self, collection_name: str, element: dict):
        self.__db[collection_name].insert_one(element)

    def add_many_to_collection(self, documents, collection_name="Papers"):
        self.__db[collection_name].insert_many(documents=documents)

    def database(self):
        return self.__db

    def get_vegan_restaurants_in_cities(self, cities: list[str]):
        result = self.__db["FoodInfo"].find({"vegetarian_friendly": "Y",
                                             "gluten_free": "Y",
                                             "restaurant_link":
                                                 {"$in": self.__db["Positions"]
                                            .find({"city": {"$in": cities}}).distinct("restaurant_link")}}).distinct("restaurant_link")

        restaurant = []
        for row in result:
            restaurant.append(row)
        return restaurant

    def sort_with_weighted_rating(self, country: str):
        cursor = self.__db["Ratings"].find(filter={"restaurant_link":
                            {"$in": self.__db["Positions"].find({"country": country}).distinct("restaurant_link")}},
                                           projection={"weightedRating": {'$add': ["$food", "$atmosphere", "$value","$service"]},
                                                       "restaurant_link": 1}).sort({"avg_rating": -1}).limit(10)

        result = []
        for row in cursor:
            result.append(row)
        return result

    def get_english_speaking_always_open_restaurants(self, number_of_open_days: int, review_count: int, min_price: int, max_price: int):

        cursor = self.__db["Schedule"].find({"open_days_per_week": number_of_open_days,
                                             "restaurant_link": {"$in":
                                                 self.__db["Review"].find({"total_reviews_count": {"$gte": review_count},
                                                                           "default_language": "English",
                                                                           "restaurant_link": {"$in":
                                                                               self.__db["Price"].find({
                                                                                   "min_price": {"$ne": None, "$gte": min_price},
                                                                                   "max_price": {"$ne": None, "$lte": max_price}
                                                                               }).distinct("restaurant_link")
                                                                           }}).distinct("restaurant_link")
                                             }})


        result = []
        for row in cursor:
            result.append(row)
        return result
