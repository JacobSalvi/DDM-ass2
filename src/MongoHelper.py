from enum import Enum

from pymongo import MongoClient


class Rating(Enum):
    excellent = 5
    very_good = 4
    average = 3
    poor = 2
    terrible = 1


class MongoHelper:
    def __init__(self, host: str, port: int, dbName: str):
        self.__client: MongoClient = MongoClient(host=host, port=port)
        self.__db = self.__client[dbName]

    def add_to_collection(self, collection_name: str, element: dict):
        self.__db[collection_name].insert_one(element)

    def add_many_to_collection(self, documents, collection_name="Papers"):
        self.__db[collection_name].insert_many(documents=documents)

    def get_restaurants(self, restaurants_link: list) -> list:
        restaurants = self.__db["Restaurants"].find({"restaurant_link": {"$in": restaurants_link}})
        return [restaurant for restaurant in restaurants]

    def search_in_city(self, city_name: str) -> list:
        """
        helper functions to return restaurants in a single city
        :param city_name: name of the city
        :return: list of links to restaurants
        """
        restaurants = self.__db["Position"].find({"city": city_name})
        return [restaurant.get("restaurant_link") for restaurant in restaurants]

    # Query
    def search_with_feature(self, feature: str, city: str) -> list:
        """
        filter restaurants in an area that posses a feature
        :param feature: word to search
        :param city: city to search in
        :return: list of restaurants
        """
        city_link: list = self.search_in_city(city_name=city)
        restaurants = self.__db["Restaurants"].find({"restaurant_link": {"$in": city_link}, "features": {"$regex": f".*{feature}.*"}})
        return [restaurant for restaurant in restaurants]

    # Query
    def search_popular_in_city(self, city_name: str) -> list:
        """
        return the 3 most popular places (generic) in a city
        :param city_name: name of the city
        :return: list of restaurants link
        """
        restaurants = self.__db["Popularity"].find({"popularity_generic": {"$regex": f"^#[0-9]\D.*{city_name}$"}}).sort({"popularity_generic": 1}).limit(3)
        return [restaurant.get("restaurant_link") for restaurant in restaurants]
        # return [restaurant for restaurant in restaurants]  # for testing

    # Query
    def search_close_restaurants(self, my_latitude: float, my_longitude: float, max_distance: float) -> list:
        """
        find all restaurants in an area, Warning, the database seem to have incorrect values!!!
        :param my_latitude: latitude of center point to search
        :param my_longitude: longitude of center point to search
        :param max_distance: maximum distance from center point of search (is in degree, so very small value
        should be provided, 1 deg of lat is around 111km, 1 del of long is 71 km)
        :return: list of restaurants links
        """
        expression = {
            "$sqrt": {
                "$add": [
                    {
                        "$pow": [
                            {
                                "$subtract": [my_longitude, "$longitude"]
                            },
                            2
                        ]
                    },
                    {
                        "$pow": [
                            {
                                "$subtract": [my_latitude, "$latitude"]
                            },
                            2
                        ]
                    }
                ]
            }
        }
        restaurants = self.__db["Positions"].find({"$expr": {"$lte": [expression, max_distance]}}).limit(10)
        return [restaurant.get("restaurant_link") for restaurant in restaurants]
        # return [{"city": restaurant.get("city"), "lat": restaurant.get("latitude"), "lon": restaurant.get("longitude")} for restaurant in restaurants]  # for test


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

    def get_english_speaking_always_open_restaurants(self):
        cursor = self.__db["Schedule"].find({"open_days_per_week": 7,
                                             "restaurant_link": {"$in": {
                                                 self.__db["Review"].find({"total_review_count": {"$gte": 0},
                                                                           "default_language": "English",
                                                                           "restaurant_link": {"$in":{
                                                                               self.__db["Price"].find({
                                                                                   ""
                                                                               })

                                                                           }}}).distinct("restaurant_link")
                                             }}})

        result = []
        for row in cursor:
            result.append(row)
        return result

    # Command
    def update_ratings(self, restaurant_link: str, rating: Rating):
        """
        update the rating of a restaurant
        :param restaurant_link: the link to the restaurant
        :param rating: new rating to add
        :return:
        """
        old_rating = self.__db["Ratings"].find_one({"restaurant_link": restaurant_link})
        if not rating:
            print(f"Restaurant link: {restaurant_link} not found in DB")
            return
        old_rating_table: dict = {
            "excellent": old_rating.get("excellent"),
            "very_good": old_rating.get("very_good"),
            "average": old_rating.get("average"),
            "poor": old_rating.get("poor"),
            "terrible": old_rating.get("terrible"),
        }
        old_rating_table[rating.name] = old_rating_table[rating.name] + 1

        total = 0
        review_count = 0
        for key, val in old_rating_table.items():
            review_count = review_count + val
            total = total + (val * Rating[key].value)
        new_average = total / review_count

        self.__db["Ratings"].update_one({"restaurant_link": restaurant_link},
                                        {"$set": {
                                            "avg_rating": new_average,
                                            rating.name: old_rating_table[rating.name]
                                        }})

        self.__db["Review"].update_one({"restaurant_link": restaurant_link},
                                       {"$inc": {"total_reviews_count": 1}})
