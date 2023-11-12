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
        result = self.__db["Restaurants"].find({"FoodInfo.vegetarian_friendly": "Y",
                                                "FoodInfo.gluten_free": "Y",
                                                "Position.city": {"$in": cities}
                                                }).distinct("restaurant_link")

        restaurant = []
        for row in result:
            restaurant.append(row)
        return restaurant

    def sort_with_weighted_rating(self, country: str):
        cursor = self.__db["Restaurants"].find(filter={"Position.country": country},
                                               projection={"weightedRating": {
                                                   '$add': ["$Rating.food", "$Rating.atmosphere", "$Rating.value", "$Rating.service"]},
                                                   "restaurant_link": 1}).sort({"weightedRating": -1}).limit(10)

        result = []
        for row in cursor:
            result.append(row)
        return result

    def get_english_speaking_always_open_restaurants(self, open_days: int, reviews: int, min_price: int, max_price: int):
        cursor = self.__db["Restaurants"].find({"Schedule.open_days_per_week": open_days,
                                                "Review.total_reviews_count": {"$gte": reviews},
                                                "Review.default_language": "English",
                                                "Price.min_price": {"$gte": min_price},
                                                "Price.max_price": {"$lte": max_price}})

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

    # Command
    def update_restaurant_feature(self, restaurant_link: str, new_feature: str):
        """
        add a feature to a restaurant
        :param restaurant_link: link to restaurant
        :param new_feature: feature to add
        """
        old_features: str = self.__db["Restaurants"].find_one({"restaurant_link": restaurant_link}).get("features")
        feature_list: list = old_features.split(",")
        feature_list.append(new_feature)
        new_list: str = ",".join(feature_list)
        print(new_list)
        self.update_field(restaurant_link=restaurant_link,
                          collection="Restaurants",
                          update={"$set": {"features": new_list}})

    def update_field(self, restaurant_link: str, collection: str, update: dict):
        """
        Helper function to update any field of any collection
        :param restaurant_link: link to restaurant
        :param collection: collection name
        :param update: query to use as update
        """
        self.__db[collection].update_one({"restaurant_link": restaurant_link}, update)
