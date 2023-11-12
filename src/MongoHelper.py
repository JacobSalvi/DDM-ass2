import itertools
import math
from enum import Enum
from pymongo import MongoClient
from geopy.distance import great_circle


class Rating(Enum):
    excellent = 5
    very_good = 4
    average = 3
    poor = 2
    terrible = 1


def prettify(elements):
    for element in elements:
        if element is not None:
            print(element)


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
        restaurants = self.__db["Restaurants"].find({"Position.city": city_name})
        return [restaurant.get("restaurant_link") for restaurant in restaurants]

    # Query ok
    def search_with_feature(self, feature: str, city: str) -> list:
        """
        filter restaurants in an area that posses a feature
        :param feature: word to search
        :param city: city to search in
        :return: list of restaurants
        """
        city_link: list = self.search_in_city(city_name=city)
        restaurants = self.__db["Restaurants"].find(
            {"restaurant_link": {"$in": city_link}, "features": {"$regex": f".*{feature}.*"}})
        return [restaurant for restaurant in restaurants]

    # Query ok
    def search_popular_in_city(self, city_name: str) -> list:
        """
        return the 3 most popular places (generic) in a city
        :param city_name: name of the city
        :return: list of restaurants link
        """
        restaurants = self.__db["Restaurants"].find({"Popularity.popularity_generic":
                                                         {"$regex": f"^#[0-9]\D.*{city_name}$"}}).sort(
            {"Popularity.popularity_generic": 1}).limit(3)
        return [restaurant.get("restaurant_link") for restaurant in restaurants]

    # Query ok
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
                                "$subtract": [my_longitude, "$Position.longitude"]
                            },
                            2
                        ]
                    },
                    {
                        "$pow": [
                            {
                                "$subtract": [my_latitude, "$Position.latitude"]
                            },
                            2
                        ]
                    }
                ]
            }
        }
        restaurants = self.__db["Restaurants"].find({"$expr": {"$lte": [expression, max_distance]}}).limit(10)
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
                                                   '$add': ["$Rating.food", "$Rating.atmosphere", "$Rating.value",
                                                            "$Rating.service"]},
                                                   "restaurant_link": 1}).sort({"weightedRating": -1}).limit(10)

        result = []
        for row in cursor:
            result.append(row)
        return result

    def get_english_speaking_always_open_restaurants(self, open_days: int, reviews: int, min_price: int,
                                                     max_price: int):
        cursor = self.__db["Restaurants"].find({"Schedule.open_days_per_week": open_days,
                                                "Review.total_reviews_count": {"$gte": reviews},
                                                "Review.default_language": "English",
                                                "Price.min_price": {"$gte": min_price},
                                                "Price.max_price": {"$lte": max_price}})
        result = []
        for row in cursor:
            result.append(row)
        return result

    def find_most_expensive_restaurant_in_each_country(self, pretty=True):
        cursor = self.__db["Restaurants"].aggregate([
            {"$match": {"Price.price_level": "€€-€€€"}},
            {"$match": {"Price.max_price": {"$exists": True}}},
            {"$sort": {"Price.max_price": -1}},
            {"$group": {
                "id": "$Position.country",
                "most_expensive_restaurant": {"$first": "$$ROOT"}
            }}
        ])
        if not pretty:
            return list(cursor)
        else:
            return prettify([{"Country": row['id'],
                              "restaurant": row["most_expensive_restaurant"]["restaurant_name"],
                              "Max Price":  row["most_expensive_restaurant"]["Price"]["max_price"],
                              "Symbolic price": row["most_expensive_restaurant"]["Price"]["price_level"]
                              }
                             for row in cursor])

    def find_top10_highest_rating_restaurant_in_the_5most_popular_cities(self, pretty=True):
        # Find the most popular cities in the world,
        # in order to do it we assumed that the most popular cities are the cities with most entries in the db
        top_cities_cursor = self.__db["Restaurants"].aggregate([
            {"$match": {"Position.city": {"$exists": True, "$ne": ""}}},
            {"$group": {"id": "$Position.city", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 5}
        ])

        # Retrive the highest revived restaurant in the most popular cities. The reviewed score is determined useing
        # both the average rating of a restaurant and the number of excellent reviews
        top_cities = [city['id'] for city in top_cities_cursor]
        restaurant_cursor = self.__db["Restaurants"].find({
            "Position.city": {"$in": top_cities},
        }).sort([("Rating.avg_rating", -1), ("Rating.excellent", -1)]).limit(10)

        if not pretty:
            return [restaurant for restaurant in restaurant_cursor]
        else:
            return prettify([{
                "City": restaurant["Position"]["city"],
                "Restaurant": restaurant["restaurant_name"],
                "Average rating": restaurant["Rating"]["avg_rating"],
                "Number of excellent ratings": restaurant["Rating"]["excellent"]
            }
                for restaurant in restaurant_cursor])

    def get_top5_countries_with__highest_average_excellent_reviews(self, pretty=True):
        cursor = self.__db["Restaurants"].aggregate([
            {"$match": {"Rating.excellent": {"$exists": True}}},
            {"$group": {
                "country": "$Position.country",
                "total_excellents": {"$sum": "$Rating.excellent"},
                "num_restaurants": {"$sum": 1}
            }},
            {"$project": {
                "avg_excellent": {"$divide": ["total_excellents", "num_restaurants"]}
            }},
            {"$sort": {"avg_excellent_Reviews": -1}},
            {"$limit": 5}
        ])
        if not pretty:
            return [result for result in cursor]
        else:
            return prettify([{"Country": row['country'],
                              "Average of excellent reviews": math.floor(row['avg_excellent'])}
                             for row in cursor])

    def find_the_closest_three_restaurant_in_randon_city(self, city="Rome"):

        # find a rancom city with at least 10 restaurant and at most 100
        cities = [
            {"$group": {"_id": "$Position.city", "counts": {"$sum": 1}}},
            {"$match": {"counts": {"$gte": 10, "$lte": 100}}},
            {"$sample": {"size": 1}}
        ]
        city = list(self.__db["Restaurants"].aggregate(cities))

        city_name = city[0]["_id"] if city else None

        if city_name is None:
            raise ValueError("city name is none")

        # Create a map with all restaurants in city,
        # containing the key res
        query = {
            "Position.city": city_name,
            "Position.latitude": {"$exists": True},
            "Position.longitude": {"$exists": True}
        }
        restaurants_positions = list(self.__db["Restaurants"].find(query,
                       {"restaurant_name": 1, "Position.latitude": 1,"Position.longitude": 1}))

        if restaurants_positions is None:
            raise ValueError("no positions for restaurant")
        # Calculating pairwise distances
        min_distance = float("inf")
        triple = None

        for r1, r2, r3 in itertools.combinations(restaurants_positions, 3):

            location1 = (r1["Position"]["latitude"], r1["Position"]["longitude"])
            location2 = (r2["Position"]["latitude"], r2["Position"]["longitude"])
            location3 = (r3["Position"]["latitude"], r3["Position"]["longitude"])
            distance = great_circle(location1, location2, location3).meters

            if distance < min_distance:
                min_distance = distance
                triple = (r1["restaurant_name"], r2["restaurant_name"], r3["restaurant_name"])

        print(f"in City {city_name} closest restaurants between each oter are: {triple[0]},{triple[1]} and {triple[2]}, "
              f"distance between the them is {min_distance} m")

    def increase_price_for_restaurants_with_seating(self, minimum_price: int, increase: int):
        self.__db["Restaurants"].update_one(filter={"Position.city": "Paris",
                                                    "features": {"$all": ["Seating", "ServesAlcohol"]},
                                                    "FoodInfo.cuisines": {"$in": ["French"]},
                                                    "Schedule.open_days_per_week": {"$gte": 5}
                                                    },
                                            update=[{
                                                "$set": {
                                                    "Price.min_price": {
                                                        "$switch": {
                                                            "branches": [
                                                                {"case":
                                                                     {"$eq": ["Price.min_price", None]},
                                                                 "then": minimum_price
                                                                 }
                                                            ],
                                                            "default": {"$sum": ["Price.min_price", increase]}
                                                        }
                                                    }

                                                },
                                            }])

    # Command ok
    def update_ratings(self, restaurant_link: str, rating: Rating):
        """
        update the rating of a restaurant
        :param restaurant_link: the link to the restaurant
        :param rating: new rating to add
        :return:
        """
        old_rating = self.__db["Restaurants"].find_one({"restaurant_link": restaurant_link}).get("Rating")
        if not rating or not old_rating:
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

        self.__db["Restaurants"].update_one({"restaurant_link": restaurant_link},
                                            {"$set": {
                                                "Rating.avg_rating": new_average,
                                                f"Rating.{rating.name}": old_rating_table[rating.name]
                                            },
                                                "$inc": {"Review.total_reviews_count": 1}
                                            })

    # Command ok
    def update_restaurant_feature(self, restaurant_link: str, new_feature: str):
        """
        add a feature to a restaurant if it do not exist
        :param restaurant_link: link to restaurant
        :param new_feature: feature to add
        """
        self.__db["Restaurants"].update_one({"restaurant_link": restaurant_link}, {
            "$addToSet": {"features": new_feature}
        })
