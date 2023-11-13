import json
import time
from enum import Enum

from src.CsvHandler import CsvHandler
from src.MongoHelper import MongoHelper, Rating


# (restaurant_link,restaurant_name,claimed,awards,keywords, features
#  original_location,country,region,province,city,address,latitude,longitude,
#  popularity_detailed,popularity_generic,top_tags,
#  price_level,price_range,
#  meals,cuisines,special_diets,vegetarian_friendly,vegan_options,gluten_free,
#  original_open_hours,open_days_per_week,open_hours_per_week,working_shifts_per_week,
#  total_reviews_count,default_language,reviews_count_in_default_language,
#  avg_rating,excellent,very_good,average,poor,terrible,food,service,value,atmosphere)



def initializeDB():
    csv_handler: CsvHandler = CsvHandler("tripadvisor_european_restaurants.csv")
    content = csv_handler.content()
    headers: dict[str, int] = csv_handler.header()
    restaurants = []
    for line in content:
        features = [] if line[headers["features"]] == "" else line[headers["features"]].replace(" ", "").split(",")
        awards = [] if line[headers["awards"]] == "" else line[headers["awards"]].split(",")
        keywords = [] if line[headers["keywords"]] == "" else line[headers["keywords"]].split(",")
        res = {"restaurant_link": line[headers["restaurant_link"]],
               "restaurant_name": line[headers["restaurant_name"]],
               "claimed": line[headers["claimed"]],
               "awards": awards,
               "keywords": keywords,
               "features": features
               }
        continent = line[headers["original_location"]].replace("[", "").replace('"', "").split(",")[0]
        position = {
            # "original_location": line[headers["original_location"]],
            "continent": continent,
            'country': line[headers["country"]],
            "region": line[headers["region"]],
            "province": line[headers["province"]],
            "city": line[headers["city"]],
            "address": line[headers["address"]],
            "latitude": float(line[headers["latitude"]]) if line[headers["latitude"]] != "" else 0,
            "longitude": float(line[headers["longitude"]]) if line[headers["longitude"]] != "" else 0
        }
        top_args = [] if line[headers["top_tags"]] == "" else line[headers["top_tags"]].replace(" ", "").split(",")
        popularity = {
            "popularity_detailed": line[headers["popularity_detailed"]],
            "popularity_generic": line[headers["popularity_generic"]],
            "top_tags": top_args,
        }
        pr = line[headers["price_range"]].replace(",", "")
        pr = pr.replace("CHF\u00A0", "$")
        min_price = None
        max_price = None
        if pr != "":
            min_price = int(pr.split("-")[0][1:])
            max_price = int(pr.split("-")[1][1:])

        priceInfo = {
            "price_level": line[headers["price_level"]],
            "min_price": min_price,
            "max_price": max_price
        }
        meals = [] if line[headers["meals"]] == "" else line[headers["meals"]].replace(" ", "").split(",")
        cuisine = [] if line[headers["cuisines"]] == "" else line[headers["cuisines"]].replace(" ", "").split(",")
        special_diets = [] if line[headers["special_diets"]] == "" else line[headers["special_diets"]].replace(" ",
                                                                                                               "").split(
            ",")
        foodInf = {
            "meals": meals,
            "cuisines": cuisine,
            "special_diets": special_diets,
            "vegetarian_friendly": line[headers["vegetarian_friendly"]],
            "vegan_options": line[headers["vegan_options"]],
            "gluten_free": line[headers["gluten_free"]],
        }
        schedule = {
            "original_open_hours": {} if line[headers["original_open_hours"]] == "" else json.loads(
                line[headers["original_open_hours"]]),
            "open_days_per_week": None if line[headers["open_days_per_week"]] == "" else float(
                line[headers["open_days_per_week"]]),
            "open_hours_per_week": None if line[headers["open_hours_per_week"]] == "" else float(
                line[headers["open_hours_per_week"]]),
            "working_shifts_per_week": None if line[headers["working_shifts_per_week"]] == "" else float(
                line[headers["working_shifts_per_week"]]),
        }
        reviews = {
            "total_reviews_count": float(line[headers["total_reviews_count"]]) if line[headers[
                "total_reviews_count"]] != "" else 0,
            'default_language': line[headers["default_language"]],
            'reviews_count_in_default_language': float(line[headers["reviews_count_in_default_language"]]) if line[
                                                                                                                  headers[
                                                                                                                      "reviews_count_in_default_language"]] != "" else 0,
        }
        ratings = {
            "avg_rating": float(line[headers["avg_rating"]]) if line[headers["avg_rating"]] != "" else 0,
            "excellent": float(line[headers["excellent"]]) if line[headers["excellent"]] != "" else 0,
            "very_good": float(line[headers["very_good"]]) if line[headers["very_good"]] != "" else 0,
            "average": float(line[headers["average"]]) if line[headers["average"]] != "" else 0,
            "poor": float(line[headers["poor"]]) if line[headers["poor"]] != "" else 0,
            "terrible": float(line[headers["terrible"]]) if line[headers["terrible"]] != "" else 0,
            "food": float(line[headers["food"]]) if line[headers["food"]] != "" else 0,
            "service": float(line[headers["service"]]) if line[headers["service"]] != "" else 0,
            "value": float(line[headers["value"]]) if line[headers["value"]] != "" else 0,
            "atmosphere": float(line[headers["atmosphere"]]) if line[headers["atmosphere"]] != "" else 0
        }
        res["Position"] = position
        res["Popularity"] = popularity
        res["Price"] = priceInfo
        res["FoodInfo"] = foodInf
        res["Schedule"] = schedule
        res["Review"] = reviews
        res["Rating"] = ratings
        restaurants.append(res)

    mh: MongoHelper = MongoHelper(host="localhost", port=27017, dbName="DDM")
    mh.add_many_to_collection(collection_name="Restaurants", documents=restaurants)


def get_restaurant_in_radius(mh: MongoHelper, lat: float, long: float, radius: float):
    pass


def get_best_restaurant_in_city(mh: MongoHelper, city: str):
    pass


def sort_with_weighted_average(mh: MongoHelper, ):
    pass


# without give the Json file result, with will give yuu a human read-able print of the solution

class Command(Enum):
    VEGAN_RESTAURANTS = 1
    WEIGHTED_RATING = 2
    ENGLISH_SPEAKING_RESTAURANTS = 3
    RESTAURANTS_IN_RADIUS = 4
    POPULAR_IN_CITY = 5
    RESTAURANT_WITH_FEATURE = 6
    MOST_EXPENSIVE_EACH_COUNTRY = 7
    TOP10_HIGHEST_RATING = 8
    TOP5_COUNTRIES_REVIEWS = 9
    CLOSEST_THREE_RANDOM_CITY = 10
    ADD_WEEKEND_AVAILABILITY = 11
    UPDATE_RATINGS = 12
    UPDATE_RESTAURANT_FEATURE = 13
    INCREASE_PRICE_SEATING = 14
    ASSIGN_SIMILAR_PRICED_OSNABRUCK = 15
    IMPORT_DB = 17
    ALL = 99


def execute_command(command, PRETTY, SEPARATOR):
    if command == Command.IMPORT_DB:
        initializeDB()
    mongoHelper = MongoHelper(host="localhost", port=27017, dbName="DDM")
    before = time.time()
    # --------------------------------------------------------------------- Queries
    if command == Command.VEGAN_RESTAURANTS or command == Command.ALL:
        print(mongoHelper.get_vegan_restaurants_in_cities(["Franconville"], pretty=PRETTY))
    if command == Command.WEIGHTED_RATING or command == Command.ALL:
        addSeparator(SEPARATOR)
        print(mongoHelper.sort_with_weighted_rating("France", pretty=PRETTY))
    if command == Command.ENGLISH_SPEAKING_RESTAURANTS or command == Command.ALL:
        addSeparator(SEPARATOR)
        print(mongoHelper.get_english_speaking_always_open_restaurants(6, 0, 10, 200, pretty=PRETTY))
    if command == Command.RESTAURANTS_IN_RADIUS or command == Command.ALL:
        addSeparator(SEPARATOR)
        print(mongoHelper.search_restaurants_in_radius(48.85341, 2.3488, 0.01, pretty=PRETTY))
    if command == Command.POPULAR_IN_CITY or command == Command.ALL:
        addSeparator(SEPARATOR)
        print(mongoHelper.search_popular_in_city("Paris", pretty=PRETTY))
    if command == Command.RESTAURANT_WITH_FEATURE or command == Command.ALL:
        addSeparator(SEPARATOR)
        print(mongoHelper.search_with_feature("WheelchairAccessible", "Paris", pretty=PRETTY))
    if command == Command.MOST_EXPENSIVE_EACH_COUNTRY or command == Command.ALL:
        addSeparator(SEPARATOR)
        print(mongoHelper.find_most_expensive_restaurant_in_each_country(pretty=PRETTY))
    if command == Command.TOP10_HIGHEST_RATING or command == Command.ALL:
        addSeparator(SEPARATOR)
        print(mongoHelper.find_top10_highest_rating_restaurant_in_the_5most_popular_cities(pretty=PRETTY))
    if command == Command.TOP5_COUNTRIES_REVIEWS or command == Command.ALL:
        addSeparator(SEPARATOR)
        print(mongoHelper.get_top5_countries_with_the_highest_average_excellent_reviews(pretty=PRETTY))
    if command == Command.CLOSEST_THREE_RANDOM_CITY or command == Command.ALL:
        addSeparator(SEPARATOR)
        print(mongoHelper.find_the_closest_three_restaurant_in_randon_city())
    # --------------------------------------------------------------------- Commands
    if command == Command.ADD_WEEKEND_AVAILABILITY or command == Command.ALL:
        mongoHelper.add_weekend_availability()
    if command == Command.UPDATE_RATINGS or command == Command.ALL:
        mongoHelper.update_ratings("g10001637-d10002227", Rating.average)
    if command == Command.UPDATE_RESTAURANT_FEATURE or command == Command.ALL:
        mongoHelper.update_restaurant_feature("g10001637-d10002227", "toilets")
    if command == Command.INCREASE_PRICE_SEATING or command == Command.ALL:
        mongoHelper.increase_price_for_restaurants_with_seating(10, 5)
    if command == Command.ASSIGN_SIMILAR_PRICED_OSNABRUCK or command == Command.ALL:
        mongoHelper.update_restaurant_by_assigning_a_similarly_priced_resturant_to_each_other_in_Osnabruck()
        addSeparator(SEPARATOR)
        print(mongoHelper.print_restaurants_connection_in_Osnabruck())
    after = time.time()
    print(f"Time: {(after - before) * 1000}")
    pass


def addSeparator(separator: bool):
    if separator:
        print("\n\n\n")


if __name__ == '__main__':
    # initializeDB()
    execute_command(Command.ALL, True, True)
    pass

# Example usage
