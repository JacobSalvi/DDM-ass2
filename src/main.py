import json
import time

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


if __name__ == '__main__':
    # initializeDB()
    mongoHelper = MongoHelper(host="localhost", port=27017, dbName="DDM")
    before = time.time()
    # mongoHelper.get_vegan_restaurants_in_cities(["Franconville"])
    # mongoHelper.sort_with_weighted_rating("France")  # 846.56
    # mongoHelper.get_english_speaking_always_open_restaurants(6, 0, 10, 200)  # 0.37
    # mongoHelper.add_weekend_availability()  # 5671.51
    # mongoHelper.search_restaurants_in_radius(my_latitude=48.85341,my_longitude=2.3488,max_distance=0.01)  # 62.22
    # mongoHelper.update_ratings(restaurant_link="g10001637-d10002227", rating=Rating.average)  # 4.01
    # mongoHelper.update_restaurant_feature(restaurant_link="g10001637-d10002227", new_feature="toilets")  # 3.44
    # mongoHelper.search_popular_in_city(city_name="Paris")  # 574.81
    # mongoHelper.search_with_feature(feature="WheelchairAccessible", city="Paris")  # 751.57
    # print(mongoHelper.find_most_expensive_restaurant_in_each_country())  # 11815.973
    # print((mongoHelper.find_top10_highest_rating_restaurant_in_the_5most_popular_cities())) #3050.03
    # print((mongoHelper.get_top5_countries_with_the_highest_average_excellent_reviews())) # 1874.02 +
    # print(mongoHelper.find_the_closest_three_restaurant_in_randon_city()) # 2572.020,2497.96,2444,2947, 2469.03,
    # 2415.98,2383.03,2481, 3912.00, 2492.03

    # --------------------------------------------------------------------- Commands
    # mongoHelper.increase_price_for_restaurants_with_seating(10, 5))  # 576.23

    mongoHelper.update_restaurant_by_assigning_a_similarly_priced_resturant_to_each_other_in_Rivarennes() #  3003
    after = time.time()
    print(f"Time: {(after - before) * 1000}")
    pass
