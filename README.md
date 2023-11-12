
# Requirements:
- Python 3
- MongoDB
- csv file of TripAdvisor db

# Installation:
1. clone repo
2. (optional) make virtual environment 
```shell
python3 -m venv ./.venv
```
3. install requirement with pip
```shell
python3 -m pip install -r requirements.txt
```
4. Keep open MongoDB on localhost on the default port 27017.
5. In the src/main.py file pass the path to the csv db to CsvHandler at line 18
6. Also in src/main.py uncomment function initializeDB() at line 134 in the main function
7. After the DB is imported in mongo you can call the queries in file src/MongoHelper.py
   