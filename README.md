
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
6. Also in src/main.py call modify the paraeter of the function wirth execute_command(Command.IMPORT_DB, True, True)
7. After the DB is imported in mongo you can call the queries in file src/MongoHelper.py
8. Simply call the function with the execute_command passing the query/command that you want to execure : 
   9. execute_command(Command.ALL, True, True) : will execute all query and commands
   10. execute_command(Command.TOP5_COUNTRIES_REVIEWS, True, True) : will execute only the query specified
   11. the second parameter of execute_command:
       12. true : if you want the output pretify so human readble
       13. false : the JSON file unstructured 
   14. the third parameter of execute_command will simply split the outputs by adding some new line, usefull when you are running all of them at once.

#Important 
if the DB is not on the queries will break