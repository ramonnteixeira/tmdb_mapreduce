import pymongo
import pandas
from bson import json_util
import ast
import datetime

print(datetime.datetime.now())

def CustomParser(data):
    j1 = ast.literal_eval(data)
    return j1

df = pandas.read_csv('credits.csv', converters={'cast':CustomParser, 'crew':CustomParser},header=0)
mongocli = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongocli["bigdata"]
movies = db["movies"]

data = json_util.loads(df.to_json(orient='records'))
movies.drop()
movies.insert(data)

print(datetime.datetime.now())