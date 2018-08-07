import pymongo
import csv
import json
import sys
import pandas
from bson import json_util
from bson.code import Code

def CustomParser(data):
    import json
    j1 = json.loads(data)
    return j1

df = pandas.read_csv('tmdb_5000_credits.csv', converters={'cast':CustomParser, 'crew':CustomParser},header=0)
mongocli = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongocli["bigdata"]
movies = db["movies"]

data = json_util.loads(df.to_json(orient='records'))
movies.drop()
movies.insert(data)

map = Code("function () {"
           "  for(var i in this.cast) {"
           "     emit({name:this.cast[i].name}, 1);"
           "  }" 
           "}")
reduce = Code("function (key, value) {"
              "    return Array.sum(value);"
              "}")

result = movies.map_reduce(map, reduce, "myresults")
for doc in result.find().sort("value", -1).limit(20):
    print(doc)
