import pymongo
from bson.code import Code
import datetime

print(datetime.datetime.now())

mongocli = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongocli["bigdata"]
movies = db["movies"]

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

print(datetime.datetime.now())