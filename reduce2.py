import collections
import itertools
import multiprocessing

class SimpleMapReduce(object):
    
    def __init__(self, map_func, reduce_func, num_workers=None):
        self.map_func = map_func
        self.reduce_func = reduce_func
        self.pool = multiprocessing.Pool(num_workers)
    
    def partition(self, mapped_values):
        partitioned_data = collections.defaultdict(list)
        for key, value in mapped_values:
            partitioned_data[key].append(value)
        return partitioned_data.items()
    
    def __call__(self, inputs, chunksize=1):
        map_responses = self.pool.map(self.map_func, inputs, chunksize=chunksize)
        partitioned_data = self.partition(itertools.chain(*map_responses))
        reduced_values = self.pool.map(self.reduce_func, partitioned_data)
        return reduced_values



import pymongo
from bson.code import Code
import datetime
import operator

print(datetime.datetime.now())

mongocli = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongocli["bigdata"]
movies = db["movies"]
result = movies.find()


def cast_to_person(movie):
    output = []
    for person in movie['cast']:
        output.append( (person['name'], 1) )

    return output

def person_count(item):
    name, occurances = item
    return (name, sum(occurances))

mapper = SimpleMapReduce(cast_to_person, person_count, 4)
person_counts = mapper(result)
person_counts.sort(key=operator.itemgetter(1))
person_counts.reverse()

print("\nTOP 20 Artistas\n")
top20 = person_counts[:20]
longest = max(len(person) for person, count in top20)
for person, count in top20:
    print("%s - %i" % (person, count))

print(datetime.datetime.now())