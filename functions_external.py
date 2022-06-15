from pymongo import MongoClient
from bson import Code

# function to get the keys from a mongodb collection
def get_keys(db, collection):
    client = MongoClient()
    db = client[db]
    map = Code("function() { for (var key in this) { emit(key, null); } }")
    reduce = Code("function(key, stuff) { return null; }")
    result = db[collection].map_reduce(map, reduce, "myresults")
    return result.distinct('_id')
