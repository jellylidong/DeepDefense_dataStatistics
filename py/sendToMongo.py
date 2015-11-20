def sendToMongo(data):
    from pymongo import MongoClient
    client = MongoClient('mongodb://10.227.119.213:27017/')
    db = client['deepdefense']
    collection = db['StatisticsSchema']
    collection.insert_one(data)

test = {'a': 1}
sendToMongo(test)
