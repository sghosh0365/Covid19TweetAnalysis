from pymongo import MongoClient
import json
import datetime
import bson.json_util

def pushDatatoMongo(filename, collection_nm, del_prev_day_data = 0, date_since = None):
    try:
        # connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
        client = MongoClient("mongodb://localhost:27017/")
        db = client.TwitterCovid19
        # Issue the serverStatus command and print the results
        serverStatusResult = db.command("serverStatus")
        with open(filename) as template:
            template_dct = json.load(template)
            if del_prev_day_data in [1, 2]:
                if del_prev_day_data == 1:
                    delete_query = {"created_at": {"$regex": f"^{date_since}"}}
                    print(delete_query)
                else:
                    delete_query = {"Date": {"$regex": f"^{date_since}"}}
                d = db[collection_nm].delete_many(delete_query)
                print(d.deleted_count, " documents deleted !!")
            if del_prev_day_data == 2:
                result = db[collection_nm].insert_one(template_dct)
            else:
                result = db[collection_nm].insert_many(template_dct, ordered=False)
            client.close()
    except Exception as e:
        print(f'Error occurred in inserting data into MongoDB collection {collection_nm}: {e}')
