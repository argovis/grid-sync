from pymongo import MongoClient, UpdateOne
import sys

collection = sys.argv[1]

# Connect to the MongoDB instance
client = MongoClient('mongodb://database/argo')
db = client.argo

# Find the earliest and latest timestamps in the 'rg09' collection
earliest_doc = db[collection].find_one(sort=[('timestamp', 1)])
latest_doc = db[collection].find_one(sort=[('timestamp', -1)])

# Extract the timestamps
start_date = earliest_doc['timestamp']
end_date = latest_doc['timestamp']

# Upcert the summary document
summary_doc = db['summaries'].find_one({'_id': 'ratelimiter'})
entry = {"metagroups": ["id"], "startDate": start_date, "endDate": end_date}

if summary_doc:
    summary_doc['metadata'][collection] = entry
else:
    summary_doc = {
        '_id': 'ratelimiter',
        'metadata': {
            collection: entry
        }
    }

# Write the summary document back to the database using upsert
result = db['summaries'].update_one(
    {'_id': 'ratelimiter'},  # Match by _id
    {'$set': summary_doc},  # Update document
    upsert=True             # Insert if it doesn't exist
)

