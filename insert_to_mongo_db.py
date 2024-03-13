from pymongo import MongoClient

# Establish a connection to the MongoDB server
client = MongoClient('localhost', 27017)

# Select the database and collection
db = client['band_db']  
collection = db['bands']  

# Document to be inserted
band_document = {
    "name": "Green Day",
    "year_created": 1987,
    "albums": [
        "Kerplunk",
        "Insomniac",
        "Dookie"
    ]
}

# Insert the document into the collection
insert_result = collection.insert_one(band_document)

# Output the ID of the inserted document
print("Inserted document ID:", insert_result.inserted_id)
