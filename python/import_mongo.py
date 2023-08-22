from pymongo import MongoClient
import json
import os

# MongoDB connection parameters
mongo_host = 'localhost'
mongo_port = 27017
mongo_db = 'DATABASE_TPCH'
mongo_collection_region = 'Region'
mongo_collection_customer = 'Customer'
mongo_collection_orders = 'Orders'
mongo_collection_supplier = 'Supplier'
mongo_collection_part = 'Part'
mongo_collection_partsupp = 'Partsupp'
mongo_collection_lineitem = 'Lineitem'


# Path to the directory containing the JSON files
json_directory_region = 'C:/fenil/snowflake_project/region'
json_directory_customer = 'C:/fenil/snowflake_project/customer'
json_directory_orders = 'C:/fenil/snowflake_project/orders'
json_directory_supplier = 'C:/fenil/snowflake_project/supplier'
json_directory_part = 'C:/fenil/snowflake_project/part'
json_directory_partsupp = 'C:/fenil/snowflake_project/partsupp'
json_directory_lineitem = 'C:/fenil/snowflake_project/lineitem'

# Connect to MongoDB
client = MongoClient(mongo_host, mongo_port)
db = client[mongo_db]
collection_region = db[mongo_collection_region]
collection_customer = db[mongo_collection_customer]
collection_orders = db[mongo_collection_orders]
collection_supplier = db[mongo_collection_supplier]
collection_part = db[mongo_collection_part]
collection_partsupp = db[mongo_collection_partsupp]
collection_lineitem = db[mongo_collection_lineitem]

# Function to import JSON data into MongoDB
def import_json_data(json_directory, collection):
    # Iterate over each JSON file in the directory
    for filename in os.listdir(json_directory):
        if filename.endswith('.json'):
            filepath = os.path.join(json_directory, filename)
            
            # Open and read the JSON file
            with open(filepath) as file:
                json_data = file.read()
                
                # Split the JSON data into individual objects
                json_objects = json_data.strip().split('\n')
                
                # Create an empty list to store the parsed objects
                data_list = []
                
                # Parse each JSON object and append it to the list
                for json_str in json_objects:
                    data = json.loads(json_str)
                    data_list.append(data)
                    
                # Insert the list of objects into MongoDB
                collection.insert_many(data_list)

# Import region JSON data into the region collection
import_json_data(json_directory_region, collection_region)

# Import customer JSON data into the customer collection
import_json_data(json_directory_customer, collection_customer)

# Import orders JSON data into the orders collection
import_json_data(json_directory_orders, collection_orders)

# Import supplier JSON data into the supplier collection
import_json_data(json_directory_supplier, collection_supplier)

# Import part JSON data into the part collection
import_json_data(json_directory_part, collection_part)

# Import partsupp JSON data into the partsupp collection
import_json_data(json_directory_partsupp, collection_partsupp)

# Import lineitem JSON data into the lineitem collection
import_json_data(json_directory_lineitem, collection_lineitem)


# Close the MongoDB connection
client.close()
