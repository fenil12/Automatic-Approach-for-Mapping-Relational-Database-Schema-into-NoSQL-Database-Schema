# Automatic-Approach-for-Mapping-Relational-Database-Schema-into-NoSQL-Database-Schema
This project aims to automate the transformation of relational database schemas into NoSQL formats for enhanced scalability and flexibility. By leveraging innovative algorithms and Snowflake's tools, it streamlines schema mapping, reduces mapping time, and ensures accurate data migration to MongoDB. The project offers a systematic solution to a common data management challenge, catering to industries seeking efficient schema migration strategies.

# Snowflake SQL Execution and Data Migration Guide

## Introduction

This guide provides step-by-step instructions for executing Snowflake SQL files, retrieving collection data using SnowSQL, and migrating the data into a MongoDB database using the `import_mongo` Python script.

## Execution Steps

### Execute Snowflake SQL Files

1. Open your Snowflake environment.
2. In your Snowflake worksheet, run the SQL files in the following sequence:
   - `creation_and_loading.sql`: Creates the necessary database, schema, tables, and loads TPCH table data.
   - `final_procedure_norelation.sql`: Creates the SCHEMA_AUTOMATE_INDEPENDENT procedure.
   - `final_procedure_reference.sql`: Creates the SCHEMA_AUTOMATE_REFERENCE procedure.
   - `final_modified_procedure_embedded.sql`: Creates the SCHEMA_AUTOMATE_EMBEDDED procedure.
   - `automate_master_procedure.sql`: Executes the mapping procedures in the specified sequence.
   - `migration_procedure.sql`: Creates stages to store collection data.

### Retrieve Collection Data using SnowSQL

1. After executing the SQL files, run the SnowSQL command to retrieve collection data.
2. Note down the directory where the collection data files are saved.

### Configure MongoDB

1. Set up a MongoDB instance on your local system or server.
2. Create a MongoDB database where you want to load the data.
3. Note down the MongoDB connection URI and the name of the database.

### Configure and Run `import_mongo` Script

1. Open the `import_mongo.py` script in a text editor.
2. Set the following parameters in the script:
   - `SNOWFLAKE_DATA_PATH`: Set the path to the directory containing the collection data files.
   - `MONGO_URI`: Set the MongoDB connection URI.
   - `MONGO_DB_NAME`: Set the name of the MongoDB database.
   - `MONGO_COLLECTION_NAME`: Set the name of the MongoDB collection.

3. Open a terminal or command prompt.
4. Navigate to the directory containing the `import_mongo.py` script.
5. Run the script using the command: `python import_mongo.py`
6. Monitor the terminal for progress updates and potential errors during data migration.
