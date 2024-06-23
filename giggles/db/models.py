#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
from pprint import pprint
from pydantic import BaseModel
from typing import Optional, Any, Dict
import pymongo
from pymongo.database import Database
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.results import InsertOneResult, InsertManyResult, DeleteResult


ROOT_PATH = os.path.abspath(__file__).rsplit("/", 2)[0]
sys.path.append(ROOT_PATH)

from config.constants import *


class MongoDBConfig(BaseModel):
    connection_instance: str = "localhost"
    host: str
    port: int
    database_name: str
    collection_name : Optional[str] = ""
    username: Optional[str] = ""
    password: Optional[str] = ""
    cluster_name : Optional[str] = ""
    conn : Optional[Any] = None


class MongoConnection:

    def __init__(self, db_config: MongoDBConfig):
        self.connection_instance = db_config.connection_instance
        self.host = db_config.host
        self.port = db_config.port
        self.username = db_config.username
        self.password = db_config.password
        self.database_name = db_config.database_name
        self.collection_name = db_config.collection_name
        self.conn = db_config.conn

    def create_connection(self, db_config: MongoDBConfig):
        if self.connection_instance == "localhost":
            MONGO_URI = f"mongodb://{db_config.host}:{db_config.port}/"
        else:
            MONGO_URI = f"mongodb+srv://{db_config.username}:{db_config.password}\
                            @{db_config.cluster_name}.mongodb.net/"
        try:
            client = MongoClient(MONGO_URI)
            db_conn = client[db_config.database_name]

            # Run the ping command to check the health of the instance
            ping_result = db_conn.command('ismaster')
            # Check if the database is healthy
            if ping_result["ok"] == 1:
                print("MongoDB instance is healthy!")
            else:
                print("MongoDB instance is not healthy!")

            print(f"Established connection to database '{db_config.database_name}'.")
            self.conn = db_conn

        except:
            print("Connection failed. Please check connection URI.")

    def check_conn_valid(self):
        if isinstance(self.conn, Database):
            print("Connection string is an instance of Mongo.")
            return True
        else:
            print("Connection string is not an instance of Mongo.")
            return False

    def connect_collection(self, db_conn: Database, collection_name: str):
        collection_list = db_conn.list_collection_names()
        if collection_name in collection_list:
            print(f"Established connection to collection '{collection_name}'.")

        else:
            print("Collection does not exist.")

    def insert_one_into_collection(self, db_conn:Database, collection_name:str, data:Dict):
        record = db_conn[collection_name].insert_one(data)
        if type(record) == InsertOneResult:
            print(f"Successfully inserted data into collection {collection_name} with ID {record.inserted_id}.")
        else:
            print(f"Unable to insert data into collection {collection_name}. Please check your data structure.")

    def insert_many_into_collection(self, db_conn:Database, collection_name:str, data_list:list):
        records = db_conn[collection_name].insert_many(data_list)
        if type(records) == InsertManyResult:
            print(f"Successfully inserted data into collection {collection_name} with ID {records.inserted_ids}.")
        else:
            print(f"Unable to insert data into collection {collection_name}. Please check your data structure.")

    def update_record_in_collection(self, db_conn:Database, collection_name:str, query:Dict = {}):
        pass

    def replace_record_in_collection(self, db_conn:Database, collection_name:str, query:Dict = {}):
        pass

    def delete_record_in_collection(self, db_conn:Database, collection_name:str, query:Dict = {}, delete_type:str = "one"):
        result = DeleteResult
        try:
            if delete_type == "one":
                result = db_conn[collection_name].delete_one(query)
            elif delete_type == "many":
                result = db_conn[collection_name].delete_many(query)
        except Exception as e:
                print(e)
        if result.acknowledged:
            print("Deleting 1 record successful.")
        else:
            print("Unable to delete record.")
        # if delete_type == "one":
        #     result = db_conn[collection_name].delete_one(query)
        #     print(result)
        #     if result.acknowledged:
        #         print(result.acknowledged)
        #         print("Deleting 1 record successful.")
        #     else:
        #         print("Unable to delete record.")
        # elif delete_type == "many":
        #     result = db_conn[collection_name].delete_many(query)
        #     if result.acknowledged:
        #         print("Deleting 1 record successful.")
        #     else:
        #         print("Unable to delete record.")

    def fetch_first_from_collection(self, db_conn:Database, collection_name:str):
        collection_conn = db_conn[collection_name]
        result = collection_conn.find_one()
        return result

    # def fetch_all_from_collection(self, db_conn:Database, collection_name:str, query:Dict={},
    #                                 return_column:Dict={}, limit=FETCH_LIMIT, sort_column:str = "_id", sort_order = SORT_ORDER):
    def fetch_all_from_collection(self, db_conn:Database, collection_name:str, query:Dict={},
                                    return_column:Dict={}, limit=FETCH_LIMIT):
        collection_conn = db_conn[collection_name]
        try:
            # result = [record for record in collection_conn.find(query, return_column, limit=limit, sort={sort_column, sort_order})]
            result = [record for record in collection_conn.find(query, return_column, limit=limit)]
            print(f"Fetched data from collection {collection_name}.")
            return result
        except:
            print(f"Unable to fetch data from collection {collection_name}. Please check your search query.")

    def get_document_count(self, db_conn:Database, collection_name:str, query:Dict={}):
        return db_conn[collection_name].count_documents(query)

if __name__ == "__main__":
    mongo_config = MongoDBConfig(host=HOSTNAME, port=PORT, database_name=DB_NAME)
    mongo_conn = MongoConnection(mongo_config)
    mongo_conn.create_connection(mongo_config)
    mongo_conn.check_conn_valid()
    mongo_conn.connect_collection(db_conn=mongo_conn.conn,
                                    collection_name=COLLECTION_NAME)

    """
    # Insert multiple values in collection

    data_list = [
        {
            "name": "A",
            "url": "https://www.a.com/",
            "type": "Test"
        },
        {
            "name": "B",
            "url": "https://www.b.com/",
            "type": "Test"
        },
        {
            "name": "C",
            "url": "https://www.c.com/",
            "type": "Test"
        }
    ]
    mongo_conn.insert_many_into_collection(db_conn=mongo_conn.conn,
                                            collection_name=COLLECTION_NAME,
                                            data_list=data_list)

    # Fetch first document from collection

    first_record = mongo_conn.fetch_first_from_collection(db_conn=mongo_conn.conn,
                                        collection_name=COLLECTION_NAME)
    pprint(first_record)

    # Conditional Query Filtering

    conditional_query_1 = {
            "$or": [
                    {"type" : { "$eq" : "Test"}},
                    {"type" : { "$eq" : "DB"}},
            ]
        }

    conditional_query = { "type" : { "$eq" : "Test"} }
    return_cols = {"_id": 0, "name": 1, "url": 1}

    all_records = mongo_conn.fetch_all_from_collection(db_conn=mongo_conn.conn,
                                            collection_name=COLLECTION_NAME,
                                            query=conditional_query,
                                            return_column=return_cols)
    pprint(all_records)

    # Get document count

    conditional_query = { "type" : { "$eq" : "Test"} }
    count = mongo_conn.get_document_count(db_conn=mongo_conn.conn,
        collection_name=COLLECTION_NAME,query=conditional_query)
    print(count)

    # Insert multiple data, fetch & delete them using filter
    data_list = [
            {
                "name": "A",
                "url": "https://www.a.com/",
                "type": "Test1"
            },
            {
                "name": "B",
                "url": "https://www.b.com/",
                "type": "Test1"
            },
            {
                "name": "C",
                "url": "https://www.c.com/",
                "type": "Test1"
            }
        ]
    mongo_conn.insert_many_into_collection(db_conn=mongo_conn.conn,
                                            collection_name=COLLECTION_NAME,
                                            data_list=data_list)

    conditional_query = { "type" : { "$eq" : "Test1"} }
    mongo_conn.fetch_all_from_collection(db_conn=mongo_conn.conn,
                                            collection_name=COLLECTION_NAME,
                                            query=conditional_query)

    mongo_conn.delete_record_in_collection(db_conn=mongo_conn.conn,
                                            collection_name=COLLECTION_NAME,
                                            query=conditional_query)

    """
