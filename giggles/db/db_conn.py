#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
from pymongo import MongoClient


ROOT_PATH = os.path.abspath(__file__).rsplit("/", 2)[0]
sys.path.append(ROOT_PATH)

from config.constants import *



def create_connection_string(CONNECTION_INSTANCE, HOSTNAME, PORT, USERNAME, PASSWORD, DB_NAME):
    if CONNECTION_INSTANCE == "local":
        MONGO_URI = f"mongodb://{HOSTNAME}:{PORT}/"
    else:
        MONGO_URI = f"mongodb+srv://{USERNAME}:{PASSWORD}@{CLUSTER_NAME}.mongodb.net/"

    try:
        client = MongoClient(MONGO_URI)
        db_conn = client[DB_NAME]

        # Run the ping command to check the health of the instance
        ping_result = db_conn.command('ismaster')
        # Check if the database is healthy
        if ping_result["ok"] == 1:
            print("MongoDB instance is healthy!")
        else:
            print("MongoDB instance is not healthy!")

        print(f"Established connection to database '{DB_NAME}'.")
        return client, db_conn

    except:
        print("Connection failed. Please check connection URI.")



def connect_collection(db_conn, collection_name):
    collection_list = db_conn.list_collection_names()
    if collection_name in collection_list:
        print(f"Established connection to collection '{collection_name}'.")

    else:
        print("Collection does not exist.")

if __name__ == "__main__":
    client, db_conn = create_connection_string(CONNECTION_INSTANCE, HOSTNAME, PORT, USERNAME, PASSWORD, DB_NAME)
    connect_collection(db_conn, COLLECTION_NAME)
