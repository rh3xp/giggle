import os
import sys
from pprint import pprint

ROOT_PATH = os.path.abspath(__file__).rsplit("/", 2)[0]
sys.path.append(ROOT_PATH)

from config.constants import *
from db.models import MongoDBConfig, MongoConnection


if __name__ == "__main__":
    mongo_config = MongoDBConfig(host=HOSTNAME, port=PORT, database_name=DB_NAME)
    mongo_conn = MongoConnection(mongo_config)
    mongo_conn.create_connection(mongo_config)
    mongo_conn.check_conn_valid()
    mongo_conn.connect_collection(db_conn=mongo_conn.conn,
                                    collection_name="Test")
    mongo_conn.drop_collection(db_conn=mongo_conn.conn,
                                    collection_name="Test")
    mongo_conn.connect_collection(db_conn=mongo_conn.conn,
                                    collection_name=COLLECTION_NAME)


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
