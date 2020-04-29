#!/usr/bin/python
# Andrew Ebl Mar 18 2020

import sys
import pymongo

VALUE_TO_FIND={ "age" : 91 }
UPDATE_VALUE_TO={ "age" : 95 }
NEW_DATABASE="andrew"
NEW_COLLECTION="g2_updated"

def make_client_connection(mongo_string):
    try:
        client = pymongo.MongoClient(mongo_string)
    except pymongo.errors.PyMongoError as message:
        print("Could not connect to server: %s" % message)
    return client


def use_database(client_connection, database_to_use):
    try:
        db = client_connection[database_to_use]
    except pymongo.errors.PyMongoError as message:
        print("No such database: %s" % message)
    return db


def use_collection(database, collection):
    try:
        col = database[collection]
    except pymongo.errors.PyMongoError as message:
        print("No such collection: %s" % message)
    return col


def find_relevant_data(collection):
    result = collection.find(VALUE_TO_FIND)
    return result


def copy_data_to_new_collection(data, target):
    for document in data:
        target.insert_one(document)


#def data_updater(collection):



def print_results(data):
    for document in data:
        print(document)


def main():
    connection_string = sys.argv[1]
    database = sys.argv[2]
    collection = sys.argv[3]

    connection = make_client_connection(connection_string)
    db_original = use_database(connection, database)
    col_original = use_collection(db_original, collection)
    db_updated = use_database(connection, NEW_DATABASE)
    col_updated = use_collection(db_updated, NEW_COLLECTION)

    result = find_relevant_data(col_original)
    print_results(result)
    col_updated.insert_many(result)

    print("\nUpdated Data")
    updated_info = col_updated.find(VALUE_TO_FIND)
    print_results(updated_info)


if __name__ == '__main__':
    main()
