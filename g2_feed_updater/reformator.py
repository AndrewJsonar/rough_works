#!/usr/bin/python
# Andrew Ebl Mar 18 2020
import sys

import pymongo


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
    result = collection.find()
    return result


def data_updater(data):



def main():
    connection_string = sys.argv[1]
    database = sys.argv[2]
    collection = sys.argv[3]

    connection = make_client_connection(connection_string)
    db = use_database(connection, database)
    col = use_collection(db, collection)
    result = find_relevant_data(col)

    print(result)


if __name__ == '__main__':
    main()
