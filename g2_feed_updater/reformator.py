#!/usr/bin/python
# Andrew Ebl Mar 18 2020
import sys

import pymongo


def make_client_connection(mongo_string):
    try:
        client = pymongo.MongoClient(mongo_string)
    except pymongo.errors.ConnectionFailure as message:
        print("Could not connect to server: %s" % message)
    return client


def use_database(client_connection, database_to_use):
    try:
        my_db = client_connection[database_to_use]
    except pymongo.errors.CollectionInvalid as message:
        print("No such database: %s" % message)
    return my_db


def main():
    connection_string = sys.argv[1]
    database = sys.argv[2]
    connection = make_client_connection(connection_string)
    db = use_database(connection, database)


if __name__ == '__main__':
    main()
