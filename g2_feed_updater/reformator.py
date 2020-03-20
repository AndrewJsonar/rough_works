#!/usr/bin/python
# Andrew Ebl Mar 18 2020

import pymongo


def make_client_connection(connection_string, database_to_use):
    my_client = pymongo.MongoClient(connection_string)
    my_db = my_client[database_to_use]
    return my_db


def main():
    make_client_connection()


if __name__ == '__main__':
    main()
