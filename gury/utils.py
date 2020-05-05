import subprocess
import os
import pickle
from operator import itemgetter

from pymongo import errors, MongoClient

def get_sonar_home():
    try:
        return os.environ["SONAR_HOME"]
    except:
        print "The SONAR_HOME environment variable must be set."
        raise


# Example path returned by this function: /local/raid0/sonarw/data/test
def get_db_path(db):
    return get_sonar_home() + "/data/" + db.name


# Returns list of documents with 2 fields each.
# First field is "header" (see blockinfo documentation for its structure).
# Second field is "disk_blocks", its value is a list of documents (see blockinfo documentation -
# the part after "verbose: true" for the structure of each document).
# The list's length is the number of blocks for this column.
# FIXME: these comments should be docstrings.
def get_blockinfo_data(column_name, db, coll):
    data = db.command("blockinfo", {"cll": coll.name, "column": column_name,
                                    "verbose": 1})["columns"]
    return data


# Returns list of indexes of the last document in each block in column column_name
def get_last_documents(column_name, db, coll):
    index_list = []
    raw_data = get_blockinfo_data(column_name, db, coll)
    for doc in raw_data:
        for block_doc in doc["disk_blocks"]:
            index_list.append(block_doc["last_document"])
    return index_list


# Returns data in header file split into lines
def get_header_data(header_file_path):
    hdrdump_path = "/usr/lib/sonarw/hdrdump"  # Assumes this is the path of hdrdump
    args = [hdrdump_path, header_file_path]
    data = subprocess.check_output(args).splitlines()
    return data


# Uses pickle to save obj to path
def write_pickle_object(path, obj):
    with open(path, "wb") as f:
        pickle.dump(obj, f)


def append_pickle_object(path, obj):
    with open(path, "a") as f:
        pickle.dump(obj, f)


# Returns pickled object from path
def get_pickled_object(path):
    with open(path, "rb") as f:
        pickle.load(f)


# Returns list of all pickled objects from path
def get_all_pickled_objects(path):
    pickle_list = list()
    with open(path, "rb") as f:
        while True:
            try:
                pickle_list.append(pickle.load(f))
            except EOFError:
                return pickle_list


# Returns list of all documents in the cursor
def get_list_from_cursor(cursor):
    counter = 0
    return_list = []
    try:
        for doc in cursor:
            return_list.append(doc)
            counter += 1
        return return_list
    except errors.CursorNotFound as e:
        append_to_file("/tmp/cursor_counter.txt", str(counter) + "\n")
        raise




def write_to_file(path, data):
    """Writes data to file in path. If data is a list, every element is
    written in a new line."""
    with open(path, "wb") as f:
        if type(data) == list:
            for element in data:
                f.write(element)
        else:
            f.write(data)


def append_to_file(path, data):
    """Appends data to file in path. If data is a list, every element is
    written in a new line."""
    with open(path, "a") as f:
        if type(data) == list:
            for element in data:
                f.write(element)
        else:
            f.write(data)


def mongo_connector_dictionary(db_name, collection_name, port=None,
                               user_name=None, password=None):
    client = MongoClient(port=port)
    if user_name is not None and password is not None:
        client.admin.authenticate(user_name, password)
    db = client[db_name]
    coll = db[collection_name]
    connector_dictionary = {"db": db, "collection": coll}
    return connector_dictionary


def sonar_connector_dictionary(db_name, collection_name, port=27117,
                               user_name="cale", password="jj"):
    return mongo_connector_dictionary(db_name, collection_name, port=port,
                                      user_name=user_name,
                                      password=password)


def compare_lists_as_sets(list1, list2, sort_key):
    """Sorts list1 and list2 by sort_key and checks if they are equal using
    compare_sorted_lists."""
    item_getter = itemgetter(sort_key)
    return compare_sorted_lists(sorted(list1, key=item_getter),
                                sorted(list2, key=item_getter))


def compare_sorted_lists(list1, list2):
    """Checks if lists are equal. Returns dictionary with 2 pairs. Value at
    'comparison' key is True if they are equal and False if not. Value at
    'item' key is a string explaining what went wrong if anything. Assumes
    lists are sorted by sort_key."""
    return_dict = dict()
    if list1 == list2:
        return_dict["comparison"] = True
        return_dict["item"] = "Lists are equal! :)"
        return return_dict
    if len(list1) < len(list2):
        return_dict["comparison"] = False
        return_dict["item"] = "Lists are not the same length. first list's " \
                              "length is " + str(len(list1)) + ", second " \
                              "list's length is " + str(len(list2)) + ". " \
                              "Extra item in list 2 is " +\
                              str(list2[len(list1)])
        return return_dict
    if len(list2) < len(list1):
        return_dict["comparison"] = False
        return_dict["item"] = "Lists are not the same length. first list's " \
                              "length is " + str(len(list1)) + ", second " \
                              "list's length is " + str(len(list2)) + ". " \
                              "Extra item in list 1 is " +\
                              str(list1[len(list2)])
        return return_dict
    for i in xrange(len(list1)):
        if list1[i] != list2[i]:
            return_dict["comparison"] = False
            return_dict["item"] = "Lists are not the same." \
                                  " First difference is at index " + str(i)\
                                  + "." + " First list has element " + \
                                  str(list1[i]) + " and second list has" \
                                  " element " + str(list2[i])
            return return_dict