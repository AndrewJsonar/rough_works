from argparse import ArgumentParser
from pymongo import MongoClient

from generate_pipelines import PipelineCreator
from constants import FIELD_LIST_DICTIONARY

GROUP_COLLECTION_NAME = "group_pipelines"
GROUP_RESULTS_COLLECTION_NAME = "group_pipeline_results"
GROUP_ITERATIONS = 500
FIND_COLLECTION_NAME = "queries"
FIND_RESULTS_COLLECTION_NAME = "query_results"
FIND_ITERATIONS = 5000
REGULAR_MODE_ITERATIONS = 1500
REGULAR_MODE_COLLECTION_NAME = "pipelines"
REGULAR_MODE_RESULTS_COLLECTION_NAME = "pipeline_results"
MODE_CHOICES = ["group", "find", "regular"]


class PipelineInserter(object):
    """Generate pipelines using 'generate_pipelines.py and insert them to
    the 'INSERT_COLLECTION' collection. Has two modes: 'default' and 'group'.
    In group mode the method 'create_group_pipelines' is used and in default
    mode the method 'create_pipelines' is used. The target collection also
    depends on the mode.
    """

    def __init__(self, uri, mode, include_this_month, agg_full_sql,
                 db_name="pipes", port=27117):
        """Initialize connection to SonarW and check for mode. Clears old queries,
        pipelines and results
        """
        self.uri = uri
        self.port = port
        self.connection = MongoClient(uri)
        self.db = self.connection[db_name]
        if mode == "regular":
            self.collection = self.db[REGULAR_MODE_COLLECTION_NAME]
            print(
                "Dropping old " + REGULAR_MODE_COLLECTION_NAME + " and " + REGULAR_MODE_RESULTS_COLLECTION_NAME + " collections")
            self.db.drop_collection(REGULAR_MODE_COLLECTION_NAME)
            self.db.drop_collection(REGULAR_MODE_RESULTS_COLLECTION_NAME)
            self.iterations = REGULAR_MODE_ITERATIONS
        elif mode == "group":
            self.collection = self.db[GROUP_COLLECTION_NAME]
            print("Dropping old " + GROUP_COLLECTION_NAME + " and " + GROUP_RESULTS_COLLECTION_NAME + " collections")
            self.db.drop_collection(GROUP_COLLECTION_NAME)
            self.db.drop_collection(GROUP_RESULTS_COLLECTION_NAME)
            self.iterations = GROUP_ITERATIONS
        elif mode == "find":
            self.collection = self.db[FIND_COLLECTION_NAME]
            print("Dropping old " + FIND_COLLECTION_NAME + " and " + FIND_RESULTS_COLLECTION_NAME + " collections")
            self.db.drop_collection(FIND_COLLECTION_NAME)
            self.db.drop_collection(FIND_RESULTS_COLLECTION_NAME)
            self.iterations = FIND_ITERATIONS
        self.mode = mode
        self.include_this_month = True if include_this_month == 'y' else False
        self.agg_full_sql = True if agg_full_sql == 'y' else False

    def insert_pipelines(self):
        """Insert pipelines to collection."""
        creators = {}
        first_pass = {"exception": True,
                      "instance": True,
                      "session": True,
                      "full_sql": True}
        doc_id = 0
        insert = []
        for i in xrange(self.iterations):
            for field in FIELD_LIST_DICTIONARY:
                if not self.agg_full_sql and field == "full_sql":
                    continue
                if first_pass[field]:
                    first_pass[field] = False
                    creators[field] = PipelineCreator(
                        field, self.include_this_month,
                        uri=self.uri
                    )
                if self.mode == "regular" or self.mode == "all":
                    pipelines = creators[field].create_pipelines()
                elif self.mode == "group" or self.mode == "all":
                    pipelines = creators[field].create_group_pipelines()
                elif self.mode == "find" or self.mode == "all":
                    pipelines = creators[field].create_queries()
                for doc in pipelines:
                    doc["id"] = doc_id
                    insert.append(doc)
                    doc_id += 1
                self.db.command("insert", self.collection.name,
                                documents=insert)
                insert = []


def main():
    """Check if this has been run in 'group' or 'default' mode, initialize an
    instance of the inserter and use it to insert pipelines to target
    collection.
    """
    parser = ArgumentParser()
    parser.add_argument('--uri',
                        default='mongodb://admin:sonarw321@localhost:27117/admin',
                        help='SonarW credentials')
    parser.add_argument('--include_this_month', choices=['y', 'n'],
                        default='n',
                        help='Determines if queries include current month.')
    parser.add_argument('--mode', choices=["group", "find", "regular", "all"],
                        default="regular",
                        help='Determines the type of pipelines generated.')
    parser.add_argument('--agg_full_sql', choices=['y', 'n'], default='y')
    args = vars(parser.parse_args())
    if args['mode'] == "all":
        for mode in MODE_CHOICES:
            inserter = PipelineInserter(args['uri'],
                                        mode, args['include_this_month'],
                                        args["agg_full_sql"])
            inserter.insert_pipelines()
    else:
        inserter = PipelineInserter(args['uri'],
                                    args['mode'], args['include_this_month'],
                                    args["agg_full_sql"])
        inserter.insert_pipelines()


if __name__ == "__main__":
    main()
