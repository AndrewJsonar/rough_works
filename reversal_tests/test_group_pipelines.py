from time import time, strftime, gmtime, sleep
from datetime import datetime
from argparse import ArgumentParser

from pip._vendor.urllib3.connectionpool import xrange
from pymongo import MongoClient


class GroupTester(object):
    """Compare a pair of pipelines from input collection (named
    'input_collection_name') and insert results to target collection (named
    'target_collection_name').
    """

    def __init__(self, uri, use_dist=False,
                 target_collection_name="group_pipeline_results",
                 input_collection_name="group_pipelines", info_db_name="pipes",
                 agg_db_name="sonargd"):
        """Initialize connection to SonarW, db and collections."""
        self.connection = MongoClient(uri)
        self.agg_db = self.connection[agg_db_name]
        self.info_db = self.connection[info_db_name]
        self.target_collection = self.info_db[target_collection_name]
        self.input_collection = self.info_db[input_collection_name]

        self.use_dist = use_dist

    def get_failure_reason(self, result1, result2):
        if len(result1) != len(result2):
            return "Pipeline 1 gave " + str(len(result1)) + " results and" \
                                                            " pipeline 2 gave " + str(
                len(result2)) + " results."
        for i in xrange(len(result1)):
            if result1[i] != result2[i]:
                return "Results are same length but have a difference. First" \
                       " different element is at index " + str(i) + ". First" \
                                                                    " pipeline has element " + str(
                    result1[i]) + " Second" \
                                  " pipeline has element " + str(result2[i])

    def test_pipelines(self, index):
        """Aggregate pair of pipelines, compare counters and insert results
        to collection.
        """
        aggregation_info = self.input_collection.find_one({"id": index})
        if not aggregation_info:
            print(datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S") + "UTC")
            print(datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S") + "UTC - OMG I'M A GROUP PIPELINE RUNNER"
                                                                    " AND I CAN'T FIND A PIPELINE TO RUN")
        aggregation_collection = self.agg_db[
            aggregation_info["collection_name"]
        ]
        pair = aggregation_info["pair"]
        pipeline1 = pair[0]
        pipeline2 = pair[1]

        if self.use_dist:
            pipeline1 = [{"$dist": True}] + pipeline1
            pipeline2 = [{"$dist": True}] + pipeline2

        print(datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S") + "UTC - group_pipelines - Starting first group_pipeline aggregation (" + str(aggregation_info["id"]) + "/" + str(self.input_collection.count()) + ")")
        time1 = time()
        cursor1 = aggregation_collection.aggregate(pipeline1)
        time2 = time()
        result1 = list(cursor1)
        print(datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S") + "UTC - group_pipelines - Starting second group_pipeline aggregation (" + str(aggregation_info["id"]) + "/" + str(self.input_collection.count()) + ")")
        time3 = time()
        cursor2 = aggregation_collection.aggregate(pipeline2)
        time4 = time()
        result2 = list(cursor2)
        failure_message = None
        success = result1 == result2
        if not success:
            failure_message = self.get_failure_reason(result1, result2)
        agg_time1 = str(time2 - time1) + " seconds"
        agg_time2 = str(time4 - time3) + " seconds"
        self.insert_to_collection(aggregation_info["collection_name"],
                                  success, agg_time1, agg_time2, index,
                                  failure_message, self.use_dist)

    def insert_to_collection(self, pipe_collection_name, success, agg_time1,
                             agg_time2, pair_id, failure_message, use_dist,
                             warning=False):
        """Insert results to collection."""
        insert_doc = {}
        insert_doc["collection_name"] = pipe_collection_name
        insert_doc["pipeline1"] = {
            "index_in_pair": 0,
            "aggregation_time": agg_time1
        }
        insert_doc["pipeline2"] = {
            "index_in_pair": 1,
            "aggregation_time": agg_time2
        }
        insert_doc["id"] = float(pair_id)
        insert_doc["success"] = success
        insert_doc["run_date"] = datetime.utcnow()
        insert_doc["failure_message"] = failure_message
        insert_doc["Distributed"] = use_dist
        insert_doc["warning"] = warning
        self.target_collection.insert_one(insert_doc)


def get_command_line_args():
    parser = ArgumentParser()
    parser.add_argument('start_index',
                        help='Index of the first pipeline you want to run.')
    parser.add_argument('end_index',
                        help="Index of the last pipeline you want to run.")
    parser.add_argument('--uri',
                        default='mongodb://admin:sonarw321@localhost:27117',
                        help='SonarW credentials.')
    parser.add_argument('--use_dist', choices=['y', 'n'], default='n')
    parser.add_argument('--ignore_time', choices=['y', 'n'], default='n')
    return vars(parser.parse_args())


def try_pipeline(pipeline_index, tester):
    fail_count = 0
    while fail_count < 2:
        try:
            tester.test_pipelines(pipeline_index)
            return True
        except Exception as agg_exception:
            try:
                fail_count += 1
                if fail_count == 2:
                    tester.insert_to_collection(None, False, None, None,
                                                pipeline_index, str(agg_exception), tester.use_dist)
                else:
                    tester.insert_to_collection(None, True, None, None,
                                                pipeline_index, str(agg_exception), tester.use_dist, True)
            except Exception as insert_exception:
                print(datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S") +
                      "UTC - Failed to insert warning to collection. Aggregation exception's stack trace: "
                      + str(agg_exception) +
                      "\n\nInsert exception stack trace: " + str(insert_exception))
            finally:
                if fail_count < 2:
                    sleep(3600)
    return False


def main():
    """Initialize instance of 'GroupTester', expects additional command line
    arguments indicating first and last pipeline pair id's, test each pipeline
    pair.
    """
    args = get_command_line_args()
    use_dist = True if args['use_dist'] == "y" else False
    tester = GroupTester(uri=args['uri'], use_dist=use_dist)
    start_index = int(args['start_index'])
    end_index = int(args['end_index'])
    fail_counter = 0
    for i in xrange(start_index, end_index + 1):
        if args['ignore_time'] == 'n' and strftime("%H", gmtime()) == "11":
            break
        else:
            if fail_counter < 2:
                if try_pipeline(i, tester):
                    fail_counter = 0
                else:
                    fail_counter += 1
            else:
                exit(datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S") +
                     "UTC - Failed to aggregate two sequential group_pipeline. Exiting. Sorry!")


if __name__ == "__main__":
    main()
