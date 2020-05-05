#!/usr/bin/python
# -*- coding: utf-8 -*-
# Uri Hershenhorn, July 10, 2017
from operator import itemgetter
from os import environ
from os.path import expanduser
import sys

from __init__ import *
from permu_create_pipeline import *

MODIFIED_TOP_LIMIT = 5000000
DEFAULT_TOP_LIMIT = 100000
OUT_COLLECTION_NAME = "permu_out"
SIMPLE_OUT_COLLECTION_NAME = "permu_out_simple"
WRITE_PATH = expanduser('~/tmp/')
BAD_PICKLE_FILE_NAME = "new_bad_pickles"
BAD_STRING_FILE_NAME = "new_bad_pipes.txt"
ATTEMPTED_PIPELINES_STRING_FILE_NAME = "attempted_pipelines.txt"
ATTEMPTED_PIPELINES_PICKLE_FILE_NAME = "attempted_pipelines"


class Test_Large_Collections_Aggregations(TestCase):

    @classmethod
    def get_pipelines(self, mode=None):
        if "pipeline_path" in environ:
            return utils.get_all_pickled_objects(environ["pipeline_path"])
        else:
            pipe_creator = PipelineCreator()
            if mode is not None:
                return [self.unobjectify(pipe_creator.create_full_pipeline())]
            else:
                return [pipe_creator.create_full_pipeline()]

    @classmethod
    def unobjectify(self, pipeline):
        for stage in pipeline:
            for stage_definition in stage:
                if stage_definition == "$project" or \
                                stage_definition == "$group":
                    del stage[stage_definition]["object_array"]
                    del stage[stage_definition]["variable_array"]
        del pipeline[-2]
        pipeline[-1]["$match"] = {"string": {"$regex": 'o'}}
        return pipeline

    def compare_results(self, result1, result2, pipe):
        self.result = utils.compare_lists_as_sets(result1,
                                                  result2, "id")
        if not self.result["comparison"]:
            self.write_bad_pipe(pipe)
            self.fail(self.result["item"])
        self.result = utils.compare_sorted_lists(result1,
                                                 result2)
        if not self.result["comparison"]:
            self.write_bad_pipe(pipe)
            self.fail(self.result["item"])
        self.assertTrue(self.result['comparison'])

    @classmethod
    @change_permissions("root")
    def modify_top_limit(cls, modified_top_limit):
        cls.admin = cls.dbconnection["admin"]
        cls.admin.command("setParameter", 1, max_top_limit=modified_top_limit)

    @classmethod
    def setUpClass(cls):
        super(Test_Large_Collections_Aggregations, cls).setUpClass()
        cls.item_getter = itemgetter("id")
        cls.modify_top_limit(MODIFIED_TOP_LIMIT)
        cls.db_name = "permu"
        cls.coll_name = "permu"
        cls.db = cls.dbconnection[cls.db_name]  # This is the sonar db
        cls.coll = cls.db[cls.coll_name]  # This is the sonar collection
        cls.mongo_db = MongoClient()[cls.db_name]
        cls.mongo_coll = cls.mongo_db[cls.coll_name]
        if "mode" in environ:
            if environ["mode"] == "simple":
                cls.pipelines = cls.get_pipelines("simple")
                cls.write_path = WRITE_PATH + "simple/"
                cls.out_coll_name = SIMPLE_OUT_COLLECTION_NAME
            else:
                sys.exit("The environment variable 'mode' should be 'simple'"
                         " or non-existent.")
        else:
            cls.write_path = WRITE_PATH
            cls.out_coll_name = OUT_COLLECTION_NAME
            cls.pipelines = cls.get_pipelines()
        cls.out_coll = cls.db[cls.out_coll_name]

    def run_all_test(self):
        for pipe in self.pipelines:
            self.query = "db." + self.coll_name + ".aggregate(" + str(
                pipe) + \
                        ", {allowDiskUse: true})"
            self.mongo_result = self.attempt_pipeline("mongo", self.mongo_coll,
                                                      pipe)
            self._test_pipeline(pipe)
            self._test_pipeline_with_out(pipe)
            self._test_pipeline_without_optimizer_with_out(pipe)

    @classmethod
    def tearDownClass(cls):
        super(Test_Large_Collections_Aggregations, cls).tearDownClass()
        cls.modify_top_limit(DEFAULT_TOP_LIMIT)

    def write_bad_pipe(self, pipe):
        utils.append_to_file(self.write_path + BAD_STRING_FILE_NAME,
                             str(pipe) + "\n")
        utils.append_pickle_object(self.write_path + BAD_PICKLE_FILE_NAME,
                                   pipe)

    def write_attempted_pipe(self, pipe):
        utils.append_to_file(
            self.write_path + ATTEMPTED_PIPELINES_STRING_FILE_NAME,
            time.strftime("%d/%m/%Y") + "\n" + time.strftime("%H:%M:%S") +
            "\n" + str(pipe) + "\n"
        )
        utils.append_pickle_object(
            self.write_path + ATTEMPTED_PIPELINES_PICKLE_FILE_NAME,
            pipe
        )

    def attempt_pipeline(self, platform,  collection, pipe):

        self.write_attempted_pipe(pipe)
        start = time.time()
        cursor = collection.aggregate(pipe, allowDiskUse=True)
        end = time.time()
        total = (end-start)/60.0
        utils.append_to_file(
            self.write_path + ATTEMPTED_PIPELINES_STRING_FILE_NAME,
            "Above pipeline ran on " + platform + ", took " + str(total) +
            " minutes"
        )
        results = utils.get_list_from_cursor(cursor)
        if "$out" not in pipe[-1].keys():
            utils.append_to_file(
                self.write_path + ATTEMPTED_PIPELINES_STRING_FILE_NAME,
                " and gave " + str(len(results)) + " results.\n\n\n"
            )
        return results

    # Tests aggregation result vs Mongo collection
    def _test_pipeline(self, pipe):
        self.query = "db." + self.coll_name + ".aggregate(" +\
                     str(pipe) + ")"
        self.sonar_result = self.attempt_pipeline("sonar", self.coll, pipe)
        self.compare_results(self.mongo_result, self.sonar_result, pipe)

    # Uses $out to store aggregation result, then tests vs Mongo collection
    def _test_pipeline_with_out(self, pipe):
        self.pipe = pipe + [{"$out": self.out_coll_name}]
        self.query = "db." + self.coll_name + ".aggregate(" + \
                     str(self.pipe) + ")"
        self.attempt_pipeline("sonar", self.coll, self.pipe)
        self.sonar_result = utils.get_list_from_cursor(self.out_coll.find())
        utils.append_to_file(
            self.write_path + ATTEMPTED_PIPELINES_STRING_FILE_NAME,
            " and gave " + str(len(self.sonar_result)) + " results.\n\n\n"
        )
        self.compare_results(self.mongo_result, self.sonar_result, self.pipe)

    # Same as previous test, without optimizer
    def _test_pipeline_without_optimizer_with_out(self, pipe):
        self.pipe = [{"$optimizer": 0}] + pipe + \
                    [{"$out": self.out_coll_name}]
        self.query = "db." + self.coll_name + ".aggregate(" + \
                     str(self.pipe) + ")"
        self.attempt_pipeline("sonar", self.coll, self.pipe)
        self.sonar_result = utils.get_list_from_cursor(self.out_coll.find())
        utils.append_to_file(
            self.write_path + ATTEMPTED_PIPELINES_STRING_FILE_NAME,
            " and gave " + str(len(self.sonar_result)) + " results.\n\n\n"
        )
        self.compare_results(self.mongo_result, self.sonar_result, self.pipe)