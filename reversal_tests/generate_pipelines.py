from random import choice, sample, randint
from time import time
from datetime import datetime, timedelta
from copy import deepcopy
from calendar import monthrange
import sys

from pip._vendor.urllib3.connectionpool import xrange
from pymongo import MongoClient

from constants import (TIME_STAMP_DICTIONARY, FIELD_LIST_DICTIONARY,
                       SESSION_SPECIAL_FIELDS, FULL_GROUP_FIELD_LIST,
                       USER_GROUP_DESCRIPTIONS)
from custom_exceptions import InvalidTime


class PipelineCreator(object):
    """Create a list of the following aggregation stages: match, project, group,
    project stages, copy it and add {'$natural': 1} to one copy and
    {'$natural': -1} to the other copy. Then create a list of pairs of pipelines
    that make sense to compare.
    Can also create a list of 'groupified' pipeline pairs. A 'groupified'
    pipeline is a pipeline that had a group stage grouping by a field from the
    'FULL_GROUP_FIELD_LIST' added to it and pipelines with a $bucket stage which
    also produces groups.
    Can also create a list of query pairs. A query is combination of a filter
    (which should be passed as an argument to 'find'), a sort doc (which
    should be passed as an argument to 'sort') and a hint document (which
    should be passed as an argument to 'hint').
    To get a full list of pipeline pairs use the 'create_pipelines' method.
    To get a full list of groupified pipeline pairs use the
    'create_group_pipelines' method.u
    To get a full list of query pairs, use the 'create queries' method.
    """

    def __init__(self, collection_name, include_this_month, uri):
        """Initialize class by connecting to the 'sonargd' db on the local host.
        """
        self.connection = MongoClient(uri)
        self.db_name = "sonargd"
        self.db = self.connection[self.db_name]
        self.collection = self.db[collection_name]
        self.collection_name = collection_name
        self.field_list = FIELD_LIST_DICTIONARY[collection_name]
        self.time_field_name = TIME_STAMP_DICTIONARY[collection_name]
        self.MIN_COUNT_FOR_MATCH = 1000000
        self.valid_months = self.get_valid_months(include_this_month)

    def create_queries(self):
        """Return list of query pairs."""
        query_pairs = []
        filters = self.get_time_filters()
        group_filters = [
            self.get_group_member_filter(deepcopy(match_filter)) for
            match_filter in filters
        ]
        filters += group_filters
        for match_filter in filters:
            self.add_query_pair(query_pairs, match_filter,
                                choice([None, 1, -1]))
        return query_pairs

    def get_group_member_filter(self, time_filter):
        operator = choice(["$in", "$nin"])
        field = choice(["OS User", "DB User Name"])
        description = choice(USER_GROUP_DESCRIPTIONS)
        time_filter.update({field: {operator: {"$ns": "group_members",
                                               "$q": {"Group Description": description},
                                               "$p": "Group Member"}}})
        return time_filter

    def add_query_pair(self, pairs, match_filter, sort):
        """Add a pair of queries to 'pairs'. One of them with {'$natural': 1}
        and the other with {'$natural': -1}.
        """
        pair = [self.generate_query_info_doc(match_filter, 1, sort),
                self.generate_query_info_doc(match_filter, -1, sort)]
        pairs.append({"pair": pair, "collection_name": self.collection_name})

    def generate_query_info_doc(self, match_filter, hint, sort=None):
        """Return document with 'filter', 'sort' and 'hint' info."""
        sort_doc = None if sort is None else {self.time_field_name: sort}
        return {"filter": match_filter, "sort": sort_doc,
                "hint": {"$natural": hint}}

    def get_time_filters(self):
        """Return list of time filter documents."""
        random_filter = self.get_random_time_range_filter()
        last_24_hours_filter = self.get_previous_period_filter(days=1)
        last_7_days_filter = self.get_previous_period_filter(days=7)
        last_30_days_filter = self.get_previous_period_filter(days=30)
        last_x_days_filter = self.get_previous_period_filter(
            days=self.get_number_of_days_backwards()
        )
        return [random_filter, last_24_hours_filter, last_7_days_filter,
                last_30_days_filter, last_x_days_filter]

    def get_number_of_days_backwards(self):
        """Return number of days between now and the first day of a random
        month for which there is data.
        """
        year_month = self.get_year_month_for_match(self.valid_months)
        year = year_month["year"]
        month = year_month["month"]
        return (datetime.now() - datetime(year, month, 1)).days

    def get_previous_period_filter(self, days=0, seconds=0):
        """Return time filter document beginning 'years', 'months', etc. time
        before today and ending today.
        """
        return self.get_time_range_filter_doc(
            datetime.now() - timedelta(days=days, seconds=seconds),
            datetime.now()
        )

    def get_random_time_range_filter(self):
        """Return time filter document starting from a random day in a month
        with data and ending on a day that will result in 1M documents if
        the filter is applied or the end of the month (whichever happens first).
        """
        range_list = self.get_random_time_range(
            self.get_year_month_for_match(
                self.valid_months
            )
        )
        return self.get_time_range_filter_doc(range_list[0], range_list[1])

    def get_time_range_doc(self, gte, lte):
        return {"$gte": gte, "$lte": lte}

    def create_pipelines(self):
        """Return list dictionaries where each dictionary has a 'pair' field
         containing a list of 2 pipelines that make sense to compare.
        """
        join_pairs = []
        join_pipe_lists = self.get_basic_pipelines("join")
        self._add_pairs_per_index(join_pairs, 0, 2, join_pipe_lists)
        self._add_pairs_per_index(join_pairs, 2, 5, join_pipe_lists)
        self._add_pairs_per_index(join_pairs, 5, 7, join_pipe_lists)
        self._add_pairs_per_index(join_pairs, 7, 10, join_pipe_lists)
        lookup_pairs = []
        lookup_pipe_lists = self.get_basic_pipelines("lookup")
        self._add_pairs_per_index(lookup_pairs, 0, 2, lookup_pipe_lists)
        self._add_pairs_per_index(lookup_pairs, 2, 5, lookup_pipe_lists)
        self._add_pairs_per_index(lookup_pairs, 5, 7, lookup_pipe_lists)
        self._add_pairs_per_index(lookup_pairs, 7, 10, lookup_pipe_lists)
        return join_pairs + lookup_pairs

    def _add_pairs_per_index(self, pairs, start_index, end_index, pipe_lists):
        """Add all sensible pipeline pairs to 'pairs'."""
        for i in xrange(start_index, end_index):
            for j in xrange(start_index, end_index):
                self.add_pipeline_pair(pairs, pipe_lists[0][i],
                                       pipe_lists[1][j])

    def create_stages(self, join_operator):
        """Return list of pipeline stages."""

        first_project_stage = self.get_first_project_stage(self.field_list)
        if join_operator == "join":
            match_stage = self.get_match_stage("join")
            join_stage = self.get_join_stage(match_stage)
        elif join_operator == "lookup":
            time_frame, match_stage = self.get_match_stage("lookup")
            join_stage = self.get_lookup_stage(time_frame)
        group_result = self.get_group_stage()
        group_stage = group_result[0]
        second_project_stage = self.get_second_project_stage(group_result[1],
                                                             group_result[2])
        sort_stage = self.get_sort_stage(group_result[1])
        join_pipe = [join_stage] if join_operator == "join" else [
            join_stage, {"$unwind": "$joined_array"}
        ]
        stage_list1 = [[match_stage], join_pipe, [first_project_stage],
                       [group_stage], [second_project_stage], [sort_stage]]

        return stage_list1

    def get_pipelines_from_stages(self, stage_list):
        """Return lists of all desired pipelines created from the raw pipeline
        stages (stage_list).
        """
        pipeline_list1 = [deepcopy(stage_list[0])]
        for i in xrange(1, len(stage_list)):
            pipeline_list1.append(pipeline_list1[i - 1] + stage_list[i])
        temp_list = deepcopy(pipeline_list1)
        self._delete_join_stages(temp_list)
        del pipeline_list1[0]
        pipeline_list1 = temp_list + pipeline_list1
        pipeline_list2 = deepcopy(pipeline_list1)
        self.add_natural(pipeline_list1, 1)
        self.add_natural(pipeline_list2, -1)
        return pipeline_list1, pipeline_list2

    def create_group_pipelines(self):
        """Return a list of pipeline pairs. Some of the pipelines in the pair
        is a basic pipeline that had a group stage grouping by one of the fields
        in 'FULL_GROUP_FIELD_LIST' appended to it, the rest are pipelines with
        a $bucket stage.
        """
        basic_join_pipelines = self.get_basic_pipelines("join")
        basic_lookup_pipelines = self.get_basic_pipelines("lookup")
        basic_pipelines = basic_join_pipelines + basic_lookup_pipelines
        bucket_pipelines = self.create_full_bucket_pipelines()
        pipe_list1 = self.groupify_pipeline_list(basic_pipelines[0])
        pipe_list2 = self.groupify_pipeline_list(basic_pipelines[1])
        pipe_list1 += bucket_pipelines[0]
        pipe_list2 += bucket_pipelines[1]
        pair_list = []
        for i in xrange(len(pipe_list1)):
            self.add_pipeline_pair(pair_list, pipe_list1[i], pipe_list2[i])
        return pair_list

    def groupify_pipeline_list(self, pipeline_list):
        """Return a list of pipelines. The list is comprised of pipelines from
        'pipeline_list' that had a group stage grouping by one of the fields in
        'FULL_GROUP_FIELD_LIST' appended to it.
        """
        group_pipeline_list = []
        for pipeline in pipeline_list:
            groupified_pipelines = self.groupify_pipeline(pipeline)
            for pipe in groupified_pipelines:
                group_pipeline_list.append(pipe)
        return group_pipeline_list

    def groupify_pipeline(self, pipeline):
        """For each field in 'FULL_GROUP_FIELD_LIST', append a group stage to
        'pipeline'. Add the resulting pipeline to a list. Return that list.
        """
        groupified_pipelines = []
        for field in FULL_GROUP_FIELD_LIST:
            groupified_pipelines.append(
                pipeline + [{"$group": {"_id": "$" + field, "count": {"$sum": 1}}}, {"$sort": {"_id": 1}}]
            )
        return groupified_pipelines

    def create_full_bucket_pipelines(self):
        """Return list of 2  copies of pipelines created by
        'create_bucket_pipelines', one copy with '{$natural: 1}' and one with
        '{$natural: 1}'.
        """
        pipes1 = self.create_bucket_pipelines(self.get_time_filters())
        pipes2 = deepcopy(pipes1)
        self.add_natural(pipes1, 1)
        self.add_natural(pipes2, -1)
        return pipes1, pipes2

    def create_bucket_pipelines(self, time_filters):
        """For each time_filter in 'time_filters', create a pipeline matching
        for that time range, create a bucket list (see 'divide_to_buckets') and
        use it to create a bucket stage counting how many documents per bucket.
        Return list of all such pipelines.
        """
        pipelines = []
        for time_filter in time_filters:
            boundaries = self.get_boundaries_from_time_range_filter_doc(
                time_filter
            )
            buckets = self.divide_to_buckets(
                boundaries[0], boundaries[1], randint(2, 100)
            )
            pipelines.append(self.create_bucket_pipeline(time_filter, buckets))
        return pipelines

    def create_bucket_pipeline(self, time_filter, buckets):
        """Create a single pipeline for 'create_bucket_pipelines'."""
        return [
            {"$match": time_filter},
            {"$bucket": {
                "output": {
                    "count": {"$sum": 1}
                },
                "groupBy": "$" + self.time_field_name,
                "boundaries": buckets
            }},
            {"$limit": 1000}
        ]

    def divide_to_buckets(self, start_date, end_date, number_of_buckets):
        """Return list of buckets dividing the time range between 'start_date'
        and 'end_date' to 'number_of_buckets' equal time ranges.
        """
        buckets = []
        bucket_length = self.get_bucket_length(start_date, end_date,
                                               number_of_buckets)
        for i in xrange(number_of_buckets + 1):
            buckets.append(start_date + i * bucket_length)
        return buckets

    def get_bucket_length(self, start_date, end_date, number_of_buckets):
        """Return the size of a single bucket when dividing the time range
        between 'start_date' and 'end_date' to 'number_of_buckets' equal time
        ranges.
        """
        return (end_date - start_date) / number_of_buckets

    def get_basic_pipelines(self, join_operator):
        """Return 2 lists of pipelines, one with {$natural: 1} and one with
        {$natural: -1}.
        """
        return self.get_pipelines_from_stages(self.create_stages(join_operator))

    def _delete_join_stages(self, pipeline_list):
        """Delete the join and unwind stages from every pipeline, delete the pipeline with
        only a match and a join stage.
        """
        del pipeline_list[1]
        for i in xrange(1, len(pipeline_list)):
            del pipeline_list[i][1]
            if "$unwind" in pipeline_list[i][1]:
                del pipeline_list[i][1]

    def add_pipeline_pair(self, pair_list, pipeline1, pipeline2):
        """Add 'pipeline1' and 'pipeline2' to the pair list along with the
        collection name of the collection they should be aggregated on.
        """
        pair_list.append({"pair": [pipeline1, pipeline2],
                          "collection_name": self.collection_name})

    def add_natural(self, pipeline_list, order):
        """Add $natural to the match stage in all the pipelines in
        'pipeline_list'.
        """
        for pipe in pipeline_list:
            pipe[0]["$match"]["$natural"] = order

    def get_valid_months(self, include_this_month):
        """Return list of all year, month pairs for which there is data."""
        time_field_name = "$" + self.time_field_name
        start_time = time()
        print
        "Starting aggregation on " + self.collection_name + \
        " on " + str(datetime.utcnow()) + " UTC"
        print 'Number of documents in ' + self.collection_name + ': ' + str(self.collection.count())
        month_cursor = self.collection.aggregate(
            [{"$group": {"_id": {"month": {"$month": time_field_name},
                                 "year": {"$year": time_field_name}}}}])
        end_time = time()
        total_time = str(end_time - start_time)
        print
        "Group aggregation for months on " + self.collection_name + \
        " collection took " + total_time + " seconds."
        month_list = []
        for doc in month_cursor:
            year = doc["_id"]["year"]
            month = doc["_id"]["month"]
            current_year = datetime.now().year
            current_month = datetime.now().month
            if year is not None and month is not None:
                if include_this_month:
                    month_list.append(doc)
                elif year != current_year or month != current_month:
                    month_list.append(doc)
        if not month_list:
            sys.exit("No relevant dates selected. Exiting...")
        return month_list

    def get_year_month_for_match(self, month_list):
        """Return a tuple containing a random year and month combination that
        have documents containing that time stamp.
        """
        year_month = choice(month_list)["_id"]
        return year_month

    def get_match_stage(self, join_type):
        """Return match stage matching for a time frame beginning in a random
        day of 'year_month' and ending when the time frame has at least 1M
        documents or at the end of the month if no such time frame exists.
        """
        if join_type == "join":
            return {"$match": self.get_random_time_range_filter()}
        elif join_type == "lookup":
            time_frame = self.get_time_frame_for_lookup(
                choice(self.valid_months)
            )
            match = {"$match": {
                self.time_field_name: {"$gte": time_frame[0],
                                       "$lt": time_frame[1]}
            }}
            return time_frame, match

    def get_time_range_filter_doc(self, gte, lte):
        """Return document fit for 'match' or 'find', filtering for time frame
        specified in 'time_range_doc'.
        """
        return {self.time_field_name: self.get_time_range_doc(gte, lte)}

    def get_boundaries_from_time_range_filter_doc(self, time_range_filter_doc):
        """Return start date and end date of the time range that
        'time_range_filter_doc' filters for.
        """
        start = time_range_filter_doc[self.time_field_name]["$gte"]
        end = time_range_filter_doc[self.time_field_name]["$lte"]
        return start, end

    def get_random_time_range(self, year_month):
        """Return document with a time range. Time range logic explained in
        'get_match_stage'.
        """
        year = year_month["year"]
        month = year_month["month"]
        current_date = datetime.today()
        current_date = datetime(current_date.year, current_date.month, current_date.day)
        start_day = choice(xrange(1, monthrange(year, month)[1] + 1))
        start_date = datetime(year, month, start_day)
        if start_date > current_date:
            start_date = current_date
            end_date = current_date
        else:
            end_date = self.get_end_date(start_date)
            if end_date > current_date:
                end_date = current_date
        return start_date, end_date

    def get_end_date(self, start_date):
        """Return end date the satisfies the condition described in
        'get_match_stage'.
        """
        start_day = start_date.day
        month = start_date.month
        year = start_date.year
        for i in xrange(start_day, monthrange(year, month)[1] + 1):
            end_date = datetime(year, month, i)
            if self.test_match_stage(start_date, end_date):
                break

        return end_date

    def test_match_stage(self, start_date, end_date):
        """Return True if amount of documents between start and end dates is
        greater than 'MIN_COUNT_FOR_MATCH' and False otherwise.
        """
        doc_count = self.collection.count({self.time_field_name: {
            "$gte": start_date, "$lte": end_date
        }})
        return doc_count > self.MIN_COUNT_FOR_MATCH

    def get_first_project_stage(self, field_list):
        """Return project stage that projects all fields in the collection's
        'FIELD_LIST_DICTIONARY'.
        """
        stage = {"$project": {}}
        for field in field_list:
            stage["$project"][field] = 1
        for field in SESSION_SPECIAL_FIELDS:
            stage["$project"][field] = 1
        return stage

    def get_sort_stage(self, sort_by):
        """Return sort stage with random sort order."""
        return {"$sort": {sort_by: self.get_sort_order(),
                          "_id": self.get_sort_order()}}

    def get_sort_order(self):
        """Return random sort order."""
        return choice([-1, 1])

    def get_join_stage(self, match_stage):
        """Return join stage explicitly described below."""
        gte = match_stage["$match"][self.time_field_name]["$gte"] - timedelta(
            days=1)
        lte = match_stage["$match"][self.time_field_name]["$lte"]
        stage = {
            "$join": {
                "$joined": {
                    "session": "session"
                },
                "$match": [
                    {
                        "Session Id": "$session.$_id"
                    }
                ],
                "$selector": {
                    "session": {
                        "Session Start": {
                            "$lte": lte,
                            "$gte": gte
                        }
                    }
                },
                "$project": {
                    "Login Succeeded": "$session.$Login Succeeded",
                    "Access Id": "$session.$Access Id",
                    "Session End": "$session.$Session End",
                    "Client Port": "$session.$Client Port",
                    "Uid Chain": "$session.$Uid Chain",
                    "Session Ignored": "$session.$Session Ignored",
                    "Sender IP": "$session.$Sender IP",
                    "*": 1
                }
            }
        }
        return stage

    def get_lookup_stage(self, time_frame):
        return {
            "$lookup": {
                "from": "session",
                "let": {
                    'Server IP': '$Server IP',
                    'Analyzed Client IP': '$Analyzed Client IP',
                    'Service Name': '$Service Name',
                    'Database Name': '$Database Name'
                },
                "pipeline": [
                    {"$match": {"Session Start": {"$gte": time_frame[0],
                                                  "$lt": time_frame[1]}}},
                    {"$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$Server IP", "$$Server IP"]},
                                {"$eq": ["$Analyzed Client IP",
                                         "$$Analyzed Client IP"]},
                                {"$eq": ["$Service Name", "$$Service Name"]},
                                {"$eq": ["$Database Name", "$$Database Name"]}
                            ]
                        }
                    }},
                    {"$limit": 1},
                ],
                "as": "joined_array"
            }
        }

    def get_time_frame_for_lookup(self, year_month):
        start_date = self.get_date_with_data(year_month)
        for hours in xrange(1, 100):
            end_time = start_date + timedelta(hours=hours)
            time_filter = {"$gte": start_date, "$lt": end_time}
            if self.collection.count({self.time_field_name: time_filter}) > 0:
                return start_date, end_time
        raise InvalidTime

    def get_date_with_data(self, year_month):
        year = year_month["_id"]["year"]
        month = year_month["_id"]["month"]
        for day in xrange(1, monthrange(year, month)[1] + 1):
            start_date = datetime(year, month, day)
            end_date = start_date + timedelta(days=1)
            time_filter = {"$gte": start_date, "$lt": end_date}
            if self.collection.count({self.time_field_name: time_filter}) > 0:
                return start_date
        raise InvalidTime

    def get_group_stage(self):
        """Return group stage that groups by 2 random fields chosen from the
        list of all fields that it makes sense to group by for the relevant
        collection and then counts how many groups there are.
        """
        group_by1 = choice(self.field_list)
        while True:
            group_by2 = choice(self.field_list)
            if group_by1 != group_by2:
                break
        stage = {"$group": {"_id": {group_by1: "$" + group_by1,
                                    group_by2: "$" + group_by2},
                            "count": {"$sum": 1}}}
        return stage, group_by1, group_by2

    def get_second_project_stage(self, group_by1, group_by2):
        """Return project stage that projects each field that the group stage
        grouped by to the other field grouped by.
        """
        stage = {"$project": {group_by1: "$_id." + group_by2,
                              group_by2: "$_id." + group_by1}}
        return stage


def main():
    creator = PipelineCreator("exception", False,
                              "mongodb://cale:jj@localhost:27117/admin")
    queries = creator.create_queries()
    # pipe_docs = creator.create_pipelines()
    # print pipe_docs[39]
    # exception_coll = MongoClient("mongodb://cale:jj@localhost:27117/admin")["sonargd"]["exception"]
    # pipe_coll = MongoClient("mongodb://cale:jj@localhost:27117/admin")["pipes"]["pipelines"]
    # for doc in pipe_docs:
    #     for stage in doc["pair"][0]:
    #         if "$lookup" in stage:
    #             pipe_coll.insert(doc, check_keys=False)


if __name__ == "__main__":
    main()
