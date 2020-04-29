from argparse import ArgumentParser
from subprocess import call
from multiprocessing import Pool
from os import popen, kill, getpid
import os
from signal import SIGKILL
from find_last_id import IdFinder
from pymongo.errors import OperationFailure

GROUP_TEST_TYPE = "group"
QUERY_TEST_TYPE = "query"
PIPELINE_TEST_TYPE = "pipeline"
TO_INDEX = 1000000
AUTH_FAILURE = 18


def parse():
    parser = ArgumentParser()
    parser.add_argument('--sonar_uri', help='uri to connect to SonarW, example:'
                                            'mongodb://cale:jj@localhost:27117/admin or mongodb://kojima:hideo@localhost:27117,rdist2.local:27117,rdist3.local:27117/admin?replicaSet=rs0')
    parser.add_argument('--ignore_time', dest="ignore", action="store_true",
                        help="If passed tests run forever, otherwise they stop in the morning.")
    parser.set_defaults(ignore=False)
    parser.add_argument("--kill_only", dest="kill", action="store_true",
                        help="If passed kills existing processes and exits.")
    parser.set_defaults(kill=False)
    parser.add_argument('--use_dist', dest='use_dist', action="store_true", default=False,
                        help="If passed tries to use distributed pipelines and queries")
    return vars(parser.parse_args())


def get_ignore_time(ignore_time):
    return bool_to_string(ignore_time)


def get_use_dist(use_dist):
    return bool_to_string(use_dist)


def bool_to_string(flag):
    return 'y' if flag else 'n'


def run_tests(arg_list):
    test_type, from_index, uri, ignore_time, use_dist = arg_list
    current_path = os.path.dirname(os.path.realpath(__file__))
    if test_type == GROUP_TEST_TYPE:
        call(["python", current_path + "/test_group_pipelines.py",
              str(int(from_index)),
              str(TO_INDEX), "--uri=" + uri, "--ignore_time=" + ignore_time, "--use_dist=" + use_dist])
    else:
        ignore = "true" if ignore_time == "y" else "false"
        use_dist = "true" if use_dist == "y" else "false"  # js lowercase true and false
        if test_type == PIPELINE_TEST_TYPE:
            call(["mongo", uri, "--eval",
                  "counter_flag=true; query_flag=false; from=" + str(from_index) + "; to=1000000; ignore_time=" + ignore
                  + "; use_dist=" + use_dist + ";",
                  current_path + "/test_pipelines.js"])
        else:
            call(["mongo", uri, "--eval",
                  "counter_flag=true; query_flag=true; from=" + str(from_index) + "; to=1000000; ignore_time=" + ignore
                  + ";use_dist=" + use_dist + ";",
                  current_path + "/test_pipelines.js"])


def check_kill_process(pstring):
    for line in popen("ps ax | grep " + pstring + " | grep -v grep"):
        fields = line.split()
        pid = int(fields[0])
        if getpid() != pid:
            print("Attempting to kill process " + str(pid))
            kill(pid, SIGKILL)


def main():
    args = parse()
    check_kill_process("run_everything.py")
    check_kill_process("test_pipelines.js")
    check_kill_process("test_group_pipelines.py")
    if args["kill"]:
        exit("All instances of this test have been annihilated. R.I.P")
    uri = args["sonar_uri"]
    use_dist = args["use_dist"]
    try:
        max_ids = IdFinder(args["sonar_uri"]).full_flow(False)
    except OperationFailure as e:
        if e.code == AUTH_FAILURE:
            print(str(e) + " try a correct URI for a change.")
            exit()
        else:
            raise e
    query_max_id = max_ids[3]["max_id"] or 0
    pipeline_max_id = max_ids[1]["max_id"] or 0
    group_max_id = max_ids[0]["max_id"] or 0

    query_max_id = query_max_id + 2
    pipeline_max_id = pipeline_max_id + 2
    group_max_id = group_max_id + 2
    pool = Pool(3)
    pool.map(run_tests,
             [[GROUP_TEST_TYPE, group_max_id, uri, get_ignore_time(args["ignore"]), get_use_dist(use_dist)],
              [PIPELINE_TEST_TYPE, pipeline_max_id, uri, get_ignore_time(args["ignore"]), get_use_dist(use_dist)],
              [QUERY_TEST_TYPE, query_max_id, uri, get_ignore_time(args["ignore"]), get_use_dist(use_dist)]])


if __name__ == "__main__":
    main()
