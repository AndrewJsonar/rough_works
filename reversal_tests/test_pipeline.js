var agg_db = db.getSiblingDB("sonargd");
var info_db = db.getSiblingDB("pipes")
var pipeline_collection = "pipelines";
var result_collection = "pipeline_results";
var query_collection = "queries";
var query_result_collection = "query_results";
var DEFAULT_MESSAGE = null;

function count_cursor(cursor, counter_flag, find_flag) {
  var counter = 0;
  if (counter_flag && !find_flag) {
    if (cursor.hasNext()) {
      counter = cursor.next().count;
    }
  }
  else if (counter_flag && find_flag) {
    counter = cursor;
  }
  else {
    while (cursor.hasNext()) {
      counter++;
      var next = cursor.next();
    }
  }
  return counter;
}

function aggregate(collection_name, pipeline, find_flag, counter_flag) {
  if (find_flag) {
    var hint = pipeline["hint"];
    var filter = pipeline["filter"];
    var sort = pipeline["sort"];
    if (counter_flag) {
      if (sort == null) {
        return [agg_db[collection_name].find(filter).hint(hint).count(), false];
      }
      else {
        return [agg_db[collection_name].find(filter).sort(sort).hint(hint).count(), false];
      }
    }
    else {
      if (sort == null) {
        return [agg_db[collection_name].find(filter).hint(hint).limit(500), true];
      }
      else {
        return [agg_db[collection_name].find(filter).sort(sort).hint(hint).limit(500), false];
      }
    }
  }
  else {
    return [agg_db[collection_name].aggregate(pipeline), false];
  }
}

function test_pair(id, counter_flag, find_flag, use_dist) {
  var collection_name = get_pipeline_collection(id, find_flag);
  var pair = get_pipeline_pair(id, counter_flag, find_flag, use_dist);
  var time1 = new Date();
  var agg_result1 = aggregate(collection_name, pair[0], find_flag, counter_flag);
  var time2 = new Date();
  if (!agg_result1[1]) {
    print(ISODate().toJSON() + " - "  + collection_name + " - Starting first count, query:" + find_flag);
    var counter1 = count_cursor(agg_result1[0], counter_flag, find_flag);
    var time3 = new Date();
    print(ISODate().toJSON() + " - "  + collection_name + " - First aggregation has " + counter1 + " results. Counting them took "
    + (time3 - time2)/60000 + " minutes.");
  }
  var agg_result2 = aggregate(collection_name, pair[1], find_flag, counter_flag);
  var time4 = new Date();
  if (!agg_result2[1]) {
    print(ISODate().toJSON() + " - "  + collection_name + " - Starting second count, query:" + find_flag);
    var counter2 = count_cursor(agg_result2[0], counter_flag, find_flag);
    var time5 = new Date();
    print(ISODate().toJSON() + " - "  + collection_name + " - Second aggregation has " + counter2 + " results. Counting them took "
    + (time5 - time4)/60000 + " minutes.");
    var success = counter1.valueOf() == counter2.valueOf();
    var count_time1 = String((time3 - time2)/60000).concat(" minutes");
    var count_time2 = String((time5 - time4)/60000).concat(" minutes");
    var message = null;
  }
  if (agg_result1[1] && agg_result2[1]) {
    results = compare_cursors(agg_result1[0], agg_result2[0]);
    var counter1 = results["count"];
    var counter2 = counter1;
    var success = results["success"];
    var count_time1 = results["time"];
    var count_time2 = count_time1;
    var message = results["message"]
  }
  var agg_time1 = String((time2 - time1)/60000).concat(" minutes");
  var agg_time2 = String((time4 - time3)/60000).concat(" minutes");
  insert_to_collection(collection_name, counter1, counter2, success, agg_time1,
  agg_time2, count_time1, count_time2, id, time1, counter_flag, find_flag,
  message, use_dist);
}

function compare_cursors(cursor1, cursor2) {
  var count = 0;
  var return_results = {"success": true, "message": DEFAULT_MESSAGE};
  var time1 = new Date();
  while (cursor1.hasNext()) {
    if (!cursor2.hasNext()) {
      return_results["success"] = false;
      return_results["message"] = "First cursor is shorter than second cursor."
    }
    count++;
    var current_doc1 = cursor1.next();
    var current_doc2 = cursor2.next();
    if (current_doc1._id.valueOf() != current_doc2._id.valueOf()) {
      return_results["success"] = false
      return_results["message"] = "Document number " + count + " is different."
    }
  }
  var time2 = new Date();
  return_results["time"] = String((time2 - time1)/60000).concat(" minutes");
  return_results["count"] = count;
  return return_results;
}
function get_pipeline_pair(id, counter_flag, find_flag, use_dist) {
  if (find_flag) {
    var collection_name = query_collection;
  }
  else {
    var collection_name = pipeline_collection;
  }
  var pair_doc = info_db[collection_name].findOne({"id": id});
  var pair = pair_doc.pair;
  if (counter_flag && !find_flag) {
    pair[0] = add_count_to_pipe(pair[0]);
    pair[1] = add_count_to_pipe(pair[1]);
    // remove_lower_bound(pair[0]);
    // remove_lower_bound(pair[1]);
  }
  if (use_dist && !find_flag){
    pair[0] = add_dist_to_pipe(pair[0])
    pair[1] = add_dist_to_pipe(pair[1])

  }
  if (use_dist && find_flag){
   pair[0] = add_dist_to_query(pair[0])
   pair[1] = add_dist_to_query(pair[1])
  }
  return pair;
}

function add_count_to_pipe(pipe) {
  pipe = pipe.concat([{"$count": "count"}]);
  return pipe;
}

function add_dist_to_pipe(pipe){
    new_pipe = [{"$dist": true}];
    pipe = new_pipe.concat(pipe);
    return pipe;
}

function remove_lower_bound(pipe) {
  for (time_field in pipe[0]["$match"]) {
    delete pipe[0]["$match"][time_field]["$gte"];
  }
}

function add_dist_to_query(query){
    query["$dist"]=true;
    return query;
}

function get_pipeline_collection(id, find_flag) {
  if (find_flag) {
    var runner_name = "query";
    var coll_name_doc = info_db[query_collection].findOne({"id": id});
    print('Looking for a pipeline with id: ', id, ' out of ', info_db[query_collection].count(), " documents");
  }
  else {
    var runner_name = "pipeline";
    var coll_name_doc = info_db[pipeline_collection].findOne({"id": id});
  }
  if (coll_name_doc != null) {
    return coll_name_doc.collection_name;
  } else {
    print(ISODate().toJSON());
    print(ISODate().toJSON() + " - OMG I'M A " + runner_name + " RUNNER AND I CAN'T FIND A PIPELINE TO RUN");
    quit();
  }
}

function insert_to_collection(pipe_collection_name,
    counter1, counter2, success, agg_time1, agg_time2,
    count_time1, count_time2, pair_id, run_date, counter_flag, find_flag,
    message, use_dist, warning=false) {
      var insert_doc = {
        "collection_name": pipe_collection_name,
        "pipeline1": {
          "index_in_pair": 0, "aggregation_time": agg_time1, "count_time": count_time1, "result_count": counter1
        },
        "pipeline2": {
          "index_in_pair": 1, "aggregation_time": agg_time2, "count_time": count_time2, "result_count": counter2
        },
        "success": success,
        "id": pair_id,
        "run_date": run_date,
        "counter_flag": counter_flag,
        "message": message,
        "Distributed": use_dist,
        "warning": warning

      };
      if (find_flag) {
        info_db[query_result_collection].insert(insert_doc);
      }
      else {
        info_db[result_collection].insert(insert_doc);
      }
    }

function try_pipeline(pipeline_index, counter_flag, query_flag, use_dist) {
  var fail_count = 0;
  while (fail_count < 2) {
    try {
      var collection_name = null
      collection_name = get_pipeline_collection(pipeline_index, query_flag);
      test_pair(pipeline_index, counter_flag, query_flag, use_dist);
      return true;
    }
    catch(err1) {
      try {
        fail_count++;
        if (fail_count == 2) {
          insert_to_collection(collection_name,
              null, null, false, null, null,
              null, null, pipeline_index, new Date(), counter_flag, query_flag,
              err1.message, use_dist);
        }
        else {
          insert_to_collection(collection_name,
              null, null, true, null, null,
              null, null, pipeline_index, new Date(), counter_flag, query_flag,
              err1.message, use_dist, true );
        }
      }
      catch(err2) {
        print(ISODate().toJSON() + " - Failed to insert to collection query:" + query_flag);
        print(ISODate().toJSON() + " - " + err1.message);
        print(ISODate().toJSON() + " - " + err2.message);
      }
      finally {
        if (fail_count < 2) {
          sleep(360000);
      }
    }
  }
  return false;
}
}

function main(query_flag, counter_flag, from, to, ignore_time, use_dist) {
  var fail_counter = 0;
  if (query_flag) {
    var coll_name = "query_results";
  }  else {
    var coll_name = "pipeline_results";
  }
  for (var i = from; i <= to; i++) {
    var current_time = new Date();
    var current_hour_gmt = current_time.getHours();
    if (current_hour_gmt == 11 && !ignore_time) {
      break;
    }
    else {
      if (fail_counter < 2) {
        if (try_pipeline(i, counter_flag, query_flag, use_dist)) {
          fail_counter = 0;
        }
        else {
          fail_counter++;
        }
      }
      else {
        print(ISODate().toJSON() + " - Failed to aggregate two sequential pipelines(query:"+query_flag+"). Exiting. Sorry!");
        break;
      }
    }
  }
}
main(query_flag, counter_flag, from, to, ignore_time, use_dist);