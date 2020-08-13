/*******************
		Vars
*******************/
var result_obj = {};
var g2_feed_machines = ["main.local", "dr.local", "rambo.local", "big4-azure", "big4-aws-v3", 
						"big4-google", "ip-172-30-2-56.ec2.internal", "ip-172-30-2-247.ec2.internal",
						"ip-172-30-2-74.ec2.internal", "g2"];
var g2_collectors = ["gibm32", "gibm34", "gibm38", "glight", "china2", "orna-vm01", "varun-vm01", 
					"DMv2_ghp02", "ghp02", "tsnider-vm04", "gpart1-col11"];
var sonargd_collections = ["session", "instance", "full_sql", "exception", "policy_violations"];
var yesterday = ISODate();
var db = db.getSiblingDB('admin');

yesterday.setDate(yesterday.getDate()-1);

/*********************
        Main
*********************/

result_obj.host = getHostName();
result_obj.is_master = db.runCommand({"ismaster": 1}).ismaster;
result_obj.did_ranges = get_sonargd_did_ranges(sonargd_collections);
result_obj.op_counters = get_op_counters();
result_obj.storage = get_storage_stats('full_sql');
result_obj.gp = get_gp_stats(result_obj.is_master);
result_obj.stats = get_data_and_memory();
result_obj.feeds = get_all_feeds(result_obj.is_master, result_obj.host, g2_collectors, sonargd_collections, result_obj.did_ranges);
result_obj.purge = get_purge_stats(sonargd_collections);
result_obj.collection_counts = get_collection_counts(sonargd_collections, result_obj.purge);
result_obj.ueba_counts = get_uebe_counts();

//PRINT RESULT(json)
printjson(result_obj);


/*************************
		Functions
*************************/

//get collection local and cloud storage
function get_storage_stats(collection_name){
	db = db.getSiblingDB('sonargd');
	var response = {};
	var i = 0;
	var local_sum = 0;
	var cloud_sum = 0;
	try{
		while(i != -1){ 
			var tmp = db.runCommand({get_storage_usage: collection_name, "token" : i}).current; 
			if(tmp != null){
				local_sum += tmp.local; 
				cloud_sum += tmp.cloud;
				i=tmp.token;
			}
			else{
				i = -1;
			}
		}
	}catch(err){
		print(err);
	}
	response[collection_name + "_local_storage"] =  local_sum;
	response[collection_name + "_cloud_storage"] = cloud_sum;
	return response;
}

function get_col_did_range(db_name, col_name){
	db = db.getSiblingDB(db_name);
	try{
		var have_saved_rdid = db.daily_report.findOne({name: col_name});
		if (! have_saved_rdid) {
	    	db.daily_report.insert({name: col_name, last_saved_rdid: db[col_name].stats().last_document});
		}
		var did_range = {};
		did_range.from = db.daily_report.find({name: col_name}).toArray()[0].last_saved_rdid;
		did_range.to = db[col_name].stats().last_document;
		db.daily_report.update({name: col_name}, {name: col_name ,last_saved_rdid: did_range.to});
		if(did_range.from && did_range.to){
			return did_range;
		}else{
			return null;
		}
	}catch(err){
		print(err);
		return null;
	}
}

function get_sonargd_did_ranges(collections){
	var sonargd_did_ranges = {};
	try{
		for(i=0; i<collections.length; i++){
		sonargd_did_ranges[collections[i]] = get_col_did_range('sonargd', collections[i]);
		}
		sonargd_did_ranges["subarray_object_feed"] = get_col_did_range('sonargd', 'subarray_object_feed');
	}catch(err){
		print(err);
	}
	return sonargd_did_ranges;
}

//returns an object with 6 purge collections pathes and purge stats
function get_purge_stats(collections){
	var col_array = collections;
	col_array.push("subarray_object_feed")
	db = db.getSiblingDB('sonargd');
	var result = {};
	try{
		for(i=0; i<col_array.length; i++){
		result[col_array[i]+"_path"] = db[col_array[i]].stats().folder;
		}
		db = db.getSiblingDB('sonar_log');
		for(i=0; i<col_array.length; i++){
			var last_purge = db.purge_log.aggregate([{$match: {collection: "sonargd."+col_array[i]}}, {$sort: {_id: -1}}, {$limit: 1}, {$project: {_id: 0}}])._batch[0];
			if(last_purge){
				result[col_array[i]+"_last_purge"] = JSON.stringify(last_purge).replace(/,/g, "|");
			}
		}	
	}catch(err){
		print(err);
	}
	return result
}

//returns an opcounter object 
function get_op_counters(){
	db = db.getSiblingDB('admin');
	try{
		var actions = db.serverStatus().opcounters;
		return { 
			"actions.counts.query": actions.query,
			"actions.counts.insert": actions.insert,
			"actions.counts.update": actions.update,
			"actions.counts.remove": actions.delete
		}
	}catch(err){
		print(err);
		return {};
	}
}

//return an object with main 5 collections counts
function get_collection_counts(collections, purge_stats){
	db = db.getSiblingDB('sonargd');
	var result = {};
	try{
		for(i=0; i<collections.length; i++){
				result["sonargd."+collections[i]+".count"] = Number(db[collections[i]].count().toString().match(/\d+/)[0]);
		}
		var subarray_col_exists = db.system.namespaces.find( { name: 'sonargd.subarray_object_feed' } );
			result["sonargd.subarray_object_feed.count"] = Number(db.subarray_object_feed.count().toString().match(/\d+/)[0]);
	}catch(err){
		print(err);
	}
	return result;
}
//return gp stats
function get_gp_stats(is_master){
	db = db.getSiblingDB("pipes");
	var gp_col_exists = db.system.namespaces.find( { name: 'pipes.queries' } );
	var gp_query_failed = [];
	var gp_pipeline_failed = [];
	var gp_group_failed =[];
	if(is_master && gp_col_exists.count()){
		var gp_failed_count = gp_query_count = gp_pipeline_count = gp_group_count =0;
		gp_query_count = db.query_results.count({"run_date": {$gt: yesterday},"success": true});
		cursor=db.query_results.find({"run_date": {$gt: yesterday},$or: [{"success": false}, {"warning": true}]});
		gp_failed_count += cursor.count();
		while(cursor.hasNext()){
			gp_query_failed.push(JSON.stringify(cursor.next()));
		}
		gp_pipeline_count = db.pipeline_results.count({"run_date": {$gt: yesterday},"success": true});
		cursor = db.pipeline_results.find({"run_date": {$gt: yesterday},$or: [{"success": false}, {"warning": true}]});
		gp_failed_count += cursor.count();
		while(cursor.hasNext()){
				gp_pipeline_failed.push(JSON.stringify(cursor.next()));
		}
		gp_group_count = db.group_pipeline_results.count({"run_date": {$gt: yesterday},"success": true});
		cursor = db.group_pipeline_results.find({"run_date": {$gt: yesterday},$or: [{"success": false}, {"warning": true}]});
		gp_failed_count +=cursor.count();
		while(cursor.hasNext()){
				gp_group_failed.push(JSON.stringify(cursor.next()));
		}
	}else{
            var gp_pipeline_count = gp_group_count = gp_failed_count = gp_query_count = "N/R";
    }

	return {
		"gp_failed_count": gp_failed_count, 
		"gp_query_count": gp_query_count,
		"gp_pipeline_count": gp_pipeline_count,
		"gp_group_count": gp_group_count, 
		"gp_query_failed": gp_query_failed, 
		"gp_pipeline_failed": gp_pipeline_failed, 
		"gp_group_failed": gp_group_failed
	}
}
//return machine stats from sonar_log.sonar_stats
function get_data_and_memory(){
	db = db.getSiblingDB('sonar_log');
	try{
		var stats = db.sonar_state.aggregate([
			{
				"$match": {
					"ts": {"$gt": yesterday}
				}
			},
			{
				"$group": {
					"_id": null,
					"avg_res_mem": {"$avg": "$sonarResidentMemoryKb"}, 
					"avg_virt_mem":{"$avg": "$sonarVirtualMemoryKb"},
					"avg_jam_active": {"$avg": "$jemalloc_active"},
					"avg_jam_mapped": {"$avg": "$jemalloc_mapped"},
					"avg_cur_users": {"$avg": "$currentUsers"}
				}
			}
		])
	}catch(err){
		print(err);
	}
	return stats._batch[0];
}

function get_azure_feeds(sonargd_collections, did_ranges){
	var azure_sql_new = "N/R"
	db = db.getSiblingDB('sonargd');
	azure_sql_new = 0;
	for(var i=0; i<sonargd_collections.length ; i++){
	 	//new DB not RonDemo
		//var azure_sql_count = db.instance.aggregate([{"$match": {"Server Type": "MS SQL AZURE", "Service Name": "MS SQL AZURE"}},{"$group":{"_id":0,count:{$sum:1}}}])
		try{
			var azure_sql = db[sonargd_collections[i]].aggregate([
			{
				"$match": {
					"SonarG Source" : "SonarGateway",
					"_id" : {'$did': [did_ranges[sonargd_collections[i]].from, did_ranges[sonargd_collections[i]].to]}
				}
			},
			{"$group":{"_id":0, count:{$sum:1}}}
			])
		}catch(err){
			print(err);
		}
		if(azure_sql._batch[0] != null){
 		azure_sql_new += azure_sql._batch[0].count;
		}
	}	
	return azure_sql_new;
}

function get_rds_new(value, sonargd_collections, did_ranges){
	rds_new = 0;
	try{
		db = db.getSiblingDB('sonargd');
		for(var i=0; i<sonargd_collections.length ; i++){
			rds_new += db[sonargd_collections[i]].count({
					"_id" : {'$did': [did_ranges[sonargd_collections[i]].from, did_ranges[sonargd_collections[i]].to]}, 
					"Server Type": value
			});
		}
	}catch(err){
		print(err);
	}
	return rds_new;
}

function get_collection_new_count(db_name, col_name){
		var did_range = get_col_did_range(db_name, col_name);
		db = db.getSiblingDB(db_name);
		var col_new = db[col_name].count({"_id": {'$did': [did_range.from, did_range.to]}});
		return col_new;
}

function get_gfake_new(did_ranges){
	db = db.getSiblingDB('sonargd');
	var gfake_exists = db.session.count({"SonarG Source": "gfake"});
	if(gfake_exists){
		gfake_new = 0;
		for(var i=0; i<sonargd_collections.length ; i++){
			try{
				gfake_new += db[sonargd_collections[i]].count({
					"SonarG Source": "gfake", 
					"_id" : {'$did': [did_ranges[sonargd_collections[i]].from, did_ranges[sonargd_collections[i]].to]}
				});
			}catch(err){
				print(err);
			}	
		}
	}else{
		gfake_new = "N/R";
	}
	return gfake_new;
}

function get_g2_new(g2_collectors, sonargd_collections, hostname, did_ranges){
	if(g2_feed_machines.includes(hostname)){
		db = db.getSiblingDB('sonargd');
		g2_new = 0;
		for(var i=0; i<sonargd_collections.length ; i++){
			try{
				g2_new += db[sonargd_collections[i]].count({
					"SonarG Source": {$in: g2_collectors}, 
					"_id" : {'$did': [did_ranges[sonargd_collections[i]].from, did_ranges[sonargd_collections[i]].to]}
				});
			}catch(err){
				print(err);
			}
		}
	}else{
		g2_new = "N/R";
	}
	return g2_new;
}

function get_new_feeds(g2_collectors, sonargd_collections, did_ranges){
	db = db.getSiblingDB('sonargd');
	var all_known_sources=g2_collectors;
	all_known_sources.push("gfake");
	var new_feed_source = [];
	for(var i=0; i<sonargd_collections.length ; i++){
		try{
			var tmp = db[sonargd_collections[i]].aggregate([{
				$match: {
					"SonarG Source": {
						$nin: all_known_sources
					},
					"_id" : {'$did': [did_ranges[sonargd_collections[i]].from, did_ranges[sonargd_collections[i]].to]},
					"Server Type": {
						$nin: ["MSSQL AZURE"]
					}

				}
			},{
				$group: {
					_id: "$SonarG Source", 
					"new_feed_count": {
						$sum: 1
					}
				}
			}])._batch[0]
			if (tmp) {new_feed_source.push(tmp);}
		}catch(err){
			print(err);
		}
	}
	return new_feed_source;
}

function get_new_subarray(did_ranges){
	db = db.getSiblingDB('sonargd');
	var subarray_col_exists = db.system.namespaces.find( { name: 'sonargd.subarray_object_feed' } );
	if(subarray_col_exists.count()){
		sub_new = db.subarray_object_feed.count({
			"_id" : {'$did': [did_ranges["subarray_object_feed"].from, did_ranges["subarray_object_feed"].to]}
		});
	}else{
		sub_new = "N/R";
	}
	
	return sub_new;
}

function get_all_feeds(is_master,hostname, g2_collectors, sonargd_collections, did_ranges){
	var feeds = {};
	feeds["feeds.google.pubsub.new"] = feeds["feeds.gfake.new"] = feeds["feeds.google.gcpmysql.new"] =
	feeds["feeds.google.gcppostgress-11.new"] = feeds["feeds.g2.new"] = feeds["feeds.subarray.new"] =
	feeds["feeds.azure.mysql.new"] = feeds["feeds.aws.postgres11-5.new"] = feeds["feeds.aws.mysql"] =
	feeds["feeds.sacramento_traffic.new"] = feeds["feeds.odbc.teradata.new"] ="N/R";
	if(is_master){
		if(hostname == "big4-aws-v3"){
			feeds["feeds.aws.postgres11-5.new"] = get_rds_new("AWS RDS POSTGRESQL", sonargd_collections, did_ranges);
			feeds["feeds.aws.mysql.new"] = get_rds_new("AWS RDS MYSQL", sonargd_collections, did_ranges);
		}else if(hostname == "big4-google"){
			feeds["feeds.google.gcpmysql.new"] = get_rds_new("GCP MYSQL", sonargd_collections, did_ranges);
			feeds["feeds.google.gcppostgress-11.new"] = get_rds_new("GCP POSTGRESQL", sonargd_collections, did_ranges);
		}else if(hostname == "main.local" || hostname == "dr.local"){
			feeds["feeds.odbc.teradata.new"] = get_rds_new("TERADATA", sonargd_collections, did_ranges);
			feeds["feeds.sacramento_traffic.new"] = get_collection_new_count('sonargd', 'city_of_sacramento_web_traffic');
		}else if(hostname == "big4-azure"){
			feeds["feed.azureSQL.new"] = get_azure_feeds(sonargd_collections, did_ranges);
			feeds["feeds.azure.mysql.new"] = get_rds_new("AZURE MYSQL SERVER", sonargd_collections, did_ranges);
		}else if(hostname == "u1-main.local" || hostname == "u1-dr.local"){
			feeds["feeds.aws.postgres11-5.new"] = get_rds_new("AWS RDS POSTGRESQL", sonargd_collections, did_ranges);
		}else if(hostname == "u1-minor.local"){
			feeds["feeds.aws.postgres11-5.new"] = get_rds_new("AWS RDS POSTGRESQL", sonargd_collections, did_ranges);
		}
		feeds["feeds.g2.new"] = get_g2_new(g2_collectors, sonargd_collections, hostname, did_ranges);
		feeds["feeds.gfake.new"] = get_gfake_new(did_ranges);
		feeds["feeds.subarray.new"] = get_new_subarray(did_ranges);
		feeds["feeds.new_feed_source"] = get_new_feeds(g2_collectors, sonargd_collections, did_ranges);
	}
	return feeds;
}

function get_uebe_counts(){
	db = db.getSiblingDB('lmrm__ae');
	var ueba_col_exists = db.system.namespaces.find( { name: 'lmrm__ae.ueba_results' } );
	ueba_count = {};
	if(ueba_col_exists.count()){
		try{
			ueba_count_tmp = db.ueba_results.aggregate({
				"$project": {
					outliers: {
						$cond: ["$is_outlier", 1, 0 ]
					},
					model_name: 1
				}
			},{
				"$group": {
					"_id": "$model_name", count: {$sum: 1}, 
					outliers_count: {$sum: "$outliers"}
				}
			})._batch;
			for(i=0; i<ueba_count_tmp.length; i++){
				ueba_count[ueba_count_tmp[i]._id+".count"] = ueba_count_tmp[i].count
				ueba_count[ueba_count_tmp[i]._id+".outliers"] = ueba_count_tmp[i].outliers_count;
			}
		}catch(err){
			print(err);
		}
	}else{
		ueba_count = "N/R";
	}
	return ueba_count;
}
