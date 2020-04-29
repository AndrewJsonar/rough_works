database = "sonargd"
my_db = db.getSisterDB(database);
input_collection = my_db.collection1
output_collection = my_db.collection2
MAX_BATCH_SIZE = 10

var have_saved_rdid = input_collection.findOne({ _id: 0 });


if (!have_saved_rdid) {
    output_collection.insert({
        _id: 0,
        last_saved_rdid: input_collection.find({ _id: { '$did': [0] } }, { $did: 1 }).toArray()[0]['$rdid']
    });
}


var locked = input_collection.findOne({ _id: 0, locked: true });


if (!locked) {
    output_collection.update({ _id: 0 }, { $set: { locked: true } });
    first_rdid = output_collection.find({ _id: 0 }).toArray()[0].last_saved_rdid;
    last_rdid = input_collection.stats().last_document;
    start_time = ISODate();
    var inserted_count = NumberInt("0");

    my_cursor = input_collection.find({ "_id": { '$did': [first_rdid, last_rdid] } });
    while (my_cursor.hasNext()) {
        arr = [];
        THIS_BATCH = Math.floor(Math.random() * Math.floor(MAX_BATCH_SIZE));
        for (i = 0; my_cursor.hasNext() && i < THIS_BATCH; i++) {
            arr.push(my_cursor.next());
        }
        output_collection.insert({ batch_size: THIS_BATCH, arr: arr });
        inserted_count++;
    }
    output_collection.update({ _id: 0 }, { last_saved_rdid: last_rdid });
    finish_time = ISODate();

    output_collection.insert({ start_time: start_time, finish_time: finish_time, inserted_count: inserted_count });

    output_collection.update({ _id: 0 }, { $set: { locked: false } });
}
});