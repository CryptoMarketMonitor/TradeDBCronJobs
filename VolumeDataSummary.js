
var MongoClient = require('mongodb').MongoClient;

var pipe = [];

pipe.push({
  $group: {
    _id: null,
    average: { $avg: "$volume" },
    high: { $max: "$volume" },
    low: { $min: "$volume" },
    values: { $push: "$volume" }
  }
});

(function() {
  console.log('Building volume data collection');
  MongoClient.connect(process.env.MONGO_WRITER_URI, function(error, db) {
    if (error) console.error(error);
    else console.log('Successfully connected to db');
    db
      .collection('computedValues')
      .aggregate(pipe,
        { out: 'VolumeData' },
        function(error) {
          if (error) console.error(error);
          else console.log('Successfully aggregated volume data');
          db.close();
        });
  });
})();