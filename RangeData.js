var MongoClient = require('mongodb').MongoClient;
var mongoUri = process.env.MONGO_WRITER_URI || require('./config').MONGO_WRITER_URI;

var pipe = [];

pipe.push({
  $group: {
    _id: null,
    average: { $avg: "$range" },
    high: { $max: "$range" },
    low: { $min: "$range" },
    values: { $push: "$range" }
  }
});

(function() {
  console.log('Building RangeData collection');
  MongoClient.connect(mongoUri, function(error, db) {
    if (error) console.error(error);
    else console.log('Successfully connected to db');
    db
      .collection('computedValues')
      .aggregate(pipe,
        { out: 'RangeData' },
        function(error) {
          if (error) console.error(error);
          else console.log('Successfully aggregated range data');
          db.close();
        });
  });
})();