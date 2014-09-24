var MongoClient = require('mongodb').MongoClient;
var mongoUri = process.env.MONGO_WRITER_URI || require('./config').MONGO_WRITER_URI;

var pipe = [];

pipe.push({
  $group: {
    _id: null,
    std_average: { $avg: "$standardDeviation" },
    std_high: { $max: "$standardDeviation" },
    std_low: { $min: "$standardDeviation" },
    std_values: { $push: "$standardDeviation" },
    cov_average: { $avg: "$coefficientOfVariation" },
    cov_high: { $max: "$coefficientOfVariation" },
    cov_low: { $min: "$coefficientOfVariation" },
    cov_values: { $push: "$coefficientOfVariation" },
  }
});

pipe.push({
  $project: {
    standardDeviation: {
      average: "$std_average",
      high: "$std_high",
      low: "$std_low",
      values: "$std_values"
    },
    coefficientOfVariation: {
      average: "$cov_average",
      high: "$cov_high",
      low: "$cov_low",
      values: "$cov_values"
    }
  }
});

(function() {
  console.log('Building volume data collection');
  MongoClient.connect(mongoUri, function(error, db) {
    if (error) console.error(error);
    else console.log('Successfully connected to db');
    db
      .collection('computedValues')
      .aggregate(pipe,
        { out: 'VolatilityData' },
        function(error) {
          if (error) console.error(error);
          else console.log('Successfully aggregated volatility data');
          db.close();
        });
  });
})();
