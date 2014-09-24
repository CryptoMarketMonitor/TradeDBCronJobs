var _ = require('lodash');

var mongoUri = process.env.MONGO_WRITER_URI || require('./config').MONGO_WRITER_URI;
var mongoose = require('mongoose');
mongoose.connect(mongoUri);
var connection = mongoose.connection;

var Trade = mongoose.model('Trade', 
  new mongoose.Schema({}),
  'trades');

var ComputedValues = mongoose.model('ComputedValues',
  new mongoose.Schema({
    date: Date,
    high: Number,
    low: Number,
    vwap: Number,
    volume: Number,
    numTrades: Number,
    range: Number,
    variance: Number,
    standardDeviation: Number,
    coefficientOfVariation: Number
  }),
  'computedValues');

connection.on('error', function(error) {
  console.error('Mongoose encountered an error:', error);
});

connection.once('open', function() {
  console.log('Mongoose successfully connected with the database');
});

var pipe = [];

pipe.push({
  $group: {
    _id: { month: { $month: "$date" }, day: { $dayOfMonth: "$date" }, year: { $year: "$date" } },
    high: { $max: "$price" },
    low: { $min: "$price" },
    pq: { $sum: { $multiply: ["$price", "$amount"] } },
    volume: { $sum: "$amount" },
    numTrades: { $sum: 1 },
    trades: { $push: { price: "$price", amount: "$amount" } }
  }
});

pipe.push({
  $project: {
    _id: 1,
    vwap: { $divide: [ "$pq", "$volume" ] },
    high: 1,
    low: 1,
    volume: 1,
    numTrades: 1,
    trades: 1
  }
});

pipe.push({ $unwind: "$trades" });

pipe.push({
  $project: {
    _id: 1,
    high: 1,
    low: 1,
    vwap: 1,
    volume: 1,
    numTrades: 1,
    trades: 1,
    weightedSquaredError: {
      $multiply: [
        { $subtract: ["$trades.price", "$vwap"]},
        { $subtract: ["$trades.price", "$vwap"]},
        "$trades.amount"
      ]
    }
  }
});

pipe.push({
  $group: {
    _id:  "$_id",
    high: { $first: "$high" },
    low: { $first: "$low" },
    vwap: { $first: "$vwap" },
    volume: { $first: "$volume" },
    numTrades: { $first: "$numTrades" },
    sumSquares: { $sum: "$weightedSquaredError" }
  }
});

pipe.push({
  $project: {
    _id: 1,
    high: 1,
    low: 1,
    range: { $divide: [
      { $subtract: ["$high", "$low"] },
      "$low"
    ]},
    vwap: 1,
    volume: 1,
    numTrades: 1,
    variance: { $divide: ["$sumSquares", 
      { $divide: [
        { $multiply: [
          { $subtract: [ "$numTrades", 1 ]},
          "$volume"
        ]},
        "$numTrades"
      ]}
    ]}
  }
});

pipe.push({
  $sort: { _id: -1 }
});

pipe.push({
  $skip: 1
});

pipe.push({
  $sort: { _id: 1 }
});

pipe.push({
  $skip: 1
});

var data;

ComputedValues
  .remove({})
  .exec()
  .then(function() {
    return Trade
      .aggregate(pipe)
      .allowDiskUse(true)
      .exec();
  })
  .then(function(result) {
    _.each(result, function(el) {
      el.standardDeviation = Math.sqrt(el.variance);
      el.coefficientOfVariation = el.standardDeviation / el.vwap;
      el.date = new Date(Date.UTC(
          el._id.year,
          el._id.month - 1, 
          el._id.day
      ));
      delete el._id;
    });
    return ComputedValues
      .create(result);
  })
  .then(function() {
    console.log('Success');
    connection.close();
  })
  .then(null, function(error) {
    console.log(error);
    connection.close();
  });
