var mongoUri = process.env.MONGO_READER_URI || require('./config').MONGO_READER_URI;
var mongoose = require('mongoose');
mongoose.connect(mongoUri);
var connection = mongoose.connection;
var Trade = mongoose.model('Trade', 
  new mongoose.Schema({}),
  'trades');

connection.on('error', function(error) {
  console.error('Mongoose encountered an error:', error);
});

connection.once('open', function() {
  console.log('Mongoose successfully connected with the database');
});


var timeframe = 100*60*1000;
var since = new Date(Date.now() - timeframe);
var pipe = [];

pipe.push({ 
  $match : { 
    date : { $gt : since } 
  }
});

pipe.push({
  $group: {
    _id: { 
      month: { $month: "$date" }, 
      day: { $dayOfMonth: "$date" }, 
      year: { $year: "$date" }, 
      hour: { $hour: "$date"}, 
      minute: { $minute: "$date"} 
    },
    high: { $max: "$price" },
    low: { $min: "$price" },
    open: { $first: "$price" },
    close: { $last: "$price" },
    pq: { $sum: { $multiply: ["$price", "$amount"] } },
    volume: { $sum: "$amount" },
    averageTrade: { $avg: "$amount" },
    numTrades: { $sum: 1 },
    trades: { $push: { price: "$price", amount: "$amount" } }
  }
});

pipe.push({
  $project: {
    vwap: { $divide: [ "$pq", "$volume" ] },
    high: 1,
    low: 1,
    open: 1,
    close: 1,
    volume: 1,
    averageTrade: 1,
    numTrades: 1
  }
});

pipe.push({
  $sort: { _id: 1 }
});

Trade
  .aggregate(pipe)
  .exec()
  .then(function(result) {
    console.log(result);
    result.forEach(function(data) {
      var date = new Date(Date.UTC(
                          data._id.year,
                          data._id.month, 
                          data._id.day,
                          data._id.hour,
                          data._id.minute
                      ));
      console.log(date);
    })
    connection.close();
  });









