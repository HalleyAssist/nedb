var Datastore = require('../lib/datastore')
  , benchDb = 'workspace/insert.bench.db'
  , [AsyncWaterfall, AsyncApply] = [require('async/waterfall'), require('async/apply')]
  , execTime = require('exec-time')
  , profiler = new execTime('INSERT BENCH')
  , commonUtilities = require('./commonUtilities')
  , config = commonUtilities.getConfiguration(benchDb)
  , d = config.d
  , n = config.n
  ;

AsyncWaterfall([
  AsyncApply(commonUtilities.prepareDb, benchDb)
, function (cb) {
    d.loadDatabase(function (err) {
      if (err) { return cb(err); }
      if (config.program.withIndex) {
        d.ensureIndex({ fieldName: 'docNumber' });
        n = 2 * n;   // We will actually insert twice as many documents
                     // because the index is slower when the collection is already
                     // big. So the result given by the algorithm will be a bit worse than
                     // actual performance
      }
      cb();
    });
  }
, function (cb) { profiler.beginProfiling(); return cb(); }
, AsyncApply(commonUtilities.insertDocs, d, n, profiler)
], function (err) {
  profiler.step("Benchmark finished");

  if (err) { return console.log("An error was encountered: ", err); }
});
