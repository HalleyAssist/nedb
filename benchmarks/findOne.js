var Datastore = require('../lib/datastore')
  , benchDb = 'workspace/findOne.bench.db'
  , [AsyncWaterfall, AsyncApply] = [require('async/waterfall'), require('async/apply')]
  , execTime = require('exec-time')
  , profiler = new execTime('FINDONE BENCH')
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
      if (config.program.withIndex) { d.ensureIndex({ fieldName: 'docNumber' }); }
      cb();
    });
  }
, function (cb) { profiler.beginProfiling(); return cb(); }
, AsyncApply(commonUtilities.insertDocs, d, n, profiler)
, function (cb) { setTimeout(function () {cb();}, 500); }
, AsyncApply(commonUtilities.findOneDocs, d, n, profiler)
], function (err) {
  profiler.step("Benchmark finished");

  if (err) { return console.log("An error was encountered: ", err); }
});
