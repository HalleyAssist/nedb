var Datastore = require('../lib/datastore')
  , benchDb = 'workspace/remove.bench.db'
  , [AsyncWaterfall, AsyncApply] = [require('async/waterfall'), require('async/apply')]
  , execTime = require('exec-time')
  , profiler = new execTime('REMOVE BENCH')
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

// Test with remove only one document
, function (cb) { profiler.step('MULTI: FALSE'); return cb(); }
, AsyncApply(commonUtilities.removeDocs, { multi: false }, d, n, profiler)
// Test with multiple documents
, function (cb) { d.remove({}, { multi: true }, function () { return cb(); }); }
, AsyncApply(commonUtilities.insertDocs, d, n, profiler)
, function (cb) { profiler.step('MULTI: TRUE'); return cb(); }
, AsyncApply(commonUtilities.removeDocs, { multi: true }, d, n, profiler)
], function (err) {
  profiler.step("Benchmark finished");

  if (err) { return console.log("An error was encountered: ", err); }
});
