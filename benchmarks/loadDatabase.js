var Datastore = require('../lib/datastore')
  , benchDb = 'workspace/loaddb.bench.db'
  , [AsyncWaterfall, AsyncApply] = [require('async/waterfall'), require('async/apply')]
  , commonUtilities = require('./commonUtilities')
  , execTime = require('exec-time')
  , profiler = new execTime('LOADDB BENCH')
  , d = new Datastore(benchDb)
  , program = require('commander')
  , n
  ;

program
  .option('-n --number [number]', 'Size of the collection to test on', parseInt)
  .option('-i --with-index', 'Test with an index')
  .parse(process.argv);

n = program.number || 10000;

console.log("----------------------------");
console.log("Test with " + n + " documents");
console.log(program.withIndex ? "Use an index" : "Don't use an index");
console.log("----------------------------");

AsyncWaterfall([
  AsyncApply(commonUtilities.prepareDb, benchDb)
, function (cb) {
    d.loadDatabase(cb);
  }
, function (cb) { profiler.beginProfiling(); return cb(); }
, AsyncApply(commonUtilities.insertDocs, d, n, profiler)
, AsyncApply(commonUtilities.loadDatabase, d, n, profiler)
], function (err) {
  profiler.step("Benchmark finished");

  if (err) { return console.log("An error was encountered: ", err); }
});
