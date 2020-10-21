var Datastore = require('../lib/datastore')
  , benchDb = 'workspace/insert.bench.db'
  , [AsyncWaterfall, AsyncApply] = [require('async/waterfall'), require('async/apply')]
  , commonUtilities = require('./commonUtilities')
  , execTime = require('exec-time')
  , profiler = new execTime('INSERT BENCH')
  , model = require('../lib/model')
  , program = require('commander')
  , n
  ;

program
  .option('-n --number [number]', 'Number of iterations to test', parseInt)
  .parse(process.argv);

n = program.number || 1000000;

console.log("----------------------------");
console.log("Test with " + n + " documents");
console.log("----------------------------");

const row = {testObj: 1,  key: "chain", created_at: new Date(), nested: {cokateil: "tilly"}, array:[1,2,3,4]}

AsyncWaterfall([
  function (cb) { profiler.beginProfiling(); return cb(); }
, function (cb) {
  try {
      for(let i = 0; i<n; i++){
        model.deepCopy(row)
        model.deepCopy(row)
        model.deepCopy(row)
      }
    } catch(ex){
      cb(ex)
      return
    }
   cb()
  }
], function (err) {
  profiler.step("Benchmark finished");

  if (err) { return console.log("An error was encountered: ", err); }
});

