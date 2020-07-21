var fs = require('fs')
  , child_process = require('child_process')
  , [AsyncWaterfall, AsyncWhilst] = [require('async/waterfall'), require('async/whilst')]
  , Nedb = require('../lib/datastore')
  , db = new Nedb({ filename: './workspace/openfds.db', autoload: true })
  , N = 64   // Half the allowed file descriptors
  , i, fds
  ;

function multipleOpen (filename, N, callback) {
  AsyncWhilst( function () { return i < N; }
              , function (cb) {
                fs.open(filename, 'r', function (err, fd) {
                  i ++;
                  if (fd) { fds.push(fd); }
                  return cb(err);
                });
              }
              , callback);
}

AsyncWaterfall([
  // Check that ulimit has been set to the correct value
  function (cb) {
    i = 0;
    fds = [];
    multipleOpen('./test_lac/openFdsTestFile', 2 * N + 1, function (err) {
      if (!err) { console.log("No error occured while opening a file too many times"); }
      fds.forEach(function (fd) { fs.closeSync(fd); });
      return cb();
    })
  }
, function (cb) {
    i = 0;
    fds = [];
    multipleOpen('./test_lac/openFdsTestFile2', N, function (err) {
      if (err) { console.log('An unexpected error occured when opening file not too many times: ' + err); }
      fds.forEach(function (fd) { fs.closeSync(fd); });
      return cb();
    })
  }
  // Then actually test NeDB persistence
, function () {
    db.remove({}, { multi: true }, function (err) {
      if (err) { console.log(err); }
      db.insert({ hello: 'world' }, function (err) {
        if (err) { console.log(err); }

        i = 0;
        AsyncWhilst( function () { return i < 2 * N  + 1; }
                    , function (cb) {
                        db.persistence.persistCachedDatabase(function (err) {
                          if (err) { return cb(err); }
                          i ++;
                          return cb();
                        });
                      }
                    , function (err) {
                      if (err) { console.log("Got unexpected error during one peresistence operation: " + err); }
                      }
                    );

      });
    });
  }
]);

