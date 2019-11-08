/**
 * Handle every persistence-related task
 * The interface Datastore expects to be implemented is
 * * Persistence.loadDatabase(callback) and callback has signature err
 * * Persistence.persistNewState(newDocs, callback) where newDocs is an array of documents and callback has signature err
 */

var storage = require('./storage')
  , path = require('path')
  , model = require('./model')
  , async = require('async')
  , customUtils = require('./customUtils')
  , Index = require('./indexes')
  , LineTransform = require('node-line-reader').LineTransform
  , fs = require('fs')
  , {fallocateSync} = require('fallocate')
  ;


/**
 * Create a new Persistence object for database options.db
 * @param {Datastore} options.db
 * @param {Boolean} options.nodeWebkitAppName Optional, specify the name of your NW app if you want options.filename to be relative to the directory where
 *                                            Node Webkit stores application data such as cookies and local storage (the best place to store data in my opinion)
 */
function Persistence (options) {
  var i, j, randomString;

  this.db = options.db;
  this.inMemoryOnly = this.db.inMemoryOnly;
  this.filename = this.db.filename;
  this.corruptAlertThreshold = options.corruptAlertThreshold !== undefined ? options.corruptAlertThreshold : 0.1;
  this.writtenCount = 0

  if (!this.inMemoryOnly && this.filename && this.filename.charAt(this.filename.length - 1) === '~') {
    throw new Error("The datafile name can't end with a ~, which is reserved for crash safe backup files");
  }

  // After serialization and before deserialization hooks with some basic sanity checks
  if (options.afterSerialization && !options.beforeDeserialization) {
    throw new Error("Serialization hook defined but deserialization hook undefined, cautiously refusing to start NeDB to prevent dataloss");
  }
  if (!options.afterSerialization && options.beforeDeserialization) {
    throw new Error("Serialization hook undefined but deserialization hook defined, cautiously refusing to start NeDB to prevent dataloss");
  }
  this.afterSerialization = options.afterSerialization || function (s) { return model.serialize(s); };
  this.beforeDeserialization = options.beforeDeserialization || function (s) { return model.deserialize(s); };
  for (i = 1; i < 30; i ++) {
    for (j = 0; j < 10; j += 1) {
      randomString = "_"+customUtils.uid(i);
      if (this.beforeDeserialization(this.afterSerialization(randomString)) !== randomString) {
        throw new Error("beforeDeserialization is not the reverse of afterSerialization, cautiously refusing to start NeDB to prevent dataloss");
      }
    }
  }
};


/**
 * Check if a directory exists and create it on the fly if it is not the case
 * cb is optional, signature: err
 */
Persistence.ensureDirectoryExists = function (dir, cb) {
  var callback = cb || function () {}
    ;

  storage.mkdirp(dir, function (err) { return callback(err); });
};


/**
 * Persist cached database
 * This serves as a compaction function since the cache always contains only the number of documents in the collection
 * while the data file is append-only so it may grow larger
 * @param {Function} cb Optional callback, signature: err
 */
Persistence.prototype.persistCachedDatabase = function (reopen, cb) {
  var callback = cb || function () {}
    , self = this
    ;

  if(typeof reopen === 'function'){
    callback = reopen
    reopen = true
  }

  if (this.inMemoryOnly) { return callback(null); }

  function stage2 (toAllocate){
    // path, offset, length, mode
    try {
      fallocateSync(tempFile, 0, toAllocate, 0x01);
    }catch(ex){}// ignore fallocate failures

    let stream = fs.createWriteStream(tempFile)

    let fd = null
    stream.on('open', function (_fd) {
      fd = _fd
    })

    self.db.forEach(function (doc) {
      stream.write(self.afterSerialization(doc) + '\n')
    });
    Object.keys(self.db.indexes).forEach(function (fieldName) {
      if (fieldName != "_id") {   // The special _id index is managed by datastore.js, the others need to be persisted
        stream.write(self.afterSerialization({ $$indexCreated: { fieldName: fieldName, unique: self.db.indexes[fieldName].unique, sparse: self.db.indexes[fieldName].sparse }}) + '\n')
      }
    });

    stream.end(function () {
      function after_sync(){
        storage.crashSafeRename(tempFile, self.filename, function (err) {
          if (err) { return callback(err); }

          function complete(err){
            if (err) { return callback(err); }

            self.db.emit('compaction.done');
            
            return callback(null);
          }

          if(reopen){
            self._open(complete)
          }else{
            if(self.fd){
              fs.close(self.fd, function(){complete()})
            }
          }
        });
      }

      if(fd){
        fs.fsync(fd, after_sync)
      }else{
        after_sync()
      }
    })
  }
  const tempFile = self.filename + '~'
  fs.stat(self.filename, function(stat, err){
    if(err) return stage2(32*1024)// default to 32k
    stage2(Math.max(32*1024, stat.size))
  })
  
};


/**
 * Queue a rewrite of the datafile
 */
Persistence.prototype.compactDatafile = function (cb) {
  var callback = cb || function () {}
  this.db.executor.push({ this: this, fn: this.persistCachedDatabase, arguments: [callback] });
};

/**
 * Set automatic compaction every interval ms
 * @param {Number} interval in milliseconds, with an enforced minimum of 5 seconds
 */
Persistence.prototype.setAutocompactionInterval = function (interval, minimumWritten = 0) {
  var self = this
    , minInterval = 5000
    , realInterval = Math.max(interval || 0, minInterval)
    ;

  this.stopAutocompaction();

  let currentCompactionTimer
  const doCompaction = () => {
    currentCompactionTimer = this.autocompactionIntervalId
    if(self.writtenCount >= minimumWritten){
      self.compactDatafile(()=>{
        if(currentCompactionTimer == this.autocompactionIntervalId) this.autocompactionIntervalId = setTimeout(doCompaction, realInterval);
      });
    }
  }

  this.autocompactionIntervalId = setTimeout(doCompaction, realInterval);
};


/**
 * Stop autocompaction (do nothing if autocompaction was not running)
 */
Persistence.prototype.stopAutocompaction = function () {
  if (this.autocompactionIntervalId) { 
    clearTimeout(this.autocompactionIntervalId);
    this.autocompactionIntervalId = null
   }
};


/**
 * Persist new state for the given newDocs (can be insertion, update or removal)
 * Use an append-only format
 * @param {Array} newDocs Can be empty if no doc was updated/removed
 * @param {Function} cb Optional, signature: err
 */
Persistence.prototype.persistNewState = function (newDocs, cb) {
  var self = this
    , toPersist = ''
    , callback = cb || function () {}
    ;

  // In-memory only datastore
  if (self.inMemoryOnly) { return callback(null); }

  newDocs.forEach(function (doc) {
    toPersist += self.afterSerialization(doc) + '\n';
    self.writtenCount ++
  });

  if (toPersist.length === 0) { return callback(null); }

  storage.appendFile(self.fd || self.filename, toPersist, 'utf8', function (err) {
    if(err) return cb(err)
    if(!self.fd) return callback()

    fs.fsync(self.fd, function(err){
      return callback(err);
    })
  });
};

Persistence.prototype.readFileAndParse = function (cb) {
  var self = this
  , count = 0
  , tdata = []
  , dataById = {}
  , indexes = {}
  , corruptItems = 0
  , reader

  try {
    var readStream = fs.createReadStream(this.filename, { fd: this.fd, start: 0, autoClose:false });
    reader = new LineTransform()
    readStream.pipe(reader);
  } catch (e) {
    return cb(null, { data: tdata, indexes: indexes })
  }

  reader.on('end', function(){
    // A bit lenient on corruption
    if (count > 0 && corruptItems / count > self.corruptAlertThreshold) {
      return cb(new Error("More than " + Math.floor(100 * self.corruptAlertThreshold) + "% of the data file is corrupt, the wrong beforeDeserialization hook may be used. Cautiously refusing to start NeDB to prevent dataloss"));
    }

    Object.keys(dataById).forEach(function (k) {
      tdata.push(dataById[k]);
    });

    cb(null, { data: tdata, indexes: indexes })
  })
  reader.on('error', function(err){
    return cb(err)
  })
  reader.on('data',function(line) {
    try {
      var doc = self.beforeDeserialization(line);
      if (doc._id) {
        if (doc.$$deleted === true) {
          delete dataById[doc._id];
        } else {
          dataById[doc._id] = doc;
        }
      } else if (doc.$$indexCreated && doc.$$indexCreated.fieldName != undefined) {
        indexes[doc.$$indexCreated.fieldName] = doc.$$indexCreated;
      } else if (typeof doc.$$indexRemoved === "string") {
        delete indexes[doc.$$indexRemoved];
      }
    } catch (e) {
      corruptItems += 1;
    }

    count++
  })
}


/**
 * From a database's raw data, return the corresponding
 * machine understandable collection
 */
Persistence.prototype.treatRawData = function (rawData) {
  var data = rawData.split('\n')
    , dataById = {}
    , tdata = []
    , i
    , indexes = {}
    , corruptItems = -1   // Last line of every data file is usually blank so not really corrupt
    ;

  for (i = 0; i < data.length; i ++) {
    var doc;

    try {
      doc = this.beforeDeserialization(data[i]);
      if (doc._id) {
        if (doc.$$deleted === true) {
          delete dataById[doc._id];
        } else {
          dataById[doc._id] = doc;
        }
      } else if (doc.$$indexCreated && doc.$$indexCreated.fieldName != undefined) {
        indexes[doc.$$indexCreated.fieldName] = doc.$$indexCreated;
      } else if (typeof doc.$$indexRemoved === "string") {
        delete indexes[doc.$$indexRemoved];
      }
    } catch (e) {
      corruptItems += 1;
    }
  }

  // A bit lenient on corruption
  if (data.length > 0 && corruptItems / data.length > this.corruptAlertThreshold) {
    throw new Error("More than " + Math.floor(100 * this.corruptAlertThreshold) + "% of the data file is corrupt, the wrong beforeDeserialization hook may be used. Cautiously refusing to start NeDB to prevent dataloss");
  }

  Object.keys(dataById).forEach(function (k) {
    tdata.push(dataById[k]);
  });

  return { data: tdata, indexes: indexes };
};

Persistence.prototype._open = function(callback){
  const self = this
  async.waterfall([
    function (cb) {
      if(self.fd){
        fs.close(self.fd, function(){
          self.fd = null
          cb()
        })
      }else{
        cb()
      }
    },
    function(cb){
      fs.open(self.filename, "a+", function(err, fd){
        if(err) {
          return cb(err)
        }
        self.fd = fd
        return cb()
      })
    }], callback)
}


/**
 * Load the database
 * 1) Create all indexes
 * 2) Insert all data
 * 3) Compact the database
 * This means pulling data out of the data file or creating it if it doesn't exist
 * Also, all data is persisted right away, which has the effect of compacting the database file
 * This operation is very quick at startup for a big collection (60ms for ~10k docs)
 * @param {Function} cb Optional callback, signature: err
 */
Persistence.prototype.loadDatabase = function (cb) {
  var callback = cb || function () {}
    , self = this
    ;

  self.db.resetIndexes();

  // In-memory only datastore
  if (self.inMemoryOnly) { return callback(null); }

  async.waterfall([
    function (cb) {
      Persistence.ensureDirectoryExists(path.dirname(self.filename), function (err) {
        self._open(function(err){
          if (err) { return cb(err); }
          storage.ensureDatafileIntegrity(self.fd, function (err) {
            self.readFileAndParse(function (err, treatedData) {
              if (err) { return cb(err); }
  
              // Recreate all indexes in the datafile
              Object.keys(treatedData.indexes).forEach(function (key) {
                self.db.indexes[key] = new Index(treatedData.indexes[key]);
              });
  
              // Fill cached database (i.e. all indexes) with data
              try {
                self.db.resetIndexes(treatedData.data);
              } catch (e) {
                self.db.resetIndexes();   // Rollback any index which didn't fail
                return cb(e);
              }
  
              self.db.persistence.persistCachedDatabase(cb);
            });
          });
        })
      });
    }
  ], function (err) {
       if (err) { return callback(err); }

       self.db.executor.processBuffer();
       return callback(null);
     });
};

/**
 * 
 */
Persistence.prototype.closeDatabase = function (cb) {
  var callback = cb || function () {};
  
  if (this.inMemoryOnly) { return callback(null); }
  
  this.persistCachedDatabase(false, cb);
};

// Interface
module.exports = Persistence;
