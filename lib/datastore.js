var customUtils = require('./customUtils')
  , model = require('./model')
  , AsyncWaterfall = require('async/waterfall')
  , Executor = require('./executor')
  , Index = require('./indexes/bstIndex')
  , NumberIndex = require('./indexes/numberIndex')
  , HashIndex = require('./indexes/hashIndex')
  , util = require('util')
  , Persistence = require('./storage/onDisk')
  , InMemory = require('./storage/inMemory')
  , Cursor = require('./cursor')
  ;

/**
 * Create a new collection
 * @param {String} options.filename Optional, datastore will be in-memory only if not provided
 * @param {Boolean} options.timestampData Optional, defaults to false. If set to true, createdAt and updatedAt will be created and populated automatically (if not specified by user)
 * @param {Boolean} options.inMemoryOnly Optional, defaults to false
 * @param {String} options.nodeWebkitAppName Optional, specify the name of your NW app if you want options.filename to be relative to the directory where
 *                                            Node Webkit stores application data such as cookies and local storage (the best place to store data in my opinion)
 * @param {Boolean} options.autoload Optional, defaults to false
 * @param {Function} options.onload Optional, if autoload is used this will be called after the load database with the error object as parameter. If you don't pass it the error will be thrown
 * @param {Function} options.afterSerialization/options.beforeDeserialization Optional, serialization hooks
 * @param {Number} options.corruptAlertThreshold Optional, threshold after which an alert is thrown if too much data is corrupt
 * @param {Function} options.compareStrings Optional, string comparison function that overrides default for sorting
 * @param {String|Function} options.ttlExtract Optional, threshold after which an alert is thrown if too much data is corrupt
 *
 * Event Emitter - Events
 * * compaction.done - Fired whenever a compaction operation was finished
 */
function Datastore (options) {
  var filename;

  // Retrocompatibility with v0.6 and before
  if (typeof options === 'string') {
    filename = options;
    this.inMemoryOnly = false;   // Default
  } else {
    options = options || {};
    filename = options.filename;
    this.inMemoryOnly = options.inMemoryOnly || false;
    this.autoload = options.autoload || false;
    this.timestampData = options.timestampData || false;
  }

  // Determine whether in memory or persistent
  if (!filename || typeof filename !== 'string' || filename.length === 0) {
    this.filename = null;
    this.inMemoryOnly = true;
  } else {
    this.filename = filename;
  }

  // String comparison function
  this.compareStrings = options.compareStrings;

  // Persistence handling
  if(this.inMemoryOnly){
    this.persistence = new InMemory(this)
  }else{
    this.persistence = new Persistence({ db: this, nodeWebkitAppName: options.nodeWebkitAppName
                                      , afterSerialization: options.afterSerialization
                                      , beforeDeserialization: options.beforeDeserialization
                                      , corruptAlertThreshold: options.corruptAlertThreshold
                                      });
  }

  // This new executor is ready if we don't use persistence
  // If we do, it will only be ready once loadDatabase is called
  this.executor = new Executor();
  if (this.inMemoryOnly) { this.executor.ready = true; }

  // Indexed by field name, dot notation can be used
  // _id is always indexed and since _ids are generated randomly the underlying
  // binary is always well-balanced
  this.indexes = {};
  this.indexes._id = new HashIndex({ fieldName: '_id', unique: true });
  if(options.ttlExtract){
    if(typeof options.ttlExtract === 'string'){
      const ttlExtract = model.getDotFn(options.ttlExtract)
      this.expired = function(doc, now){
        const ttl = ttlExtract(doc)
        return (ttl !== undefined && ttl < now)
      }
    }else{
      this.expired = options.ttlExtract
    }
  }else{
    this.expired = null
  }

  // Queue a load of the database right away and call the onload handler
  // By default (no onload handler), if there is an error there, no operation will be possible so warn the user by throwing an exception
  if (this.autoload) { this.loadDatabase(options.onload || function (err) {
    if (err) { throw err; }
  }); }
}

util.inherits(Datastore, require('events').EventEmitter);


/**
 * Load the database from the datafile, and trigger the execution of buffered commands if any
 */
Datastore.prototype.loadDatabase = function () {
  this.executor.push({ this: this.persistence, fn: this.persistence.loadDatabase, arguments: arguments }, true);
};

/**
 * Close the database and its underlying datafile.
 */
Datastore.prototype.closeDatabase = function() {
  // Push the closeDatabase command onto the queue and pass the close flag to stop any further execution on the db.
  this.executor.push({ this: this.persistence, fn: this.persistence.closeDatabase, arguments: arguments}, true, true);
};

/**
 * Get an array of all the data in the database
 */
Datastore.prototype.getAllData = function () {
  return this.indexes._id.getAll();
};

Datastore.prototype.forEach = function(cb){
  return this.indexes._id.forEach(cb)
}

/**
 * Reset all currently defined indexes
 */
Datastore.prototype.resetIndexes = function (newData) {
  if(!newData){
    for(let i in this.indexes){
      this.indexes[i].reset(newData);
    }
    return
  }

  this.indexes._id.initialize(newData)

  let newArray
  for(let i in this.indexes){
    if(i === '_id') continue
    if(!newArray) newArray = Array.from(newData.values())
    this.indexes[i].reset(newArray);
  }
}

Datastore.prototype.createIndex = function(options){
  if(options.hash){
    return new HashIndex(options)
  }else if(!options.sparse && NumberIndex.interested[options.type]){
    return new NumberIndex(options)
  }else{
    return new Index(options)
  }
}

/**
 * Ensure an index is kept for this field. Same parameters as lib/indexes
 * For now this function is synchronous, we need to test how much time it takes
 * We use an async API for consistency with the rest of the code
 * @param {String} options.fieldName
 * @param {Boolean} options.unique
 * @param {Boolean} options.sparse
 * @param {Function} cb Optional callback, signature: err
 */
Datastore.prototype._ensureIndex = function (options, cb) {
  var err
    , callback = cb || function () {};

  options = options || {};

  if (!options.fieldName) {
    err = new Error("Cannot create an index without a fieldName");
    err.missingFieldName = true;
    return callback(err);
  }

  let index = this.createIndex(options)
  if (this.indexes[options.fieldName]) { 
    if(index.constructor.name === this.indexes[options.fieldName].constructor.name) return callback(null);
  }

  this.indexes[options.fieldName] = index

  try {
    this.indexes[options.fieldName].insertMultipleDocs(this.getAllData());
  } catch (e) {
    delete this.indexes[options.fieldName];
    return callback(e);
  }

  // We may want to force all options to be persisted including defaults, not just the ones passed the index creation function
  this.persistence.persistNewState([{ $$indexCreated: options }], true, function (err) {
    if (err) { return callback(err); }
    return callback(null);
  });
};
Datastore.prototype.ensureIndex = function () {
  this.executor.push({ this: this, fn: this._ensureIndex, arguments });
};


/**
 * Remove an index
 * @param {String} fieldName
 * @param {Function} cb Optional callback, signature: err
 */
Datastore.prototype.removeIndex = function (fieldName, cb) {
  var callback = cb || function () {};

  if(!this.indexes[fieldName]){
    return cb(null)
  }
  delete this.indexes[fieldName];

  this.persistence.persistNewState([{ $$indexRemoved: fieldName }], true, function (err) {
    if (err) { return callback(err); }
    return callback(null);
  });
};


/**
 * Add one or several document(s) to all indexes
 */
Datastore.prototype.addToIndexes = function (doc) {
  var i, failingIndex, error
    , keys = Object.keys(this.indexes)
    ;

  for (i = 0; i < keys.length; i ++) {
    try {
      this.indexes[keys[i]].insert(doc);
    } catch (e) {
      failingIndex = i;
      error = e;
      break;
    }
  }

  // If an error happened, we need to rollback the insert on all other indexes
  if (error) {
    for (i = 0; i < failingIndex; i ++) {
      this.indexes[keys[i]].remove(doc);
    }

    throw error;
  }
};


/**
 * Remove one or several document(s) from all indexes
 */
Datastore.prototype.removeFromIndexes = function (doc) {
  for(const i in this.indexes){
    if(i === '_id') this.indexes._id._removeByKey(doc._id)
    else this.indexes[i].remove(doc)
  }
};


/**
 * Update one or several documents in all indexes
 * To update multiple documents, oldDoc must be an array of { oldDoc, newDoc } pairs
 * If one update violates a constraint, all changes are rolled back
 */
Datastore.prototype.updateIndexes = function (oldDoc, newDoc) {
  var i, failingIndex, error
    , keys = Object.keys(this.indexes),
      multiple = Array.isArray(oldDoc)
    ;

  for (i = 0; i < keys.length; i ++) {
    const idx = this.indexes[keys[i]]
    try {
      if(multiple){
        idx.updateMultipleDocs(oldDoc)
      }else{
        idx.update(oldDoc, newDoc);
      }
    } catch (e) {
      failingIndex = i;
      error = e;
      break;
    }
  }

  // If an error happened, we need to rollback the update on all other indexes
  if (error) {
    for (i = 0; i < failingIndex; i ++) {
      this.indexes[keys[i]].revertUpdate(oldDoc, newDoc);
    }

    throw error;
  }
};

/* eslint-disable no-prototype-builtins */
Datastore.prototype.getIndex = function(query){
  for(const k in query){
    const q = query[k]
    if (typeof q === 'string' || typeof q === 'number' || typeof q === 'boolean' || q instanceof Date || q === null) {
      const idx = this.indexes[k]
      if(idx){
        delete query[k]
        return idx.getMatching(q)
      }
    }
  }
  for(const k in query){
    const q = query[k]
    if(!q) continue
    if (q.hasOwnProperty('$in')) {
      const idx = this.indexes[k]
      if(idx){
        delete query[k]
        return idx.getMatching(q.$in)
      }
    }
  }
  for(const k in query){
    const q = query[k]
    if(!q) continue
    if (q.hasOwnProperty('$lt') || q.hasOwnProperty('$lte') || q.hasOwnProperty('$gt') || q.hasOwnProperty('$gte')) {
      const idx = this.indexes[k]
      if(idx){
        const ret = idx.getBetweenBounds(q)
        if(ret){        
          delete query[k]
          return ret
        }
      }
    }
  }

  return this.indexes._id
}
/* eslint-enable no-prototype-builtins */

/**
 * Return the list of candidates for a given query
 * Crude implementation for now, we return the candidates given by the first usable index if any
 * We try the following query types, in this order: basic match, $in match, comparison match
 * One way to make it better would be to enable the use of multiple indexes if the first usable index
 * returns too much data. I may do it in the future.
 *
 * Returned candidates will be scanned to find and remove all expired documents
 *
 * @param {Query} query
 * @param {Boolean} dontExpireStaleDocs Optional, defaults to false, if true don't remove stale docs. Useful for the remove function which shouldn't be impacted by expirations
 * @param {Function} callback Signature err, candidates
 */
Datastore.prototype.getCandidates = function (query, dontExpireStaleDocs, callback) {
  if (typeof dontExpireStaleDocs === 'function') {
    callback = dontExpireStaleDocs;
    dontExpireStaleDocs = false;
  }

  if(query['$and']){
    var newQuery = {}
    var queryParts = query['$and']
    for(var part of queryParts){
      var key = Object.keys(part)[0]
      newQuery[key] = part[key]
    }
    query = newQuery
  }

  // For a basic match
  let docs
  try {
    docs = this.getIndex(query)
  } catch (ex) {
    return callback(ex)
  }
  callback(null, docs)
};


/**
 * Insert a new document
 * @param {Function} cb Optional callback, signature: err, insertedDoc
 *
 * @api private Use Datastore.insert which has the same signature
 */
Datastore.prototype._insert = function (newDoc, deepCopy, cb) {
  var callback = cb || function () {}
    , preparedDoc
    ;

  try {
    preparedDoc = this.prepareDocumentForInsertion(newDoc, deepCopy)
    this._insertInCache(preparedDoc);
  } catch (e) {
    return callback(e);
  }

  newDoc = Array.isArray(preparedDoc) ? preparedDoc : [preparedDoc]
  this.persistence.persistNewState(newDoc, false, function (err) {
    if (err) { return callback(err); }
    return callback(null, newDoc);
  });
};

/**
 * Create a new _id that's not already in use
 */
Datastore.prototype.createNewId = function () {
  let tentativeId
  const idIndex = this.indexes._id
  do {
    tentativeId = customUtils.uid(16);
  } while(idIndex.getMatchingSingle(tentativeId).length)
  
  return tentativeId;
};

/**
 * Prepare a document (or array of documents) to be inserted in a database
 * Meaning adds _id and timestamps if necessary on a copy of newDoc to avoid any side effect on user input
 * @api private
 */
Datastore.prototype.prepareDocumentForInsertion = function (newDoc, deepCopy = true) {
  var preparedDoc

  if (Array.isArray(newDoc)) {
    preparedDoc = [];
    for(const doc of newDoc){
      preparedDoc.push(this.prepareDocumentForInsertion(doc, deepCopy));
    }
  } else {
    preparedDoc = deepCopy ? model.deepCopy(newDoc) : newDoc;
    if (preparedDoc._id === undefined) preparedDoc._id = this.createNewId();
    if(this.timestampData){
      const now = new Date();
      if (preparedDoc.createdAt === undefined) preparedDoc.createdAt = now;
      if (preparedDoc.updatedAt === undefined) preparedDoc.updatedAt = now;
    }
    model.checkObject(preparedDoc);
  }

  return preparedDoc;
};

/**
 * If newDoc is an array of documents, this will insert all documents in the cache
 * @api private
 */
Datastore.prototype._insertInCache = function (preparedDoc) {
  if (Array.isArray(preparedDoc)) {
    this._insertMultipleDocsInCache(preparedDoc);
  } else {
    this.addToIndexes(preparedDoc);
  }
};

/**
 * If one insertion fails (e.g. because of a unique constraint), roll back all previous
 * inserts and throws the error
 * @api private
 */
Datastore.prototype._insertMultipleDocsInCache = function (preparedDocs) {
  var i, failingI, error;

  for (i = 0; i < preparedDocs.length; i ++) {
    try {
      this.addToIndexes(preparedDocs[i]);
    } catch (e) {
      error = e;
      failingI = i;
      break;
    }
  }

  if (error) {
    for (i = 0; i < failingI; i ++) {
      this.removeFromIndexes(preparedDocs[i]);
    }

    throw error;
  }
};

Datastore.prototype.insertUnsafe = function (document, cb) {
  this.executor.push({ this: this, fn: this._insert, arguments: [document, true, cb] });
};


Datastore.prototype.insertUnsafest = function (document, cb) {
  this.executor.push({ this: this, fn: this._insert, arguments: [document, true, cb] });
};

Datastore.prototype.insert = function () {
  this.executor.push({ this: this, fn: function(newDoc, cb){
    this._insert(newDoc, true, function(err, refDocs){
        if(err) return cb(err)
        let docs = new Array(refDocs.length)
        for(let i=refDocs.length;i--;){
          docs[i] = model.deepCopy(refDocs[i])
        }
        cb(null, docs)
    })
  }, arguments: arguments });
};


/**
 * Count all documents matching the query
 * @param {Object} query MongoDB-style query
 */
Datastore.prototype.count = function(query, callback) {
  var cursor = new Cursor(this, query, function(err, docs, callback) {
    if (err) { return callback(err); }
    return callback(null, docs.length);
  });

  if (typeof callback === 'function') {
    cursor.exec(callback);
  } else {
    return cursor;
  }
};


/**
 * Find all documents matching the query
 * If no callback is passed, we return the cursor so that user can limit, skip and finally exec
 * @param {Object} query MongoDB-style query
 * @param {Object} projection MongoDB-style projection
 */
Datastore.prototype.findUnsafe = function (query, projection, callback) {
  if (arguments.length == 2 && typeof projection === 'function') {
    callback = projection;
    projection = undefined;
  }

  const cursor = new Cursor(this, query, callback);

  if(projection) cursor.projection(projection);
  if (typeof callback !== 'function') return cursor;
  cursor.exec(callback)
};
Datastore.prototype.find = function (query, projection, callback) {
  if (arguments.length == 2 && typeof projection === 'function') {
    callback = projection;
    projection = undefined;
  }

  const cursor = new Cursor(this, query, function(err, docs, callback) {
    if (err) { return callback(err); }

    let res = new Array(docs.length);
    for (let i = docs.length; i--;) {
      res[i] = model.deepCopy(docs[i]);
    }
    return callback(null, res);
  });

  if(projection) cursor.projection(projection);
  if (typeof callback !== 'function') return cursor;
  cursor.exec(callback)
}


/**
 * Find one document matching the query
 * @param {Object} query MongoDB-style query
 * @param {Object} options options, including: MongoDB-style projection
 */
Datastore.prototype.findOne = function (query, options, callback) {
  var cursor = new Cursor(this, query, function(err, docs, callback) {
    if (err) { return callback(err); }
    if (docs.length === 0) return callback(null, null);
    let d = docs[0]
    if(!options || !options.unsafe) d = model.deepCopy(d)
    return callback(null, d)
  });

  
  if (arguments.length == 2 && typeof options === 'function') {
    callback = options;
  }
  if(typeof options === 'object'){
    if(options.projection) cursor.projection(options.projection)
    if(options.stale) cursor.stale(options.stale)
  }

  cursor.limit(1);
  if (typeof callback === 'function') {
    cursor.exec(callback);
  } else {
    return cursor;
  }
};



Datastore.prototype._upsert = function (query, updateQuery, options, cb) {
  if (typeof options === 'function') { cb = options; options = {}; }
  cb = cb || function () {};
  const returnValue = options.return !== false


  const upsertQuery = Object.assign({}, query)
  let toBeInserted;
  try {
    model.checkObject(updateQuery);
    // updateQuery is a simple object with no modifier, use it as the document to insert
    toBeInserted = model.deepCopy(updateQuery);
  } catch (e) {
    toBeInserted = null
  }

  // Need to use an internal function not tied to the executor to avoid deadlock
  var cursor = new Cursor(this, upsertQuery);
  cursor.limit(1)._exec((err, docs) => {
    if (err) { return callback(err); }
    if (docs.length === 1) {
      if(!toBeInserted){
        // updateQuery contains modifiers, use the find query as the base,
        // strip it from all operators and update it according to updateQuery
        try {
          toBeInserted = model.modify(model.deepCopyStrictKeys(docs[0]), updateQuery);
        } catch (err) {
          return cb(err);
        }
      }
      
      this.updateIndexes(docs[0], toBeInserted)

      // Update the datafile
      return this.persistence.persistNewState([toBeInserted], true, function (err) {
        if (err) { return cb(err); }
        if(returnValue){
          cb(null, [model.deepCopy(toBeInserted)])
        }else{
          cb(null, 1)
        }
      });
    } 

    if(!toBeInserted){
      // updateQuery contains modifiers, use the find query as the base,
      // strip it from all operators and update it according to updateQuery
      try {
        toBeInserted = model.modify(model.deepCopyStrictKeys(upsertQuery), updateQuery);
      } catch (err) {
        return cb(err);
      }
    }


    // deep copy not needed, one has already been made above
    return this._insert(toBeInserted, false, function (err, newDoc) {
      if (err) { return cb(err); }
      return cb(null, 1, returnValue?model.deepCopy(newDoc):newDoc.length, true);
    });
  });
}
Datastore.prototype.upsert = function () {
  this.executor.push({ this: this, fn: this._upsert, arguments: arguments });
};


/**
 * Update all docs matching query
 * @param {Object} query
 * @param {Object} updateQuery
 * @param {Object} options Optional options
 *                 options.multi If true, can update multiple documents (defaults to false)
 *                 options.returnUpdatedDocs Defaults to false, if true return as third argument the array of updated matched documents (even if no change actually took place)
 * @param {Function} cb Optional callback, signature: (err, numAffected, affectedDocuments, upsert)
 *                      If update was an upsert, upsert flag is set to true
 *                      affectedDocuments can be one of the following:
 *                        * For an update with returnUpdatedDocs option false, null
 *                        * For an update with returnUpdatedDocs true and multi false, the updated document
 *                        * For an update with returnUpdatedDocs true and multi true, the array of updated documents
 *
 * WARNING: The API was changed between v1.7.4 and v1.8, for consistency and readability reasons. Prior and including to v1.7.4,
 *          the callback signature was (err, numAffected, updated) where updated was the updated document in case of an upsert
 *          or the array of updated documents for an update if the returnUpdatedDocs option was true. That meant that the type of
 *          affectedDocuments in a non multi update depended on whether there was an upsert or not, leaving only two ways for the
 *          user to check whether an upsert had occured: checking the type of affectedDocuments or running another find query on
 *          the whole dataset to check its size. Both options being ugly, the breaking change was necessary.
 *
 * @api private Use Datastore.update which has the same signature
 */
Datastore.prototype._update = function (query, updateQuery, options, cb) {
  var callback
    , numReplaced = 0
    , multi
    ;

  if (typeof options === 'function') { cb = options; options = {}; }
  callback = cb || function () {};
  multi = options.multi !== undefined ? options.multi : false;
  const returnValue = options.return !== false

  var modifiedDoc , modifications = [], updatedDocs = [], createdAt;

  this.getCandidates(query, (err, candidates) => {
    if (err) { return callback(err); }

    // Preparing update (if an error is thrown here neither the datafile nor
    // the in-memory indexes are affected)
    try {
      const match = model.prepare(query)
      for(const doc of candidates){
        if (!match(doc) || !(multi || numReplaced === 0)) continue
        numReplaced += 1;
        if (this.timestampData) { createdAt = doc.createdAt; }
        modifiedDoc = model.modify(doc, updateQuery);
        if (this.timestampData) {
          modifiedDoc.createdAt = createdAt;
          modifiedDoc.updatedAt = new Date();
        }
        modifications.push({ oldDoc: doc, newDoc: modifiedDoc });
        updatedDocs.push(modifiedDoc)
      }
    } catch (err) {
      return callback(err);
    }

    // Change the docs in memory
    try {
      this.updateIndexes(modifications);
    } catch (err) {
      return callback(err);
    }

    // Update the datafile
    this.persistence.persistNewState(updatedDocs, true, function (err) {
      if (err) { return callback(err); }
      if (!options.returnUpdatedDocs || !returnValue) {
        return callback(null, numReplaced);
      } 
      for(let i=0;i<updatedDocs.length;i++){
        updatedDocs[i] = model.deepCopy(updatedDocs[i])
      }
      if (! multi) { updatedDocs = updatedDocs[0]; }
      return callback(null, numReplaced, updatedDocs);
    });
  });
};

Datastore.prototype.update = function () {
  this.executor.push({ this: this, fn: this._update, arguments: arguments });
};

Datastore.prototype._removeFn = function(match, cb) {
  // Uses HashIndex internals
  let removedDocs = []
  for(const [key, doc] of this.indexes._id.data){
    if(!match(doc)) continue
    this.indexes._id._removeByKey(key)
    for(const indexName in this.indexes){
      if(indexName==='_id') continue
      this.indexes[indexName].remove(doc)
    }
    if(!this.inMemoryOnly) removedDocs.push(key);
  }

  if(removedDocs.length){
    this.persistence.persistNewState([{ $$deleted: removedDocs}], true, function (err) {
      return cb(err, true);
    });
  }else{
    cb(null, true)
  }
}


/**
 * Remove all docs matching the query
 * For now very naive implementation (similar to update)
 * @param {Object} query
 * @param {Object} options Optional options
 *                 options.multi If true, can update multiple documents (defaults to false)
 * @param {Function} cb Optional callback, signature: err, numRemoved
 *
 * @api private Use Datastore.remove which has the same signature
 */
Datastore.prototype._remove = function (query, options, cb) {
  if (typeof options === 'function') { cb = options; options = {}; }
  const callback = cb || function () {};

  if(typeof query === 'function'){
    return this._removeFn(query, callback)
  }
  const multi = options.multi !== undefined ? options.multi : false;

  this.getCandidates(query, true, (err, candidates) => {
    if (err) { return callback(err); }

    const removedDocs = []
    try {
      const match = model.prepare(query)
      for(const doc of candidates){
        if (!match(doc) || (!multi && removedDocs.length !== 0)) continue
        removedDocs.push(doc._id);
        this.removeFromIndexes(doc);
      }
    } catch (err) { return callback(err); }

    if(removedDocs.length){
      this.persistence.persistNewState([{ $$deleted: removedDocs}], true, function (err) {
        if (err) { return callback(err); }
        return callback(null, removedDocs.length);
      });
    }else{
      return callback(null, removedDocs.length);
    }
  });
};

Datastore.prototype.remove = function () {
  this.executor.push({ this: this, fn: this._remove, arguments: arguments });
};

Datastore.prototype.cleanupExpired = function(cb){
  if(!this.expired) return cb(false)
  const now = Date.now()
  this.executor.push({ this: this, fn: this._remove, arguments: [(doc)=>this.expired(doc, now), cb] });
}

Datastore.prototype._removeById = function (id) {
  const doc = this.indexes._id.removeByKey(id)
  if(doc){
    for(const indexName in this.indexes){
      if(indexName==='_id') continue
      this.indexes[indexName].remove(doc)
    }
    return doc
  }
}

Datastore.prototype._persistRemoval = function(removedDocs, cb){
  cb = cb || function () {};
  if(!removedDocs.length) return cb(null, removedDocs.length);

  this.persistence.persistNewState([{ $$deleted: removedDocs}], true, function (err) {
    if (err) { return cb(err); }
    return cb(null, removedDocs.length);
  });
}

module.exports = Datastore;