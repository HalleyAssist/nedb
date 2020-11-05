var model = require('../model')

/**
 * Create a new index
 * All methods on an index guarantee that either the whole operation was successful and the index changed
 * or the operation was unsuccessful and an error is thrown while the index is unchanged
 * @param {String} options.fieldName On which field should the index apply (can use dot notation to index on sub fields)
 * @param {Boolean} options.unique Optional, enforce a unique constraint (default: false)
 * @param {Boolean} options.sparse Optional, allow a sparse index (we can have documents for which fieldName is undefined) (default: false)
 */
function HashIndex (options) {
  this.fieldName = options.fieldName;
  this.unique = options.unique || false;
  this.sparse = options.sparse || false;
  this.data = new Map()
  this._extractKey = model.getDotFn(this.fieldName);
}


/**
 * Reset an index
 * @param {Document or Array of documents} newData Optional, data to initialize the index with
 *                                                 If an error is thrown during insertion, the index is not modified
 */
HashIndex.prototype.reset = function (newData) {
  this.data.clear()

  if(newData){
    if (Array.isArray(newData)) { this.insertMultipleDocs(newData); }
    else this.insert(newData);
  }
};

HashIndex.prototype.initialize = function (newData){
  this.data = newData
}
/**
 * Insert a new document in the index
 * If an array is passed, we insert all its elements (if one insertion fails the index is not modified)
 * O(log(n))
 */
HashIndex.prototype.insert = function (doc) {
  const key = this._extractKey(doc);

  // We don't index documents that don't contain the field if the index is sparse
  if (key === undefined && this.sparse) { return; }

  if (!Array.isArray(key)) {
    if(this.data.has(key)) {
      const err = new Error(`duplicate key ${key}`)
      err.key = key;
      err.errorType = 'uniqueViolated';
      throw err
    }
    this.data.set(key, doc)
  } else {
    // If an insert fails due to a unique constraint, roll back all inserts before it
    const keys = key
    let failingI, error

    for (let i = 0; i < keys.length; i ++) {
      const key = keys[i]
      if(this.data.has(key)) {
        
        error = new Error(`duplicate key ${key}`)
        error.key = key;
        error.errorType = 'uniqueViolated';

        failingI = i;
        break;
      }
      this.data.set(key, doc)
    }

    if (error) {
      for (let i = 0; i < failingI; i ++) {
        this.data.delete(keys[i])
      }

      throw error;
    }
  }
};


/**
 * Set a specific entry (insert or update)
 */
HashIndex.prototype.set = function(doc, key){
  this.data.set(key, doc)
}


/**
 * Insert an array of documents in the index
 * If a constraint is violated, the changes should be rolled back and an error thrown
 *
 * @API private
 */
HashIndex.prototype.insertMultipleDocs = function (docs) {
  var i, error, failingI;

  for (i = 0; i < docs.length; i ++) {
    try {
      this.insert(docs[i]);
    } catch (e) {
      error = e;
      failingI = i;
      break;
    }
  }

  if (error) {
    for (i = 0; i < failingI; i ++) {
      this.remove(docs[i]);
    }

    throw error;
  }
};


/**
 * Remove a document from the index
 * If an array is passed, we remove all its elements
 * The remove operation is safe with regards to the 'unique' constraint
 * O(log(n))
 */
HashIndex.prototype.remove = function (doc) {
  var key, self = this;

  if (Array.isArray(doc)) { doc.forEach(function (d) { self.remove(d); }); return; }

  key = this._extractKey(doc);

  if (key === undefined && this.sparse) { return; }

  if (!Array.isArray(key)) {
    this.data.delete(key)
  } else {
    for(const _key of key){
      this.data.delete(_key)
    }
  }
};

/**
 * Remove a document by it's key (fast)
 * Specific to HashIndex as it's implemented on top of an ES6 map
 * Returns the removed document
 */
HashIndex.prototype.removeByKey = function(key){
  const doc = this.data.get(key)
  if(doc) this.data.delete(key)
  return doc
}
/**
 * Remove a document by it's key (fastest)
 * Specific to HashIndex as it's implemented on top of an ES6 map
 */
HashIndex.prototype._removeByKey = function(key){
  this.data.delete(key)
}

/**
 * Update a document in the index
 * If a constraint is violated, changes are rolled back and an error thrown
 * Naive implementation, still in O(log(n))
 */
HashIndex.prototype.update = function (oldDoc, newDoc) {
  const oldKey = this._extractKey(oldDoc);
  const newKey = this._extractKey(newDoc);
  if(oldKey == newKey) this.data.set(oldKey, newDoc)
  else{
    this.remove(oldDoc);
    try {
      this.insert(newDoc);
    } catch (e) {
      this.insert(oldDoc);
      throw e;
    }
  }
};


/**
 * Update multiple documents in the index
 * If a constraint is violated, the changes need to be rolled back
 * and an error thrown
 * @param {Array of oldDoc, newDoc pairs} pairs
 *
 * @API private
 */
HashIndex.prototype.updateMultipleDocs = function (pairs) {
  var i, failingI, error;

  for (i = 0; i < pairs.length; i ++) {
    this.remove(pairs[i].oldDoc);
  }

  for (i = 0; i < pairs.length; i ++) {
    try {
      this.insert(pairs[i].newDoc);
    } catch (e) {
      error = e;
      failingI = i;
      break;
    }
  }

  // If an error was raised, roll back changes in the inverse order
  if (error) {
    for (i = 0; i < failingI; i ++) {
      this.remove(pairs[i].newDoc);
    }

    for (i = 0; i < pairs.length; i ++) {
      this.insert(pairs[i].oldDoc);
    }

    throw error;
  }
};


/**
 * Revert an update
 */
HashIndex.prototype.revertUpdate = function (oldDoc, newDoc) {
  var revert = [];

  if (!Array.isArray(oldDoc)) {
    this.update(newDoc, oldDoc);
  } else {
    oldDoc.forEach(function (pair) {
      revert.push({ oldDoc: pair.newDoc, newDoc: pair.oldDoc });
    });
    this.updateMultipleDocs(revert);
  }
};

HashIndex.prototype.getMatchingSingle = function(value){
  const ret = this.data.get(value)
  if(ret) return [ret]
  return []
}

/**
 * Get all documents in index whose key match value (if it is a Thing) or one of the elements of value (if it is an array of Things)
 * @param {Thing} value Value to match the key against
 * @return {Array of documents}
 */
HashIndex.prototype.getMatching = function (value) {
  if (!Array.isArray(value)) {
    return this.getMatchingSingle(value)
  } 
  
  const res = {}
  for(const v of value){
    for(const doc of this.getMatchingSingle(v)){
      res[doc._id] = doc;
    }
  }

  return Object.values(res)
};


/**
 * Get all documents in index whose key is between bounds are they are defined by query
 * Documents are sorted by key
 * @param {Query} query
 * @return {Array of documents}
 */
HashIndex.prototype.getBetweenBounds = function () {
  // can ignore query
  return null
};


/**
 * Get all elements in the index
 * @return {Array of documents}
 */
HashIndex.prototype.getAll = function () {
  return [...this.data.values()]
};

/**
 * Execute callback for each element in an index
 */
HashIndex.prototype.forEach = function (cb) {
  for(const [,value] of this.data){
    cb(value)
  }
}

HashIndex.prototype[Symbol.iterator] = function*(){
  yield *this.data.values()
}

// Interface
module.exports = HashIndex;