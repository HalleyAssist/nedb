class InMemory {
    constructor(db){
        this.db = db;
    }
    persistCachedDatabase (_reopen, cb){
        if(cb) cb(null)
    }

    persistNewState (_newDocs, _modification, cb) {
        if(cb) return cb(null);
    }
    
    loadDatabase (cb) {
      this.db.resetIndexes();
    
      // In-memory only datastore
      if(cb) return cb(null)
    }

    closeDatabase(cb){
        if(cb) cb(null);
    }
}
module.exports = InMemory