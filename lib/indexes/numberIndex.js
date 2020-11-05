const Index = require('./bstIndex')

/*
Special index type for a single (non array) number
*/
class NumberIndex extends Index {
    constructor(options){
        super(options, null)

        //NOTE: this.unique === 'strict' doesnt matter as we are ensuring that
        //      keys are only of type number with our keyFn

        if(options.type === 'int') this.keyFn = a=>parseInt(a)
        else if (options.type === 'float' || options.type === 'number') this.keyFn = a=>parseFloat(a)
        else throw new Error(`Unknown data type ${options.type}`)
    }

    /**
     * Update a document in the index
     * If a constraint is violated, changes are rolled back and an error thrown
     * 
     */
    update (oldDoc, newDoc) {
        const oldKey = this.keyFn(this._extractKey(oldDoc));
        const newKey = this.keyFn(this._extractKey(newDoc));


        if (Array.isArray(oldKey) || Array.isArray(newKey)) {
            throw new Error('no indexing of arrays')
        }

        // fast pa
        if(oldKey === newKey){
            // can swap documents
            const nodeResult = this.tree.search(oldKey)
            if(!nodeResult) throw new Error('oldDoc not found')
            for(let i=nodeResult.length;i--;){
                if(nodeResult[i] === oldDoc) {
                    nodeResult[i] = newDoc
                    return
                }
            }
            throw new Error('no document found')
        }

        // Naive implementation, still in O(log(n))
        this._remove(oldKey, oldDoc);
        try {
            this._insert(newKey, newDoc);
        } catch (e) {
            this._insert(oldKey, oldDoc);
            throw e;
        }
    }

    _insert(key, doc){
        this.tree.insert(key, doc)
    }
    insert (doc) {
        let key = this._extractKey(doc);
      
        // We don't index documents that don't contain the field if the index is sparse
        if (Array.isArray(key)) throw new Error('no indexing of arrays')
        key = this.keyFn(key)
        if (isNaN(key)) return
        

        this._insert(key, doc)
    }

    _remove(key, doc){
        this.tree.delete(key, doc);
    }
    remove (doc) {
        let key = this._extractKey(doc)
      
        if (Array.isArray(key)) throw new Error('no indexing of arrays')
        key = this.keyFn(key)
        if (isNaN(key)) return
        
        this._remove(key, doc)
    }
    //todo: fast upsert
  
}

NumberIndex.interested = {
    int: true,
    float: true,
    number: true
}
module.exports = NumberIndex