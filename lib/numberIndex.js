const Index = require('./index')

class NumberIndex extends Index {
    constructor(options){
        super(options, null)

        //NOTE: this.unique === 'strict' doesnt matter as we are ensuring that
        //      keys are only of type number with our keyFn

        if(options.type === 'int') this.keyFn = a=>parseInt(a)
        else if (options.type === 'float' || options.type === 'number') this.keyFn = a=>parseFloat(a)
        else throw new Error(`Unknown data type ${options.type}`)
    }
}

NumberIndex.interested = {
    int: true,
    float: true,
    number: true
}
module.exports = NumberIndex