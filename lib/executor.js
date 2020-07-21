/**
 * Responsible for sequentially executing actions on the database
 */

var [AsyncQueue] = [require('async/queue')]

function Executor () {
  this.buffer = [];
  this.ready = false;
  this.open = true;

  // This queue will execute all commands, one-by-one in order
  this.queue = AsyncQueue(function (task, cb) {
    // task.arguments is an array-like object on which adding a new field doesn't work, so we transform it into a real array
    var newArguments = [...task.arguments];
    var lastArg = task.arguments[task.arguments.length - 1];

    // Always tell the queue task is complete. Execute callback if any was given.
    if (typeof lastArg === 'function') {
      // Callback was supplied
      newArguments[newArguments.length - 1] = function () {
        setImmediate(cb)
        try {
          lastArg.apply(null, arguments);
        } catch(ex){
          process.emit('uncaughtException', ex)
        }
      };
    } else if (!lastArg && task.arguments.length !== 0) {
      // false/undefined/null supplied as callbback
      newArguments[newArguments.length - 1] = function () { cb(); };
    } else {
      // Nothing supplied as callback
      newArguments.push(function () { cb(); });
    }

    task.fn.apply(task.this, newArguments);
  }, 1);
}


/**
 * If executor is ready, queue task (and process it immediately if executor was idle)
 * If not, buffer task for later processing
 * @param {Object} task
 *                 task.this - Object to use as this
 *                 task.fn - Function to execute
 *                 task.arguments - Array of arguments, IMPORTANT: only the last argument may be a function (the callback)
 *                                                                 and the last argument cannot be false/undefined/null
 * @param {Boolean} forceQueuing Optional (defaults to false) force executor to queue task even if it is not ready
 * @param {Boolean} close Optional (defaults to false) stop further tasks from being pushed to the queue or buffer
 */
Executor.prototype.push = function (task, forceQueuing, close) {
  if (close === true) {
    // stop further tasks from being added.
    this.open = false;
    this.processBuffer();
    this.queue.push(task);
  } else {
    if (this.open) {
      this._push(task, forceQueuing);
    } else {
      // return an error if a callback exists, otherwise throw an exception
      var err = new Error("Attempting operation on closed database.");
      var args = task.arguments;
      if (args.length > 0) {
        var cb = args[args.length-1];
        if (typeof(cb) === 'function') {
          return cb(err);
        }
      }
      throw err;
    }
  }
};

Executor.prototype._push = function (task, forceQueuing) {
  if (this.ready || forceQueuing) {
    this.queue.push(task);
  } else {
    this.buffer.push(task);
  }
};

/**
 * Queue all tasks in buffer (in the same order they came in)
 * Automatically sets executor as ready
 */
Executor.prototype.processBuffer = function () {
  var i;
  this.ready = true;
  for (i = 0; i < this.buffer.length; i ++) { this.queue.push(this.buffer[i]); }
  this.buffer = [];
};

// Interface
module.exports = Executor;
