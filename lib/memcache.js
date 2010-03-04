var impl = require('memcache-impl'),
    sys = require('sys'),
    events = require('events');

var Connection = impl.Connection;

exports.MODULA = impl.MEMCACHED_DISTRIBUTION_MODULA;
exports.CONSISTENT = impl.MEMCACHED_DISTRIBUTION_CONSISTENT;
exports.CONSISTENT_KATAMA = impl.MEMCACHED_DISTRIBUTION_CONSISTENT_KETAMA;
exports.RANDOM = impl.MEMCACHED_DISTRIBUTION_RANDOM;
exports.CONSISTENT_KATAMA_SPY = impl.MEMCACHED_DISTRIBUTION_CONSISTENT_KETAMA_SPY;
exports.CONSISTENT_MAX = impl.MEMCACHED_DISTRIBUTION_CONSISTENT_MAX;

Connection.prototype.maybeDispatch = function() {
  if (!this.queries) return;

  if (!this.currentQuery && this.queries.length > 0) {
    this.currentQuery = this.queries.shift();
    switch (this.currentQuery.type) {
      case impl.MEMC_GET:
        this._get(this.currentQuery.key);
        break;
      case impl.MEMC_SET:
        this._set(this.currentQuery.key, this.currentQuery.value, this.currentQuery.expiration);
        break;
      case impl.MEMC_INCR:
        this._incr(this.currentQuery.key, this.currentQuery.offset);
        break;
      case impl.MEMC_DECR:
        this._decr(this.currentQuery.key, this.currentQuery.offset);
        break;
      case impl.MEMC_ADD:
        this._add(this.currentQuery.key, this.currentQuery.value);
        break;
      case impl.MEMC_REPLACE:
        this._replace(this.currentQuery.key, this.currentQuery.value);
        break;
      case impl.MEMC_PREPEND:
	this._prepend(this.currentQuery.key, this.currentQuery.value);
	break;
      case impl.MEMC_APPEND:
        this._append(this.currentQuery.key, this.currentQuery.value);
        break;
      case impl.MEMC_CAS:
        this._cas(this.currentQuery.key, this.currentQuery.value, this.currentQuery.cas_arg);
        break;
      case impl.MEMC_REMOVE:
        this._remove(this.currentQueue.key, this.currentQuery.expiration);
        break;
      case impl.MEMC_FLUSH:
        this._flush(this.currentQuery.expiration);
        break;
      default:
        sys.puts('error');
    }
  }
};

Connection.prototype.get = function(key, cb) {
  if (!this.queries) this.queries = [];
  cb.type = impl.MEMC_GET;
  cb.key = key;
  this.queries.push(cb);
  this.maybeDispatch();
};

Connection.prototype.set = function(key, value, expiration, cb) {
  if (!this.queries) this.queries = [];
  cb.type = impl.MEMC_SET;
  cb.key = key;
  cb.value = value;
  cb.expiration = expiration;
  this.queries.push(cb);
  this.maybeDispatch();
};

Connection.prototype.incr = function(key, offset, cb) {
  if (!this.queries) this.queries = [];
  cb.type = impl.MEMC_INCR;
  cb.key = key;
  cb.offset = offset;
  this.queries.push(cb);
  this.maybeDispatch();
}

Connection.prototype.decr = function(key, offset, cb) {
  if (!this.queries) this.queries = [];
  cb.type = impl.MEMC_DECR;
  cb.key = key;
  cb.offset = offset;
  this.queries.push(cb);
  this.maybeDispatch();
}

Connection.prototype.add= function(key, value, cb) {
  if (!this.queries) this.queries = [];
  cb.type = impl.MEMC_ADD;
  cb.key = key;
  cb.value = value;
  this.queries.push(cb);
  this.maybeDispatch();
}

Connection.prototype.replace = function(key, value, cb) {
  if (!this.queries) this.queries = [];
  cb.type = impl.MEMC_REPLACE;
  cb.key = key;
  cb.value = value;
  this.queries.push(cb);
  this.maybeDispatch();
}

Connection.prototype.prepend = function(key, value, cb) {
  if (!this.queries) this.queries = [];
  cb.type = impl.MEMC_PREPEND;
  cb.key = key;
  cb.value = value;
  this.queries.push(cb);
  this.maybeDispatch();
}

Connection.prototype.append = function(key, value, cb) {
  if (!this.queries) this.queries = [];
  cb.type = impl.MEMC_APPEND;
  cb.key = key;
  cb.value = value;
  this.queries.push(cb);
  this.maybeDispatch();
}

Connection.prototype.cas = function(key, value, cas_arg, cb) {
  if (!this.queries) this.queries = [];
  cb.type = impl.MEMC_CAS;
  cb.key = key;
  cb.value = value;
  cb.cas_arg = cas_arg;
  this.queries.push(cb);
  this.maybeDispatch();
}

Connection.prototype.remove = function(key, expiration, cb) {
  if (!this.queries) this.queries = [];
  cb.type = impl.MEMC_REMOVE;
  cb.key = key;
  cb.expiration = expiration;
  this.queries.push(cb);
  this.maybeDispatch();
}

Connection.prototype.flush = function(expiration, cb) {
  if (!this.queries) this.queries = [];
  cb.type = impl.MEMC_FLUSH;
  cb.expiration = expiration;
  this.queries.push(cb);
  this.maybeDispatch();
}

exports.createConnection = function(conninfo) {
  var c = new Connection;

  c.addListener('connect', function() {
    c.maybeDispatch();
  });

  c.addListener('close', function() {
  });

  c.addListener('ready', function() {
    c.maybeDispatch();
  });

  c.addListener('result', function(result) {
    process.assert(c.currentQuery);
    var cb = c.currentQuery;
    c.currentQuery = null;
    if (result instanceof Error) {
      cb(result);
    } else {
      cb(null, result);
    }
  });

  c.addServer(conninfo.server, conninfo.port);

  return c;
};
