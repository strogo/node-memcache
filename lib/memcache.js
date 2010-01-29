var impl = require('memcache-impl'),
    sys = require('sys'),
    events = require('events');

var _type = {
  MEMC_GET: 0,
  MEMC_SET: 1,
  MEMC_INCR: 2,
  MEMC_DECR: 3,
  MEMC_ADD: 4,
  MEMC_REPLACE: 5,
  MEMC_APPEND: 6,
  MEMC_PREPEND: 7,
  MEMC_CAS: 8,
  MEMC_REMOVE: 9,
  MEMC_FLUSH: 10
};

var Connection = impl.Connection;

Connection.prototype.maybeDispatch = function() {
  if (!this.queries) return;

  if (!this.currentQuery && this.queries.length > 0) {
    this.currentQuery = this.queries.shift();
    switch (this.currentQuery.type) {
      case _type.MEMC_GET:
        this._get(this.currentQuery.key);
	break;
      case _type.MEMC_SET:
	this._set(this.currentQuery.key, this.currentQuery.value, this.currentQuery.expiration);
	break;
      case _type.MEMC_INCR:
	this._incr(this.currentQuery.key, this.currentQuery.offset);
	break;
      case _type.MEMC_DECR:
	this._decr(this.currentQuery.key, this.currentQuery.offset);
	break;
      case _type.MEMC_ADD:
	this._add(this.currentQuery.key, this.currentQuery.value);
	break;
      case _type.MEMC_REPLACE:
	this._replace(this.currentQuery.key, this.currentQuery.value);
	break;
      case _type.MEMC_PREPEND:
	this._prepend(this.currentQuery.key, this.currentQuery.value);
	break;
      case _type.MEMC_APPEND:
	this._append(this.currentQuery.key, this.currentQuery.value);
	break;
      case _type.MEMC_CAS:
	this._cas(this.currentQuery.key, this.currentQuery.value, this.currentQuery.cas_arg);
	break;
      default:
	sys.puts('error');
    }
  }
};

Connection.prototype.get = function(key) {
  if (!this.queries) this.queries = [];
  var promise = new events.Promise();
  promise.type = _type.MEMC_GET;
  promise.key = key;
  this.queries.push(promise);
  this.maybeDispatch();
  return promise;
};

Connection.prototype.set = function(key, value, expiration) {
  if (!this.queries) this.queries = [];
  var promise = new events.Promise();
  promise.type = _type.MEMC_SET;
  promise.key = key;
  promise.value = value;
  promise.expiration = expiration;
  this.queries.push(promise);
  this.maybeDispatch();
  return promise;
};

Connection.prototype.incr = function(key, offset) {
  if (!this.queries) this.queries = [];
  var promise = new events.Promise();
  promise.type = _type.MEMC_INCR;
  promise.key = key;
  promise.offset = offset;
  this.queries.push(promise);
  this.maybeDispatch();
  return promise;
}

Connection.prototype.decr = function(key, offset) {
  if (!this.queries) this.queries = [];
  var promise = new events.Promise();
  promise.type = _type.MEMC_DECR;
  promise.key = key;
  promise.offset = offset;
  this.queries.push(promise);
  this.maybeDispatch();
  return promise;
}

Connection.prototype.add= function(key, value) {
  if (!this.queries) this.queries = [];
  var promise = new events.Promise();
  promise.type = _type.MEMC_ADD;
  promise.key = key;
  promise.value = value;
  this.queries.push(promise);
  this.maybeDispatch();
  return promise;
}

Connection.prototype.replace = function(key, value) {
  if (!this.queries) this.queries = [];
  var promise = new events.Promise();
  promise.type = _type.MEMC_REPLACE;
  promise.key = key;
  promise.value = value;
  this.queries.push(promise);
  this.maybeDispatch();
  return promise;
}

Connection.prototype.prepend = function(key, value) {
  if (!this.queries) this.queries = [];
  var promise = new events.Promise();
  promise.type = _type.MEMC_PREPEND;
  promise.key = key;
  promise.value = value;
  this.queries.push(promise);
  this.maybeDispatch();
  return promise;
}

Connection.prototype.append = function(key, value) {
  if (!this.queries) this.queries = [];
  var promise = new events.Promise();
  promise.type = _type.MEMC_APPEND;
  promise.key = key;
  promise.value = value;
  this.queries.push(promise);
  this.maybeDispatch();
  return promise;
}

Connection.prototype.cas = function(key, value, cas_arg) {
  if (!this.queries) this.queries = [];
  var promise = new events.Promise();
  promise.type = _type.MEMC_CAS;
  promise.key = key;
  promise.value = value;
  promise.cas_arg = cas_arg;
  this.queries.push(promise);
  this.maybeDispatch();
  return promise;
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

  c.addListener('result', function(data) {
    process.assert(c.currentQuery);
    var promise = c.currentQuery;
    c.currentQuery = null;
    if (data instanceof Error) {
      promise.emitError(data);
    } else {
      promise.emitSuccess(data);
    }
  });

  c.addServer(conninfo.server, conninfo.port);

  return c;
};
