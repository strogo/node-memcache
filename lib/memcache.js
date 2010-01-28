var impl = require('memcache-impl'),
    sys = require('sys'),
    events = require('events');

var Connection = impl.Connection;

Connection.prototype.maybeDispatch = function() {
  if (!this.queries) return;

  if (!this.currentQuery && this.queries.length > 0) {
    this.currentQuery = this.queries.shift();
    switch (this.currentQuery.type) {
      case impl.MEMC_GET:
        this.rget(this.currentQuery.key);
	break;
      case impl.MEMC_SET:
	this.rset(this.currentQuery.key, this.currentQuery.value, this.currentQuery.expiration);
	break;
      default:
	sys.puts('error');
    }
  }
};

Connection.prototype.get = function(key) {
  if (!this.queries) this.queries = [];
  var promise = new events.Promise();
  promise.type = impl.MEMC_GET;
  promise.key = key;
  this.queries.push(promise);
  this.maybeDispatch();
  return promise;
};

Connection.prototype.set = function(key, value, expiration) {
  if (!this.queries) this.queries = [];
  var promise = new events.Promise();
  promise.type = impl.MEMC_SET;
  promise.key = key;
  promise.value = value;
  promise.expiration = expiration;
  this.queries.push(promise);
  this.maybeDispatch();
  return promise;
};

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
