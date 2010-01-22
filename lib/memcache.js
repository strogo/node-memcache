var impl = require('memcache-impl'),
    sys = require('sys');

var Connection = impl.Connection;

//Connection.prototype.get = function(key) {
//  this.get(key);
//}

exports.createConnection = function(conninfo) {
  var c = new Connection;

  sys.puts(JSON.stringify(conninfo));
  c.addServer(conninfo.server, conninfo.port);
  c.addListener('ready', function() {
    sys.puts('here');
  });

  return c;
};
