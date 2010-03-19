var path = require('path');

var testDir = path.dirname(__filename);
require.paths.unshift(path.join(testDir, '../build/default'));
require.paths.unshift(path.join(testDir, '../lib'));

var sys = require('sys'),
    memcache = require('memcache'),
    assert = require('assert');

var c = new memcache.Connection;
c.addServer('localhost', 7788);
c.get('test', function(err, result) {
  if (err) {
    sys.puts('error');
    c.set('test', '123', 10, function(err) {
      if (err) {
        sys.puts('error to set');
      }
      c.incr('test', 2, function(err, result) {
	if (err) {
          sys.puts('error to incr');
	} else {
	  sys.puts('result: ' + result);
	}
      });
    });
  } else {
    sys.puts(result);
  }
});
