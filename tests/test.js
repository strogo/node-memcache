var path = require('path');

var testDir = path.dirname(__filename);
require.paths.unshift(path.join(testDir, '../build/default'));
require.paths.unshift(path.join(testDir, '../lib'));

var sys = require('sys'),
    memcache = require('memcache'),
    assert = require('assert');

t = memcache.createConnection({server: '127.0.0.1', port: 7788});
t.get('test', function(err, data) {
  if (err) {
    sys.puts('error');
  } else {
    sys.puts('success');
  }
});

sys.puts(t.distribution);
t.distribution = memcache.Dist_Type.RANDOM;
sys.puts(t.distribution);
t.set('test', '123', 20, function(err, data) {
  t.get('test', function(err1, data1) {
    sys.puts(data1);
    t.incr('test', 3, function(err2, data2) {
      sys.puts('after incr: ' + data2);
    });
  });
});
