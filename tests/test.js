var path = require('path');

var testDir = path.dirname(__filename);
require.paths.unshift(path.join(testDir, '../build/default'));
require.paths.unshift(path.join(testDir, '../lib'));

var sys = require('sys'),
    memcache = require('memcache'),
    assert = require('assert');

t = memcache.createConnection({server: '127.0.0.1', port: 7788});
t.get('test').addErrback(function(data) {
  sys.puts('error');
}).addCallback(function(data) {
  sys.puts('success');
});;
t.set('test', 'abcd123', 20).addCallback(function(data) {
  t.get('test').addCallback(function(data) {
	  sys.puts(data);
});});
