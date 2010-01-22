var path = require('path');

var testDir = path.dirname(__filename);
require.paths.unshift(path.join(testDir, '../build/default'));
require.paths.unshift(path.join(testDir, '../lib'));

var sys = require('sys'),
    memcache = require('memcache'),
    assert = require('assert');

t = memcache.createConnection({server: '127.0.0.1', port: 7788});
//t.setServers("127.0.0.1:7788");
//sys.puts("addServer: " + t.addServer("127.0.0.1", 7788));
//t.set("test", "abc", 20);
//t.addListener("ready", function() {
//	sys.puts("here");
//});
//t.addListener("result", function(msg) {
//	sys.puts(msg);
//});
t.get('test');
//t.append('test', 'def');
//sys.puts(t.get('test'));
//t.prepend('test', '123');
//sys.puts(t.get('test'));
//t.remove('test', 0);
//sys.puts(t.get('test'));
//memcache.test("test");
//memcache.test(function(aa) { sys.puts("test; blah blah"); sys.puts(aa);});
