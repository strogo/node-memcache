var sys = require("sys"),
    memcache = require("./build/default/memcache");

t = new memcache.Connection;
//t.setServers("127.0.0.1:7788");
sys.puts("addServer: " + t.addServer("127.0.0.1", 7788));
//sys.puts("removeServer: " + t.removeServer("127.0.0.1", 7788));
t.set("test", "abc", 20);
sys.puts(t.get('test'));
t.append('test', 'def');
sys.puts(t.get('test'));
t.prepend('test', '123');
sys.puts(t.get('test'));
t.remove('test', 0);
sys.puts(t.get('test'));
//sys.puts("removeServer: " + t.removeServer("127.0.0.1", 7788));
//memcache.test("test");
//memcache.test(function(aa) { sys.puts("test; blah blah"); sys.puts(aa);});
