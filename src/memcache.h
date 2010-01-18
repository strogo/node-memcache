// Copyright 2010 Vanilla Hsu <vanilla@FreeBSD.org>

#include <v8.h>
#include <node.h>
#include <node_object_wrap.h>
#include <node_events.h>
#include <libmemcached/memcached.hpp>
#include <string>

#define DEBUGMODE 1
#define pdebug(...) do{if(DEBUGMODE)printf(__VA_ARGS__);}while(0)
#define THROW_BAD_ARGS \
  v8::ThrowException(v8::Exception::TypeError(v8::String::New("Bad arguments")))

using namespace v8;
using namespace memcache;
using namespace node;

class Connection : EventEmitter {
 public:
  static void Initialize(Handle<Object> target);
        
 protected:
  static Handle<Value> New(const Arguments &args);
  static Handle<Value> setServers(const Arguments &args);
  static Handle<Value> addServer(const Arguments &args);
  static Handle<Value> removeServer(const Arguments &args);
  static Handle<Value> get(const Arguments &args);
  static Handle<Value> set(const Arguments &args);
  static Handle<Value> incr(const Arguments &args);
  static Handle<Value> decr(const Arguments &args);
  static Handle<Value> add(const Arguments &args);
  static Handle<Value> replace(const Arguments &args);
  static Handle<Value> prepend(const Arguments &args);
  static Handle<Value> append(const Arguments &args);
  static Handle<Value> cas(const Arguments &args);
  static Handle<Value> remove(const Arguments &args);
  static Handle<Value> flush(const Arguments &args);

  Connection() : node::EventEmitter() {
    HandleScope scope;

    memc = new Memcache;
  }

  ~Connection() {

  }

 private:
  memcache::Memcache *memc;
};
