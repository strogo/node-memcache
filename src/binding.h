// Copyright 2010 Vanilla Hsu <vanilla@FreeBSD.org>

#include <v8.h>
#include <node.h>
#include <node_object_wrap.h>
#include <node_events.h>
#include <libmemcached/memcached.h>
#include <string>

#define DEBUGMODE 1
#define pdebug(...) do{if(DEBUGMODE)printf(__VA_ARGS__);}while(0)
#define THROW_BAD_ARGS \
  v8::ThrowException(v8::Exception::TypeError(v8::String::New("Bad arguments")))

using namespace v8;
using namespace node;

class Connection : EventEmitter {
 public:
  static void Initialize(Handle<Object>);
  bool addServer(const char *, int32_t);
  bool get(const char*, int32_t);
  bool set(const char*, int32_t, const char *, int32_t, time_t);
        
 protected:
  static Handle<Value> New(const Arguments &);
  static Handle<Value> addServer(const Arguments &);
  static Handle<Value> get(const Arguments &);
  static Handle<Value> set(const Arguments &);
  static Handle<Value> incr(const Arguments &);
  static Handle<Value> decr(const Arguments &);
  static Handle<Value> add(const Arguments &);
  static Handle<Value> replace(const Arguments &);
  static Handle<Value> prepend(const Arguments &);
  static Handle<Value> append(const Arguments &);
  static Handle<Value> cas(const Arguments &);
  static Handle<Value> remove(const Arguments &);
  static Handle<Value> flush(const Arguments &);
  void Event(int);

  Connection() : node::EventEmitter() {
    HandleScope scope;

    connecting_ = false;

    memcached_create(&memc_);
    memcached_behavior_set(&memc_, MEMCACHED_BEHAVIOR_NO_BLOCK, 1);

    output_len = 0;
    ev_init(&read_watcher_, io_event);
    read_watcher_.data = this;
    ev_init(&write_watcher_, io_event);
    write_watcher_.data = this;
  }

  ~Connection() {
    memcached_free(&memc_);
  }

 private:
  static void io_event(EV_P_ ev_io *, int);
  int memcached_socket(const char *, int);
  memcached_st memc_;
  ev_io read_watcher_;
  ev_io write_watcher_;
  int output_len;
  char *output;

  bool connecting_;
};
