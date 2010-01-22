// Copyright 2010 Vanilla Hsu<vanilla@FreeBSD.org>

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

static Persistent<String> ready_symbol;
static Persistent<String> result_symbol;

class Connection : EventEmitter {
 public:
  static void Initialize(Handle<Object> target)
  {
    HandleScope scope;

    Local<FunctionTemplate> t = FunctionTemplate::New(Connection::New);
    t->Inherit(EventEmitter::constructor_template);
    t->InstanceTemplate()->SetInternalFieldCount(1);

    ready_symbol = NODE_PSYMBOL("ready");
    result_symbol = NODE_PSYMBOL("result");

    NODE_SET_PROTOTYPE_METHOD(t, "addServer", addServer);
    NODE_SET_PROTOTYPE_METHOD(t, "get", get);
    NODE_SET_PROTOTYPE_METHOD(t, "set", set);
    NODE_SET_PROTOTYPE_METHOD(t, "incr", incr);
    NODE_SET_PROTOTYPE_METHOD(t, "decr", decr);
    NODE_SET_PROTOTYPE_METHOD(t, "add", add);
    NODE_SET_PROTOTYPE_METHOD(t, "replace", replace);
    NODE_SET_PROTOTYPE_METHOD(t, "prepend", prepend);
    NODE_SET_PROTOTYPE_METHOD(t, "append", append);
    NODE_SET_PROTOTYPE_METHOD(t, "cas", cas);
    NODE_SET_PROTOTYPE_METHOD(t, "remove", remove);
    NODE_SET_PROTOTYPE_METHOD(t, "flush", flush);

    target->Set(String::NewSymbol("Connection"), t->GetFunction());
  }

  bool addServer(const char *server_name, int32_t port)
  {
    memcached_return rc;

    rc = memcached_server_add(&memc_, server_name, port);

    if (rc == MEMCACHED_SUCCESS) {
      memcached_behavior_set(&memc_, MEMCACHED_BEHAVIOR_NO_BLOCK, 1);
      return true;
    }

    return false;
  }

  bool get(const char *key, int32_t key_len)
  {
    uint32_t fd, flags;

    fd = memcached_socket(key, key_len);
    if (fd == -1)
      return false;

    ev_io_set(&write_watcher_, fd, EV_WRITE);
    ev_io_start(EV_DEFAULT_ &write_watcher_);

    output = memcached_get(&memc_, key, key_len,
        &output_len, &flags, &rc);
    if (output != NULL)
      return true;

    return false;
  }

  bool set(const char *key, int32_t key_len, const char *data, int32_t data_len, time_t expiration)
  {
    return true;
  }
        
 protected:
  static Handle<Value> New(const Arguments &args)
  {
    HandleScope scope;

    Connection *c = new Connection();
    c->Wrap(args.This());

    return args.This();
  }

  static Handle<Value> addServer(const Arguments &args)
  {
    HandleScope scope;

    if (args.Length() < 2 || !args[0]->IsString() || !args[1]->IsInt32()) {
      return THROW_BAD_ARGS;
    }

    Connection *c = ObjectWrap::Unwrap<Connection>(args.This());
    String::Utf8Value server_name(args[0]->ToString());
    int32_t port = args[1]->Int32Value();

    bool ret = c->addServer(*server_name, port);
  
    return scope.Close(Boolean::New(ret));
  }

  static Handle<Value> get(const Arguments &args)
  {
    HandleScope scope;

    if (args.Length() < 1 || !args[0]->IsString()) {
      return THROW_BAD_ARGS;
    }

    Connection *c = ObjectWrap::Unwrap<Connection>(args.This());
    String::Utf8Value key(args[0]->ToString());

    c->get(*key, key.length());
  
    return Undefined();
  }

  static Handle<Value> set(const Arguments &args)
  {
    HandleScope scope;

    if (args.Length() < 3 || !args[0]->IsString() || !args[1]->IsString()
        || !args[2]->IsInt32()) {
      return THROW_BAD_ARGS;
    }

    Connection *c = ObjectWrap::Unwrap<Connection>(args.This());
    String::Utf8Value key(args[0]->ToString());
    String::Utf8Value data(args[1]->ToString());
    int32_t expiration = args[2]->Int32Value();

    bool ret;
    if (ret == true) {
      return scope.Close(Boolean::New(ret));
    }

    return Undefined();
  }

  static Handle<Value> incr(const Arguments &args)
  {
    HandleScope scope;
    uint64_t value;

    if (args.Length() < 2 || !args[0]->IsString() || !args[1]->IsInt32()) {
      return THROW_BAD_ARGS;
    }

    Connection *c = ObjectWrap::Unwrap<Connection>(args.This());
    String::Utf8Value key(args[0]->ToString());
    std::string nk(*key);
    int32_t offset = args[1]->Int32Value();

    bool ret;
    if (ret == true) {
      return scope.Close(Integer::New(value));
    }

    return Undefined();
  }

  static Handle<Value> decr(const Arguments &args)
  {
    HandleScope scope;
    uint64_t value;

    if (args.Length() < 2 || !args[0]->IsString() || !args[1]->IsInt32()) {
      return THROW_BAD_ARGS;
    }

    Connection *c = ObjectWrap::Unwrap<Connection>(args.This());
    String::Utf8Value key(args[0]->ToString());
    std::string nk(*key);
    int32_t offset = args[1]->Int32Value();

    bool ret;
    if (ret == true) {
      return scope.Close(Integer::New(value));
    }

    return Undefined();
  }

  static Handle<Value> add(const Arguments &args)
  {
    HandleScope scope;

    if (args.Length() < 2 || !args[0]->IsString() || !args[1]->IsString()) {
      return THROW_BAD_ARGS;
    }

    Connection *c = ObjectWrap::Unwrap<Connection>(args.This());
    bool ret;
    if (ret == true)
      return scope.Close(Boolean::New(ret));

    return Undefined();
  }

  static Handle<Value> replace(const Arguments &args)
  {
    HandleScope scope;

    if (args.Length() < 2 || !args[0]->IsString() || !args[1]->IsString()) {
      return THROW_BAD_ARGS;
    }

    Connection *c = ObjectWrap::Unwrap<Connection>(args.This());
    bool ret;
    if (ret == true)
      return scope.Close(Boolean::New(ret));

    return Undefined();
  }

  static Handle<Value> prepend(const Arguments &args)
  {
    HandleScope scope;

    if (args.Length() < 2 || !args[0]->IsString() || !args[1]->IsString()) {
      return THROW_BAD_ARGS;
    }

    Connection *c = ObjectWrap::Unwrap<Connection>(args.This());
    bool ret;
    if (ret == true)
      return scope.Close(Boolean::New(ret));

    return Undefined();
  }

  static Handle<Value> append(const Arguments &args)
  {
    HandleScope scope;

    if (args.Length() < 2 || !args[0]->IsString() || !args[1]->IsString()) {
      return THROW_BAD_ARGS;
    }

    Connection *c = ObjectWrap::Unwrap<Connection>(args.This());
    bool ret;
    if (ret == true)
      return scope.Close(Boolean::New(ret));

    return Undefined();
  }

  static Handle<Value> cas(const Arguments &args)
  {
    HandleScope scope;

    if (args.Length() < 2 || !args[0]->IsString() || !args[1]->IsString()) {
      return THROW_BAD_ARGS;
    }

    Connection *c = ObjectWrap::Unwrap<Connection>(args.This());
    bool ret;
    if (ret == true)
      return scope.Close(Boolean::New(ret));

    return Undefined();
  }

  static Handle<Value> remove(const Arguments &args)
  {
    HandleScope scope;

    if (args.Length() < 2 || !args[0]->IsString() || !args[1]->IsInt32()) {
      return THROW_BAD_ARGS;
    }

    Connection *c = ObjectWrap::Unwrap<Connection>(args.This());
    String::Utf8Value key(args[0]->ToString());
    int32_t expiration = args[1]->Int32Value();

    bool ret;
    if (ret == true)
      return scope.Close(Boolean::New(ret));

    return Undefined();
  }

  static Handle<Value> flush(const Arguments &args)
  {
    HandleScope scope;

    if (args.Length() < 1 || !args[0]->IsInt32()) {
      return THROW_BAD_ARGS;
    }

    Connection *c = ObjectWrap::Unwrap<Connection>(args.This());
    int32_t expiration = args[0]->Int32Value();

    bool ret;
    if (ret == true)
      return scope.Close(Boolean::New(ret));

    return Undefined();
  }


  void Event(int revents)
  {
    if (revents & EV_WRITE) {
      if (output_len) {
        EmitResult();
      } else
        ev_io_stop(EV_DEFAULT_ &write_watcher_);

      Emit(ready_symbol, 0, NULL);
    }
  
    if (revents & EV_READ) {
      return;
    }

    if (revents & EV_ERROR) {
      pdebug("hit error\n");
    }
  }

  Connection() : node::EventEmitter() {
    HandleScope scope;

    memcached_create(&memc_);

    output = NULL;
    output_len = 0;
    ev_init(&read_watcher_, io_event);
    read_watcher_.data = this;
    ev_init(&write_watcher_, io_event);
    write_watcher_.data = this;
  }

  ~Connection() {
    ev_io_stop(EV_DEFAULT_ &write_watcher_);
    ev_io_stop(EV_DEFAULT_ &read_watcher_);
    memcached_free(&memc_);
  }

 private:
  static void io_event(EV_P_ ev_io *w, int revents)
  {
    Connection *c = static_cast<Connection *>(w->data);
    c->Event(revents);
  }

  void EmitResult()
  {
    Local<Value> exception;
    Local<Value> result;

    switch(rc) {
      case MEMCACHED_SUCCESS:
        result = String::New(output);
        free(output);
        output_len = 0;
        Emit(result_symbol, 1, &result);
        break;
      default:
        exception = Exception::Error(String::New(memcached_strerror(NULL, rc)));
        Emit(result_symbol, 1, &exception);
    }
  }

  int memcached_socket(const char *key, int key_len)
  {
    int server;
    memcached_return rc;

    server = memcached_generate_hash(&memc_, key, key_len);
    if (memc_.hosts[server].cursor_active)
      return memc_.hosts[server].fd;

    memc_.hosts[server].write_buffer_offset = 1;
    rc = memcached_flush_buffers(&memc_);
    if (rc != MEMCACHED_SUCCESS)
      return -1;

    memc_.hosts[server].cursor_active++;
    return memc_.hosts[server].fd;
  }

  memcached_st memc_;
  ev_io read_watcher_;
  ev_io write_watcher_;

  memcached_return rc;
  size_t output_len;
  char *output;
};

extern "C" void
init(Handle<Object> target)
{
  HandleScope scope;

  Connection::Initialize(target);
}
