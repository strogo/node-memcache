// Copyright 2010 Vanilla Hsu<vanilla@FreeBSD.org>

#include <libmemcached/memcached.h>
#include <node.h>
#include <node_events.h>

#define DEBUGMODE 1
#define pdebug(...) do{if(DEBUGMODE)printf(__VA_ARGS__);}while(0)
#define THROW_BAD_ARGS \
  v8::ThrowException(v8::Exception::TypeError(v8::String::New("Bad arguments")))

typedef enum {
  MVAL_STRING = 1,
  MVAL_LONG,
  MVAL_BOOL
} mval_type;

typedef struct {
  mval_type type;
  union {
    char *c;
    uint64_t l;
    int b;
  } u;
} mval;

using namespace v8;
using namespace node;

static Persistent<String> ready_symbol;
static Persistent<String> result_symbol;
static Persistent<String> close_symbol;
static Persistent<String> connect_symbol;

class Connection : EventEmitter {
 public:
  static void
  Initialize(Handle<Object> target)
  {
    HandleScope scope;

    Local<FunctionTemplate> t = FunctionTemplate::New(New);

    t->Inherit(EventEmitter::constructor_template);
    t->InstanceTemplate()->SetInternalFieldCount(1);

    ready_symbol = NODE_PSYMBOL("ready");
    result_symbol = NODE_PSYMBOL("result");
    close_symbol = NODE_PSYMBOL("close");
    connect_symbol = NODE_PSYMBOL("connect");

    NODE_SET_PROTOTYPE_METHOD(t, "addServer", AddServer);
    NODE_SET_PROTOTYPE_METHOD(t, "_get", _Get);
    NODE_SET_PROTOTYPE_METHOD(t, "_set", _Set);
    NODE_SET_PROTOTYPE_METHOD(t, "_incr", _Incr);
    NODE_SET_PROTOTYPE_METHOD(t, "_decr", _Decr);
    NODE_SET_PROTOTYPE_METHOD(t, "close", Close);

    target->Set(String::NewSymbol("Connection"), t->GetFunction());
  }

  bool AddServer(const char *hostname, int port)
  {
    rc = memcached_server_add(&memc_, hostname, port);
    if (rc != MEMCACHED_SUCCESS)
      return false;

    memcached_behavior_set(&memc_, MEMCACHED_BEHAVIOR_NO_BLOCK, 1);
    memcached_version(&memc_);

    return true;
  }

  void _Get(const char *key, int key_len)
  {
    size_t value;
    uint32_t flags;

    memcached_attach_fd(key, key_len);
    mval_.u.c = memcached_get(&memc_, key, key_len, &value, &flags, &rc);
    if (mval_.u.c != NULL) {
      mval_.type = MVAL_STRING;
    }
  }

  void _Set(const char *key, int key_len, const char *data, int data_len,
      time_t expiration)
  {
    memcached_attach_fd(key, key_len);
    rc = memcached_set(&memc_, key, key_len, data, data_len, expiration, 0);
    if (rc == MEMCACHED_SUCCESS) {
      mval_.type = MVAL_BOOL;
      mval_.u.b = 1;
    }
  }

  void _Incr(const char *key, int key_len, uint32_t offset)
  {
    uint64_t value;

    memcached_attach_fd(key, key_len);
    rc = memcached_increment(&memc_, key, key_len, offset, &value);
    if (rc == MEMCACHED_SUCCESS) {
      mval_.type = MVAL_LONG;
      mval_.u.l = value;
    }
  }

  void _Decr(const char *key, int key_len, uint32_t offset)
  {
    uint64_t value;

    memcached_attach_fd(key, key_len);
    rc = memcached_decrement(&memc_, key, key_len, offset, &value);
    if (rc == MEMCACHED_SUCCESS) {
      mval_.type = MVAL_LONG;
      mval_.u.l = value;
    }
  }

  void Close(Local<Value> exception = Local<Value>())
  {
    HandleScope scope;

    ev_io_stop(EV_DEFAULT_ &read_watcher_);
    ev_io_stop(EV_DEFAULT_ &write_watcher_);

    if (exception.IsEmpty()) {
      Emit(close_symbol, 0, NULL);
    } else {
      Emit(close_symbol, 1, &exception);
    }
  }

  const char *ErrorMessage()
  {
    return memcached_strerror(NULL, rc);
  }

 protected:
  static Handle<Value> New(const Arguments &args)
  {
    HandleScope scope;

    Connection *c = new Connection();
    c->Wrap(args.This());

    return args.This();
  }

  static Handle<Value> AddServer(const Arguments &args)
  {
    HandleScope scope;

    if (args.Length() < 2 || !args[0]->IsString() || !args[1]->IsInt32()) {
      return THROW_BAD_ARGS;
    }

    Connection *c = ObjectWrap::Unwrap<Connection>(args.This());
    String::Utf8Value server_name(args[0]->ToString());
    int32_t port = args[1]->Int32Value();
    bool r = c->AddServer(*server_name, port);

    if (!r) {
      return ThrowException(Exception::Error(
            String::New(c->ErrorMessage())));
    }

    c->Emit(connect_symbol, 0, NULL);
    return Undefined();
  }

  static Handle<Value> _Get(const Arguments &args)
  {
    HandleScope scope;

    if (args.Length() < 1 || !args[0]->IsString()) {
      return THROW_BAD_ARGS;
    }

    Connection *c = ObjectWrap::Unwrap<Connection>(args.This());
    String::Utf8Value key(args[0]->ToString());

    c->_Get(*key, key.length());

    return Undefined();
  }

  static Handle<Value> _Set(const Arguments &args)
  {
    HandleScope scope;

    if (args.Length() < 3 || !args[0]->IsString() || !args[1]->IsString() || !args[2]->IsInt32()) {
      return THROW_BAD_ARGS;
    }

    Connection *c = ObjectWrap::Unwrap<Connection>(args.This());
    String::Utf8Value key(args[0]->ToString());
    String::Utf8Value value(args[1]->ToString());
    uint32_t expiration = args[2]->Int32Value();

    c->_Set(*key, key.length(), *value, value.length(), expiration);

    return Undefined();
  }

  static Handle<Value> _Incr(const Arguments &args)
  {
    HandleScope scope;

    if (args.Length() < 2 || !args[0]->IsString() || !args[1]->IsInt32()) {
      return THROW_BAD_ARGS;
    }

    Connection *c = ObjectWrap::Unwrap<Connection>(args.This());
    String::Utf8Value key(args[0]->ToString());
    uint32_t offset = args[1]->Int32Value();

    c->_Incr(*key, key.length(), offset);

    return Undefined();
  }

  static Handle<Value> _Decr(const Arguments &args)
  {
    HandleScope scope;

    if (args.Length() < 2 || !args[0]->IsString() || !args[1]->IsInt32()) {
      return THROW_BAD_ARGS;
    }

    Connection *c = ObjectWrap::Unwrap<Connection>(args.This());
    String::Utf8Value key(args[0]->ToString());
    uint32_t offset = args[1]->Int32Value();

    c->_Decr(*key, key.length(), offset);

    return Undefined();
  }

  static Handle<Value> Close(const Arguments &args)
  {
    HandleScope scope;
    Connection *c = ObjectWrap::Unwrap<Connection>(args.This());
    c->Close();
    return Undefined();
  }

  void Event(int revents)
  {
    Handle<Value> result;

    if (revents & EV_ERROR) {
      Close();
      return;
    }

    if (revents & EV_READ) {
      return;
    }

    if (revents & EV_WRITE) {
      switch (mval_.type) {
        case MVAL_STRING:
          result = String::New(mval_.u.c);
          free(mval_.u.c);
          mval_.type = (mval_type)0;
          rc = (memcached_return_t)-1;
          break;
        case MVAL_LONG:
          result = Integer::New(mval_.u.l);
          mval_.u.l = 0;
          mval_.type = (mval_type)0;
          rc = (memcached_return_t)-1;
          break;
        case MVAL_BOOL:
          result = Boolean::New(mval_.u.b);
          mval_.u.b = 0;
          mval_.type = (mval_type)0;
          rc = (memcached_return_t)-1;
          break;
        default:
          result = Exception::Error(String::New(memcached_strerror(NULL, rc)));
      }

      ev_io_stop(EV_DEFAULT_ &write_watcher_);
      Emit(result_symbol, 1, &result);
      Emit(ready_symbol, 0, NULL);
    }
  }

  Connection() : node::EventEmitter() {
    HandleScope scope;

    memcached_create(&memc_);
    rc = (memcached_return_t)-1;
    mval_.type = (mval_type)0;
    
    ev_init(&read_watcher_, io_event);
    read_watcher_.data = this;
    ev_init(&write_watcher_, io_event);
    write_watcher_.data = this;
  }

 private:
  static void io_event(EV_P_ ev_io *w, int revents)
  {
    Connection *c = static_cast<Connection *>(w->data);
    c->Event(revents);
  }

  void memcached_attach_fd(const char *key, int key_len)
  {
    int server, fd;

    server = memcached_generate_hash(&memc_, key, key_len);
    if (memc_.hosts[server].fd == -1)
      memcached_version(&memc_);

    fd = memc_.hosts[server].fd;

    ev_io_set(&read_watcher_, fd, EV_READ);
    ev_io_set(&write_watcher_, fd, EV_WRITE);
    ev_io_start(EV_DEFAULT_ &write_watcher_);
  }

  memcached_st memc_;
  ev_io read_watcher_;
  ev_io write_watcher_;
  mval mval_;
  memcached_return_t rc;
};

extern "C" void
init(Handle<Object> target)
{
  HandleScope scope;

  Connection::Initialize(target);
}
