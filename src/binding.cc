// Copyright 2010 Vanilla Hsu<vanilla@FreeBSD.org>

#include <string.h>
#include <stdlib.h>

#include <v8.h>
#include <node.h>
#include <node_events.h>

#include <libmemcached/memcached.h>

#define DEBUGMODE 1
#define pdebug(...) do {if(DEBUGMODE)printf(__VA_ARGS__);} while (0)
#define THROW_BAD_ARGS \
  v8::ThrowException(v8::Exception::TypeError(v8::String::New("Bad arguments")))

typedef enum {
  MEMC_SET,
  MEMC_ADD,
  MEMC_REPLACE,
  MEMC_APPEND,
  MEMC_PREPEND,
  MEMC_INCR,
  MEMC_DECR
} _type;

using namespace v8;
using namespace node;

static Persistent<String> ready_symbol;
static Persistent<String> distribution_symbol;

class Connection : EventEmitter {
 public:
  static void
  Initialize(Handle<Object> target) {
    HandleScope scope;

    Local<FunctionTemplate> t = FunctionTemplate::New(New);

    t->Inherit(EventEmitter::constructor_template);
    t->InstanceTemplate()->SetInternalFieldCount(1);

    ready_symbol = NODE_PSYMBOL("ready");
    distribution_symbol = NODE_PSYMBOL("distribution");

    NODE_SET_PROTOTYPE_METHOD(t, "addServer", addServer);
    NODE_SET_PROTOTYPE_METHOD(t, "get", get);
    NODE_SET_PROTOTYPE_METHOD(t, "getSync", getSync);
    NODE_SET_PROTOTYPE_METHOD(t, "set", set);
    NODE_SET_PROTOTYPE_METHOD(t, "incr", incr);
    NODE_SET_PROTOTYPE_METHOD(t, "cas", cas);
    NODE_SET_PROTOTYPE_METHOD(t, "delete", remove);
    NODE_SET_PROTOTYPE_METHOD(t, "flush", flush);

    t->PrototypeTemplate()->SetAccessor(distribution_symbol,
        DistributionGetter, DistributionSetter);

    target->Set(String::NewSymbol("Connection"), t->GetFunction());
  }

  bool addServer(const char *hostname, int port) {
    memcached_return rc;

    rc = memcached_server_add(&memc_, hostname, port);
    if (rc != MEMCACHED_SUCCESS)
      return false;

    memcached_behavior_set(&memc_, MEMCACHED_BEHAVIOR_NO_BLOCK, 1);

    return true;
  }

 protected:
  memcached_st memc_;

  static Handle<Value> DistributionGetter(Local<String> property,
      const AccessorInfo& info) {
    HandleScope scope;
    uint64_t data;

    Connection *c = ObjectWrap::Unwrap<Connection>(info.This());
    assert(c);
    assert(property == distribution_symbol);

    data = memcached_behavior_get(&c->memc_, MEMCACHED_BEHAVIOR_DISTRIBUTION);
    Local<Integer> v = Integer::New(data);
    return scope.Close(v);
  }

  static void DistributionSetter(Local<String> property, Local<Value> value,
      const AccessorInfo& info) {
    Connection *c = ObjectWrap::Unwrap<Connection>(info.This());
    assert(c);

    memcached_behavior_set(&c->memc_, MEMCACHED_BEHAVIOR_DISTRIBUTION,
        value->IntegerValue());
  }

  static Handle<Value> New(const Arguments &args) {
    HandleScope scope;
    Connection *c = new Connection();
    c->Wrap(args.This());
    return args.This();
  }

  static Handle<Value> addServer(const Arguments &args) {
    HandleScope scope;

    if (args.Length() < 2 || !args[0]->IsString() || !args[1]->IsInt32()) {
      return THROW_BAD_ARGS;
    }

    String::Utf8Value server_name(args[0]->ToString());
    int32_t port = args[1]->Int32Value();
    Connection *c = ObjectWrap::Unwrap<Connection>(args.This());

    memcached_server_add(&c->memc_, *server_name, port);

    return Undefined();
  }

  struct get_request {
    Persistent<Function> cb;
    Connection *c;
    char *key;
    void *content;
    size_t key_len;
    size_t content_len;
  };

  static int EIO_AfterGet(eio_req *req) {
    ev_unref(EV_DEFAULT_UC);
    HandleScope scope;
    struct get_request *get_req = (struct get_request *)(req->data);

    Local<Value> argv[2];
    bool err = false;
    if (req->result) {
      err = true;
      argv[0] = Exception::Error(String::New(memcached_strerror(NULL,
              (memcached_return)req->result)));
    } else {
      argv[0] = Local<Value>::New(Undefined());
      argv[1] = Local<Value>::New(String::New((const char*)get_req->content,
            static_cast<int>(get_req->content_len)));
    }

    TryCatch try_catch;

    get_req->cb->Call(Context::GetCurrent()->Global(), err ? 1 : 2, argv);

    if (try_catch.HasCaught()) {
      FatalException(try_catch);
    }

    get_req->c->Emit(ready_symbol, 0, NULL);
    get_req->cb.Dispose();

    free(get_req->content);
    free(get_req);

    get_req->c->Unref();

    return 0;
  }

  static int EIO_Get(eio_req *req) {
    struct get_request *get_req = (struct get_request*)(req->data);
    memcached_return rc;
    uint32_t flags;

    get_req->content = memcached_get(&get_req->c->memc_, get_req->key,
        get_req->key_len, &get_req->content_len, &flags, &rc);
    req->result = rc;

    return 0;
  }

  static Handle<Value> getSync(const Arguments &args) {
    HandleScope scope;
    memcached_return rc;
    char *value;
    size_t value_len;
    uint32_t flags;
    Local<Value> ret;

    if (args.Length() < 1 || !args[0]->IsString()) {
      pdebug("%d\n", args.Length());
      return THROW_BAD_ARGS;
    }

    String::Utf8Value key(args[0]->ToString());
    Connection *c = ObjectWrap::Unwrap<Connection>(args.This());

    value = memcached_get(&c->memc_, *key, key.length(), &value_len,
        &flags, &rc);

    if (value_len > 0) {
      ret = String::New(value, value_len);
      free(value);
      return scope.Close(ret);
    }

    return Undefined();
  }

  static Handle<Value> get(const Arguments &args) {
    HandleScope scope;

    if (args.Length() < 2 || !args[0]->IsString() || !args[1]->IsFunction()) {
      return THROW_BAD_ARGS;
    }

    String::Utf8Value key(args[0]->ToString());
    Local<Function> cb = Local<Function>::Cast(args[1]);
    Connection *c = ObjectWrap::Unwrap<Connection>(args.This());

    struct get_request *get_req = (struct get_request*)
      calloc(1, sizeof(struct get_request));

    if (!get_req) {
      V8::LowMemoryNotification();
      return ThrowException(Exception::Error(
            String::New("Could not allocate enough memory")));
    }

    get_req->key = strdup(*key);
    get_req->key_len = key.length();
    get_req->cb = Persistent<Function>::New(cb);
    get_req->c = c;

    eio_custom(EIO_Get, EIO_PRI_DEFAULT, EIO_AfterGet, get_req);

    ev_ref(EV_DEFAULT_UC);
    c->Ref();

    return Undefined();
  }

  struct set_request {
    Persistent<Function> cb;
    Connection *c;
    _type type;
    char *key;
    char *content;
    size_t key_len;
    size_t content_len;
    time_t expiration;
  };

  static int EIO_AfterSet(eio_req *req) {
    ev_unref(EV_DEFAULT_UC);
    HandleScope scope;
    struct set_request *set_req = (struct set_request *)(req->data);

    Local<Value> argv[1];
    bool err = false;
    if (req->result) {
      err = true;
      argv[0] = Exception::Error(String::New(memcached_strerror(NULL,
              (memcached_return)req->result)));
    }

    TryCatch try_catch;

    set_req->cb->Call(Context::GetCurrent()->Global(), err ? 1 : 0, argv);

    if (try_catch.HasCaught()) {
      FatalException(try_catch);
    }

    set_req->c->Emit(ready_symbol, 0, NULL);
    set_req->cb.Dispose();

    free(set_req->content);
    free(set_req->key);
    free(set_req);

    set_req->c->Unref();

    return 0;
  }

  static int EIO_Set(eio_req *req) {
    struct set_request *set_req = (struct set_request*)(req->data);
    memcached_return rc;

    switch (set_req->type) {
      case MEMC_SET:
        rc = memcached_set(&set_req->c->memc_, set_req->key,
            set_req->key_len, set_req->content, set_req->content_len,
            set_req->expiration, 0);
        break;
      case MEMC_ADD:
        rc = memcached_add(&set_req->c->memc_, set_req->key,
            set_req->key_len, set_req->content, set_req->content_len,
            set_req->expiration, 0);
        break;
      case MEMC_REPLACE:
        rc = memcached_replace(&set_req->c->memc_, set_req->key,
            set_req->key_len, set_req->content, set_req->content_len,
            set_req->expiration, 0);
        break;
      case MEMC_APPEND:
        rc = memcached_append(&set_req->c->memc_, set_req->key,
            set_req->key_len, set_req->content, set_req->content_len,
            set_req->expiration, 0);
        break;
      case MEMC_PREPEND:
        rc = memcached_prepend(&set_req->c->memc_, set_req->key,
            set_req->key_len, set_req->content, set_req->content_len,
            set_req->expiration, 0);
        break;
      default:
        rc = (memcached_return)0;
    }

    req->result = rc;

    return 0;
  }

  static Handle<Value> set(const Arguments &args) {
    HandleScope scope;
    
    if (args.Length() < 5 || !args[0]->IsInt32() || !args[1]->IsString() ||
        !args[2]->IsString() || !args[3]->IsInt32() || !args[4]->IsFunction()) {
      return THROW_BAD_ARGS;
    }

    _type type = (_type)args[0]->Int32Value();
    String::Utf8Value key(args[1]->ToString());
    String::Utf8Value content(args[2]->ToString());
    uint32_t expiration = args[3]->Int32Value();
    Local<Function> cb = Local<Function>::Cast(args[4]);
    Connection *c = ObjectWrap::Unwrap<Connection>(args.This());

    struct set_request *set_req = (struct set_request*)
      calloc(1, sizeof(struct set_request));

    if (!set_req) {
      V8::LowMemoryNotification();
      return ThrowException(Exception::Error(
            String::New("Could not allocate enough memory")));
    }

    set_req->type = type;
    set_req->key = strdup(*key);
    set_req->content = strdup(*content);
    set_req->key_len = key.length();
    set_req->content_len = content.length();
    set_req->expiration = expiration;
    set_req->cb = Persistent<Function>::New(cb);
    set_req->c = c;

    eio_custom(EIO_Set, EIO_PRI_DEFAULT, EIO_AfterSet, set_req);

    ev_ref(EV_DEFAULT_UC);
    c->Ref();

    return Undefined();
  }

  struct incr_request {
    Persistent<Function> cb;
    Connection *c;
    _type type;
    char *key;
    size_t key_len;
    uint32_t offset;
    uint64_t value;
  };

  static int EIO_AfterIncr(eio_req *req) {
    ev_unref(EV_DEFAULT_UC);
    HandleScope scope;
    struct incr_request *incr_req = (struct incr_request *)(req->data);

    Local<Value> argv[2];
    bool err = false;
    if (req->result) {
      err = true;
      argv[0] = Exception::Error(String::New(memcached_strerror(NULL,
              (memcached_return)req->result)));
    } else {
      argv[0] = Local<Value>::New(Undefined());
      argv[1] = Local<Value>::New(Integer::New(incr_req->value));
    }

    TryCatch try_catch;

    incr_req->cb->Call(Context::GetCurrent()->Global(), err ? 1 : 2, argv);

    if (try_catch.HasCaught()) {
      FatalException(try_catch);
    }

    incr_req->c->Emit(ready_symbol, 0, NULL);
    incr_req->cb.Dispose();

    free(incr_req->key);
    free(incr_req);

    incr_req->c->Unref();

    return 0;
  }

  static int EIO_Incr(eio_req *req) {
    struct incr_request *incr_req = (struct incr_request*)(req->data);
    memcached_return rc;

    switch (incr_req->type) {
      case MEMC_INCR:
        rc = memcached_increment(&incr_req->c->memc_, incr_req->key,
            incr_req->key_len, incr_req->offset, &incr_req->value);
        break;
      case MEMC_DECR:
        rc = memcached_decrement(&incr_req->c->memc_, incr_req->key,
            incr_req->key_len, incr_req->offset, &incr_req->value);
        break;
      default:
        rc = (memcached_return)0;
    }

    req->result = rc;

    return 0;
  }

  static Handle<Value> incr(const Arguments &args) {
    HandleScope scope;
    
    if (args.Length() < 4 || !args[0]->IsInt32() || !args[1]->IsString() ||
        !args[2]->IsInt32() || !args[3]->IsFunction()) {
      return THROW_BAD_ARGS;
    }

    _type type = (_type)args[0]->Int32Value();
    String::Utf8Value key(args[1]->ToString());
    uint32_t offset = args[2]->Int32Value();
    Local<Function> cb = Local<Function>::Cast(args[3]);
    Connection *c = ObjectWrap::Unwrap<Connection>(args.This());

    struct incr_request *incr_req = (struct incr_request*)
      calloc(1, sizeof(struct incr_request));

    if (!incr_req) {
      V8::LowMemoryNotification();
      return ThrowException(Exception::Error(
            String::New("Could not allocate enough memory")));
    }

    incr_req->type = type;
    incr_req->key = strdup(*key);
    incr_req->key_len = key.length();
    incr_req->offset = offset;
    incr_req->cb = Persistent<Function>::New(cb);
    incr_req->c = c;

    eio_custom(EIO_Incr, EIO_PRI_DEFAULT, EIO_AfterIncr, incr_req);

    ev_ref(EV_DEFAULT_UC);
    c->Ref();

    return Undefined();
  }

  struct cas_request {
    Persistent<Function> cb;
    Connection *c;
    _type type;
    char *key;
    char *content;
    size_t key_len;
    size_t content_len;
    time_t expiration;
    uint64_t cas;
  };

  static int EIO_Cas(eio_req *req) {
    struct cas_request *cas_req = (struct cas_request*)(req->data);
    memcached_return rc;

    rc = memcached_cas(&cas_req->c->memc_, cas_req->key, cas_req->key_len,
        cas_req->content, cas_req->content_len, cas_req->expiration, 0,
        cas_req->cas);

    req->result = rc;

    return 0;
  }

  static Handle<Value> cas(const Arguments &args) {
    HandleScope scope;
    
    if (args.Length() < 5 || !args[0]->IsString() || !args[1]->IsString() ||
        !args[2]->IsInt32() || !args[3]->IsNumber() || !args[4]->IsFunction()) {
      return THROW_BAD_ARGS;
    }

    String::Utf8Value key(args[0]->ToString());
    String::Utf8Value content(args[1]->ToString());
    uint32_t expiration = args[2]->Int32Value();
    uint64_t cas_arg = args[3]->IntegerValue();
    Local<Function> cb = Local<Function>::Cast(args[4]);
    Connection *c = ObjectWrap::Unwrap<Connection>(args.This());

    struct cas_request *cas_req = (struct cas_request*)
      calloc(1, sizeof(struct cas_request));

    if (!cas_req) {
      V8::LowMemoryNotification();
      return ThrowException(Exception::Error(
            String::New("Could not allocate enough memory")));
    }

    cas_req->key = strdup(*key);
    cas_req->content = strdup(*content);
    cas_req->key_len = key.length();
    cas_req->content_len = content.length();
    cas_req->expiration = expiration;
    cas_req->cas = cas_arg;
    cas_req->cb = Persistent<Function>::New(cb);
    cas_req->c = c;

    eio_custom(EIO_Cas, EIO_PRI_DEFAULT, EIO_AfterSet, cas_req);

    ev_ref(EV_DEFAULT_UC);
    c->Ref();

    return Undefined();
  }

  struct remove_request {
    Persistent<Function> cb;
    Connection *c;
    char *key;
    size_t key_len;
    time_t expiration;
  };

  static int EIO_AfterRemove(eio_req *req) {
    ev_unref(EV_DEFAULT_UC);
    HandleScope scope;
    struct remove_request *remove_req = (struct remove_request *)(req->data);

    Local<Value> argv[1];
    bool err = false;
    if (req->result) {
      err = true;
      argv[0] = Exception::Error(String::New(memcached_strerror(NULL,
              (memcached_return)req->result)));
    }

    TryCatch try_catch;

    remove_req->cb->Call(Context::GetCurrent()->Global(), err ? 1 : 0, argv);

    if (try_catch.HasCaught()) {
      FatalException(try_catch);
    }

    remove_req->c->Emit(ready_symbol, 0, NULL);
    remove_req->cb.Dispose();

    free(remove_req->key);
    free(remove_req);

    remove_req->c->Unref();

    return 0;
  }

  static int EIO_Remove(eio_req *req) {
    struct remove_request *remove_req = (struct remove_request*)(req->data);
    memcached_return rc;

    rc = memcached_delete(&remove_req->c->memc_, remove_req->key,
        remove_req->key_len, remove_req->expiration);
    req->result = rc;

    return 0;
  }

  static Handle<Value> remove(const Arguments &args) {
    HandleScope scope;
    
    if (args.Length() < 3 || !args[0]->IsString() ||
        !args[1]->IsInt32() || !args[2]->IsFunction()) {
      return THROW_BAD_ARGS;
    }

    String::Utf8Value key(args[0]->ToString());
    uint32_t expiration = args[1]->Int32Value();
    Local<Function> cb = Local<Function>::Cast(args[2]);
    Connection *c = ObjectWrap::Unwrap<Connection>(args.This());

    struct remove_request *remove_req = (struct remove_request*)
      calloc(1, sizeof(struct remove_request));

    if (!remove_req) {
      V8::LowMemoryNotification();
      return ThrowException(Exception::Error(
            String::New("Could not allocate enough memory")));
    }

    remove_req->key = strdup(*key);
    remove_req->key_len = key.length();
    remove_req->expiration = expiration;
    remove_req->cb = Persistent<Function>::New(cb);
    remove_req->c = c;

    eio_custom(EIO_Remove, EIO_PRI_DEFAULT, EIO_AfterRemove, remove_req);

    ev_ref(EV_DEFAULT_UC);
    c->Ref();

    return Undefined();
  }

  struct flush_request {
    Persistent<Function> cb;
    Connection *c;
    time_t expiration;
  };

  static int EIO_AfterFlush(eio_req *req) {
    ev_unref(EV_DEFAULT_UC);
    HandleScope scope;
    struct flush_request *flush_req = (struct flush_request *)(req->data);

    Local<Value> argv[1];
    bool err = false;
    if (req->result) {
      err = true;
      argv[0] = Exception::Error(String::New(memcached_strerror(NULL,
              (memcached_return)req->result)));
    }

    TryCatch try_catch;

    flush_req->cb->Call(Context::GetCurrent()->Global(), err ? 1 : 0, argv);

    if (try_catch.HasCaught()) {
      FatalException(try_catch);
    }

    flush_req->c->Emit(ready_symbol, 0, NULL);
    flush_req->cb.Dispose();

    free(flush_req);

    flush_req->c->Unref();

    return 0;
  }

  static int EIO_Flush(eio_req *req) {
    struct flush_request *flush_req = (struct flush_request*)(req->data);
    memcached_return rc;

    rc = memcached_flush(&flush_req->c->memc_, flush_req->expiration);
    req->result = rc;

    return 0;
  }

  static Handle<Value> flush(const Arguments &args) {
    HandleScope scope;
    
    if (args.Length() < 2 || !args[0]->IsInt32() || !args[1]->IsFunction()) {
      return THROW_BAD_ARGS;
    }

    time_t expiration = args[0]->Int32Value();
    Local<Function> cb = Local<Function>::Cast(args[1]);
    Connection *c = ObjectWrap::Unwrap<Connection>(args.This());

    struct flush_request *flush_req = (struct flush_request*)
      calloc(1, sizeof(struct flush_request));

    if (!flush_req) {
      V8::LowMemoryNotification();
      return ThrowException(Exception::Error(
            String::New("Could not allocate enough memory")));
    }

    flush_req->expiration = expiration;
    flush_req->cb = Persistent<Function>::New(cb);
    flush_req->c = c;

    eio_custom(EIO_Flush, EIO_PRI_DEFAULT, EIO_AfterFlush, flush_req);

    ev_ref(EV_DEFAULT_UC);
    c->Ref();

    return Undefined();
  }

  Connection() : node::EventEmitter() {
    memcached_create(&memc_);
  }

  operator memcached_st() const { return memc_; }
};

extern "C" void
init(Handle<Object> target) {
  NODE_DEFINE_CONSTANT(target, MEMC_SET);
  NODE_DEFINE_CONSTANT(target, MEMC_ADD);
  NODE_DEFINE_CONSTANT(target, MEMC_REPLACE);
  NODE_DEFINE_CONSTANT(target, MEMC_APPEND);
  NODE_DEFINE_CONSTANT(target, MEMC_PREPEND);
  NODE_DEFINE_CONSTANT(target, MEMC_INCR);
  NODE_DEFINE_CONSTANT(target, MEMC_DECR);

  NODE_DEFINE_CONSTANT(target, MEMCACHED_DISTRIBUTION_MODULA);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_DISTRIBUTION_CONSISTENT);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_DISTRIBUTION_CONSISTENT_KETAMA);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_DISTRIBUTION_RANDOM);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_DISTRIBUTION_CONSISTENT_KETAMA_SPY);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_DISTRIBUTION_CONSISTENT_MAX);

  Connection::Initialize(target);
}
