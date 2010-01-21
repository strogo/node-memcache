// Copyright 2010 Vanilla Hsu<vanilla@FreeBSD.org>
#include "binding.h"

static Persistent<String> ready_symbol;
static Persistent<String> result_symbol;

enum Cmds {
  CMD_ADD,
  CMD_REPLACE,
  CMD_PREPEND,
  CMD_APPEND
};
  
bool
Connection::addServer(const char *server_name, int32_t port)
{
  memcached_return rc;

  rc = memcached_server_add(&memc_, server_name, port);

  if (rc == MEMCACHED_SUCCESS)
    return true;

  return false;
}

bool
Connection::get(const char *key, int32_t key_len)
{
  memcached_return rc;
  int fd;

  fd = memcached_socket(key, key_len);
  if (fd == -1)
    return false;

  ev_io_set(&read_watcher_, fd, EV_READ);
  ev_io_set(&write_watcher_, fd, EV_WRITE);

  ev_io_start(EV_DEFAULT_ &write_watcher_);
  return true;
}

bool
Connection::set(const char *key, int32_t key_len,
    const char *value, int32_t value_len, time_t expiration)
{
  return true;
}

int
Connection::memcached_socket(const char *key, int key_len)
{
  int server;
  memcached_return rc;

  server = memcached_generate_hash(&memc_, key, key_len);
  rc = memcached_connect(&memc_.hosts[server]);
  if (rc != MEMCACHED_SUCCESS)
    return -1;

  return memc_.hosts[server].fd;
}

void
Connection::io_event(EV_P_ ev_io *w, int revents)
{
  Connection *c = static_cast<Connection *>(w->data);
  c->Event(revents);
}

void
Connection::Event(int revents)
{
  if (revents & EV_ERROR) {
    pdebug("hit error\n");
    return;
  }

  if (revents & EV_READ) {
    pdebug("hit read event\n");
    return;
  }

  if (revents & EV_WRITE) {
    pdebug ("hit write event\n");
    if (output_len)
      pdebug("%d\n", output_len);
    else
      ev_io_stop(EV_DEFAULT_ &write_watcher_);

    Emit(ready_symbol, 0, NULL);
  }
}

void
Connection::Initialize(Handle<Object> target)
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

Handle<Value>
Connection::New(const Arguments& args)
{
  HandleScope scope;

  Connection *c = new Connection();
  c->Wrap(args.This());

  return args.This();
}

Handle<Value>
Connection::addServer(const Arguments& args)
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

Handle<Value>
Connection::get(const Arguments& args)
{
  HandleScope scope;

  if (args.Length() < 1 || !args[0]->IsString()) {
    return THROW_BAD_ARGS;
  }

  Connection *c = ObjectWrap::Unwrap<Connection>(args.This());
  String::Utf8Value key(args[0]->ToString());

  c->get(*key, key.length());
    //return scope.Close(String::New(r.c_str()));
  
  return Undefined();
}

Handle<Value>
Connection::set(const Arguments& args)
{
  HandleScope scope;

  if (args.Length() < 3 || !args[0]->IsString() || !args[1]->IsString()
      || !args[2]->IsInt32()) {
    return THROW_BAD_ARGS;
  }

  Connection *c = ObjectWrap::Unwrap<Connection>(args.This());
  String::Utf8Value key(args[0]->ToString());
  std::string nk(*key);
  String::Utf8Value data(args[1]->ToString());
  int32_t expiration = args[2]->Int32Value();

  bool ret;
  if (ret == true) {
    return scope.Close(Boolean::New(ret));
  }

  return Undefined();
}

Handle<Value>
Connection::incr(const Arguments& args)
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

Handle<Value>
Connection::decr(const Arguments& args)
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

Handle<Value>
Connection::add(const Arguments& args)
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

Handle<Value>
Connection::replace(const Arguments& args)
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

Handle<Value>
Connection::prepend(const Arguments& args)
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

Handle<Value>
Connection::append(const Arguments& args)
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

Handle<Value>
Connection::cas(const Arguments& args)
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

Handle<Value>
Connection::remove(const Arguments& args)
{
  HandleScope scope;

  if (args.Length() < 2 || !args[0]->IsString() || !args[1]->IsInt32()) {
    return THROW_BAD_ARGS;
  }

  Connection *c = ObjectWrap::Unwrap<Connection>(args.This());
  String::Utf8Value key(args[0]->ToString());
  int32_t expiration = args[1]->Int32Value();
  std::string nk(*key);

  bool ret;
  if (ret == true)
    return scope.Close(Boolean::New(ret));

  return Undefined();
}

Handle<Value>
Connection::flush(const Arguments& args)
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

extern "C" void
init(Handle<Object> target)
{
  HandleScope scope;

  Connection::Initialize(target);
}
