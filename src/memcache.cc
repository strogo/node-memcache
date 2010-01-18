// Copyright 2010 Vanilla Hsu<vanilla@FreeBSD.org>
#include "memcache.h"

enum Cmds {
  CMD_ADD,
  CMD_REPLACE,
  CMD_PREPEND,
  CMD_APPEND
};

static bool
_run(Memcache *memc, enum Cmds cmd, const Arguments& args)
{
  bool ret;
  String::Utf8Value key(args[0]->ToString());
  String::Utf8Value data(args[1]->ToString());
  std::string nk = std::string(*key);
  std::vector<char> content;

  content.reserve(data.length());
  content.assign(*data, *data + data.length());

  switch(cmd) {
    case CMD_ADD:
      ret = memc->append(nk, content);
      break;
    case CMD_REPLACE:
      ret = memc->replace(nk, content);
      break;
    case CMD_PREPEND:
      ret = memc->prepend(nk, content);
      break;
    case CMD_APPEND:
      ret = memc->append(nk, content);
      break;
    default:
      ret = false;
  }

  return ret;
}

void
Connection::Initialize(Handle<Object> target)
{
  HandleScope scope;

  Local<FunctionTemplate> t = FunctionTemplate::New(Connection::New);
  t->Inherit(EventEmitter::constructor_template);
  t->InstanceTemplate()->SetInternalFieldCount(1);

  /* as property ?
  NODE_SET_PROTOTYPE_METHOD(t, "setBehavior", setBehavior);
  NODE_SET_PROTOTYPE_METHOD(t, "getBehavior", getBEhavior);
  */
  NODE_SET_PROTOTYPE_METHOD(t, "setServers", setServers);
  NODE_SET_PROTOTYPE_METHOD(t, "addServer", addServer);
  NODE_SET_PROTOTYPE_METHOD(t, "removeServer", removeServer);
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
Connection::setServers(const Arguments& args)
{
  HandleScope scope;

  if (args.Length() < 1 || !args[0]->IsString()) {
    return THROW_BAD_ARGS;
  }

  Connection *c = ObjectWrap::Unwrap<Connection>(args.This());
  String::Utf8Value server_name(args[0]->ToString());
  std::string nk(*server_name);

  bool ret = c->memc->setServers(nk);

  return scope.Close(Boolean::New(ret));
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
  std::string nk(*server_name);
  int port = args[1]->Int32Value();

  bool ret = c->memc->addServer(nk, port);
  
  return scope.Close(Boolean::New(ret));
}

Handle<Value>
Connection::removeServer(const Arguments& args)
{
  HandleScope scope;

  if (args.Length() < 2 || !args[0]->IsString() || !args[1]->IsInt32()) {
    return THROW_BAD_ARGS;
  }

  Connection *c = ObjectWrap::Unwrap<Connection>(args.This());
  String::Utf8Value server_name(args[0]->ToString());
  std::string nk(*server_name);
  int32_t port = args[1]->Int32Value();

  bool ret = c->memc->removeServer(nk, port);
  
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
  std::string nk(*key);

  std::vector<char> ret_val;
  c->memc->get(nk, ret_val);

  if (ret_val.size() > 0) {
    std::string r(ret_val.begin(), ret_val.end());
    return scope.Close(String::New(r.c_str()));
  }
  
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

  std::vector<char> content;
  content.reserve(data.length());
  content.assign(*data, *data + data.length());

  bool ret = c->memc->set(nk, content, expiration, 0);
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

  bool ret = c->memc->increment(nk, offset, &value);
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

  bool ret = c->memc->decrement(nk, offset, &value);
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
  bool ret = _run(c->memc, CMD_ADD, args);
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
  bool ret = _run(c->memc, CMD_REPLACE, args);
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
  bool ret = _run(c->memc, CMD_PREPEND, args);
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
  bool ret = _run(c->memc, CMD_APPEND, args);
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
  bool ret = _run(c->memc, CMD_APPEND, args);
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

  bool ret = c->memc->remove(nk, expiration);
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

  bool ret = c->memc->flush(expiration);
  if (ret == true)
    return scope.Close(Boolean::New(ret));

  return Undefined();
}
/*
Handle<Value>
test(const Arguments& args)
{
  if (args[0]->IsFunction()) {
    Local<Function> *fn = new Local<Function>();
    *fn = Local<Function>::New(Local<Function>::Cast(args[0]));
    (*fn)->Call(args.This(), 0, NULL);
  } else {
    return Undefined();
  }
}
*/

extern "C" void
init(Handle<Object> target)
{
  HandleScope scope;

  // target->Set(String::New("test"), FunctionTemplate::New(test)->GetFunction());
  Connection::Initialize(target);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_SUCCESS);

  NODE_DEFINE_CONSTANT(target, MEMCACHED_DISTRIBUTION_MODULA);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_DISTRIBUTION_CONSISTENT);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_DISTRIBUTION_CONSISTENT_KETAMA);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_DISTRIBUTION_RANDOM);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_DISTRIBUTION_CONSISTENT_KETAMA_SPY);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_DISTRIBUTION_CONSISTENT_MAX);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_NO_BLOCK);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_TCP_NODELAY);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_HASH);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_KETAMA);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_SOCKET_SEND_SIZE);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_SOCKET_RECV_SIZE);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_CACHE_LOOKUPS);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_SUPPORT_CAS);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_POLL_TIMEOUT);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_DISTRIBUTION);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_BUFFER_REQUESTS);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_USER_DATA);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_SORT_HOSTS);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_VERIFY_KEY);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_CONNECT_TIMEOUT);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_RETRY_TIMEOUT);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_KETAMA_WEIGHTED);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_KETAMA_HASH);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_SND_TIMEOUT);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_RCV_TIMEOUT);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_SERVER_FAILURE_LIMIT);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_IO_MSG_WATERMARK);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_IO_BYTES_WATERMARK);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_IO_KEY_PREFETCH);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_HASH_WITH_PREFIX_KEY);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_NOREPLY);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_USE_UDP);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_AUTO_EJECT_HOSTS);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_NUMBER_OF_REPLICAS);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_RANDOMIZE_REPLICA_READ);
  NODE_DEFINE_CONSTANT(target, MEMCACHED_BEHAVIOR_MAX);
}
