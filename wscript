srcdir = "."
blddir = "build"
VERSION = "0.0.1"

def set_options(opt):
  opt.tool_options("compiler_cxx")

def configure(conf):
  conf.check_tool("compiler_cxx")
  conf.check_tool("node_addon")
  conf.check(lib="memcached", libpath=['/usr/lib', '/usr/local/lib'], uselib="MEMCACHED")
  conf.env.append_value('CXXFLAGS', ['-I/usr/local/include'])

def build(bld):
  obj = bld.new_task_gen("cxx", "shlib", "node_addon")
  obj.target = "memcache"
  obj.source = "src/memcache.cc"
  obj.uselib = "MEMCACHED"
