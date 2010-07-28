from os import popen

srcdir = "."
blddir = "build"
VERSION = "0.0.1"

def set_options(opt):
  opt.tool_options("compiler_cxx")

def configure(conf):
  conf.check_tool("compiler_cxx")
  conf.check_tool("node_addon")
  pkg_config = conf.find_program('pkg-config', var='PKG_CONFIG', mandatory=True)
  tmp_version = popen("%s libmemcached --modversion" % pkg_config).readline().strip()
  libmemcached_version = float(tmp_version)
  if libmemcached_version < 0.40:
    conf.fatal("should use libmemcached 0.40 or later version")
  libmemcached_libdir = popen("%s libmemcached --libs-only-L" % pkg_config).readline().strip();
  if libmemcached_libdir == "":
    libmemcached_libdir = "/usr/lib"
  libmemcached_libdir = libmemcached_libdir.replace("-L", "")
  libmemcached_includedir = popen("%s libmemcached --cflags-only-I" % pkg_config).readline().strip();
  if libmemcached_includedir == "":
    libmemcached_includedir = "/usr/include"
  libmemcached_includedir = libmemcached_includedir.replace("-I", "")
  conf.env.append_value("LIBPATH_MEMCACHED", libmemcached_libdir)
  conf.env.append_value('CPPPATH_MEMCACHED', libmemcached_includedir)
  conf.env.append_value("LIB_MEMCACHED", "memcached")

def build(bld):
  obj = bld.new_task_gen("cxx", "shlib", "node_addon")
  obj.target = "memcache-impl"
  obj.source = "src/binding.cc"
  obj.uselib = "MEMCACHED"
  obj.cxxflags = ["-g", "-D_FILE_OFFSET_BITS=64", "-D_LARGEFILE_SOURCE", "-Wall"]
