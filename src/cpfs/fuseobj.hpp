#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Defines the cpfs::BaseFuseRunner template for running a Fuse
 * object.  The clients define their own Fuse object, which should
 * provide the Init, Destroy, ParseFsOpts and fuse_method_policy
 * methods, perhaps by inheriting from the cpfs::BaseFuseObj class.
 * The class can have member functions for other methods in the FUSE
 * API (recorded in the FUSEOBJ_METHODS macro), with naming convention
 * changed to CamelCase.  The cpfs::BaseFuseRunner template will
 * detect its presence and generate code to set a stub in the
 * fuse_lowlevel_ops structure when running the filesystem.
 */

#include <pthread.h>
#include <signal.h>
#include <stdint.h>

#include <fuse/fuse_lowlevel.h>
#include <fuse/fuse_opt.h>

#include <sys/types.h>

#include <cerrno>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <stdexcept>

#include <boost/preprocessor/punctuation/comma_if.hpp>  // IWYU pragma: export
#include <boost/preprocessor/seq/for_each.hpp>  // IWYU pragma: export
#include <boost/preprocessor/seq/for_each_i.hpp>  // IWYU pragma: export
#include <boost/scope_exit.hpp>
#include <boost/typeof/typeof.hpp>
#include <boost/utility/enable_if.hpp>

#include "defs.hpp"
#include "service.hpp"

struct flock;
struct fuse_chan;
struct fuse_pollhandle;
struct fuse_session;
struct iovec;

namespace cpfs {

// Macros for defining forwarding functions

/**
 * Creates a formal argument preceded by comma in a
 * BOOST_PP_SEQ_FOR_EACH_I macro iterating a sequence of data types.
 *
 * @param r The recursion argument.
 *
 * @param data The data passed.
 *
 * @param i The index of the BOOST_PP_SEQ_FOR_EACH_I iteration.
 *
 * @param type The data type.
 */
#define FUSEOBJ_FORMAL(r, data, i, type) , type arg##i
/**
 * Creates an actual argument preceded by comma in a
 * BOOST_PP_SEQ_FOR_EACH_I macro iterating a sequence of data types.
 *
 * @param r The recursion argument.
 *
 * @param data The data passed.
 *
 * @param i The index of the BOOST_PP_SEQ_FOR_EACH_I iteration.
 *
 * @param type The data type.
 */
#define FUSEOBJ_ACTUAL(r, data, i, type) , arg##i
/**
 * Creates a formal argument preceded by comma unless i = 0 in a
 * BOOST_PP_SEQ_FOR_EACH_I macro iterating a sequence of data types.
 *
 * @param r The recursion argument.
 *
 * @param data The data passed.
 *
 * @param i The index of the BOOST_PP_SEQ_FOR_EACH_I iteration.
 *
 * @param type The data type.
 */
#define FUSEOBJ_FORMAL_1(r, data, i, type) BOOST_PP_COMMA_IF(i) type arg##i
/**
 * Creates an actual argument preceded by comma unless i = 0 in a
 * BOOST_PP_SEQ_FOR_EACH_I macro iterating a sequence of data types.
 *
 * @param r The recursion argument.
 *
 * @param data The data passed.
 *
 * @param i The index of the BOOST_PP_SEQ_FOR_EACH_I iteration.
 *
 * @param type The data type.
 */
#define FUSEOBJ_ACTUAL_1(r, data, i, type) BOOST_PP_COMMA_IF(i) arg##i

/**
 * Calls a macro given some argument, for BOOST_PP_SEQ_FOR_EACH to
 * iterate a sequence of arguments.  The main purpose of this macro is
 * to remove one matching pair of parentheses in each BOOST_PP
 * sequence element.
 *
 * @param r The recursion argument.
 *
 * @param macro The macro to call.
 *
 * @param args The arguments, parenthesized.  Note that the elements
 * in the sequence should, as usual in BOOST_PP sequences of
 * parenthesized values, be double-parenthesized.
 */
#define FUSEOBJ_CALL_MACRO(r, macro, args) macro args

/**
 * A BOOST_PP sequence of FUSE function descriptions.  It is mainly
 * for generation of policy.  It would also be useful outside the
 * header, e.g., to create a mock policy.
 */
#define FUSEOBJ_FUSEFUNCS                                                     \
  ((fuse_reply_err, ReplyErr, int, (fuse_req_t)(int)))                        \
  ((fuse_reply_none, ReplyNone, void, (fuse_req_t)))                          \
  ((fuse_reply_entry, ReplyEntry, int, (fuse_req_t)(const fuse_entry_param*)))\
  ((fuse_reply_create, ReplyCreate, int,                                      \
    (fuse_req_t)(const fuse_entry_param*)(const fuse_file_info*)))            \
  ((fuse_reply_attr, ReplyAttr, int,                                          \
    (fuse_req_t)(const struct stat*)(double)))                                \
  ((fuse_reply_readlink, ReplyReadlink, int, (fuse_req_t)(const char*)))      \
  ((fuse_reply_open, ReplyOpen, int, (fuse_req_t)(const fuse_file_info*)))    \
  ((fuse_reply_write, ReplyWrite, int, (fuse_req_t)(size_t)))                 \
  ((fuse_reply_buf, ReplyBuf, int, (fuse_req_t)(const char*)(size_t)))        \
  ((fuse_reply_data, ReplyData, int,                                          \
    (fuse_req_t)(fuse_bufvec*)(fuse_buf_copy_flags)))                         \
  ((fuse_reply_iov, ReplyIov, int, (fuse_req_t)(const iovec*)(int)))          \
  ((fuse_reply_statfs, ReplyStatfs, int,                                      \
    (fuse_req_t)(const struct statvfs*)))                                     \
  ((fuse_reply_xattr, ReplyXattr, int, (fuse_req_t)(size_t)))                 \
  ((fuse_reply_lock, ReplyLock, int, (fuse_req_t)(const flock*)))             \
  ((fuse_reply_bmap, ReplyBmap, int, (fuse_req_t)(uint64_t)))                 \
  ((fuse_lowlevel_notify_inval_inode, NotifyInvalInode, int,                  \
    (fuse_chan *)(fuse_ino_t)(off_t)(off_t)))                                 \
  ((fuse_lowlevel_notify_inval_entry, NotifyInvalEntry, int,                  \
    (fuse_chan *)(fuse_ino_t)(const char *)(size_t)))                         \
  ((fuse_add_direntry, AddDirentry, size_t,                                   \
    (fuse_req_t)(char*)(size_t)(const char*)(const struct stat *)(off_t)))    \
  ((fuse_req_ctx, ReqCtx, const fuse_ctx*, (fuse_req_t)))

/**
 * Calls macro for each reply description in FUSEOBJ_FUSEFUNCS.
 *
 * @param macro The macro to call.
 */
#define FUSEOBJ_FOR_FUSEFUNC(macro)                                           \
  BOOST_PP_SEQ_FOR_EACH(FUSEOBJ_CALL_MACRO, macro, FUSEOBJ_FUSEFUNCS)

/**
 * FUSE method policy.  The class has a (non-static) method per
 * function in FUSEOBJ_FUSEFUNCS.  To make it possible to perform unit
 * tests of the Fuse objects (while not actually calling Fuse various
 * functions like reply methods), they can take a template argument
 * which is normally a FuseMethodPolicy, and use it to call the FUSE
 * functions.
 */
class FuseMethodPolicy {
 public:
/** @cond */
# define FUSEOBJ_MAKE_FUSE_FORWARDER(fname, mname, rtype, args)               \
    rtype mname(BOOST_PP_SEQ_FOR_EACH_I(FUSEOBJ_FORMAL_1, _, args)) {         \
      return fname(BOOST_PP_SEQ_FOR_EACH_I(FUSEOBJ_ACTUAL_1, _, args));       \
    }
/** @endcond */
  FUSEOBJ_FOR_FUSEFUNC(FUSEOBJ_MAKE_FUSE_FORWARDER)
# undef FUSEOBJ_MAKE_FUSE_FORWARDER
};

/**
 * A base class for creating Fuse objects.
 *
 * @tparam TFuseMethodPolicy The FUSE method policy class to use.
 */
template <typename TFuseMethodPolicy = FuseMethodPolicy>
class BaseFuseObj : protected TFuseMethodPolicy {
 public:
  /**
   * @param fuse_method_policy The Fuse method policy to use.
   */
  explicit BaseFuseObj(TFuseMethodPolicy fuse_method_policy
                       = TFuseMethodPolicy())
      : TFuseMethodPolicy(fuse_method_policy) {}
  virtual ~BaseFuseObj() {}
  /**
   * Initializes the filesystem.
   *
   * @param conn The FUSE connection information object.
   */
  virtual void Init(struct fuse_conn_info* conn) {
      (void) conn;
  }
  /**
   * Destroys the filesystem.
   */
  virtual void Destroy() {
  }
  /**
   * Parses additional filesystem options.
   *
   * @param args The arguments to parse.
   */
  virtual void ParseFsOpts(fuse_args* args) {
      (void) args;
  }
  /**
   * Call when fuse_chan is ready.
   */
  virtual void SetFuseChannel(fuse_chan* chan) {
      (void) chan;
  }
  /**
   * Get the fuse method policy.
   */
  virtual TFuseMethodPolicy* fuse_method_policy() {
    return this;
  }
};

// FUSE method descriptions

/**
 * A BOOST_PP sequence of FUSE method descriptions in FUSE 2.4.
 */
#define FUSEOBJ_METHODS_24                                                    \
  ((lookup, Lookup, (fuse_ino_t)(const char*)))                               \
  ((forget, Forget, (fuse_ino_t)(unsigned long)))                             \
  ((getattr, Getattr, (fuse_ino_t)(fuse_file_info*)))                         \
  ((setattr, Setattr, (fuse_ino_t)(struct stat*)(int)(fuse_file_info*)))      \
  ((readlink, Readlink, (fuse_ino_t)))                                        \
  ((mknod, Mknod, (fuse_ino_t)(const char*)(mode_t)(dev_t)))                  \
  ((mkdir, Mkdir, (fuse_ino_t)(const char*)(mode_t)))                         \
  ((unlink, Unlink, (fuse_ino_t)(const char*)))                               \
  ((rmdir, Rmdir, (fuse_ino_t)(const char*)))                                 \
  ((symlink, Symlink, (const char*)(fuse_ino_t)(const char*)))                \
  ((rename, Rename, (fuse_ino_t)(const char*)(fuse_ino_t)(const char*)))      \
  ((link, Link, (fuse_ino_t)(fuse_ino_t)(const char*)))                       \
  ((open, Open, (fuse_ino_t)(fuse_file_info*)))                               \
  ((read, Read, (fuse_ino_t)(size_t)(off_t)(fuse_file_info*)))                \
  ((write, Write, (fuse_ino_t)(const char*)(size_t)(off_t)(fuse_file_info*))) \
  ((flush, Flush, (fuse_ino_t)(fuse_file_info*)))                             \
  ((release, Release, (fuse_ino_t)(fuse_file_info*)))                         \
  ((fsync, Fsync, (fuse_ino_t)(int)(fuse_file_info*)))                        \
  ((opendir, Opendir, (fuse_ino_t)(fuse_file_info*)))                         \
  ((readdir, Readdir, (fuse_ino_t)(size_t)(off_t)(fuse_file_info*)))          \
  ((releasedir, Releasedir, (fuse_ino_t)(fuse_file_info*)))                   \
  ((fsyncdir, Fsyncdir, (fuse_ino_t)(int)(fuse_file_info*)))                  \
  ((statfs, Statfs, (fuse_ino_t)))                                            \
  ((setxattr, Setxattr, (fuse_ino_t)(const char*)(const char*)(size_t)(int))) \
  ((getxattr, Getxattr, (fuse_ino_t)(const char*)(size_t)))                   \
  ((listxattr, Listxattr, (fuse_ino_t)(size_t)))                              \
  ((removexattr, Removexattr, (fuse_ino_t)(const char*)))
/**
 * A BOOST_PP sequence of FUSE method descriptions added in FUSE 2.5.
 */
#define FUSEOBJ_METHODS_25                                                    \
  ((access, Access, (fuse_ino_t)(int)))                                       \
  ((create, Create, (fuse_ino_t)(const char*)(mode_t)(fuse_file_info*)))
/**
 * A BOOST_PP sequence of FUSE method descriptions added in FUSE 2.6.
 */
#define FUSEOBJ_METHODS_26                                                    \
  ((getlk, Getlk, (fuse_ino_t)(fuse_file_info*)(flock*)))                     \
  ((setlk, Setlk, (fuse_ino_t)(fuse_file_info*)(flock*)(int)))                \
  ((bmap, Bmap, (fuse_ino_t)(size_t)(uint64_t)))
/**
 * A BOOST_PP sequence of FUSE method descriptions added in FUSE 2.8.
 */
#define FUSEOBJ_METHODS_28                                                    \
  ((ioctl, Ioctl, (fuse_ino_t)(int)(void*)(fuse_file_info*)(unsigned)         \
    (const void*)(size_t)(size_t)))                                           \
  ((poll, Poll, (fuse_ino_t)(fuse_file_info*)(fuse_pollhandle*)))
/**
 * A BOOST_PP sequence of FUSE method descriptions added in FUSE 2.9.
 */
#define FUSEOBJ_METHODS_29                                                    \
  ((write_buf, WriteBuf, (fuse_ino_t)(fuse_bufvec*)(off_t)(fuse_file_info*))) \
  ((forget_multi, ForgetMulti, (size_t)(fuse_forget_data*)))                  \
  ((flock, Flock, (fuse_ino_t)(fuse_file_info*)(int)))                        \
  ((fallocate, Fallocate, (fuse_ino_t)(int)(off_t)(off_t)(fuse_file_info*)))

/**
 * A BOOST_PP sequence of FUSE method descriptions.  Each description
 * is a parenthesized (FUSE method name, FuseObj method name, arg
 * list).
 */
#if FUSE_USE_VERSION < 26
# define FUSEOBJ_METHODS FUSEOBJ_METHODS_24 FUSEOBJ_METHODS_25
#else
# define FUSEOBJ_METHODS FUSEOBJ_METHODS_24 FUSEOBJ_METHODS_25                \
    FUSEOBJ_METHODS_26 FUSEOBJ_METHODS_28 FUSEOBJ_METHODS_29
#endif

/**
 * Calls macro for each method description in FUSEOBJ_METHODS.
 *
 * @param macro The macro to call.
 */
#define FUSEOBJ_FOR_METHOD(macro)                                             \
  BOOST_PP_SEQ_FOR_EACH(FUSEOBJ_CALL_MACRO, macro, FUSEOBJ_METHODS)

/** @cond */
// Defines a meta-function called FuseObjHasMname, to check whether
// Mname is in a type.  It is used like FuseObjHasMname<T>::value =>
// true iff Mname is defined in T.  The fname and args argument are
// not used.
#define FUSEOBJ_DEFINE_MEMFN_CHECK(fname, mname, args)                        \
  template <typename T>                                                       \
  class FuseObjHas ## mname {                                                 \
    typedef char yes;                                                         \
    typedef struct { char val[2]; } no;                                       \
    template <typename TTest> static yes test(BOOST_TYPEOF(&TTest::mname));   \
    template <typename TTest> static no test(...);                            \
   public:                                                                    \
    enum { value = sizeof(test<T>(0)) == sizeof(yes) };                       \
  };
FUSEOBJ_FOR_METHOD(FUSEOBJ_DEFINE_MEMFN_CHECK)
#undef FUSEOBJ_DEFINE_MEMFN_CHECK
/** @endcond */

/** @cond */
#define ENSURE(cond) do { if (!(cond)) throw std::runtime_error(#cond); }      \
  while (0)
/** @endcond */

/**
 * A base class for FUSE FS runner.  It runs a FS using a Fuse object.
 *
 * @tparam TFuseObj The Fuse object class to use.
 */
template <typename TFuseObj>
class BaseFuseRunner : public IService {
 public:
  /**
   * @param fuseobj The FS object.
   */
  explicit BaseFuseRunner(TFuseObj* fuseobj)
      : fuseobj_(fuseobj), running_(false) {}

  /**
   * Set the arguments to be used for during Run().
   *
   * @param argc The number of arguments.
   *
   * @param argv The arguments.
   */
  void SetArgs(int argc, char** argv) {
    argc_ = argc;
    argv_ = argv;
  }

  /**
   * Do nothing, just to satisfy IService interface.
   */
  void Init() {}

  /**
   * IService interface to run server.
   */
  int Run() {
    return Main(argc_, argv_);
  }

  /**
   * Ask FUSE to exit.
   */
  void Shutdown() {
    if (running_)
      pthread_kill(runner_, SIGTERM);
  }

  /**
   * Runs the filesystem.
   *
   * @param argc The number of arguments.
   *
   * @param argv The arguments.
   */
  int Main(int argc, char* argv[]) {
    std::memset(&fs_ops_, 0, sizeof(fs_ops_));
    MakeFsOps();

    fuse_args args = FUSE_ARGS_INIT(argc, argv);
    BOOST_SCOPE_EXIT_TPL(&args) {
      fuse_opt_free_args(&args);
    } BOOST_SCOPE_EXIT_END;

    char* mountpoint = 0;
    int foreground;
    ENSURE(fuse_parse_cmdline(&args, &mountpoint,
                              &multithreaded_, &foreground) != -1);
    BOOST_SCOPE_EXIT_TPL(&mountpoint) {
      free(mountpoint);
    } BOOST_SCOPE_EXIT_END;

    fuseobj_->ParseFsOpts(&args);

    fuse_chan* ch = fuse_mount(mountpoint, &args);
    if (ch == NULL)
      return 1;
    fuseobj_->SetFuseChannel(ch);
    BOOST_SCOPE_EXIT_TPL(&mountpoint, &ch) {
      fuse_unmount(mountpoint, ch);
    } BOOST_SCOPE_EXIT_END;

    ENSURE(fuse_daemonize(foreground) == 0);
    fuse_session* se =
        fuse_lowlevel_new(&args, &fs_ops_, sizeof(fs_ops_), fuseobj_);
    if (se == NULL)
      return 0;
    BOOST_SCOPE_EXIT_TPL(se) {
      fuse_session_destroy(se);
    } BOOST_SCOPE_EXIT_END;

    ENSURE(fuse_set_signal_handlers(se) != -1);
    BOOST_SCOPE_EXIT_TPL(se) {
      fuse_remove_signal_handlers(se);
    } BOOST_SCOPE_EXIT_END;

    fuse_session_add_chan(se, ch);
    BOOST_SCOPE_EXIT_TPL(&ch) {
      fuse_session_remove_chan(ch);
    } BOOST_SCOPE_EXIT_END;

    runner_ = pthread_self();
    running_ = true;
    RunLoop(se);
    running_ = false;
    return 0;
  }

  /**
   * Runs the Fuse event loop.  Allow subclasses to override to use an
   * implementation other than calling fuse_session_loop.
   */
  virtual int RunLoop(fuse_session* se) {
    if (multithreaded_)
      return fuse_session_loop_mt(se);
    else
      return fuse_session_loop(se);
  }

 private:
  TFuseObj* fuseobj_; /**< The fuse object */
  int argc_; /**< The argument count when Run() interface is used */
  char** argv_; /**< The arguments when Run() interface is used */
  int multithreaded_; /**< Whether the fuse object is running in multithread */
  pthread_t runner_; /**< The Pthread executing the loop */
  bool running_; /**< Whether runner_ is valid */

  /**
   * Creates the fuse_lowlevel_ops structure according to the method
   * provided by the Fuse object.  It uses C++ SFINAE tricks to test
   * whether methods are defined in the Fuse object provided in the
   * template argument.  If the method is not provided, the structure
   * content remains to be the NULL pointer, allowing FUSE to use the
   * default implementation.  Exceptions thrown in such methods will
   * be converted to FUSE error replies of the error EIO.
   */
  void MakeFsOps() {
    fs_ops_.init = ForwardInit;
    fs_ops_.destroy = ForwardDestroy;
/** @cond */
    // Here we call the SetMname method for each FUSE method.  The
    // SetMname methods set the mname field in the Fuse lowlevel ops
    // structure to ForwardMname if FuseObjHasMname::value is true.
    // The structure is actually the same as Init and Destroy, but (1)
    // they are done using SFINAE techniques, so that the structure is
    // not touched if the method is not defined; and (2) they receives
    // the fuse_req_t argument, and call fuse_reply_err if anything
    // goes wrong.
#   define FUSEOBJ_CALL_SETTER(fname, mname, args)                            \
      Set ## mname ## Op<TFuseObj>();
/** @endcond */
    FUSEOBJ_FOR_METHOD(FUSEOBJ_CALL_SETTER)
#   undef FUSEOBJ_CALL_SETTER
  }

# if FUSE_USE_VERSION < 26
    static void ForwardInit(void* userdata) {
      reinterpret_cast<TFuseObj*>(userdata)->Init(0);
    }
# else
    static void ForwardInit(void* userdata, struct fuse_conn_info* conn) {
      reinterpret_cast<TFuseObj*>(userdata)->Init(conn);
    }
# endif

  static void ForwardDestroy(void* userdata) {
    reinterpret_cast<TFuseObj*>(userdata)->Destroy();
  }

/** @cond */
# define FUSEOBJ_DEFINE_SETTER(fname, mname, args)                            \
    /* SetXxxOp: set xxx to forwarding function &Xxx, if defined */           \
    template<typename TTest>                                                  \
    typename boost::enable_if<                                                \
      cpfs::FuseObjHas ## mname<TTest>, void>::type                           \
    Set ## mname ## Op() {                                                    \
      fs_ops_.fname = Forward ## mname<TTest>;                                \
    }                                                                         \
    template<typename TTest>                                                  \
    typename boost::disable_if<                                               \
      cpfs::FuseObjHas ## mname<TTest>, void>::type                           \
    Set ## mname ## Op() {}                                                   \
    /* Forwarding function Xxx, defined only if pred::value is true */        \
    template<typename TTest>                                                  \
    static typename boost::enable_if<                                         \
      cpfs::FuseObjHas ## mname<TTest>, void>::type                           \
    Forward ## mname(fuse_req_t req                                           \
                     BOOST_PP_SEQ_FOR_EACH_I(FUSEOBJ_FORMAL, _, args)) {      \
      try {                                                                   \
        reinterpret_cast<TFuseObj*>(fuse_req_userdata(req))->mname(req        \
          BOOST_PP_SEQ_FOR_EACH_I(FUSEOBJ_ACTUAL, _, args));                  \
      } catch (...) {                                                         \
        reinterpret_cast<TFuseObj*>(fuse_req_userdata(req))->                 \
          fuse_method_policy()->ReplyErr(req, EIO);                           \
      }                                                                       \
    };
/** @endcond */
  FUSEOBJ_FOR_METHOD(FUSEOBJ_DEFINE_SETTER)
# undef FUSEOBJ_DEFINE_SETTER

  struct fuse_lowlevel_ops fs_ops_;
};

#undef ENSURE

}  // namespace cpfs
