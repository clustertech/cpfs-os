/* Copyright 2013 ClusterTech Ltd */
#include "server/ms/ugid_handler_impl.hpp"

#include <grp.h>
#include <pwd.h>
#include <unistd.h>

#include <boost/scope_exit.hpp>
#include <boost/scoped_ptr.hpp>

#include <gtest/gtest.h>

#include "server/ms/ugid_handler.hpp"

namespace cpfs {
namespace server {
namespace ms {
namespace {

TEST(UgidHandlerTest, Basic) {
  boost::scoped_ptr<IUgidHandler> handler(MakeUgidHandler(1));
  handler->SetCheck(true);
  {
    handler->SetFsIds(getuid(), getgid());
  }
  {
    handler->SetFsIds(0, 0);
  }
  handler->SetCheck(false);
  {
    handler->SetFsIds(0, 0);
  }
}

TEST(UgidHandlerTest, SupplementaryGroupId) {
  boost::scoped_ptr<IUgidHandler> handler(MakeUgidHandler(1));
  handler->SetCheck(true);
  handler->Clean();
  setgrent();
  BOOST_SCOPE_EXIT(void) {
    endgrent();
  } BOOST_SCOPE_EXIT_END;
  uid_t my_uid = getuid();
  bool found_nonempty = false, found_nonmember = false;
  struct group* grp;
  while ((!found_nonempty || !found_nonmember) && (grp = getgrent())) {
    bool is_member = false;
    for (int i = 0; ; ++i) {
      if (!grp->gr_mem[i])
        break;
      uid_t uid = getpwnam(grp->gr_mem[i])  // NOLINT(runtime/threadsafe_fn)
          ->pw_uid;
      if (uid == my_uid)
        is_member = true;
      found_nonempty = true;
      EXPECT_TRUE(handler->HasSupplementaryGroup(uid, grp->gr_gid));
    }
    if (!is_member) {
      found_nonmember = true;
      EXPECT_FALSE(handler->HasSupplementaryGroup(my_uid, grp->gr_gid));
    }
  }
  sleep(1);
  handler->Clean();
  uid_t unused_uid;
  for (unused_uid = my_uid + 1; ; ++unused_uid) {
    char buf[16384];
    struct passwd pwd_ent, *pwd_res;
    getpwuid_r(unused_uid, &pwd_ent, buf, sizeof(buf), &pwd_res);
    if (pwd_res == 0)
      break;
  }
  EXPECT_FALSE(handler->HasSupplementaryGroup(unused_uid, grp->gr_gid));
}

}  // namespace
}  // namespace ms
}  // namespace server
}  // namespace cpfs
