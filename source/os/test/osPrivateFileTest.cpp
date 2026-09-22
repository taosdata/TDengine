/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#define ALLOW_FORBID_FUNC
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>
#include "os.h"
#ifdef WINDOWS
#include <aclapi.h>
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace {
class FileHandle {
 public:
  explicit FileHandle(TdFilePtr value) : value_(value) {}
  ~FileHandle() { taosCloseFile(&value_); }
  TdFilePtr get() const { return value_; }

 private:
  TdFilePtr value_;
};

class PrivateFileTest : public testing::Test {
 protected:
  std::string root;
  std::string path(const char* name) const { return root + TD_DIRSEP + name; }
  void        SetUp() override {
#ifdef WINDOWS
    char temp[PATH_MAX] = {0};
    ASSERT_GT(GetTempPathA(sizeof(temp), temp), 0U);
    char candidate[PATH_MAX] = {0};
    taosGetTmpfilePath(temp, "private-file-test", candidate);
    ASSERT_TRUE(CreateDirectoryA(candidate, nullptr));
    root = candidate;
#else
    char candidate[] = "/tmp/tdengine-private-file-XXXXXX";
    ASSERT_NE(mkdtemp(candidate), nullptr);
    root = candidate;
#endif
  }
  void TearDown() override {
    if (root.empty()) return;
    // Remove links explicitly; recursive cleanup must never enter their targets.
#ifdef WINDOWS
    RemoveDirectoryA(path("link").c_str());
    DeleteFileA(path("file-link").c_str());
#else
    unlink(path("link").c_str());
    unlink(path("file-link").c_str());
#endif
    taosRemoveDir(root.c_str());
  }
};

TEST_F(PrivateFileTest, ExclusiveCreateAndReadIdentity) {
  const auto name = path("data");
  FileHandle first(taosOpenPrivateFile(name.c_str(), TD_FILE_CREATE | TD_FILE_EXCL | TD_FILE_READ | TD_FILE_WRITE));
  ASSERT_NE(first.get(), nullptr);
  ASSERT_EQ(taosWriteFile(first.get(), "data", 4), 4);
  FileHandle duplicate(taosOpenPrivateFile(name.c_str(), TD_FILE_CREATE | TD_FILE_EXCL | TD_FILE_WRITE));
  EXPECT_EQ(duplicate.get(), nullptr);
  FileHandle reader(taosOpenPrivateFile(name.c_str(), TD_FILE_READ));
  ASSERT_NE(reader.get(), nullptr);
  int64_t dev1 = 0, ino1 = 0, dev2 = 0, ino2 = 0;
  ASSERT_EQ(taosDevInoFile(first.get(), &dev1, &ino1), 0);
  ASSERT_EQ(taosDevInoFile(reader.get(), &dev2, &ino2), 0);
  EXPECT_EQ(dev1, dev2);
  EXPECT_EQ(ino1, ino2);
  char data[4] = {0};
  ASSERT_EQ(taosReadFile(reader.get(), data, sizeof(data)), sizeof(data));
  EXPECT_EQ(std::string(data, sizeof(data)), "data");
}

TEST_F(PrivateFileTest, MissingReadDoesNotCreate) {
  const auto name = path("missing");
  FileHandle reader(taosOpenPrivateFile(name.c_str(), TD_FILE_READ));
  EXPECT_EQ(reader.get(), nullptr);
  EXPECT_FALSE(taosCheckExistFile(name.c_str()));
}

TEST_F(PrivateFileTest, RejectsDestructiveOrAmbiguousFlags) {
  for (int flags : {TD_FILE_CREATE | TD_FILE_WRITE, TD_FILE_CREATE | TD_FILE_EXCL | TD_FILE_TRUNC | TD_FILE_WRITE,
                    TD_FILE_WRITE, TD_FILE_READ | TD_FILE_EXCL}) {
    const auto name = path("invalid");
    FileHandle file(taosOpenPrivateFile(name.c_str(), flags));
    EXPECT_EQ(file.get(), nullptr) << flags;
  }
  EXPECT_FALSE(taosCheckExistFile(path("invalid").c_str()));
}

TEST_F(PrivateFileTest, RejectsDirectoryAsFileAndFileAsDirectory) {
  FileHandle file(taosOpenPrivateFile(path("data").c_str(), TD_FILE_CREATE | TD_FILE_EXCL | TD_FILE_WRITE));
  ASSERT_NE(file.get(), nullptr);
  EXPECT_NE(taosEnsurePrivateDir(path("data").c_str()), 0);
  FileHandle dir(taosOpenPrivateFile(root.c_str(), TD_FILE_READ));
  EXPECT_EQ(dir.get(), nullptr);
}

#ifdef WINDOWS
static void expectPrivateAcl(const std::string& name, PSID expectedOwner = nullptr) {
  HANDLE token = nullptr;
  ASSERT_TRUE(OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &token));
  DWORD size = 0;
  GetTokenInformation(token, TokenUser, nullptr, 0, &size);
  std::vector<char> user(size);
  const BOOL        gotUser = GetTokenInformation(token, TokenUser, user.data(), size, &size);
  CloseHandle(token);
  ASSERT_TRUE(gotUser);
  PSID                 owner = nullptr;
  PACL                 acl = nullptr;
  PSECURITY_DESCRIPTOR descriptor = nullptr;
  ASSERT_EQ(GetNamedSecurityInfoA(const_cast<char*>(name.c_str()), SE_FILE_OBJECT,
                                  OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION, &owner, nullptr, &acl,
                                  nullptr, &descriptor),
            ERROR_SUCCESS);
  // Keep the system-allocated descriptor alive through all owner and ACE checks.
  const auto cleanup = std::unique_ptr<void, decltype(&LocalFree)>(descriptor, &LocalFree);
  const auto userSid = reinterpret_cast<TOKEN_USER*>(user.data())->User.Sid;
  EXPECT_TRUE(EqualSid(owner, expectedOwner != nullptr ? expectedOwner : userSid));
  ASSERT_NE(acl, nullptr);
  EXPECT_EQ(acl->AceCount, 1);
  void* rawAce = nullptr;
  ASSERT_TRUE(GetAce(acl, 0, &rawAce));
  const auto ace = static_cast<ACCESS_ALLOWED_ACE*>(rawAce);
  EXPECT_EQ(ace->Header.AceType, ACCESS_ALLOWED_ACE_TYPE);
  EXPECT_EQ(ace->Header.AceFlags, 0);
  EXPECT_TRUE(EqualSid(&ace->SidStart, userSid));
  EXPECT_EQ(ace->Mask & FILE_ALL_ACCESS, FILE_ALL_ACCESS);
  SECURITY_DESCRIPTOR_CONTROL control = 0;
  DWORD                       revision = 0;
  ASSERT_TRUE(GetSecurityDescriptorControl(descriptor, &control, &revision));
  EXPECT_NE(control & SE_DACL_PROTECTED, 0);
}

TEST_F(PrivateFileTest, DirectoryAndFileAcls) {
  const auto dir = path("private");
  ASSERT_EQ(taosEnsurePrivateDir(dir.c_str()), 0);
  expectPrivateAcl(dir);
  ASSERT_EQ(SetNamedSecurityInfoA(const_cast<char*>(dir.c_str()), SE_FILE_OBJECT, DACL_SECURITY_INFORMATION, nullptr,
                                  nullptr, nullptr, nullptr),
            ERROR_SUCCESS);
  ASSERT_EQ(taosEnsurePrivateDir((dir + "\\").c_str()), 0);
  expectPrivateAcl(dir);
  FileHandle file(taosOpenPrivateFile(path("data").c_str(), TD_FILE_CREATE | TD_FILE_EXCL | TD_FILE_WRITE));
  ASSERT_NE(file.get(), nullptr);
  expectPrivateAcl(path("data"));
}

TEST_F(PrivateFileTest, ReusesDirectoryWithDefaultTokenOwner) {
  HANDLE token = nullptr;
  ASSERT_TRUE(OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &token));
  DWORD size = 0;
  GetTokenInformation(token, TokenOwner, nullptr, 0, &size);
  std::vector<char> owner(size);
  const BOOL        gotOwner = GetTokenInformation(token, TokenOwner, owner.data(), size, &size);
  CloseHandle(token);
  ASSERT_TRUE(gotOwner);
  const auto ownerSid = reinterpret_cast<TOKEN_OWNER*>(owner.data())->Owner;
  const auto dir = path("existing");
  // Windows services can create directories owned by their default group SID.
  ASSERT_TRUE(CreateDirectoryA(dir.c_str(), nullptr));
  ASSERT_EQ(taosEnsurePrivateDir(dir.c_str()), 0);
  expectPrivateAcl(dir, ownerSid);
  ASSERT_EQ(taosEnsurePrivateDir((dir + "\\").c_str()), 0);
  expectPrivateAcl(dir, ownerSid);
  const auto name = dir + TD_DIRSEP + "data";
  FileHandle file(taosOpenPrivateFile(name.c_str(), TD_FILE_CREATE | TD_FILE_EXCL | TD_FILE_WRITE));
  ASSERT_NE(file.get(), nullptr);
  ASSERT_EQ(taosWriteFile(file.get(), "data", 4), 4);
  expectPrivateAcl(name);
  FileHandle reader(taosOpenPrivateFile(name.c_str(), TD_FILE_READ));
  ASSERT_NE(reader.get(), nullptr);
  char data[4] = {0};
  ASSERT_EQ(taosReadFile(reader.get(), data, sizeof(data)), sizeof(data));
  EXPECT_EQ(std::string(data, sizeof(data)), "data");
}

TEST_F(PrivateFileTest, RejectsReparsePointsIncludingTrailingSeparator) {
  const auto target = path("target");
  ASSERT_TRUE(CreateDirectoryA(target.c_str(), nullptr));
  if (!CreateSymbolicLinkA(path("link").c_str(), target.c_str(), SYMBOLIC_LINK_FLAG_DIRECTORY | 0x2)) {
    GTEST_SKIP() << "This Windows test process cannot create symbolic links: " << GetLastError();
  }
  EXPECT_NE(taosEnsurePrivateDir(path("link").c_str()), 0);
  EXPECT_NE(taosEnsurePrivateDir((path("link") + "\\").c_str()), 0);
  FileHandle file(taosOpenPrivateFile(path("data").c_str(), TD_FILE_CREATE | TD_FILE_EXCL | TD_FILE_WRITE));
  ASSERT_NE(file.get(), nullptr);
  ASSERT_TRUE(CreateSymbolicLinkA(path("file-link").c_str(), path("data").c_str(), 0x2));
  FileHandle reader(taosOpenPrivateFile(path("file-link").c_str(), TD_FILE_READ));
  EXPECT_EQ(reader.get(), nullptr);
}

TEST_F(PrivateFileTest, RejectsForeignOwnerWithoutChangingPermissions) {
  GTEST_SKIP() << "No isolated foreign-owner Windows fixture is available";
}
#else
TEST_F(PrivateFileTest, DirectoryAndFileModes) {
  const auto dir = path("private");
  ASSERT_EQ(mkdir(dir.c_str(), 0777), 0);
  ASSERT_EQ(chmod(dir.c_str(), 0777), 0);
  ASSERT_EQ(taosEnsurePrivateDir((dir + "/").c_str()), 0);
  struct stat info = {};
  ASSERT_EQ(stat(dir.c_str(), &info), 0);
  EXPECT_EQ(info.st_uid, geteuid());
  EXPECT_EQ(info.st_mode & 07777, 0700);
  const auto created = path("new");
  ASSERT_EQ(taosEnsurePrivateDir(created.c_str()), 0);
  ASSERT_EQ(stat(created.c_str(), &info), 0);
  EXPECT_EQ(info.st_mode & 07777, 0700);
  const mode_t saved = umask(0);
  FileHandle   file(taosOpenPrivateFile(path("data").c_str(), TD_FILE_CREATE | TD_FILE_EXCL | TD_FILE_WRITE));
  umask(saved);
  ASSERT_NE(file.get(), nullptr);
  ASSERT_EQ(stat(path("data").c_str(), &info), 0);
  EXPECT_EQ(info.st_uid, geteuid());
  EXPECT_EQ(info.st_mode & 07777, 0600);
}

TEST_F(PrivateFileTest, RejectsLinksIncludingTrailingSeparator) {
  const auto target = path("target");
  ASSERT_EQ(mkdir(target.c_str(), 0755), 0);
  ASSERT_EQ(chmod(target.c_str(), 0755), 0);
  ASSERT_EQ(symlink(target.c_str(), path("link").c_str()), 0);
  EXPECT_NE(taosEnsurePrivateDir(path("link").c_str()), 0);
  EXPECT_NE(taosEnsurePrivateDir((path("link") + "/").c_str()), 0);
  struct stat info = {};
  ASSERT_EQ(stat(target.c_str(), &info), 0);
  EXPECT_EQ(info.st_mode & 07777, 0755);
  FileHandle file(taosOpenPrivateFile(path("data").c_str(), TD_FILE_CREATE | TD_FILE_EXCL | TD_FILE_WRITE));
  ASSERT_NE(file.get(), nullptr);
  ASSERT_EQ(symlink(path("data").c_str(), path("file-link").c_str()), 0);
  FileHandle reader(taosOpenPrivateFile(path("file-link").c_str(), TD_FILE_READ));
  EXPECT_EQ(reader.get(), nullptr);
}

TEST_F(PrivateFileTest, RejectsNonRegularFile) {
  ASSERT_EQ(mkfifo(path("fifo").c_str(), 0600), 0);
  const int keeper = open(path("fifo").c_str(), O_RDWR | O_NONBLOCK);
  ASSERT_GE(keeper, 0);
  FileHandle file(taosOpenPrivateFile(path("fifo").c_str(), TD_FILE_READ));
  close(keeper);
  EXPECT_EQ(file.get(), nullptr);
}

TEST_F(PrivateFileTest, RejectsForeignOwnerWithoutChangingPermissions) {
  if (geteuid() != 0) GTEST_SKIP() << "Changing ownership of an isolated fixture requires an already-root test process";
  const auto dir = path("foreign");
  ASSERT_EQ(mkdir(dir.c_str(), 0755), 0);
  ASSERT_EQ(chmod(dir.c_str(), 0755), 0);
  ASSERT_EQ(chown(dir.c_str(), 1, static_cast<gid_t>(-1)), 0);
  EXPECT_NE(taosEnsurePrivateDir(dir.c_str()), 0);
  struct stat info = {};
  ASSERT_EQ(stat(dir.c_str(), &info), 0);
  EXPECT_EQ(info.st_uid, 1U);
  EXPECT_EQ(info.st_mode & 07777, 0755);
  ASSERT_EQ(chown(dir.c_str(), geteuid(), static_cast<gid_t>(-1)), 0);
  FileHandle file(taosOpenPrivateFile(path("data").c_str(), TD_FILE_CREATE | TD_FILE_EXCL | TD_FILE_WRITE));
  ASSERT_NE(file.get(), nullptr);
  ASSERT_EQ(chown(path("data").c_str(), 1, static_cast<gid_t>(-1)), 0);
  FileHandle reader(taosOpenPrivateFile(path("data").c_str(), TD_FILE_READ));
  EXPECT_EQ(reader.get(), nullptr);
  ASSERT_EQ(chown(path("data").c_str(), geteuid(), static_cast<gid_t>(-1)), 0);
}
#endif
}  // namespace
