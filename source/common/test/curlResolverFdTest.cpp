/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

// Regression test for CVE-2025-0665 (curl 8.11.1): with the threaded resolver and eventfd, libcurl closed the
// resolver's eventfd twice. The second close() returns EBADF, or, if another thread has just opened a file or socket
// that reused the fd number, silently closes that unrelated descriptor. In a taosd with shared storage (S3) enabled
// and a host-name endpoint this surfaced as random EBADF / garbage reads on tsdb files and, in the worst case, as a
// crashed process.
//
// The test is hermetic: the linker wraps getaddrinfo() so a made-up host name resolves to 127.0.0.1 (this still goes
// through libcurl's resolver thread, unlike an IP literal or "localhost"), and wraps close() so every EBADF is counted.
// The wraps (-Wl,--wrap=close -Wl,--wrap=getaddrinfo) also apply to the statically linked libcurl objects.

#include <arpa/inet.h>
#include <curl/curl.h>
#include <errno.h>
#include <gtest/gtest.h>
#include <netdb.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>

extern "C" {
int __real_close(int fd);
int __real_getaddrinfo(const char *node, const char *service, const struct addrinfo *hints, struct addrinfo **res);
}

static const char *kFakeHost = "td-curl-resolver-fd-test.invalid";
static std::atomic<int> gBadClose(0);
static std::atomic<int> gFakeResolve(0);

extern "C" int __wrap_close(int fd) {
  int rc = __real_close(fd);
  int err = errno;
  if (rc == -1 && err == EBADF) {
    gBadClose.fetch_add(1);
  }
  errno = err;
  return rc;
}

extern "C" int __wrap_getaddrinfo(const char *node, const char *service, const struct addrinfo *hints,
                                  struct addrinfo **res) {
  if (node != NULL && strcmp(node, kFakeHost) == 0) {
    gFakeResolve.fetch_add(1);
    return __real_getaddrinfo("127.0.0.1", service, hints, res);
  }
  return __real_getaddrinfo(node, service, hints, res);
}

static size_t discardBody(char *, size_t size, size_t nmemb, void *) { return size * nmemb; }

// returns a loopback port nobody listens on, so that the request fails fast with "connection refused" and
// the test never waits on the network
static int pickClosedLoopbackPort() {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return -1;
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  socklen_t len = sizeof(addr);
  int       port = -1;
  if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) == 0 && getsockname(fd, (struct sockaddr *)&addr, &len) == 0) {
    port = ntohs(addr.sin_port);
  }
  (void)close(fd);
  return port;
}

TEST(curlResolverFd, resolverThreadDoesNotCloseEventfdTwice) {
  ASSERT_EQ(curl_global_init(CURL_GLOBAL_ALL), CURLE_OK);

  int port = pickClosedLoopbackPort();
  ASSERT_GT(port, 0);
  char url[128];
  snprintf(url, sizeof(url), "http://%s:%d/", kFakeHost, port);

  gBadClose.store(0);
  gFakeResolve.store(0);

  const int kRequests = 8;
  for (int i = 0; i < kRequests; ++i) {
    // a fresh handle every time: libcurl caches DNS results per handle, a fresh one forces a new resolver thread
    CURL *curl = curl_easy_init();
    ASSERT_NE(curl, nullptr);
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, discardBody);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 3000L);
    CURLcode rc = curl_easy_perform(curl);
    curl_easy_cleanup(curl);

    // the name must have been resolved (through the wrapped getaddrinfo), the connect is expected to be refused
    ASSERT_NE(rc, CURLE_COULDNT_RESOLVE_HOST) << "request " << i << ": the wrapped getaddrinfo was not used";
  }

  // newer libcurl versions resolve A and AAAA separately, so a request may call getaddrinfo() more than once
  EXPECT_GE(gFakeResolve.load(), kRequests) << "every request must go through the resolver thread";
  EXPECT_EQ(gBadClose.load(), 0) << "libcurl closed a file descriptor that was already closed (eventfd closed "
                                    "twice, CVE-2025-0665); this must not happen with libcurl >= 8.12.0, which "
                                    "contains the upstream fix (libcurl 8.11.1 is affected)";

  curl_global_cleanup();
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
