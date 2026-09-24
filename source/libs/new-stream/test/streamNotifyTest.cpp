/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#define ALLOW_FORBID_FUNC

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/sha.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#include <poll.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdarg>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "cJSON.h"
#include "cmdnodes.h"
#include "streamInt.h"
#include "stub.h"
#include "tglobal.h"

extern "C" {
#include "tcurl.h"
}

namespace {

constexpr size_t kFramePayloadBytes = 256 * 1024;
constexpr size_t kTestPayloadBytes = 2 * 1024 * 1024 + 37;
constexpr int    kSocketTimeoutSec = 10;

struct WebSocketFrame {
  bool        fin = false;
  uint8_t     opcode = 0;
  bool        masked = false;
  std::string payload;
};

struct TcurlSendCall {
  size_t       requested = 0;
  size_t       sent = 0;
  curl_off_t   fragsize = 0;
  unsigned int flags = 0;
};

struct TcurlSendState {
  int32_t                    connectCalls = 0;
  std::vector<TcurlSendCall> calls;
  std::string                payload;
};

TcurlSendState gTcurlSendState;
int32_t        gCurlWsSendCalls = 0;
curl_socket_t  gCurlActiveSocket = CURL_SOCKET_BAD;

struct BlockingTcurlState {
  std::mutex              mutex;
  std::condition_variable entered;
  std::condition_variable released;
  bool                    workerEntered = false;
  bool                    workerReleased = false;
};

BlockingTcurlState gBlockingTcurlState;

CURLcode FailCurlWsSend(CURL*, const void*, size_t, size_t* sent, curl_off_t, unsigned int) {
  ++gCurlWsSendCalls;
  *sent = 0;
  return CURLE_SEND_ERROR;
}

CURLcode RetryCurlWsSend(CURL*, const void*, size_t buflen, size_t* sent, curl_off_t, unsigned int) {
  ++gCurlWsSendCalls;
  if (gCurlWsSendCalls == 1) {
    *sent = 0;
    return CURLE_AGAIN;
  }

  *sent = buflen;
  return CURLE_OK;
}

CURLcode PartiallySendBeforeAgain(CURL*, const void*, size_t buflen, size_t* sent, curl_off_t, unsigned int) {
  ++gCurlWsSendCalls;
  *sent = buflen - 1;
  return CURLE_AGAIN;
}

CURLcode MockCurlEasyGetinfo(CURL*, int info, ...) {
  if (info != CURLINFO_ACTIVESOCKET) {
    return CURLE_BAD_FUNCTION_ARGUMENT;
  }

  va_list args;
  va_start(args, info);
  curl_socket_t* socket = va_arg(args, curl_socket_t*);
  va_end(args);
  *socket = gCurlActiveSocket;
  return CURLE_OK;
}

int32_t MockTcurlConnect(CURL** ppConn, const char*) {
  ++gTcurlSendState.connectCalls;
  *ppConn = nullptr;
  return TSDB_CODE_SUCCESS;
}

int32_t RejectDirectTcurlConnect(CURL**, const char*) { return TSDB_CODE_FAILED; }

int32_t BlockTcurlGetConnection(const char*, SCURL** ppConn) {
  *ppConn = nullptr;
  std::unique_lock<std::mutex> lock(gBlockingTcurlState.mutex);
  gBlockingTcurlState.workerEntered = true;
  gBlockingTcurlState.entered.notify_one();
  gBlockingTcurlState.released.wait(lock, [] { return gBlockingTcurlState.workerReleased; });
  return TSDB_CODE_RPC_NETWORK_BUSY;
}

int32_t MockTcurlSend(SCURL*, const void* buffer, size_t buflen, size_t* sent, curl_off_t fragsize,
                      unsigned int flags) {
  size_t accepted = buflen;
  if (fragsize > 0 && buflen > 1) {
    accepted = buflen - 1;
  }

  TcurlSendCall call;
  call.requested = buflen;
  call.sent = accepted;
  call.fragsize = fragsize;
  call.flags = flags;
  gTcurlSendState.calls.push_back(call);
  gTcurlSendState.payload.append(static_cast<const char*>(buffer), accepted);
  *sent = accepted;
  return TSDB_CODE_SUCCESS;
}

class LoopbackWebSocketServer {
 public:
  ~LoopbackWebSocketServer() {
    if (listenFd_ >= 0) {
      shutdown(listenFd_, SHUT_RDWR);
    }
    Wait();
    if (listenFd_ >= 0) {
      close(listenFd_);
    }
  }

  bool Start(SSL_CTX* tlsContext = nullptr) {
    tlsContext_ = tlsContext;
    listenFd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd_ < 0) {
      error_ = std::string("socket failed: ") + strerror(errno);
      return false;
    }

    int reuse = 1;
    if (setsockopt(listenFd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) != 0) {
      error_ = std::string("setsockopt failed: ") + strerror(errno);
      return false;
    }

    sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (bind(listenFd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
      error_ = std::string("bind failed: ") + strerror(errno);
      return false;
    }
    if (listen(listenFd_, 1) != 0) {
      error_ = std::string("listen failed: ") + strerror(errno);
      return false;
    }

    socklen_t addrLen = sizeof(addr);
    if (getsockname(listenFd_, reinterpret_cast<sockaddr*>(&addr), &addrLen) != 0) {
      error_ = std::string("getsockname failed: ") + strerror(errno);
      return false;
    }
    port_ = ntohs(addr.sin_port);
    thread_ = std::thread(&LoopbackWebSocketServer::Run, this);
    return true;
  }

  void Wait() {
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  std::string Url() const {
    return std::string(tlsContext_ == nullptr ? "ws://" : "wss://") + "127.0.0.1:" + std::to_string(port_) + "/notify";
  }

  const std::string&                 Error() const { return error_; }
  const std::vector<WebSocketFrame>& Frames() const { return frames_; }

 private:
  bool SendAll(int fd, const void* data, size_t len, std::string* error) {
    const char* pos = static_cast<const char*>(data);
    while (len > 0) {
#ifdef MSG_NOSIGNAL
      ssize_t n = tls_ ? SSL_write(tls_.get(), pos, static_cast<int>(len)) : send(fd, pos, len, MSG_NOSIGNAL);
#else
      ssize_t n = tls_ ? SSL_write(tls_.get(), pos, static_cast<int>(len)) : send(fd, pos, len, 0);
#endif
      if (!tls_ && n < 0 && errno == EINTR) {
        continue;
      }
      if (n <= 0) {
        *error = std::string("send failed: ") + strerror(errno);
        return false;
      }
      pos += n;
      len -= static_cast<size_t>(n);
    }
    return true;
  }

  bool ReadExact(int fd, void* data, size_t len) {
    char* pos = static_cast<char*>(data);
    while (len > 0) {
      ssize_t n = tls_ ? SSL_read(tls_.get(), pos, static_cast<int>(len)) : recv(fd, pos, len, 0);
      if (!tls_ && n < 0 && errno == EINTR) {
        continue;
      }
      if (n <= 0) {
        if (n < 0) {
          error_ = std::string("recv failed: ") + strerror(errno);
        }
        return false;
      }
      pos += n;
      len -= static_cast<size_t>(n);
    }
    return true;
  }

  bool ReadHttpRequest(int fd, std::string* request) {
    char buffer[1024];
    while (request->find("\r\n\r\n") == std::string::npos) {
      ssize_t n = tls_ ? SSL_read(tls_.get(), buffer, sizeof(buffer)) : recv(fd, buffer, sizeof(buffer), 0);
      if (!tls_ && n < 0 && errno == EINTR) {
        continue;
      }
      if (n <= 0) {
        error_ = n == 0 ? "connection closed during WebSocket handshake"
                        : std::string("handshake recv failed: ") + strerror(errno);
        return false;
      }
      request->append(buffer, static_cast<size_t>(n));
      if (request->size() > 16 * 1024) {
        error_ = "WebSocket handshake headers are too large";
        return false;
      }
    }
    return true;
  }

  bool SendHandshakeResponse(int fd, const std::string& request) {
    const std::string header = "Sec-WebSocket-Key:";
    size_t            begin = request.find(header);
    if (begin == std::string::npos) {
      error_ = "missing Sec-WebSocket-Key";
      return false;
    }
    begin += header.size();
    while (begin < request.size() && (request[begin] == ' ' || request[begin] == '\t')) {
      ++begin;
    }
    size_t end = request.find("\r\n", begin);
    if (end == std::string::npos) {
      error_ = "unterminated Sec-WebSocket-Key";
      return false;
    }

    std::string challenge = request.substr(begin, end - begin);
    challenge += "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    unsigned char digest[SHA_DIGEST_LENGTH];
    SHA1(reinterpret_cast<const unsigned char*>(challenge.data()), challenge.size(), digest);

    unsigned char encoded[4 * ((SHA_DIGEST_LENGTH + 2) / 3) + 1] = {};
    int           encodedLen = EVP_EncodeBlock(encoded, digest, SHA_DIGEST_LENGTH);
    if (encodedLen <= 0) {
      error_ = "failed to encode Sec-WebSocket-Accept";
      return false;
    }

    std::string response =
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Accept: ";
    response.append(reinterpret_cast<const char*>(encoded), static_cast<size_t>(encodedLen));
    response += "\r\n\r\n";
    return SendAll(fd, response.data(), response.size(), &error_);
  }

  bool ReadFrame(int fd, WebSocketFrame* frame) {
    uint8_t header[2];
    if (!ReadExact(fd, header, sizeof(header))) {
      return false;
    }

    frame->fin = (header[0] & 0x80) != 0;
    frame->opcode = header[0] & 0x0f;
    frame->masked = (header[1] & 0x80) != 0;
    uint64_t payloadLen = header[1] & 0x7f;
    if (payloadLen == 126) {
      uint8_t extended[2];
      if (!ReadExact(fd, extended, sizeof(extended))) {
        return false;
      }
      payloadLen = (static_cast<uint64_t>(extended[0]) << 8) | extended[1];
    } else if (payloadLen == 127) {
      uint8_t extended[8];
      if (!ReadExact(fd, extended, sizeof(extended))) {
        return false;
      }
      payloadLen = 0;
      for (uint8_t byte : extended) {
        payloadLen = (payloadLen << 8) | byte;
      }
    }
    if (payloadLen > 16 * 1024 * 1024) {
      error_ = "WebSocket frame exceeds test safety limit";
      return false;
    }

    uint8_t mask[4] = {};
    if (frame->masked && !ReadExact(fd, mask, sizeof(mask))) {
      return false;
    }
    frame->payload.resize(static_cast<size_t>(payloadLen));
    if (payloadLen > 0 && !ReadExact(fd, &frame->payload[0], static_cast<size_t>(payloadLen))) {
      return false;
    }
    if (frame->masked) {
      for (size_t i = 0; i < frame->payload.size(); ++i) {
        frame->payload[i] = static_cast<char>(static_cast<uint8_t>(frame->payload[i]) ^ mask[i % 4]);
      }
    }
    return true;
  }

  void Run() {
    pollfd pollFd = {listenFd_, POLLIN, 0};
    int    pollResult = poll(&pollFd, 1, kSocketTimeoutSec * 1000);
    if (pollResult <= 0) {
      error_ = pollResult == 0 ? "timed out waiting for WebSocket connection"
                               : std::string("poll failed: ") + strerror(errno);
      return;
    }

    int clientFd = accept(listenFd_, nullptr, nullptr);
    if (clientFd < 0) {
      error_ = std::string("accept failed: ") + strerror(errno);
      return;
    }

    timeval timeout = {kSocketTimeoutSec, 0};
    setsockopt(clientFd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(clientFd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    if (tlsContext_ != nullptr) {
      // A client rejecting the certificate can close while OpenSSL is still writing handshake data.
      sigset_t signals;
      sigemptyset(&signals);
      sigaddset(&signals, SIGPIPE);
      pthread_sigmask(SIG_BLOCK, &signals, nullptr);
      tls_.reset(SSL_new(tlsContext_));
      if (!tls_ || SSL_set_fd(tls_.get(), clientFd) != 1 || SSL_accept(tls_.get()) != 1) {
        error_ = "TLS handshake failed";
        close(clientFd);
        return;
      }
    }

    std::string request;
    if (!ReadHttpRequest(clientFd, &request) || !SendHandshakeResponse(clientFd, request)) {
      close(clientFd);
      return;
    }

    bool messageComplete = false;
    while (true) {
      WebSocketFrame frame;
      if (!ReadFrame(clientFd, &frame)) {
        if (messageComplete && error_.empty()) {
          break;
        }
        if (error_.empty()) {
          error_ = "connection closed before a complete WebSocket message";
        }
        break;
      }
      if (frame.opcode == 0x8) {
        break;
      }
      if (frame.opcode == 0x1 || frame.opcode == 0x0) {
        frames_.push_back(std::move(frame));
        messageComplete = frames_.back().fin;
        if (messageComplete) {
          break;
        }
      }
    }
    close(clientFd);
  }

  int                         listenFd_ = -1;
  uint16_t                    port_ = 0;
  std::thread                 thread_;
  std::string                 error_;
  std::vector<WebSocketFrame> frames_;
  SSL_CTX*                                  tlsContext_ = nullptr;
  std::unique_ptr<SSL, decltype(&SSL_free)> tls_{nullptr, SSL_free};
};

CURL* gTlsConnectionTemplate = nullptr;

CURL* InitCurlWithTestTrust() { return curl_easy_duphandle(gTlsConnectionTemplate); }

class TcurlTlsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::unique_ptr<EVP_PKEY_CTX, decltype(&EVP_PKEY_CTX_free)> keyContext(EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr),
                                                                           EVP_PKEY_CTX_free);
    ASSERT_NE(keyContext, nullptr);
    ASSERT_EQ(EVP_PKEY_keygen_init(keyContext.get()), 1);
    ASSERT_EQ(EVP_PKEY_CTX_set_rsa_keygen_bits(keyContext.get(), 2048), 1);
    EVP_PKEY* key = nullptr;
    ASSERT_EQ(EVP_PKEY_keygen(keyContext.get(), &key), 1);
    key_.reset(key);
    certificate_.reset(X509_new());
    ASSERT_NE(certificate_, nullptr);
    ASSERT_EQ(X509_set_version(certificate_.get(), 2), 1);
    ASSERT_EQ(ASN1_INTEGER_set(X509_get_serialNumber(certificate_.get()), 1), 1);
    ASSERT_NE(X509_gmtime_adj(X509_getm_notBefore(certificate_.get()), -7200), nullptr);
    ASSERT_EQ(X509_set_pubkey(certificate_.get(), key_.get()), 1);
    X509_NAME* name = X509_get_subject_name(certificate_.get());
    ASSERT_EQ(X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                                         reinterpret_cast<const unsigned char*>("notification test"), -1, -1, 0),
              1);
    ASSERT_EQ(X509_set_issuer_name(certificate_.get(), name), 1);
    context_.reset(SSL_CTX_new(TLS_server_method()));
    ASSERT_NE(context_, nullptr);
    connectionTemplate_.reset(curl_easy_init());
    ASSERT_NE(connectionTemplate_, nullptr);
    ASSERT_EQ(curl_easy_setopt(connectionTemplate_.get(), CURLOPT_NOPROXY, "*"), CURLE_OK);
  }

  void TearDown() override {
    stub_.reset(curl_easy_init);
    gTlsConnectionTemplate = nullptr;
  }

  void StartServer(const char* subjectAltName, bool trustCertificate, long expiresInSeconds = 3600) {
    ASSERT_NE(X509_gmtime_adj(X509_getm_notAfter(certificate_.get()), expiresInSeconds), nullptr);
    std::unique_ptr<X509_EXTENSION, decltype(&X509_EXTENSION_free)> extension(
        X509V3_EXT_conf_nid(nullptr, nullptr, NID_subject_alt_name, const_cast<char*>(subjectAltName)),
        X509_EXTENSION_free);
    ASSERT_NE(extension, nullptr);
    ASSERT_EQ(X509_add_ext(certificate_.get(), extension.get(), -1), 1);
    ASSERT_GT(X509_sign(certificate_.get(), key_.get(), EVP_sha256()), 0);
    ASSERT_EQ(SSL_CTX_use_certificate(context_.get(), certificate_.get()), 1);
    ASSERT_EQ(SSL_CTX_use_PrivateKey(context_.get(), key_.get()), 1);

    if (trustCertificate) {
      std::unique_ptr<BIO, decltype(&BIO_free)> pem(BIO_new(BIO_s_mem()), BIO_free);
      ASSERT_NE(pem, nullptr);
      ASSERT_EQ(PEM_write_bio_X509(pem.get(), certificate_.get()), 1);
      char* data = nullptr;
      long  size = BIO_get_mem_data(pem.get(), &data);
      ASSERT_GT(size, 0);
      curl_blob ca = {data, static_cast<size_t>(size), CURL_BLOB_COPY};
      ASSERT_EQ(curl_easy_setopt(connectionTemplate_.get(), CURLOPT_CAINFO_BLOB, &ca), CURLE_OK);
    }
    // Inject only a test-local trust anchor; tcurlConnect and the TLS/WebSocket handshake remain real.
    gTlsConnectionTemplate = connectionTemplate_.get();
    stub_.set(curl_easy_init, InitCurlWithTestTrust);
    ASSERT_TRUE(server_.Start(context_.get())) << server_.Error();
  }

  void CheckConnection(bool expectedSuccess) {
    const std::string url = server_.Url();
    SCURL             connection = {};
    connection.url = taosStrdup(url.c_str());
    ASSERT_NE(connection.url, nullptr);
    int32_t code = tcurlConnect(&connection.pConn, url.c_str());
    EXPECT_EQ(code == TSDB_CODE_SUCCESS, expectedSuccess);
    EXPECT_EQ(connection.pConn != nullptr, expectedSuccess);
    if (code == TSDB_CODE_SUCCESS && connection.pConn != nullptr) {
      const char payload[] = "notification";
      size_t     sent = 0;
      EXPECT_EQ(tcurlSend(&connection, payload, sizeof(payload) - 1, &sent, 0, CURLWS_TEXT, taosGetMonoTimestampMs()),
                TSDB_CODE_SUCCESS);
      EXPECT_EQ(sent, sizeof(payload) - 1);
    }
    tcurlClose(&connection);
    server_.Wait();
    if (expectedSuccess) {
      EXPECT_TRUE(server_.Error().empty()) << server_.Error();
      ASSERT_EQ(server_.Frames().size(), 1U);
      EXPECT_EQ(server_.Frames()[0].payload, "notification");
    } else {
      EXPECT_TRUE(server_.Frames().empty());
    }
  }

 private:
  std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key_{nullptr, EVP_PKEY_free};
  std::unique_ptr<X509, decltype(&X509_free)>         certificate_{nullptr, X509_free};
  std::unique_ptr<SSL_CTX, decltype(&SSL_CTX_free)>   context_{nullptr, SSL_CTX_free};
  std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> connectionTemplate_{nullptr, curl_easy_cleanup};
  LoopbackWebSocketServer                             server_;
  Stub                                                stub_;
};

// Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7120477311
TEST_F(TcurlTlsTest, RejectsUntrustedCertificate) {
  ASSERT_NO_FATAL_FAILURE(StartServer("IP:127.0.0.1", false));
  CheckConnection(false);
}

TEST_F(TcurlTlsTest, RejectsTrustedCertificateWithWrongHost) {
  ASSERT_NO_FATAL_FAILURE(StartServer("DNS:notification.invalid", true));
  CheckConnection(false);
}

TEST_F(TcurlTlsTest, RejectsExpiredTrustedCertificate) {
  ASSERT_NO_FATAL_FAILURE(StartServer("IP:127.0.0.1", true, -3600));
  CheckConnection(false);
}

TEST_F(TcurlTlsTest, SendsNotificationWithTrustedMatchingCertificate) {
  ASSERT_NO_FATAL_FAILURE(StartServer("IP:127.0.0.1", true));
  CheckConnection(true);
}

struct ArrayDeleter {
  void operator()(SArray* array) const { taosArrayDestroy(array); }
};

struct JsonDeleter {
  void operator()(cJSON* json) const { cJSON_Delete(json); }
};

class StreamNotifyTest : public ::testing::Test {
 protected:
  void SetUp() override {
    originalFrameSizeKb_ = tsStreamNotifyFrameSize;
    tsStreamNotifyFrameSize = static_cast<int32_t>(kFramePayloadBytes / 1024);
  }

  void TearDown() override { tsStreamNotifyFrameSize = originalFrameSizeKb_; }

 private:
  int32_t originalFrameSizeKb_ = 0;
};

class StreamNotifyAsyncTest : public StreamNotifyTest {
 protected:
  void SetUp() override {
    StreamNotifyTest::SetUp();
    ASSERT_EQ(streamNotifyWorkerPoolInit(&gStreamMgmt.notifyWorkerPool, 1), TSDB_CODE_SUCCESS);
  }

  void TearDown() override {
    streamNotifyWorkerPoolDestroy(&gStreamMgmt.notifyWorkerPool);
    StreamNotifyTest::TearDown();
  }
};

class StreamNotifyBlockedWorkerTest : public StreamNotifyTest {
 protected:
  void SetUp() override {
    StreamNotifyTest::SetUp();
    originalParamPoolSizeKb_ = tsStreamNotifyParamPoolSize;
    tsStreamNotifyParamPoolSize = 1;
    {
      std::lock_guard<std::mutex> lock(gBlockingTcurlState.mutex);
      gBlockingTcurlState.workerEntered = false;
      gBlockingTcurlState.workerReleased = false;
    }
    stub_.reset(new Stub());
    stub_->set(tcurlGetConnection, BlockTcurlGetConnection);
    ASSERT_EQ(streamNotifyWorkerPoolInit(&gStreamMgmt.notifyWorkerPool, 1), TSDB_CODE_SUCCESS);
    poolInited_ = true;
  }

  void TearDown() override {
    DestroyPool();
    stub_.reset();
    tsStreamNotifyParamPoolSize = originalParamPoolSizeKb_;
    StreamNotifyTest::TearDown();
  }

  int32_t Enqueue(int64_t streamId) {
    const char*                           url = "ws://unused/block-worker";
    std::unique_ptr<SArray, ArrayDeleter> urls(taosArrayInit(1, sizeof(char*)));
    if (urls == nullptr) {
      return terrno;
    }
    char* urlPtr = const_cast<char*>(url);
    if (taosArrayPush(urls.get(), &urlPtr) == nullptr) {
      return terrno;
    }

    SStreamTask task = {};
    task.type = STREAM_TRIGGER_TASK;
    task.streamId = streamId;
    task.taskId = streamId + 1;

    SSTriggerCalcParam param = {};
    param.notifyType = STRIGGER_EVENT_WINDOW_OPEN;
    return streamSendNotifyContent(&task, "1.notify_blocked_worker_test", "result_table", STREAM_TRIGGER_SESSION, 42,
                                   TSDB_TIME_PRECISION_MILLI, urls.get(), 0, &param, 1);
  }

  bool WaitForBlockedWorker() {
    std::unique_lock<std::mutex> lock(gBlockingTcurlState.mutex);
    return gBlockingTcurlState.entered.wait_for(lock, std::chrono::seconds(5),
                                                [] { return gBlockingTcurlState.workerEntered; });
  }

  void DestroyPool() {
    {
      std::lock_guard<std::mutex> lock(gBlockingTcurlState.mutex);
      gBlockingTcurlState.workerReleased = true;
    }
    gBlockingTcurlState.released.notify_all();
    if (poolInited_) {
      streamNotifyWorkerPoolDestroy(&gStreamMgmt.notifyWorkerPool);
      poolInited_ = false;
    }
  }

 private:
  std::unique_ptr<Stub> stub_;
  int32_t               originalParamPoolSizeKb_ = 0;
  bool                  poolInited_ = false;
};

TEST(TcurlSendTest, SendFailureKeepsConnectionForMessageLevelRecovery) {
  Stub stub;
  stub.set(curl_ws_send, FailCurlWsSend);

  SCURL conn = {};
  conn.pConn = curl_easy_init();
  ASSERT_NE(conn.pConn, nullptr);
  CURL* originalConn = conn.pConn;
  conn.url = taosStrdup("ws://unused/send-failure");
  ASSERT_NE(conn.url, nullptr);

  const char payload[] = "payload";
  size_t     sent = 0;
  gCurlWsSendCalls = 0;
  EXPECT_EQ(tcurlSend(&conn, payload, sizeof(payload) - 1, &sent, sizeof(payload) - 1, CURLWS_TEXT | CURLWS_OFFSET,
                      taosGetMonoTimestampMs()),
            TSDB_CODE_FAILED);
  EXPECT_EQ(gCurlWsSendCalls, 1);
  EXPECT_EQ(sent, 0);
  EXPECT_EQ(conn.pConn, originalConn);

  if (conn.pConn != nullptr) {
    curl_easy_cleanup(conn.pConn);
    conn.pConn = nullptr;
  }
  taosMemoryFreeClear(conn.url);
}

TEST(TcurlSendTest, RetryAfterSocketBecomesWritable) {
  int sockets[2] = {-1, -1};
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);

  Stub wsSendStub;
  Stub getinfoStub;
  wsSendStub.set(curl_ws_send, RetryCurlWsSend);
  getinfoStub.set(curl_easy_getinfo, MockCurlEasyGetinfo);

  SCURL conn = {};
  conn.pConn = reinterpret_cast<CURL*>(1);
  conn.url = const_cast<char*>("ws://unused/retry");
  const char payload[] = "payload";
  size_t     sent = 0;
  gCurlWsSendCalls = 0;
  gCurlActiveSocket = sockets[0];

  EXPECT_EQ(tcurlSend(&conn, payload, sizeof(payload) - 1, &sent, sizeof(payload) - 1, CURLWS_TEXT | CURLWS_OFFSET,
                      taosGetMonoTimestampMs()),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(gCurlWsSendCalls, 2);
  EXPECT_EQ(sent, sizeof(payload) - 1);

  close(sockets[0]);
  close(sockets[1]);
  gCurlActiveSocket = CURL_SOCKET_BAD;
}

TEST(TcurlSendTest, MessageRetryAfterSocketBecomesWritable) {
  int sockets[2] = {-1, -1};
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);

  Stub wsSendStub;
  Stub getinfoStub;
  wsSendStub.set(curl_ws_send, RetryCurlWsSend);
  getinfoStub.set(curl_easy_getinfo, MockCurlEasyGetinfo);

  SCURL conn = {};
  conn.pConn = reinterpret_cast<CURL*>(1);
  conn.url = const_cast<char*>("ws://unused/message-retry");
  const char payload[] = "payload";
  gCurlWsSendCalls = 0;
  gCurlActiveSocket = sockets[0];

  EXPECT_EQ(tcurlSendMessage(&conn, payload, sizeof(payload) - 1, sizeof(payload) - 1), TSDB_CODE_SUCCESS);
  EXPECT_EQ(gCurlWsSendCalls, 2);

  close(sockets[0]);
  close(sockets[1]);
  gCurlActiveSocket = CURL_SOCKET_BAD;
}

TEST(TcurlSendTest, RetrySupportsSocketOutsideSelectFdSet) {
  struct rlimit originalLimit = {};
  ASSERT_EQ(getrlimit(RLIMIT_NOFILE, &originalLimit), 0);
  bool limitRaised = false;
  if (originalLimit.rlim_cur <= FD_SETSIZE) {
    struct rlimit raisedLimit = originalLimit;
    raisedLimit.rlim_cur = std::min<rlim_t>(originalLimit.rlim_max, FD_SETSIZE + 1);
    if (raisedLimit.rlim_cur <= FD_SETSIZE) {
      GTEST_SKIP() << "hard file descriptor limit is too low";
    }
    ASSERT_EQ(setrlimit(RLIMIT_NOFILE, &raisedLimit), 0);
    limitRaised = true;
  }

  int sockets[2] = {-1, -1};
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
  int highSocket = fcntl(sockets[0], F_DUPFD, FD_SETSIZE);
  ASSERT_GE(highSocket, FD_SETSIZE);
  close(sockets[0]);

  Stub wsSendStub;
  Stub getinfoStub;
  wsSendStub.set(curl_ws_send, RetryCurlWsSend);
  getinfoStub.set(curl_easy_getinfo, MockCurlEasyGetinfo);

  SCURL conn = {};
  conn.pConn = reinterpret_cast<CURL*>(1);
  conn.url = const_cast<char*>("ws://unused/high-fd-retry");
  const char payload[] = "payload";
  size_t     sent = 0;
  gCurlWsSendCalls = 0;
  gCurlActiveSocket = highSocket;

  EXPECT_EQ(tcurlSend(&conn, payload, sizeof(payload) - 1, &sent, sizeof(payload) - 1, CURLWS_TEXT | CURLWS_OFFSET,
                      taosGetMonoTimestampMs()),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(gCurlWsSendCalls, 2);

  close(highSocket);
  close(sockets[1]);
  if (limitRaised) {
    EXPECT_EQ(setrlimit(RLIMIT_NOFILE, &originalLimit), 0);
  }
  gCurlActiveSocket = CURL_SOCKET_BAD;
}

TEST(TcurlSendTest, ExpiredMessageDeadlineStopsRetry) {
  int sockets[2] = {-1, -1};
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);

  Stub wsSendStub;
  Stub getinfoStub;
  wsSendStub.set(curl_ws_send, RetryCurlWsSend);
  getinfoStub.set(curl_easy_getinfo, MockCurlEasyGetinfo);

  SCURL conn = {};
  conn.pConn = reinterpret_cast<CURL*>(1);
  conn.url = const_cast<char*>("ws://unused/expired-retry");
  const char payload[] = "payload";
  size_t     sent = 0;
  gCurlWsSendCalls = 0;
  gCurlActiveSocket = sockets[0];

  int64_t sendStartMonoMs = taosGetMonoTimestampMs() - 11000;
  EXPECT_EQ(tcurlSend(&conn, payload, sizeof(payload) - 1, &sent, sizeof(payload) - 1, CURLWS_TEXT | CURLWS_OFFSET,
                      sendStartMonoMs),
            TSDB_CODE_FAILED);
  EXPECT_EQ(gCurlWsSendCalls, 1);

  close(sockets[0]);
  close(sockets[1]);
  gCurlActiveSocket = CURL_SOCKET_BAD;
}

TEST(TcurlSendTest, PartialProgressBeforeAgainIsReportedToCaller) {
  Stub stub;
  stub.set(curl_ws_send, PartiallySendBeforeAgain);

  SCURL conn = {};
  conn.pConn = reinterpret_cast<CURL*>(1);
  conn.url = const_cast<char*>("ws://unused/partial-progress");
  const char payload[] = "payload";
  size_t     sent = 0;
  gCurlWsSendCalls = 0;

  EXPECT_EQ(tcurlSend(&conn, payload, sizeof(payload) - 1, &sent, sizeof(payload) - 1, CURLWS_TEXT | CURLWS_OFFSET,
                      taosGetMonoTimestampMs()),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(gCurlWsSendCalls, 1);
  EXPECT_EQ(sent, sizeof(payload) - 2);
}

TEST_F(StreamNotifyTest, SmallNotificationUsesOneFinalTextFrame) {
  LoopbackWebSocketServer server;
  ASSERT_TRUE(server.Start()) << server.Error();

  std::string                           url = server.Url();
  std::unique_ptr<SArray, ArrayDeleter> urls(taosArrayInit(1, sizeof(char*)));
  ASSERT_NE(urls, nullptr);
  char* urlPtr = const_cast<char*>(url.c_str());
  ASSERT_NE(taosArrayPush(urls.get(), &urlPtr), nullptr);

  const std::string payload = "small-notification";
  std::string       extraContent = "{\"payload\":\"" + payload + "\"}";

  SStreamTask task = {};
  task.type = STREAM_TRIGGER_TASK;
  task.streamId = 0x1234;
  task.taskId = 0x5678;

  SSTriggerCalcParam param = {};
  param.wstart = 1000;
  param.wend = 2000;
  param.notifyType = STRIGGER_EVENT_WINDOW_CLOSE;
  param.extraNotifyContent = const_cast<char*>(extraContent.c_str());

  ASSERT_EQ(streamSendNotifyContent(&task, "1.notify_single_frame_test", "result_table", STREAM_TRIGGER_COUNT, 42,
                                    TSDB_TIME_PRECISION_MILLI, urls.get(), NOTIFY_ON_FAILURE_PAUSE, &param, 1),
            TSDB_CODE_SUCCESS);
  server.Wait();
  ASSERT_TRUE(server.Error().empty()) << server.Error();

  const std::vector<WebSocketFrame>& frames = server.Frames();
  ASSERT_EQ(frames.size(), 1);
  EXPECT_TRUE(frames[0].fin);
  EXPECT_EQ(frames[0].opcode, 0x1);
  EXPECT_TRUE(frames[0].masked);
  EXPECT_LE(frames[0].payload.size(), kFramePayloadBytes);

  std::unique_ptr<cJSON, JsonDeleter> root(cJSON_ParseWithLength(frames[0].payload.data(), frames[0].payload.size()));
  ASSERT_NE(root, nullptr);
  cJSON* streams = cJSON_GetObjectItemCaseSensitive(root.get(), "streams");
  ASSERT_TRUE(cJSON_IsArray(streams));
  cJSON* stream = cJSON_GetArrayItem(streams, 0);
  ASSERT_TRUE(cJSON_IsObject(stream));
  cJSON* events = cJSON_GetObjectItemCaseSensitive(stream, "events");
  ASSERT_TRUE(cJSON_IsArray(events));
  cJSON* event = cJSON_GetArrayItem(events, 0);
  ASSERT_TRUE(cJSON_IsObject(event));
  cJSON* receivedPayload = cJSON_GetObjectItemCaseSensitive(event, "payload");
  ASSERT_TRUE(cJSON_IsString(receivedPayload));
  EXPECT_EQ(std::string(receivedPayload->valuestring), payload);
}

TEST_F(StreamNotifyTest, ShortWritesContinueWithinEachFrame) {
  Stub stub;
  stub.set(tcurlConnect, MockTcurlConnect);
  stub.set(tcurlSend, MockTcurlSend);
  gTcurlSendState = {};

  std::string                           url = "ws://unused/short-write";
  std::unique_ptr<SArray, ArrayDeleter> urls(taosArrayInit(1, sizeof(char*)));
  ASSERT_NE(urls, nullptr);
  char* urlPtr = const_cast<char*>(url.c_str());
  ASSERT_NE(taosArrayPush(urls.get(), &urlPtr), nullptr);

  std::string payload(kFramePayloadBytes + 37, 'a');
  for (size_t i = 0; i < payload.size(); ++i) {
    payload[i] = static_cast<char>('a' + i % 26);
  }
  std::string extraContent = "{\"payload\":\"" + payload + "\"}";

  SStreamTask task = {};
  task.type = STREAM_TRIGGER_TASK;
  task.streamId = 0x1234;
  task.taskId = 0x5678;

  SSTriggerCalcParam param = {};
  param.wstart = 1000;
  param.wend = 2000;
  param.notifyType = STRIGGER_EVENT_WINDOW_CLOSE;
  param.extraNotifyContent = const_cast<char*>(extraContent.c_str());

  ASSERT_EQ(streamSendNotifyContent(&task, "1.notify_short_write_test", "result_table", STREAM_TRIGGER_COUNT, 42,
                                    TSDB_TIME_PRECISION_MILLI, urls.get(), NOTIFY_ON_FAILURE_PAUSE, &param, 1),
            TSDB_CODE_SUCCESS);

  EXPECT_EQ(gTcurlSendState.connectCalls, 1);
  ASSERT_EQ(gTcurlSendState.calls.size(), 4);
  const size_t finalFrameSize = gTcurlSendState.payload.size() - kFramePayloadBytes;
  ASSERT_GT(finalFrameSize, 1);
  ASSERT_LE(finalFrameSize, kFramePayloadBytes);

  const unsigned int continuedFlags = CURLWS_TEXT | CURLWS_OFFSET | CURLWS_CONT;
  const unsigned int finalFlags = CURLWS_TEXT | CURLWS_OFFSET;
  EXPECT_EQ(gTcurlSendState.calls[0].requested, kFramePayloadBytes);
  EXPECT_EQ(gTcurlSendState.calls[0].sent, kFramePayloadBytes - 1);
  EXPECT_EQ(gTcurlSendState.calls[0].fragsize, kFramePayloadBytes);
  EXPECT_EQ(gTcurlSendState.calls[0].flags, continuedFlags);
  EXPECT_EQ(gTcurlSendState.calls[1].requested, 1);
  EXPECT_EQ(gTcurlSendState.calls[1].sent, 1);
  EXPECT_EQ(gTcurlSendState.calls[1].fragsize, 0);
  EXPECT_EQ(gTcurlSendState.calls[1].flags, continuedFlags);
  EXPECT_EQ(gTcurlSendState.calls[2].requested, finalFrameSize);
  EXPECT_EQ(gTcurlSendState.calls[2].sent, finalFrameSize - 1);
  EXPECT_EQ(gTcurlSendState.calls[2].fragsize, finalFrameSize);
  EXPECT_EQ(gTcurlSendState.calls[2].flags, finalFlags);
  EXPECT_EQ(gTcurlSendState.calls[3].requested, 1);
  EXPECT_EQ(gTcurlSendState.calls[3].sent, 1);
  EXPECT_EQ(gTcurlSendState.calls[3].fragsize, 0);
  EXPECT_EQ(gTcurlSendState.calls[3].flags, finalFlags);

  std::unique_ptr<cJSON, JsonDeleter> root(
      cJSON_ParseWithLength(gTcurlSendState.payload.data(), gTcurlSendState.payload.size()));
  ASSERT_NE(root, nullptr);
  cJSON* streams = cJSON_GetObjectItemCaseSensitive(root.get(), "streams");
  ASSERT_TRUE(cJSON_IsArray(streams));
  cJSON* stream = cJSON_GetArrayItem(streams, 0);
  ASSERT_TRUE(cJSON_IsObject(stream));
  cJSON* events = cJSON_GetObjectItemCaseSensitive(stream, "events");
  ASSERT_TRUE(cJSON_IsArray(events));
  cJSON* event = cJSON_GetArrayItem(events, 0);
  ASSERT_TRUE(cJSON_IsObject(event));
  cJSON* receivedPayload = cJSON_GetObjectItemCaseSensitive(event, "payload");
  ASSERT_TRUE(cJSON_IsString(receivedPayload));
  EXPECT_EQ(std::string(receivedPayload->valuestring), payload);
}

TEST_F(StreamNotifyTest, LargeNotificationIsFragmentedWithoutChangingMessage) {
  LoopbackWebSocketServer server;
  ASSERT_TRUE(server.Start()) << server.Error();

  std::string                           url = server.Url();
  std::unique_ptr<SArray, ArrayDeleter> urls(taosArrayInit(1, sizeof(char*)));
  ASSERT_NE(urls, nullptr);
  char* urlPtr = const_cast<char*>(url.c_str());
  ASSERT_NE(taosArrayPush(urls.get(), &urlPtr), nullptr);

  std::string payload(kTestPayloadBytes, 'a');
  for (size_t i = 0; i < payload.size(); ++i) {
    payload[i] = static_cast<char>('a' + i % 26);
  }
  std::string extraContent = "{\"payload\":\"" + payload + "\"}";

  SStreamTask task = {};
  task.type = STREAM_TRIGGER_TASK;
  task.streamId = 0x1234;
  task.taskId = 0x5678;

  SSTriggerCalcParam param = {};
  param.wstart = 1000;
  param.wend = 2000;
  param.notifyType = STRIGGER_EVENT_WINDOW_CLOSE;
  param.extraNotifyContent = const_cast<char*>(extraContent.c_str());

  ASSERT_EQ(streamSendNotifyContent(&task, "1.notify_frame_test", "result_table", STREAM_TRIGGER_COUNT, 42,
                                    TSDB_TIME_PRECISION_MILLI, urls.get(), NOTIFY_ON_FAILURE_PAUSE, &param, 1),
            TSDB_CODE_SUCCESS);
  server.Wait();
  ASSERT_TRUE(server.Error().empty()) << server.Error();

  const std::vector<WebSocketFrame>& frames = server.Frames();
  ASSERT_FALSE(frames.empty());

  std::string message;
  for (const WebSocketFrame& frame : frames) {
    EXPECT_TRUE(frame.masked);
    EXPECT_LE(frame.payload.size(), kFramePayloadBytes);
    message += frame.payload;
  }

  const size_t expectedFrameCount = (message.size() + kFramePayloadBytes - 1) / kFramePayloadBytes;
  EXPECT_EQ(frames.size(), expectedFrameCount);
  size_t offset = 0;
  for (size_t i = 0; i < frames.size(); ++i) {
    EXPECT_EQ(frames[i].opcode, i == 0 ? 0x1 : 0x0);
    EXPECT_EQ(frames[i].fin, i + 1 == frames.size());
    size_t expectedPayload = std::min(kFramePayloadBytes, message.size() - offset);
    EXPECT_EQ(frames[i].payload.size(), expectedPayload);
    offset += frames[i].payload.size();
  }
  EXPECT_EQ(offset, message.size());

  std::unique_ptr<cJSON, JsonDeleter> root(cJSON_ParseWithLength(message.data(), message.size()));
  ASSERT_NE(root, nullptr);
  cJSON* streams = cJSON_GetObjectItemCaseSensitive(root.get(), "streams");
  ASSERT_TRUE(cJSON_IsArray(streams));
  cJSON* stream = cJSON_GetArrayItem(streams, 0);
  ASSERT_TRUE(cJSON_IsObject(stream));
  cJSON* streamName = cJSON_GetObjectItemCaseSensitive(stream, "streamName");
  ASSERT_TRUE(cJSON_IsString(streamName));
  EXPECT_STREQ(streamName->valuestring, "notify_frame_test");
  cJSON* events = cJSON_GetObjectItemCaseSensitive(stream, "events");
  ASSERT_TRUE(cJSON_IsArray(events));
  cJSON* event = cJSON_GetArrayItem(events, 0);
  ASSERT_TRUE(cJSON_IsObject(event));
  cJSON* receivedPayload = cJSON_GetObjectItemCaseSensitive(event, "payload");
  ASSERT_TRUE(cJSON_IsString(receivedPayload));
  EXPECT_EQ(std::string(receivedPayload->valuestring), payload);
}

TEST_F(StreamNotifyAsyncTest, LargeNotificationIsFragmentedWithoutChangingMessage) {
  LoopbackWebSocketServer server;
  ASSERT_TRUE(server.Start()) << server.Error();

  std::string                           url = server.Url();
  std::unique_ptr<SArray, ArrayDeleter> urls(taosArrayInit(1, sizeof(char*)));
  ASSERT_NE(urls, nullptr);
  char* urlPtr = const_cast<char*>(url.c_str());
  ASSERT_NE(taosArrayPush(urls.get(), &urlPtr), nullptr);

  std::string payload(kTestPayloadBytes, 'a');
  for (size_t i = 0; i < payload.size(); ++i) {
    payload[i] = static_cast<char>('a' + i % 26);
  }
  std::string extraContent = "{\"payload\":\"" + payload + "\"}";

  SStreamTask task = {};
  task.type = STREAM_TRIGGER_TASK;
  task.streamId = 0x9abc;
  task.taskId = 0xdef0;

  SSTriggerCalcParam param = {};
  param.wstart = 1000;
  param.wend = 2000;
  param.notifyType = STRIGGER_EVENT_WINDOW_CLOSE;
  param.extraNotifyContent = const_cast<char*>(extraContent.c_str());

  ASSERT_EQ(streamSendNotifyContent(&task, "1.notify_async_frame_test", "result_table", STREAM_TRIGGER_COUNT, 42,
                                    TSDB_TIME_PRECISION_MILLI, urls.get(), 0, &param, 1),
            TSDB_CODE_SUCCESS);
  server.Wait();
  ASSERT_TRUE(server.Error().empty()) << server.Error();

  const std::vector<WebSocketFrame>& frames = server.Frames();
  ASSERT_GT(frames.size(), 1);

  std::string message;
  for (size_t i = 0; i < frames.size(); ++i) {
    EXPECT_TRUE(frames[i].masked);
    EXPECT_LE(frames[i].payload.size(), kFramePayloadBytes);
    EXPECT_EQ(frames[i].opcode, i == 0 ? 0x1 : 0x0);
    EXPECT_EQ(frames[i].fin, i + 1 == frames.size());
    message += frames[i].payload;
  }

  std::unique_ptr<cJSON, JsonDeleter> root(cJSON_ParseWithLength(message.data(), message.size()));
  ASSERT_NE(root, nullptr);
  cJSON* streams = cJSON_GetObjectItemCaseSensitive(root.get(), "streams");
  ASSERT_TRUE(cJSON_IsArray(streams));
  cJSON* stream = cJSON_GetArrayItem(streams, 0);
  ASSERT_TRUE(cJSON_IsObject(stream));
  cJSON* events = cJSON_GetObjectItemCaseSensitive(stream, "events");
  ASSERT_TRUE(cJSON_IsArray(events));
  cJSON* event = cJSON_GetArrayItem(events, 0);
  ASSERT_TRUE(cJSON_IsObject(event));
  cJSON* receivedPayload = cJSON_GetObjectItemCaseSensitive(event, "payload");
  ASSERT_TRUE(cJSON_IsString(receivedPayload));
  EXPECT_EQ(std::string(receivedPayload->valuestring), payload);
}

TEST_F(StreamNotifyAsyncTest, DropResultReportsQueueAcceptanceBeforeNetworkDelivery) {
  Stub stub;
  stub.set(tcurlConnect, RejectDirectTcurlConnect);

  std::string                           url = "ws://unused/queue-acceptance";
  std::unique_ptr<SArray, ArrayDeleter> urls(taosArrayInit(1, sizeof(char*)));
  ASSERT_NE(urls, nullptr);
  char* urlPtr = const_cast<char*>(url.c_str());
  ASSERT_NE(taosArrayPush(urls.get(), &urlPtr), nullptr);

  SStreamTask task = {};
  task.type = STREAM_TRIGGER_TASK;
  task.streamId = 0xa123;
  task.taskId = 0xb456;

  SSTriggerCalcParam param = {};
  param.notifyType = STRIGGER_EVENT_WINDOW_OPEN;

  bool attempted = false;
  bool accepted = false;
  EXPECT_EQ(
      streamSendNotifyContentWithResult(&task, "1.notify_queue_acceptance_test", "result_table", STREAM_TRIGGER_SESSION,
                                        42, TSDB_TIME_PRECISION_MILLI, urls.get(), 0, &param, 1, &attempted, &accepted),
      TSDB_CODE_SUCCESS);
  EXPECT_TRUE(attempted);
  EXPECT_TRUE(accepted);
}

TEST_F(StreamNotifyAsyncTest, DestroyingQueueWithZeroStreamIdDoesNotCrash) {
  Stub stub;
  stub.set(tcurlConnect, MockTcurlConnect);
  stub.set(tcurlSend, MockTcurlSend);
  gTcurlSendState = {};

  std::string                           url = "ws://unused/zero-stream-id";
  std::unique_ptr<SArray, ArrayDeleter> urls(taosArrayInit(1, sizeof(char*)));
  ASSERT_NE(urls, nullptr);
  char* urlPtr = const_cast<char*>(url.c_str());
  ASSERT_NE(taosArrayPush(urls.get(), &urlPtr), nullptr);

  SStreamTask task = {};
  task.type = STREAM_TRIGGER_TASK;
  task.streamId = 0;
  task.taskId = 0x5678;

  SSTriggerCalcParam param = {};
  param.wstart = 1000;
  param.wend = 2000;
  param.notifyType = STRIGGER_EVENT_WINDOW_CLOSE;
  param.extraNotifyContent = const_cast<char*>("{\"payload\":\"zero-stream-id\"}");

  ASSERT_EQ(streamSendNotifyContent(&task, "0.notify_zero_stream_id_test", "result_table", STREAM_TRIGGER_COUNT, 42,
                                    TSDB_TIME_PRECISION_MILLI, urls.get(), 0, &param, 1),
            TSDB_CODE_SUCCESS);
}

TEST_F(StreamNotifyBlockedWorkerTest, BorrowedHashNodeDoesNotAccessUnregisteredQueueDuringShutdown) {
  constexpr int64_t streamId = 0x1234;
  ASSERT_EQ(Enqueue(streamId), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(WaitForBlockedWorker());

  SStreamNotifyWorkerPool* pPool = &gStreamMgmt.notifyWorkerPool;
  taosThreadMutexLock(&pPool->mapLock);
  void* pIter = taosHashIterate(pPool->queueMap, nullptr);
  taosThreadMutexUnlock(&pPool->mapLock);
  ASSERT_NE(pIter, nullptr);

  streamNotifyQueueUnregister(streamId);
  EXPECT_EQ(taosHashGetSize(pPool->queueMap), 1);
  DestroyPool();
}

TEST_F(StreamNotifyBlockedWorkerTest, ReclaimingParamReleasesQueueMapIterator) {
  constexpr int64_t blockedStreamId = 0x2000;
  ASSERT_EQ(Enqueue(blockedStreamId), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(WaitForBlockedWorker());

  SStreamNotifyWorkerPool* pPool = &gStreamMgmt.notifyWorkerPool;
  const int64_t            poolCapacity = pPool->paramPool.capacity;
  ASSERT_GE(poolCapacity, 4);

  for (int64_t i = 1; i <= poolCapacity; ++i) {
    ASSERT_EQ(Enqueue(blockedStreamId + i), TSDB_CODE_SUCCESS);
  }
  for (int64_t i = 1; i <= poolCapacity; ++i) {
    streamNotifyQueueUnregister(blockedStreamId + i);
  }

  EXPECT_EQ(taosHashGetSize(pPool->queueMap), 1);
  streamNotifyQueueUnregister(blockedStreamId);
  DestroyPool();
}

TEST_F(StreamNotifyTest, GroupedNotificationsKeepGidTbnameWindowAndLineage) {
  Stub stub;
  stub.set(tcurlConnect, MockTcurlConnect);
  stub.set(tcurlSend, MockTcurlSend);

  std::string                           url = "ws://unused/grouped-notify";
  std::unique_ptr<SArray, ArrayDeleter> urls(taosArrayInit(1, sizeof(char*)));
  ASSERT_NE(urls, nullptr);
  char* urlPtr = const_cast<char*>(url.c_str());
  ASSERT_NE(taosArrayPush(urls.get(), &urlPtr), nullptr);

  SStreamTask task = {};
  task.type = STREAM_TRIGGER_TASK;
  task.streamId = 0x1234;
  task.taskId = 0x5678;

  const int64_t gids[] = {202, 101, 303};
  for (int32_t i = 0; i < 3; ++i) {
    SCOPED_TRACE(i);
    SWindowLineage lineage = {};
    lineage.pScopes = taosArrayInit(1, sizeof(SScopeInstanceId));
    ASSERT_NE(lineage.pScopes, nullptr);
    SScopeInstanceId scope = {};
    scope.layerIndex = 0;
    scope.triggerType = WINDOW_TYPE_INTERVAL;
    scope.openingTs = 1000 + i;
    scope.nativeDiscriminator = 700 + i;
    ASSERT_NE(taosArrayPush(lineage.pScopes, &scope), nullptr);

    const TSKEY   leafStart = 2000 + i;
    const int32_t windowIndex = 10 + i;
    char          triggerId[STREAM_NESTED_TRIGGER_ID_LEN] = {0};
    ASSERT_EQ(stBuildNestedTriggerId(gids[i], &lineage, leafStart, windowIndex, triggerId), TSDB_CODE_SUCCESS);

    const std::string  tableName = "group_" + std::to_string(gids[i]);
    const std::string  lineageMarker = "lineage-" + std::to_string(i);
    const std::string  extraContent = "{\"lineageMarker\":\"" + lineageMarker + "\"}";
    SSTriggerCalcParam param = {};
    param.wstart = leafStart;
    param.wend = leafStart + 99;
    param.notifyType = STRIGGER_EVENT_WINDOW_CLOSE;
    param.extraNotifyContent = const_cast<char*>(extraContent.c_str());
    const char* triggerIds[] = {triggerId};
    gTcurlSendState = {};

    ASSERT_EQ(streamSendNestedResultNotifyContent(&task, "1.grouped_notify", tableName.c_str(), STREAM_TRIGGER_COUNT,
                                                  gids[i], urls.get(), 0, &param, triggerIds, 1),
              TSDB_CODE_SUCCESS);

    std::unique_ptr<cJSON, JsonDeleter> root(
        cJSON_ParseWithLength(gTcurlSendState.payload.data(), gTcurlSendState.payload.size()));
    ASSERT_NE(root, nullptr);
    cJSON* streams = cJSON_GetObjectItemCaseSensitive(root.get(), "streams");
    cJSON* stream = cJSON_IsArray(streams) ? cJSON_GetArrayItem(streams, 0) : nullptr;
    cJSON* events = cJSON_IsObject(stream) ? cJSON_GetObjectItemCaseSensitive(stream, "events") : nullptr;
    cJSON* event = cJSON_IsArray(events) ? cJSON_GetArrayItem(events, 0) : nullptr;
    ASSERT_TRUE(cJSON_IsObject(event));
    cJSON* groupId = cJSON_GetObjectItemCaseSensitive(event, "groupId");
    cJSON* receivedTableName = cJSON_GetObjectItemCaseSensitive(event, "tableName");
    cJSON* windowStart = cJSON_GetObjectItemCaseSensitive(event, "windowStart");
    cJSON* windowEnd = cJSON_GetObjectItemCaseSensitive(event, "windowEnd");
    cJSON* receivedTriggerId = cJSON_GetObjectItemCaseSensitive(event, "triggerId");
    cJSON* receivedLineageMarker = cJSON_GetObjectItemCaseSensitive(event, "lineageMarker");
    ASSERT_TRUE(cJSON_IsString(groupId));
    ASSERT_TRUE(cJSON_IsString(receivedTableName));
    ASSERT_TRUE(cJSON_IsNumber(windowStart));
    ASSERT_TRUE(cJSON_IsNumber(windowEnd));
    ASSERT_TRUE(cJSON_IsString(receivedTriggerId));
    ASSERT_TRUE(cJSON_IsString(receivedLineageMarker));
    EXPECT_EQ(std::string(groupId->valuestring), std::to_string(gids[i]));
    EXPECT_EQ(std::string(receivedTableName->valuestring), tableName);
    EXPECT_EQ(static_cast<TSKEY>(windowStart->valuedouble), param.wstart);
    EXPECT_EQ(static_cast<TSKEY>(windowEnd->valuedouble), param.wend);
    EXPECT_STREQ(receivedTriggerId->valuestring, triggerId);
    EXPECT_EQ(std::string(receivedLineageMarker->valuestring), lineageMarker);

    taosArrayDestroy(lineage.pScopes);
  }
}

}  // namespace

int main(int argc, char** argv) {
  signal(SIGPIPE, SIG_IGN);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
