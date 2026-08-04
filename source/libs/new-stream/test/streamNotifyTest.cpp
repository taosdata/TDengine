#define ALLOW_FORBID_FUNC

#include <arpa/inet.h>
#include <errno.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "cJSON.h"
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

CURLcode FailCurlWsSend(CURL*, const void*, size_t, size_t* sent, curl_off_t, unsigned int) {
  ++gCurlWsSendCalls;
  *sent = 0;
  return CURLE_SEND_ERROR;
}

int32_t MockTcurlConnect(CURL** ppConn, const char*) {
  ++gTcurlSendState.connectCalls;
  *ppConn = nullptr;
  return TSDB_CODE_SUCCESS;
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

  bool Start() {
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

  std::string Url() const { return "ws://127.0.0.1:" + std::to_string(port_) + "/notify"; }

  const std::string&                 Error() const { return error_; }
  const std::vector<WebSocketFrame>& Frames() const { return frames_; }

 private:
  static bool SendAll(int fd, const void* data, size_t len, std::string* error) {
    const char* pos = static_cast<const char*>(data);
    while (len > 0) {
#ifdef MSG_NOSIGNAL
      ssize_t n = send(fd, pos, len, MSG_NOSIGNAL);
#else
      ssize_t n = send(fd, pos, len, 0);
#endif
      if (n < 0 && errno == EINTR) {
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
      ssize_t n = recv(fd, pos, len, 0);
      if (n < 0 && errno == EINTR) {
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
      ssize_t n = recv(fd, buffer, sizeof(buffer), 0);
      if (n < 0 && errno == EINTR) {
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
      }
    }
    close(clientFd);
  }

  int                         listenFd_ = -1;
  uint16_t                    port_ = 0;
  std::thread                 thread_;
  std::string                 error_;
  std::vector<WebSocketFrame> frames_;
};

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
  EXPECT_EQ(tcurlSend(&conn, payload, sizeof(payload) - 1, &sent, sizeof(payload) - 1, CURLWS_TEXT | CURLWS_OFFSET),
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
                                    urls.get(), 0, &param, 1),
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
                                    urls.get(), 0, &param, 1),
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

  ASSERT_EQ(streamSendNotifyContent(&task, "1.notify_frame_test", "result_table", STREAM_TRIGGER_COUNT, 42, urls.get(),
                                    0, &param, 1),
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

}  // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
