/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <boost/test/included/unit_test_framework.hpp>

#include <boost/bind.hpp>

#ifdef HAVE_BOOST_ASIO
#include <boost/asio.hpp>
#endif
#include "buffer/BufferPrint.hh"
#include "buffer/BufferReader.hh"
#include "buffer/BufferStream.hh"

using namespace avro;
using detail::kDefaultBlockSize;
using detail::kMaxBlockSize;
using detail::kMinBlockSize;
using std::cout;
using std::endl;

std::string makeString(size_t len) {
    std::string newstring;
    newstring.reserve(len);

    for (size_t i = 0; i < len; ++i) {
        char newchar = '0' + i % 16;
        if (newchar > '9') {
            newchar += 7;
        }
        newstring.push_back(newchar);
    }

    return newstring;
}

void printBuffer(const InputBuffer &buf) {
    avro::istream is(buf);
    cout << is.rdbuf() << endl;
}

void TestReserve() {
    BOOST_TEST_MESSAGE("TestReserve");
    {
        OutputBuffer ob;
        BOOST_CHECK_EQUAL(ob.size(), 0U);
        BOOST_CHECK_EQUAL(ob.freeSpace(), 0U);
        BOOST_CHECK_EQUAL(ob.numChunks(), 0);
        BOOST_CHECK_EQUAL(ob.numDataChunks(), 0);
    }

    {
        size_t reserveSize = kMinBlockSize / 2;

        OutputBuffer ob(reserveSize);
        BOOST_CHECK_EQUAL(ob.size(), 0U);
        BOOST_CHECK_EQUAL(ob.freeSpace(), kMinBlockSize);
        BOOST_CHECK_EQUAL(ob.numChunks(), 1);
        BOOST_CHECK_EQUAL(ob.numDataChunks(), 0);

        // reserve should add a single block
        reserveSize += 8192;

        ob.reserve(reserveSize);
        BOOST_CHECK_EQUAL(ob.size(), 0U);
        BOOST_CHECK_EQUAL(ob.freeSpace(), reserveSize);
        BOOST_CHECK_EQUAL(ob.numChunks(), 2);
        BOOST_CHECK_EQUAL(ob.numDataChunks(), 0);

        // reserve should add two blocks, one of the maximum size and
        // one of the minimum size
        reserveSize += (kMaxBlockSize + kMinBlockSize / 2);

        ob.reserve(reserveSize);
        BOOST_CHECK_EQUAL(ob.size(), 0U);
        BOOST_CHECK_EQUAL(ob.freeSpace(), reserveSize + kMinBlockSize / 2);
        BOOST_CHECK_EQUAL(ob.numChunks(), 4);
        BOOST_CHECK_EQUAL(ob.numDataChunks(), 0);
    }
}

void addDataToBuffer(OutputBuffer &buf, size_t size) {
    std::string data = makeString(size);
    buf.writeTo(data.c_str(), data.size());
}

void TestGrow() {
    BOOST_TEST_MESSAGE("TestGrow");
    {
        OutputBuffer ob;

        // add exactly one block
        addDataToBuffer(ob, kDefaultBlockSize);

        BOOST_CHECK_EQUAL(ob.size(), kDefaultBlockSize);
        BOOST_CHECK_EQUAL(ob.freeSpace(), 0U);
        BOOST_CHECK_EQUAL(ob.numChunks(), 0);
        BOOST_CHECK_EQUAL(ob.numDataChunks(), 1);

        // add another block, half full
        addDataToBuffer(ob, kDefaultBlockSize / 2);

        BOOST_CHECK_EQUAL(ob.size(), kDefaultBlockSize + kDefaultBlockSize / 2);
        BOOST_CHECK_EQUAL(ob.freeSpace(), kDefaultBlockSize / 2);
        BOOST_CHECK_EQUAL(ob.numChunks(), 1);
        BOOST_CHECK_EQUAL(ob.numDataChunks(), 2);

        // reserve more capacity
        size_t reserveSize = ob.freeSpace() + 8192;
        ob.reserve(reserveSize);

        BOOST_CHECK_EQUAL(ob.size(), kDefaultBlockSize + kDefaultBlockSize / 2);
        BOOST_CHECK_EQUAL(ob.freeSpace(), reserveSize);
        BOOST_CHECK_EQUAL(ob.numChunks(), 2);
        BOOST_CHECK_EQUAL(ob.numDataChunks(), 2);

        // fill beyond capacity
        addDataToBuffer(ob, reserveSize + 1);
        BOOST_CHECK_EQUAL(ob.size(), kDefaultBlockSize + kDefaultBlockSize / 2 + reserveSize + 1);
        BOOST_CHECK_EQUAL(ob.freeSpace(), kDefaultBlockSize - 1);
        BOOST_CHECK_EQUAL(ob.numChunks(), 1);
        BOOST_CHECK_EQUAL(ob.numDataChunks(), 4);
    }
}

void TestDiscard() {
    BOOST_TEST_MESSAGE("TestDiscard");
    {
        OutputBuffer ob;
        size_t dataSize = kDefaultBlockSize * 2 + kDefaultBlockSize / 2;
        addDataToBuffer(ob, dataSize);

        BOOST_CHECK_EQUAL(ob.size(), dataSize);
        BOOST_CHECK_EQUAL(ob.freeSpace(), kDefaultBlockSize / 2);
        BOOST_CHECK_EQUAL(ob.numChunks(), 1);
        BOOST_CHECK_EQUAL(ob.numDataChunks(), 3);

        ob.discardData();

        BOOST_CHECK_EQUAL(ob.size(), 0U);
        BOOST_CHECK_EQUAL(ob.freeSpace(), kDefaultBlockSize / 2);
        BOOST_CHECK_EQUAL(ob.numChunks(), 1);
        BOOST_CHECK_EQUAL(ob.numDataChunks(), 0);
    }

    {
        // discard no bytes
        OutputBuffer ob;
        size_t dataSize = kDefaultBlockSize * 2 + kDefaultBlockSize / 2;
        addDataToBuffer(ob, dataSize);

        BOOST_CHECK_EQUAL(ob.size(), dataSize);
        BOOST_CHECK_EQUAL(ob.freeSpace(), kDefaultBlockSize / 2);
        BOOST_CHECK_EQUAL(ob.numChunks(), 1);
        BOOST_CHECK_EQUAL(ob.numDataChunks(), 3);

        ob.discardData(0);

        BOOST_CHECK_EQUAL(ob.size(), dataSize);
        BOOST_CHECK_EQUAL(ob.freeSpace(), kDefaultBlockSize / 2);
        BOOST_CHECK_EQUAL(ob.numChunks(), 1);
        BOOST_CHECK_EQUAL(ob.numDataChunks(), 3);
    }

    {
        // discard exactly one block
        OutputBuffer ob;
        size_t dataSize = kDefaultBlockSize * 2 + kDefaultBlockSize / 2;
        addDataToBuffer(ob, dataSize);

        BOOST_CHECK_EQUAL(ob.size(), dataSize);
        BOOST_CHECK_EQUAL(ob.freeSpace(), kDefaultBlockSize / 2);
        BOOST_CHECK_EQUAL(ob.numChunks(), 1);
        BOOST_CHECK_EQUAL(ob.numDataChunks(), 3);

        ob.discardData(kDefaultBlockSize);

        BOOST_CHECK_EQUAL(ob.size(), dataSize - kDefaultBlockSize);
        BOOST_CHECK_EQUAL(ob.freeSpace(), kDefaultBlockSize / 2);
        BOOST_CHECK_EQUAL(ob.numChunks(), 1);
        BOOST_CHECK_EQUAL(ob.numDataChunks(), 2);
    }

    {
        OutputBuffer ob;
        size_t dataSize = kDefaultBlockSize * 2 + kDefaultBlockSize / 2;
        addDataToBuffer(ob, dataSize);

        BOOST_CHECK_EQUAL(ob.size(), dataSize);
        BOOST_CHECK_EQUAL(ob.freeSpace(), kDefaultBlockSize / 2);
        BOOST_CHECK_EQUAL(ob.numChunks(), 1);
        BOOST_CHECK_EQUAL(ob.numDataChunks(), 3);

        size_t remainder = dataSize % 100;

        // discard data 100 bytes at a time
        size_t discarded = 0;
        while (ob.size() > 100) {
            ob.discardData(100);
            dataSize -= 100;
            discarded += 100;

            BOOST_CHECK_EQUAL(ob.size(), dataSize);
            BOOST_CHECK_EQUAL(ob.freeSpace(), kDefaultBlockSize / 2);
            BOOST_CHECK_EQUAL(ob.numChunks(), 1);

            int chunks = 3 - (discarded / kDefaultBlockSize);
            BOOST_CHECK_EQUAL(ob.numDataChunks(), chunks);
        }

        BOOST_CHECK_EQUAL(ob.size(), remainder);
        BOOST_CHECK_EQUAL(ob.freeSpace(), kDefaultBlockSize / 2);
        BOOST_CHECK_EQUAL(ob.numChunks(), 1);
        BOOST_CHECK_EQUAL(ob.numDataChunks(), 1);

        try {
            ob.discardData(ob.size() + 1);
        } catch (std::exception &e) {
            std::cout << "Intentionally triggered exception: " << e.what() << std::endl;
        }
        ob.discardData(ob.size());

        BOOST_CHECK_EQUAL(ob.size(), 0U);
        BOOST_CHECK_EQUAL(ob.freeSpace(), kDefaultBlockSize / 2);
        BOOST_CHECK_EQUAL(ob.numChunks(), 1);
        BOOST_CHECK_EQUAL(ob.numDataChunks(), 0);
    }
}

void TestConvertToInput() {
    BOOST_TEST_MESSAGE("TestConvertToInput");
    {
        OutputBuffer ob;
        size_t dataSize = kDefaultBlockSize * 2 + kDefaultBlockSize / 2;
        addDataToBuffer(ob, dataSize);

        InputBuffer ib(ob);

        BOOST_CHECK_EQUAL(ib.size(), dataSize);
        BOOST_CHECK_EQUAL(ib.numChunks(), 3);

        BOOST_CHECK_EQUAL(ob.size(), dataSize);
        BOOST_CHECK_EQUAL(ob.freeSpace(), kDefaultBlockSize / 2);
        BOOST_CHECK_EQUAL(ob.numChunks(), 1);
        BOOST_CHECK_EQUAL(ob.numDataChunks(), 3);
    }
}

void TestExtractToInput() {
    BOOST_TEST_MESSAGE("TestExtractToInput");
    {
        OutputBuffer ob;
        size_t dataSize = kDefaultBlockSize * 2 + kDefaultBlockSize / 2;
        addDataToBuffer(ob, dataSize);

        InputBuffer ib = ob.extractData();

        BOOST_CHECK_EQUAL(ib.size(), dataSize);
        BOOST_CHECK_EQUAL(ib.numChunks(), 3);

        BOOST_CHECK_EQUAL(ob.size(), 0U);
        BOOST_CHECK_EQUAL(ob.freeSpace(), kDefaultBlockSize / 2);
        BOOST_CHECK_EQUAL(ob.numChunks(), 1);
        BOOST_CHECK_EQUAL(ob.numDataChunks(), 0);
    }

    {
        // extract no bytes
        OutputBuffer ob;
        size_t dataSize = kDefaultBlockSize * 2 + kDefaultBlockSize / 2;
        addDataToBuffer(ob, dataSize);

        InputBuffer ib = ob.extractData(0);

        BOOST_CHECK_EQUAL(ib.size(), 0U);
        BOOST_CHECK_EQUAL(ib.numChunks(), 0);

        BOOST_CHECK_EQUAL(ob.size(), dataSize);
        BOOST_CHECK_EQUAL(ob.freeSpace(), kDefaultBlockSize / 2);
        BOOST_CHECK_EQUAL(ob.numChunks(), 1);
        BOOST_CHECK_EQUAL(ob.numDataChunks(), 3);
    }

    {
        // extract exactly one block
        OutputBuffer ob;
        size_t dataSize = kDefaultBlockSize * 2 + kDefaultBlockSize / 2;
        addDataToBuffer(ob, dataSize);

        InputBuffer ib = ob.extractData(kDefaultBlockSize);

        BOOST_CHECK_EQUAL(ib.size(), kDefaultBlockSize);
        BOOST_CHECK_EQUAL(ib.numChunks(), 1);

        BOOST_CHECK_EQUAL(ob.size(), dataSize - kDefaultBlockSize);
        BOOST_CHECK_EQUAL(ob.freeSpace(), kDefaultBlockSize / 2);
        BOOST_CHECK_EQUAL(ob.numChunks(), 1);
        BOOST_CHECK_EQUAL(ob.numDataChunks(), 2);
    }

    {
        OutputBuffer ob;
        size_t dataSize = kDefaultBlockSize * 2 + kDefaultBlockSize / 2;
        addDataToBuffer(ob, dataSize);

        size_t remainder = dataSize % 100;

        // extract data 100 bytes at a time
        size_t extracted = 0;
        while (ob.size() > 100) {
            ob.extractData(100);
            dataSize -= 100;
            extracted += 100;

            BOOST_CHECK_EQUAL(ob.size(), dataSize);
            BOOST_CHECK_EQUAL(ob.freeSpace(), kDefaultBlockSize / 2);
            BOOST_CHECK_EQUAL(ob.numChunks(), 1);

            int chunks = 3 - (extracted / kDefaultBlockSize);
            BOOST_CHECK_EQUAL(ob.numDataChunks(), chunks);
        }

        BOOST_CHECK_EQUAL(ob.size(), remainder);
        BOOST_CHECK_EQUAL(ob.freeSpace(), kDefaultBlockSize / 2);
        BOOST_CHECK_EQUAL(ob.numChunks(), 1);
        BOOST_CHECK_EQUAL(ob.numDataChunks(), 1);

        try {
            ob.extractData(ob.size() + 1);
        } catch (std::exception &e) {
            std::cout << "Intentionally triggered exception: " << e.what() << std::endl;
        }

        InputBuffer ib = ob.extractData(remainder);

        BOOST_CHECK_EQUAL(ib.size(), remainder);
        BOOST_CHECK_EQUAL(ib.numChunks(), 1);

        BOOST_CHECK_EQUAL(ob.size(), 0U);
        BOOST_CHECK_EQUAL(ob.freeSpace(), kDefaultBlockSize / 2);
        BOOST_CHECK_EQUAL(ob.numChunks(), 1);
        BOOST_CHECK_EQUAL(ob.numDataChunks(), 0);
    }
}

void TestAppend() {
    BOOST_TEST_MESSAGE("TestAppend");
    {
        OutputBuffer ob;
        size_t dataSize = kDefaultBlockSize + kDefaultBlockSize / 2;
        addDataToBuffer(ob, dataSize);

        OutputBuffer a;
        a.append(ob);

        BOOST_CHECK_EQUAL(a.size(), dataSize);
        BOOST_CHECK_EQUAL(a.freeSpace(), 0U);
        BOOST_CHECK_EQUAL(a.numChunks(), 0);
        BOOST_CHECK_EQUAL(a.numDataChunks(), 2);

        // reserve on a, then append from an input buffer
        a.reserve(7000);

        InputBuffer ib(ob);
        a.append(ib);

        BOOST_CHECK_EQUAL(a.size(), dataSize * 2);
        BOOST_CHECK_EQUAL(a.freeSpace(), 7000U);
        BOOST_CHECK_EQUAL(a.numChunks(), 1);
        BOOST_CHECK_EQUAL(a.numDataChunks(), 4);
    }
}

void TestBufferStream() {
    BOOST_TEST_MESSAGE("TestBufferStream");

    {
        // write enough bytes to a buffer, to create at least 3 blocks
        std::string junk = makeString(kDefaultBlockSize);
        ostream os;
        int i = 0;
        for (; i < 3; ++i) {
            os << junk;
        }

        const OutputBuffer &buf = os.getBuffer();
        cout << "Buffer has " << buf.size() << " bytes\n";
        BOOST_CHECK_EQUAL(buf.size(), junk.size() * i);
    }
}

template<typename T>
void TestEof() {
    // create a message full of eof chars
    std::vector<char> eofs(sizeof(T) * 3 / 2, -1);

    OutputBuffer buf1;
    buf1.writeTo(&eofs[0], eofs.size());

    OutputBuffer buf2;
    buf2.writeTo(&eofs[0], eofs.size());

    // append the buffers, so the first
    // character on a buffer boundary is eof
    buf1.append(buf2);

    avro::istream is(buf1);

    for (int i = 0; i < 3; ++i) {
        T d;
        char *addr = reinterpret_cast<char *>(&d);
        is.read(addr, sizeof(T));
        BOOST_CHECK_EQUAL(is.gcount(), static_cast<std::streamsize>(sizeof(T)));
        BOOST_CHECK_EQUAL(is.eof(), false);
    }

    char c;
    is.read(&c, sizeof(c));
    BOOST_CHECK_EQUAL(is.gcount(), 0);
    BOOST_CHECK_EQUAL(is.eof(), true);
}

void TestBufferStreamEof() {
    BOOST_TEST_MESSAGE("TestBufferStreamEof");

    TestEof<int32_t>();

    TestEof<int64_t>();

    TestEof<float>();

    TestEof<double>();
}

void TestSeekAndTell() {
    BOOST_TEST_MESSAGE("TestSeekAndTell");

    {
        std::string junk = makeString(kDefaultBlockSize / 2);

        ostream os;

        // write enough bytes to a buffer, to create at least 3 blocks
        int i = 0;
        for (; i < 5; ++i) {
            os << junk;
        }

        const OutputBuffer &buf = os.getBuffer();
        cout << "Buffer has " << buf.size() << " bytes\n";

        istream is(os.getBuffer());
        BOOST_CHECK_EQUAL(is.getBuffer().size(), junk.size() * i);
        is.seekg(2000);
        BOOST_CHECK_EQUAL(is.tellg(), static_cast<std::streampos>(2000));
        is.seekg(6000);
        BOOST_CHECK_EQUAL(is.tellg(), static_cast<std::streampos>(6000));
        is.seekg(is.getBuffer().size());
        BOOST_CHECK_EQUAL(is.tellg(), static_cast<std::streampos>(is.getBuffer().size()));
        is.seekg(is.getBuffer().size() + 1);
        BOOST_CHECK_EQUAL(is.tellg(), static_cast<std::streampos>(-1));
    }
}

void TestReadSome() {
    BOOST_TEST_MESSAGE("TestReadSome");
    {
        std::string junk = makeString(kDefaultBlockSize / 2);

        ostream os;

        // write enough bytes to a buffer, to create at least 3 blocks
        int i = 0;
        for (; i < 5; ++i) {
            os << junk;
        }

        cout << "Buffer has " << os.getBuffer().size() << " bytes\n";

        istream is(os.getBuffer());

        char datain[5000];

        while (is.rdbuf()->in_avail()) {
            auto bytesAvail = static_cast<size_t>(is.rdbuf()->in_avail());
            cout << "Bytes avail = " << bytesAvail << endl;
            auto in = static_cast<size_t>(is.readsome(datain, sizeof(datain)));
            cout << "Bytes read = " << in << endl;
            BOOST_CHECK_EQUAL(bytesAvail, in);
        }
    }
}

void TestSeek() {
    BOOST_TEST_MESSAGE("TestSeek");
    {
        const std::string str = "SampleMessage";

        avro::OutputBuffer tmp1, tmp2, tmp3;
        tmp1.writeTo(str.c_str(), 3);      // Sam
        tmp2.writeTo(str.c_str() + 3, 7);  // pleMess
        tmp3.writeTo(str.c_str() + 10, 3); // age

        tmp2.append(tmp3);
        tmp1.append(tmp2);

        BOOST_CHECK_EQUAL(tmp3.numDataChunks(), 1);
        BOOST_CHECK_EQUAL(tmp2.numDataChunks(), 2);
        BOOST_CHECK_EQUAL(tmp1.numDataChunks(), 3);

        avro::InputBuffer buf(tmp1);

        cout << "Starting string: " << str << '\n';
        BOOST_CHECK_EQUAL(static_cast<std::string::size_type>(buf.size()), str.size());

        avro::istream is(buf);

        const std::string part1 = "Sample";
        char buffer[16];
        is.read(buffer, part1.size());
        std::string sample1(buffer, part1.size());
        cout << "After reading bytes: " << sample1 << '\n';
        BOOST_CHECK_EQUAL(sample1, part1);

        const std::string part2 = "Message";
        is.read(buffer, part2.size());
        std::string sample2(buffer, part2.size());
        cout << "After reading remaining bytes: " << sample2 << '\n';
        BOOST_CHECK_EQUAL(sample2, part2);

        cout << "Seeking back " << '\n';
        is.seekg(-static_cast<std::streamoff>(part2.size()), std::ios_base::cur);

        std::streampos loc = is.tellg();
        cout << "Saved loc = " << loc << '\n';
        BOOST_CHECK_EQUAL(static_cast<std::string::size_type>(loc), (str.size() - part2.size()));

        cout << "Reading remaining bytes: " << is.rdbuf() << '\n';
        cout << "bytes avail = " << is.rdbuf()->in_avail() << '\n';
        BOOST_CHECK_EQUAL(is.rdbuf()->in_avail(), 0);

        cout << "Moving to saved loc = " << loc << '\n';
        is.seekg(loc);
        cout << "bytes avail = " << is.rdbuf()->in_avail() << '\n';

        std::ostringstream oss;
        oss << is.rdbuf();
        cout << "After reading bytes: " << oss.str() << '\n';
        BOOST_CHECK_EQUAL(oss.str(), part2);
    }
}

void TestIterator() {
    BOOST_TEST_MESSAGE("TestIterator");
    {
        OutputBuffer ob(2 * kMaxBlockSize + 10);
        BOOST_CHECK_EQUAL(ob.numChunks(), 3);
        BOOST_CHECK_EQUAL(ob.size(), 0U);
        BOOST_CHECK_EQUAL(ob.freeSpace(), 2 * kMaxBlockSize + kMinBlockSize);

        BOOST_CHECK_EQUAL(std::distance(ob.begin(), ob.end()), 3);

        OutputBuffer::const_iterator iter = ob.begin();
        BOOST_CHECK_EQUAL(iter->size(), kMaxBlockSize);
        ++iter;
        BOOST_CHECK_EQUAL(iter->size(), kMaxBlockSize);
        ++iter;
        BOOST_CHECK_EQUAL(iter->size(), kMinBlockSize);
        ++iter;
        BOOST_CHECK(iter == ob.end());

        size_t toWrite = kMaxBlockSize + kMinBlockSize;
        ob.wroteTo(toWrite);
        BOOST_CHECK_EQUAL(ob.size(), toWrite);
        BOOST_CHECK_EQUAL(ob.freeSpace(), kMaxBlockSize);
        BOOST_CHECK_EQUAL(ob.numChunks(), 2);
        BOOST_CHECK_EQUAL(ob.numDataChunks(), 2);

        InputBuffer ib = ob;
        BOOST_CHECK_EQUAL(std::distance(ib.begin(), ib.end()), 2);

        size_t acc = 0;
        for (auto &it : ob) {
            acc += it.size();
        }
        BOOST_CHECK_EQUAL(ob.freeSpace(), acc);

        try {
            ob.wroteTo(acc + 1);
        } catch (std::exception &e) {
            std::cout << "Intentionally triggered exception: " << e.what() << std::endl;
        }
    }
}

#ifdef HAVE_BOOST_ASIO
void server(boost::barrier &b) {
    using boost::asio::ip::tcp;
    boost::asio::io_service io_service;
    tcp::acceptor a(io_service, tcp::endpoint(tcp::v4(), 33333));
    tcp::socket sock(io_service);
    a.listen();

    b.wait();

    a.accept(sock);
    avro::OutputBuffer buf(100);

    size_t length = sock.receive(buf);
    buf.wroteTo(length);
    cout << "Server got " << length << " bytes\n";

    InputBuffer rbuf(buf);

    std::string res;

    avro::InputBuffer::const_iterator iter = rbuf.begin();
    while (iter != rbuf.end()) {
        res.append(boost::asio::buffer_cast<const char *>(*iter), boost::asio::buffer_size(*iter));
        cout << "Received Buffer size: " << boost::asio::buffer_size(*iter) << endl;
        BOOST_CHECK_EQUAL(length, boost::asio::buffer_size(*iter));
        cout << "Received Buffer: \"" << res << '"' << endl;
        ++iter;
    }

    BOOST_CHECK_EQUAL(res, "hello world");
}

void TestAsioBuffer() {
    using boost::asio::ip::tcp;
    BOOST_TEST_MESSAGE("TestAsioBuffer");
    {
        boost::barrier b(2);

        boost::thread t(boost::bind(server, boost::ref(b)));

        b.wait();

        // set up the thing
        boost::asio::io_service io_service;

        tcp::resolver resolver(io_service);
        tcp::resolver::query query(tcp::v4(), "localhost", "33333");
        tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);
        tcp::resolver::iterator end;

        tcp::socket socket(io_service);
        boost::system::error_code error = boost::asio::error::host_not_found;
        while (error && endpoint_iterator != end) {
            socket.close();
            socket.connect(*endpoint_iterator++, error);
        }
        if (error) {
            throw error;
        }

        std::string hello = "hello ";
        std::string world = "world";
        avro::OutputBuffer buf;
        buf.writeTo(hello.c_str(), hello.size());

        BOOST_CHECK_EQUAL(buf.size(), hello.size());

        avro::OutputBuffer buf2;
        buf2.writeTo(world.c_str(), world.size());
        BOOST_CHECK_EQUAL(buf2.size(), world.size());

        buf.append(buf2);
        BOOST_CHECK_EQUAL(buf.size(), hello.size() + world.size());

        cout << "Distance " << std::distance(buf.begin(), buf.end()) << endl;
        BOOST_CHECK_EQUAL(std::distance(buf.begin(), buf.end()), 1);

        const avro::InputBuffer rbuf(buf);

        avro::InputBuffer::const_iterator iter = rbuf.begin();
        while (iter != rbuf.end()) {
            std::string str(boost::asio::buffer_cast<const char *>(*iter), boost::asio::buffer_size(*iter));
            cout << "Buffer size: " << boost::asio::buffer_size(*iter) << endl;
            cout << "Buffer: \"" << str << '"' << endl;
            ++iter;
        }

        cout << "Buffer size " << rbuf.size() << endl;

        std::size_t wrote = boost::asio::write(socket, rbuf);
        cout << "Wrote " << wrote << endl;
        BOOST_CHECK_EQUAL(wrote, rbuf.size());

        t.join();
    }
}
#else
void TestAsioBuffer() {
    cout << "Skipping asio test\n";
}
#endif // HAVE_BOOST_ASIO

void TestSplit() {
    BOOST_TEST_MESSAGE("TestSplit");
    {
        const std::string str = "This message is to be split";

        avro::OutputBuffer buf;
        buf.writeTo(str.c_str(), str.size());

        char datain[12];
        avro::istream is(buf);
        auto in = static_cast<size_t>(is.readsome(datain, sizeof(datain)));
        BOOST_CHECK_EQUAL(in, sizeof(datain));
        BOOST_CHECK_EQUAL(static_cast<size_t>(is.tellg()), sizeof(datain));

        OutputBuffer part2;
        part2.append(is.getBuffer());
        BOOST_CHECK_EQUAL(part2.size(), buf.size());
        InputBuffer part1 = part2.extractData(static_cast<size_t>(is.tellg()));

        BOOST_CHECK_EQUAL(part2.size(), str.size() - in);

        printBuffer(part1);
        printBuffer(part2);
    }
}

void TestSplitOnBorder() {
    BOOST_TEST_MESSAGE("TestSplitOnBorder");
    {

        const std::string part1 = "This message";
        const std::string part2 = " is to be split";

        avro::OutputBuffer buf;
        buf.writeTo(part1.c_str(), part1.size());
        size_t firstChunkSize = buf.size();

        {
            avro::OutputBuffer tmp;
            tmp.writeTo(part2.c_str(), part2.size());
            buf.append(tmp);
            printBuffer(InputBuffer(buf));
        }

        BOOST_CHECK_EQUAL(buf.numDataChunks(), 2);
        size_t bufsize = buf.size();

        std::unique_ptr<char[]> datain(new char[firstChunkSize]);
        avro::istream is(buf);
        auto in = static_cast<size_t>(is.readsome(&datain[0], firstChunkSize));
        BOOST_CHECK_EQUAL(in, firstChunkSize);

        OutputBuffer newBuf;
        newBuf.append(is.getBuffer());
        newBuf.discardData(static_cast<size_t>(is.tellg()));
        BOOST_CHECK_EQUAL(newBuf.numDataChunks(), 1);

        BOOST_CHECK_EQUAL(newBuf.size(), bufsize - in);

        cout << is.rdbuf() << endl;
        printBuffer(newBuf);
    }
}

void TestSplitTwice() {
    BOOST_TEST_MESSAGE("TestSplitTwice");
    {
        const std::string msg1 = makeString(30);

        avro::OutputBuffer buf1;
        buf1.writeTo(msg1.c_str(), msg1.size());

        BOOST_CHECK_EQUAL(buf1.size(), msg1.size());

        printBuffer(buf1);

        avro::istream is(buf1);
        char buffer[6];
        is.readsome(buffer, 5);
        buffer[5] = 0;
        std::cout << "buffer =" << buffer << std::endl;

        buf1.discardData(static_cast<size_t>(is.tellg()));
        printBuffer(buf1);

        avro::istream is2(buf1);
        is2.seekg(15);

        buf1.discardData(static_cast<size_t>(is2.tellg()));
        printBuffer(buf1);
    }
}

void TestCopy() {
    BOOST_TEST_MESSAGE("TestCopy");

    const std::string msg = makeString(30);
    // Test1, small data, small buffer
    {
        std::cout << "Test1\n";
        // put a small amount of data in the buffer
        avro::OutputBuffer wb;

        wb.writeTo(msg.c_str(), msg.size());

        BOOST_CHECK_EQUAL(msg.size(), wb.size());
        BOOST_CHECK_EQUAL(wb.numDataChunks(), 1);
        BOOST_CHECK_EQUAL(kDefaultBlockSize - msg.size(),
                          wb.freeSpace());

        // copy starting at offset 5 and copying 10 less bytes
        BufferReader br(wb);
        br.seek(5);
        avro::InputBuffer ib = br.copyData(msg.size() - 10);

        printBuffer(ib);

        BOOST_CHECK_EQUAL(ib.numChunks(), 1);
        BOOST_CHECK_EQUAL(ib.size(), msg.size() - 10);

        // buf 1 should be unchanged
        BOOST_CHECK_EQUAL(msg.size(), wb.size());
        BOOST_CHECK_EQUAL(wb.numDataChunks(), 1);
        BOOST_CHECK_EQUAL(kDefaultBlockSize - msg.size(),
                          wb.freeSpace());

        // make sure wb is still functional
        wb.reserve(kDefaultBlockSize);
        BOOST_CHECK_EQUAL(wb.size(), msg.size());
        BOOST_CHECK_EQUAL(wb.numChunks(), 2);
        BOOST_CHECK_EQUAL(kDefaultBlockSize * 2 - msg.size(),
                          wb.freeSpace());
    }

    // Test2, small data, large buffer
    {
        std::cout << "Test2\n";
        // put a small amount of data in the buffer
        const OutputBuffer::size_type bufsize = 3 * kMaxBlockSize;

        avro::OutputBuffer wb(bufsize);
        BOOST_CHECK_EQUAL(wb.numChunks(), 3);
        BOOST_CHECK_EQUAL(wb.freeSpace(), bufsize);

        wb.writeTo(msg.c_str(), msg.size());

        BOOST_CHECK_EQUAL(wb.size(), msg.size());
        BOOST_CHECK_EQUAL(wb.numDataChunks(), 1);
        BOOST_CHECK_EQUAL(bufsize - msg.size(),
                          wb.freeSpace());

        BufferReader br(wb);
        br.seek(5);
        avro::InputBuffer ib = br.copyData(msg.size() - 10);

        printBuffer(ib);

        BOOST_CHECK_EQUAL(ib.numChunks(), 1);
        BOOST_CHECK_EQUAL(ib.size(), msg.size() - 10);

        // wb should be unchanged
        BOOST_CHECK_EQUAL(msg.size(), wb.size());
        BOOST_CHECK_EQUAL(wb.numChunks(), 3);
        BOOST_CHECK_EQUAL(wb.numDataChunks(), 1);
        BOOST_CHECK_EQUAL(bufsize - msg.size(), wb.freeSpace());

        // reserving a small amount should have no effect
        wb.reserve(1);
        BOOST_CHECK_EQUAL(msg.size(), wb.size());
        BOOST_CHECK_EQUAL(wb.numChunks(), 3);
        BOOST_CHECK_EQUAL(bufsize - msg.size(), wb.freeSpace());

        // reserve more (will get extra block)
        wb.reserve(bufsize);
        BOOST_CHECK_EQUAL(msg.size(), wb.size());
        BOOST_CHECK_EQUAL(wb.numChunks(), 4);
        BOOST_CHECK_EQUAL(kMaxBlockSize * 3 - msg.size() + kMinBlockSize,
                          wb.freeSpace());
    }

    // Test3 Border case, buffer is exactly full
    {
        std::cout << "Test3\n";
        const OutputBuffer::size_type bufsize = 2 * kDefaultBlockSize;
        avro::OutputBuffer wb;

        for (unsigned i = 0; i < bufsize; ++i) {
            wb.writeTo('a');
        }

        BOOST_CHECK_EQUAL(wb.size(), bufsize);
        BOOST_CHECK_EQUAL(wb.freeSpace(), 0U);
        BOOST_CHECK_EQUAL(wb.numChunks(), 0);
        BOOST_CHECK_EQUAL(wb.numDataChunks(), 2);

        // copy where the chunks overlap
        BufferReader br(wb);
        br.seek(bufsize / 2 - 10);
        avro::InputBuffer ib = br.copyData(20);

        printBuffer(ib);

        BOOST_CHECK_EQUAL(ib.size(), 20U);
        BOOST_CHECK_EQUAL(ib.numChunks(), 2);

        // wb should be unchanged
        BOOST_CHECK_EQUAL(wb.size(), bufsize);
        BOOST_CHECK_EQUAL(wb.freeSpace(), 0U);
        BOOST_CHECK_EQUAL(wb.numDataChunks(), 2);
    }

    // Test4, no data
    {
        const OutputBuffer::size_type bufsize = 2 * kMaxBlockSize;
        std::cout << "Test4\n";
        avro::OutputBuffer wb(bufsize);
        BOOST_CHECK_EQUAL(wb.numChunks(), 2);
        BOOST_CHECK_EQUAL(wb.size(), 0U);
        BOOST_CHECK_EQUAL(wb.freeSpace(), bufsize);

        avro::InputBuffer ib;
        try {
            BufferReader br(wb);
            br.seek(10);
        } catch (std::exception &e) {
            cout << "Intentially triggered exception: " << e.what() << endl;
        }
        try {
            BufferReader br(wb);
            avro::InputBuffer it = br.copyData(10);
        } catch (std::exception &e) {
            cout << "Intentially triggered exception: " << e.what() << endl;
        }

        BOOST_CHECK_EQUAL(ib.numChunks(), 0);
        BOOST_CHECK_EQUAL(ib.size(), 0U);

        // wb should keep all blocks remaining
        BOOST_CHECK_EQUAL(wb.numChunks(), 2);
        BOOST_CHECK_EQUAL(wb.size(), 0U);
        BOOST_CHECK_EQUAL(wb.freeSpace(), bufsize);
    }
}

// this is reproducing a sequence of steps that caused a crash
void TestBug() {
    BOOST_TEST_MESSAGE("TestBug");
    {
        OutputBuffer rxBuf;
        OutputBuffer buf;
        rxBuf.reserve(64 * 1024);

        rxBuf.wroteTo(2896);

        {
            avro::InputBuffer ib(rxBuf.extractData());
            buf.append(ib);
        }

        buf.discardData(61);

        rxBuf.reserve(64 * 1024);
        rxBuf.wroteTo(381);

        {
            avro::InputBuffer ib(rxBuf.extractData());
            buf.append(ib);
        }

        buf.discardData(3216);

        rxBuf.reserve(64 * 1024);
    }
}

bool safeToDelete = false;

void deleteForeign(const std::string &val) {
    std::cout << "Deleting foreign string containing " << val << '\n';
    BOOST_CHECK(safeToDelete);
}

void TestForeign() {
    BOOST_TEST_MESSAGE("TestForeign");
    {
        std::string hello = "hello ";
        std::string there = "there ";
        std::string world = "world ";

        OutputBuffer copy;

        {
            OutputBuffer buf;
            buf.writeTo(hello.c_str(), hello.size());
            buf.appendForeignData(there.c_str(), there.size(), boost::bind(&deleteForeign, there));
            buf.writeTo(world.c_str(), world.size());

            printBuffer(buf);
            BOOST_CHECK_EQUAL(buf.size(), 18U);
            copy = buf;
        }
        std::cout << "Leaving inner scope\n";
        safeToDelete = true;
    }
    std::cout << "Leaving outer scope\n";
    safeToDelete = false;
}

void TestForeignDiscard() {
    BOOST_TEST_MESSAGE("TestForeign");
    {
        std::string hello = "hello ";
        std::string again = "again ";
        std::string there = "there ";
        std::string world = "world ";

        OutputBuffer buf;
        buf.writeTo(hello.c_str(), hello.size());
        buf.appendForeignData(again.c_str(), again.size(), boost::bind(&deleteForeign, again));
        buf.appendForeignData(there.c_str(), there.size(), boost::bind(&deleteForeign, there));
        buf.writeTo(world.c_str(), world.size());

        printBuffer(buf);
        BOOST_CHECK_EQUAL(buf.size(), 24U);

        // discard some data including half the foreign buffer
        buf.discardData(9);
        printBuffer(buf);
        BOOST_CHECK_EQUAL(buf.size(), 15U);

        // discard some more data, which will lop off the first foreign buffer
        safeToDelete = true;
        buf.discardData(6);
        safeToDelete = false;
        printBuffer(buf);
        BOOST_CHECK_EQUAL(buf.size(), 9U);

        // discard some more data, which will lop off the second foreign buffer
        safeToDelete = true;
        buf.discardData(3);
        safeToDelete = false;
        printBuffer(buf);
        BOOST_CHECK_EQUAL(buf.size(), 6U);
    }
}

void TestPrinter() {
    BOOST_TEST_MESSAGE("TestPrinter");
    {
        OutputBuffer ob;
        addDataToBuffer(ob, 128);

        std::cout << ob << std::endl;
    }
}

struct BufferTestSuite : public boost::unit_test::test_suite {
    BufferTestSuite() : boost::unit_test::test_suite("BufferTestSuite") {
        add(BOOST_TEST_CASE(TestReserve));
        add(BOOST_TEST_CASE(TestGrow));
        add(BOOST_TEST_CASE(TestDiscard));
        add(BOOST_TEST_CASE(TestConvertToInput));
        add(BOOST_TEST_CASE(TestExtractToInput));
        add(BOOST_TEST_CASE(TestAppend));
        add(BOOST_TEST_CASE(TestBufferStream));
        add(BOOST_TEST_CASE(TestBufferStreamEof));
        add(BOOST_TEST_CASE(TestSeekAndTell));
        add(BOOST_TEST_CASE(TestReadSome));
        add(BOOST_TEST_CASE(TestSeek));
        add(BOOST_TEST_CASE(TestIterator));
        add(BOOST_TEST_CASE(TestAsioBuffer));
        add(BOOST_TEST_CASE(TestSplit));
        add(BOOST_TEST_CASE(TestSplitOnBorder));
        add(BOOST_TEST_CASE(TestSplitTwice));
        add(BOOST_TEST_CASE(TestCopy));
        add(BOOST_TEST_CASE(TestBug));
        add(BOOST_TEST_CASE(TestForeign));
        add(BOOST_TEST_CASE(TestForeignDiscard));
        add(BOOST_TEST_CASE(TestPrinter));
    }
};

boost::unit_test::test_suite *
init_unit_test_suite(int, char *[]) {
    auto *test(BOOST_TEST_SUITE("Buffer Unit Tests"));
    test->add(new BufferTestSuite());

    return test;
}
