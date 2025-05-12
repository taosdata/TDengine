Snappy is a compression/decompression library. It does not aim for maximum
compression, or compatibility with any other compression library; instead, it
aims for very high speeds and reasonable compression. For instance, compared
to the fastest mode of zlib, Snappy is an order of magnitude faster for most
inputs, but the resulting compressed files are anywhere from 20% to 100%
bigger. On a single core of a Core i7 processor in 64-bit mode, Snappy
compresses at about 250 MB/sec or more and decompresses at about 500 MB/sec
or more.

Snappy is widely used inside Google, in everything from BigTable and MapReduce
to our internal RPC systems. (Snappy has previously been referred to as "Zippy"
in some presentations and the likes.)

For more information, please see the [README](../README.md). Benchmarks against
a few other compression libraries (zlib, LZO, LZF, FastLZ, and QuickLZ) are
included in the source code distribution. The source code also contains a
[formal format specification](../format_description.txt), as well
as a specification for a [framing format](../framing_format.txt) useful for
higher-level framing and encapsulation of Snappy data, e.g. for transporting
Snappy-compressed data across HTTP in a streaming fashion. Note that the Snappy
distribution currently has no code implementing the latter, but some of the
ports do (see below).

Snappy is written in C++, but C bindings are included, and several bindings to
other languages are maintained by third parties:

* C#: [Snappy for .NET](http://snappy4net.codeplex.com/) (P/Invoke wrapper),
  [Snappy.NET](http://snappy.angeloflogic.com/) (P/Invoke wrapper),
  [Snappy.Sharp](https://github.com/jeffesp/Snappy.Sharp) (native
  reimplementation)
* [C port](http://github.com/andikleen/snappy-c)
* [C++ MSVC packaging](http://snappy.angeloflogic.com/) (plus Windows binaries,
  NuGet packages and command-line tool)
* Common Lisp: [Library bindings](http://flambard.github.com/thnappy/),
  [native reimplementation](https://github.com/brown/snappy)
* Erlang: [esnappy](https://github.com/thekvs/esnappy),
  [snappy-erlang-nif](https://github.com/fdmanana/snappy-erlang-nif)
* [Go](https://github.com/golang/snappy/)
* [Haskell](http://hackage.haskell.org/package/snappy)
* [Haxe](https://github.com/MaddinXx/hxsnappy) (C++/Neko)
* [iOS packaging](https://github.com/ideawu/snappy-ios)
* Java: [JNI wrapper](https://github.com/xerial/snappy-java) (including the
  framing format), [native reimplementation](http://code.google.com/p/jsnappy/),
  [other native reimplementation](https://github.com/dain/snappy) (including
  the framing format)
* [Lua](https://github.com/forhappy/lua-snappy)
* [Node.js](https://github.com/kesla/node-snappy) (including the [framing
  format](https://github.com/kesla/node-snappy-stream))
* [Perl](http://search.cpan.org/dist/Compress-Snappy/)
* [PHP](https://github.com/kjdev/php-ext-snappy)
* [Python](http://pypi.python.org/pypi/python-snappy) (including a command-line
  tool for the framing format)
* [R](https://github.com/lulyon/R-snappy)
* [Ruby](https://github.com/miyucy/snappy)
* [Rust](https://github.com/BurntSushi/rust-snappy)
* [Smalltalk](https://github.com/mumez/sqnappy) (including the framing format)

Snappy is used or is available as an alternative in software such as

* [MongoDB](https://www.mongodb.com/)
* [Cassandra](http://cassandra.apache.org/)
* [Couchbase](http://www.couchbase.com/)
* [Hadoop](http://hadoop.apache.org/)
* [LessFS](http://www.lessfs.com/wordpress/)
* [LevelDB](https://github.com/google/leveldb) (which is in turn used by
  [Google Chrome](http://chrome.google.com/))
* [Lucene](http://lucene.apache.org/)
* [VoltDB](http://voltdb.com/)

If you know of more, do not hesitate to let us know. The easiest way to get in
touch is via the
[Snappy discussion mailing list](http://groups.google.com/group/snappy-compression).
