# Apache Avro Thrift Compatibility

This module permits serialization of [Thrift](https://thrift.apache.org)-generated classes as
Avro data.

## Building

The thrift-generated files for tests are [checked-in](src/test/java/org/apache/avro/thrift/test)
so that every developer who runs tests need not have the Thrift compiler installed.

To regenerate the thrift files, you need to have the required version of thrift-compiler
installed and run `mvn -Pthrift-generate generate-test-sources`.
