# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#!/usr/bin/env perl

use strict;
use warnings;
use Avro::Schema;
use Config;
use Test::More;
use Test::Exception;
use Math::BigInt;

use_ok 'Avro::BinaryEncoder';

sub primitive_ok {
    my ($primitive_type, $primitive_val, $expected_enc) = @_;

    my $data;
    my $meth = "encode_$primitive_type";
    Avro::BinaryEncoder->$meth(
        undef, $primitive_val, sub { $data = ${$_[0]} }
    );
    is $data, $expected_enc, "primitive $primitive_type encoded correctly";
    return $data;
}

## some primitive testing
{
    primitive_ok null    =>    undef, '';
    primitive_ok null    => 'whatev', '';

    primitive_ok boolean => 0, "\x0";
    primitive_ok boolean => 1, "\x1";

    ## - high-bit of each byte should be set except for last one
    ## - rest of bits are:
    ## - little endian
    ## - zigzag coded
    primitive_ok long    =>        0, pack("C*", 0);
    primitive_ok long    =>        1, pack("C*", 0x2);
    primitive_ok long    =>       -1, pack("C*", 0x1);
    primitive_ok int     =>       -1, pack("C*", 0x1);
    primitive_ok int     =>      -20, pack("C*", 0b0010_0111);
    primitive_ok int     =>       20, pack("C*", 0b0010_1000);
    primitive_ok int     =>       63, pack("C*", 0b0111_1110);
    primitive_ok int     =>       64, pack("C*", 0b1000_0000, 0b0000_0001);
    my $p =
    primitive_ok int     =>      -65, pack("C*", 0b1000_0001, 0b0000_0001);
    primitive_ok int     =>       65, pack("C*", 0b1000_0010, 0b0000_0001);
    primitive_ok int     =>       99, "\xc6\x01";

    ## BigInt values still work
    primitive_ok int     => Math::BigInt->new(-65), $p;

    throws_ok {
        my $toobig;
        if ($Config{use64bitint}) {
            $toobig = 1<<32;
        }
        else {
            require Math::BigInt;
            $toobig = Math::BigInt->new(1)->blsft(32);
        }
        primitive_ok int => $toobig, undef;
    } "Avro::BinaryEncoder::Error", "33 bits";

    throws_ok {
        primitive_ok int => Math::BigInt->new(1)->blsft(63), undef;
    } "Avro::BinaryEncoder::Error", "65 bits";

    for (qw(long int)) {
        throws_ok {
            primitive_ok $_ =>  "x", undef;
        } 'Avro::BinaryEncoder::Error', 'numeric values only';
    };
    # In Unicode, there are decimals that aren't 0-9.
    # Make sure we handle non-ascii decimals cleanly.
    for (qw(long int)) {
        throws_ok {
            primitive_ok $_ =>  "\N{U+0661}", undef;
        } 'Avro::BinaryEncoder::Error', 'ascii decimals only';
    };
}

## spec examples
{
    my $enc = '';
    my $schema = Avro::Schema->parse(q({ "type": "string" }));
    Avro::BinaryEncoder->encode(
        schema => $schema,
        data => "foo",
        emit_cb => sub { $enc .= ${ $_[0] } },
    );
    is $enc, "\x06\x66\x6f\x6f", "Binary_Encodings.Primitive_Types";

    $schema = Avro::Schema->parse(<<EOJ);
          {
          "type": "record",
          "name": "test",
          "fields" : [
          {"name": "a", "type": "long"},
          {"name": "b", "type": "string"}
          ]
          }
EOJ
    $enc = '';
    Avro::BinaryEncoder->encode(
        schema => $schema,
        data => { a => 27, b => 'foo' },
        emit_cb => sub { $enc .= ${ $_[0] } },
    );
    is $enc, "\x36\x06\x66\x6f\x6f", "Binary_Encodings.Complex_Types.Records";

    $enc = '';
    $schema = Avro::Schema->parse(q({"type": "array", "items": "long"}));
    Avro::BinaryEncoder->encode(
        schema => $schema,
        data => [3, 27],
        emit_cb => sub { $enc .= ${ $_[0] } },
    );
    is $enc, "\x04\x06\x36\x00", "Binary_Encodings.Complex_Types.Arrays";

    $enc = '';
    $schema = Avro::Schema->parse(q(["string","null"]));
    Avro::BinaryEncoder->encode(
        schema => $schema,
        data => undef,
        emit_cb => sub { $enc .= ${ $_[0] } },
    );
    is $enc, "\x02", "Binary_Encodings.Complex_Types.Unions-null";

    $enc = '';
    Avro::BinaryEncoder->encode(
        schema => $schema,
        data => "a",
        emit_cb => sub { $enc .= ${ $_[0] } },
    );
    is $enc, "\x00\x02\x61", "Binary_Encodings.Complex_Types.Unions-a";
}

done_testing;
