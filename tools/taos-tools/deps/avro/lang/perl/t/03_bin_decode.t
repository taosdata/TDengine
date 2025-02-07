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
use Avro::BinaryEncoder;
use Test::More;
use Test::Exception;
use IO::String;

use_ok 'Avro::BinaryDecoder';

## spec examples
{
    my $enc = "\x06\x66\x6f\x6f";
    my $schema = Avro::Schema->parse(q({ "type": "string" }));
    my $reader = IO::String->new($enc);
    my $dec = Avro::BinaryDecoder->decode(
        writer_schema => $schema,
        reader_schema => $schema,
        reader        => $reader,
    );
    is $dec, "foo", "Binary_Encodings.Primitive_Types";

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
    $reader = IO::String->new("\x36\x06\x66\x6f\x6f");
    $dec = Avro::BinaryDecoder->decode(
        writer_schema => $schema,
        reader_schema => $schema,
        reader        => $reader,
    );
    is_deeply $dec, { a => 27, b => 'foo' },
                    "Binary_Encodings.Complex_Types.Records";

    $reader = IO::String->new("\x04\x06\x36\x00");
    $schema = Avro::Schema->parse(q({"type": "array", "items": "long"}));
    $dec = Avro::BinaryDecoder->decode(
        writer_schema => $schema,
        reader_schema => $schema,
        reader        => $reader,
    );
    is_deeply $dec, [3, 27], "Binary_Encodings.Complex_Types.Arrays";

    $reader = IO::String->new("\x02");
    $schema = Avro::Schema->parse(q(["string","null"]));
    $dec = Avro::BinaryDecoder->decode(
        writer_schema => $schema,
        reader_schema => $schema,
        reader         => $reader,
    );
    is $dec, undef, "Binary_Encodings.Complex_Types.Unions-null";

    $reader =  IO::String->new("\x00\x02\x61");
    $dec = Avro::BinaryDecoder->decode(
        writer_schema => $schema,
        reader_schema => $schema,
        reader        => $reader,
    );
    is $dec, "a", "Binary_Encodings.Complex_Types.Unions-a";
}

## enum schema resolution
{

    my $w_enum = Avro::Schema->parse(<<EOP);
{ "type": "enum", "name": "enum", "symbols": [ "a", "b", "c", "\$", "z" ] }
EOP
    my $r_enum = Avro::Schema->parse(<<EOP);
{ "type": "enum", "name": "enum", "symbols": [ "\$", "b", "c", "d" ] }
EOP
    ok ! !Avro::Schema->match( reader => $r_enum, writer => $w_enum ), "match";
    my $enc;
    for my $data (qw/b c $/) {
        Avro::BinaryEncoder->encode(
            schema  => $w_enum,
            data    => $data,
            emit_cb => sub { $enc = ${ $_[0] } },
        );
        my $dec = Avro::BinaryDecoder->decode(
            writer_schema => $w_enum,
            reader_schema => $r_enum,
            reader => IO::String->new($enc),
        );
        is $dec, $data, "decoded!";
    }

    for my $data (qw/a z/) {
        Avro::BinaryEncoder->encode(
            schema  => $w_enum,
            data    => $data,
            emit_cb => sub { $enc = ${ $_[0] } },
        );
        throws_ok { Avro::BinaryDecoder->decode(
            writer_schema => $w_enum,
            reader_schema => $r_enum,
            reader => IO::String->new($enc),
        )} "Avro::Schema::Error::Mismatch", "schema problem";
    }
}

## record resolution
{
    my $w_schema = Avro::Schema->parse(<<EOJ);
          { "type": "record", "name": "test",
            "fields" : [
                {"name": "a", "type": "long"},
                {"name": "bonus", "type": "string"} ]}
EOJ

    my $r_schema = Avro::Schema->parse(<<EOJ);
          { "type": "record", "name": "test",
            "fields" : [
                {"name": "t", "type": "float", "default": 37.5 },
                {"name": "a", "type": "long"} ]}
EOJ

    my $data = { a => 1, bonus => "i" };
    my $enc = '';
    Avro::BinaryEncoder->encode(
        schema  => $w_schema,
        data    => $data,
        emit_cb => sub { $enc .= ${ $_[0] } },
    );
    my $dec = Avro::BinaryDecoder->decode(
        writer_schema => $w_schema,
        reader_schema => $r_schema,
        reader => IO::String->new($enc),
    );
    is $dec->{a}, 1, "easy";
    ok ! exists $dec->{bonus}, "bonus extra field ignored";
    is $dec->{t}, 37.5, "default t from reader used";

    ## delete the default for t
    delete $r_schema->fields->[0]{default};
    throws_ok {
        Avro::BinaryDecoder->decode(
            writer_schema => $w_schema,
            reader_schema => $r_schema,
            reader => IO::String->new($enc),
        );
    } "Avro::Schema::Error::Mismatch", "no default value!";
}

## union resolution
{
    my $w_schema = Avro::Schema->parse(<<EOP);
[ "string", "null", { "type": "array", "items": "long" }]
EOP
    my $r_schema = Avro::Schema->parse(<<EOP);
[ "boolean", "null", { "type": "array", "items": "double" }]
EOP
    my $enc = '';
    my $data = [ 1, 2, 3, 4, 5, 6 ];
    Avro::BinaryEncoder->encode(
        schema  => $w_schema,
        data    => $data,
        emit_cb => sub { $enc .= ${ $_[0] } },
    );
    my $dec = Avro::BinaryDecoder->decode(
        writer_schema => $w_schema,
        reader_schema => $r_schema,
        reader => IO::String->new($enc),
    );

    is_deeply $dec, $data, "decoded!";
}

## map resolution
{
    my $w_schema = Avro::Schema->parse(<<EOP);
{ "type": "map", "values": { "type": "array", "items": "string" } }
EOP
    my $r_schema = Avro::Schema->parse(<<EOP);
{ "type": "map", "values": { "type": "array", "items": "int" } }
EOP
    my $enc = '';
    my $data = { "one" => [ "un", "one" ], two => [ "deux", "two" ] };

    Avro::BinaryEncoder->encode(
        schema  => $w_schema,
        data    => $data,
        emit_cb => sub { $enc .= ${ $_[0] } },
    );
    throws_ok {
        Avro::BinaryDecoder->decode(
            writer_schema => $w_schema,
            reader_schema => $r_schema,
            reader => IO::String->new($enc),
        )
    } "Avro::Schema::Error::Mismatch", "recursively... fails";

    my $dec = Avro::BinaryDecoder->decode(
        writer_schema => $w_schema,
        reader_schema => $w_schema,
        reader => IO::String->new($enc),
    );
    is_deeply $dec, $data, "decoded succeeded!";
}

## schema upgrade
{
    my $w_schema = Avro::Schema->parse(<<EOP);
{ "type": "map", "values": { "type": "array", "items": "int" } }
EOP
    my $r_schema = Avro::Schema->parse(<<EOP);
{ "type": "map", "values": { "type": "array", "items": "float" } }
EOP
    my $enc = '';
    my $data = { "one" => [ 1, 2 ], two => [ 1, 30 ] };

    Avro::BinaryEncoder->encode(
        schema  => $w_schema,
        data    => $data,
        emit_cb => sub { $enc .= ${ $_[0] } },
    );
    my $dec = Avro::BinaryDecoder->decode(
        writer_schema => $w_schema,
        reader_schema => $w_schema,
        reader => IO::String->new($enc),
    );
    is_deeply $dec, $data, "decoded succeeded! +upgrade";
    is $dec->{one}[0], 1.0, "kind of dumb test";
}

done_testing;
