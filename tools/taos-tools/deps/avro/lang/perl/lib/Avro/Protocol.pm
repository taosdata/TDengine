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

package Avro::Protocol;
use strict;
use warnings;

use Carp;
use JSON::XS();
use Try::Tiny;
use Avro::Protocol::Message;
use Avro::Schema;
use Error;
use Object::Tiny qw{
    name
    namespace
    doc
    types
    messages
};

our $VERSION = '++MODULE_VERSION++';

my $json = JSON::XS->new->allow_nonref;

sub parse {
    my $class     = shift;
    my $enc_proto = shift
        or throw Avro::Protocol::Error::Parse("protocol cannot be empty");

    my $struct = try {
        $json->decode($enc_proto);
    }
    catch {
        throw Avro::Protocol::Error::Parse(
            "Cannot parse json string: $_"
        );
    };
    return $class->from_struct($struct);
}

sub from_struct {
    my $class = shift;
    my $struct = shift || {};
    my $name = $struct->{protocol};
    unless (defined $name or length $name) {
        throw Avro::Protocol::Error::Parse("protocol name is required");
    }

    my $types = $class->parse_types($struct->{types});

    my $messages = $struct->{messages} ?
        $class->parse_messages($struct->{messages}, $types) : undef;

    my $protocol = $class->SUPER::new(
        name      => $name,
        namespace => $struct->{namespace},
        doc       => $struct->{doc},
        types     => $types,
        messages  => $messages,
    );
    return $protocol;
}

sub parse_types {
    my $class = shift;
    my $types = shift || [];

    my %types;
    my $names = {};
    for (@$types) {
        try {
            my $schema = Avro::Schema->parse_struct($_, $names);
            $types{ $schema->fullname } = $schema;
        }
        catch {
            throw Avro::Protocol::Error::Parse("errors in parsing types: $_");
        };
    }
    return \%types;
}

sub parse_messages {
    my $class = shift;
    my $messages = shift || {};
    my $types = shift;
    my $m = {};
    for my $name (keys %$messages) {
        $m->{$name} = Avro::Protocol::Message->new($messages->{$name}, $types);
    }
    return $m;
}

sub fullname {
    my $protocol = shift;
    return join ".", grep { $_ } map { $protocol->$_ } qw{ namespace name };
}

package Avro::Protocol::Error::Parse;
use parent 'Error::Simple';

1;
