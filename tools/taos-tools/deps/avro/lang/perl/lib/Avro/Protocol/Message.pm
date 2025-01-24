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

package Avro::Protocol::Message;

use strict;
use warnings;

use Avro::Schema;
use Avro::Protocol;
use Error;

use Object::Tiny qw{
    doc
    request
    response
    errors
};

our $VERSION = '++MODULE_VERSION++';

sub new {
    my $class = shift;
    my $struct = shift;
    my $types = shift;

    my $resp_struct = $struct->{response}
        or throw Avro::Protocol::Error::Parse("response is missing");

    my $req_struct = $struct->{request}
        or throw Avro::Protocol::Error::Parse("request is missing");

    my $request = [
        map { Avro::Schema::Field->new($_, $types) } @$req_struct
    ];

    my $err_struct = $struct->{errors};

    my $response = Avro::Schema->parse_struct($resp_struct, $types);
    my $errors = $err_struct ? Avro::Schema->parse_struct($err_struct, $types) : undef;

    return $class->SUPER::new(
        doc      => $struct->{doc},
        request  => $request,
        response => $response,
        errors   => $errors,
    );

}

1;
