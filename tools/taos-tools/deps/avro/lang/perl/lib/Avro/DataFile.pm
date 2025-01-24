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

package Avro::DataFile;
use strict;
use warnings;

use constant AVRO_MAGIC => "Obj\x01";

use Avro::Schema;

our $VERSION = '++MODULE_VERSION++';

our $HEADER_SCHEMA = Avro::Schema->parse(<<EOH);
{"type": "record", "name": "org.apache.avro.file.Header",
  "fields" : [
    {"name": "magic", "type": {"type": "fixed", "name": "Magic", "size": 4}},
    {"name": "meta", "type": {"type": "map", "values": "bytes"}},
    {"name": "sync", "type": {"type": "fixed", "name": "Sync", "size": 16}}
  ]
}
EOH

our %ValidCodec = (
    null      => 1,
    deflate   => 1,
    bzip2     => 1,
    zstandard => 1,
);

sub is_codec_valid {
    my $datafile = shift;
    my $codec = shift || '';
    return $ValidCodec{$codec};
}

+1;
