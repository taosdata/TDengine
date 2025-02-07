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

use Config;
use inc::Module::Install;

my $version;
for ('VERSION.txt', '../../share/VERSION.txt') {
    if (-f) {
        $version = `cat $_`;
        last;
    }
}
chomp $version;

license 'apache';
version $version;
readme_from 'lib/Avro.pm';
all_from 'lib/Avro.pm';
build_requires 'Test::More', 0.88;
test_requires 'Math::BigInt';
test_requires 'Perl::Critic';
test_requires 'Test::Exception';
requires 'Compress::Zlib';
requires 'Compress::Zstd';
requires 'Encode';
requires 'Error::Simple';
requires 'IO::String';
requires 'JSON::XS';
requires 'Object::Tiny';
requires 'Regexp::Common';
requires 'Try::Tiny';
requires 'parent';
unless ($Config{use64bitint}) {
    requires 'Math::BigInt';
}
auto_set_repository();

my %packages = (
    'Avro'                    => 'lib/Avro.pm',
    'Avro::BinaryDecoder'     => 'lib/Avro/BinaryDecoder.pm',
    'Avro::BinaryEncoder'     => 'lib/Avro/BinaryEncoder.pm',
    'Avro::DataFile'          => 'lib/Avro/DataFile.pm',
    'Avro::DataFileReader'    => 'lib/Avro/DataFileReader.pm',
    'Avro::DataFileWriter'    => 'lib/Avro/DataFileWriter.pm',
    'Avro::Protocol'          => 'lib/Avro/Protocol.pm',
    'Avro::Protocol::Message' => 'lib/Avro/Protocol/Message.pm',
    'Avro::Schema'            => 'lib/Avro/Schema.pm',
);
my %provides = map { $_ => { file => $packages{$_}, version => $version } } keys %packages;
provides(%provides);

WriteMakefile(PM_FILTER => "sed -e 's/\+\+MODULE_VERSION\+\+/$version/'");
