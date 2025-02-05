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

package Avro::BinaryDecoder;
use strict;
use warnings;

use Config;
use Encode();
use Error::Simple;
use Avro::Schema;

our $VERSION = '++MODULE_VERSION++';

our $complement = ~0x7F;
unless ($Config{use64bitint}) {
    require Math::BigInt;
    $complement = Math::BigInt->new("0b" . ("1" x 57) . ("0" x 7));
}

=head2 decode(%param)

Resolve the given writer and reader_schema to decode the data provided by the
reader.

=over 4

=item * writer_schema

The schema that was used to encode the data provided by the C<reader>

=item * reader_schema

The schema we want to use to decode the data.

=item * reader

An object implementing a straightforward interface. C<read($buf, $nbytes)> and
C<seek($nbytes, $whence)> are expected. Typically a IO::String object or a
IO::File object. It is expected that this calls will block the decoder, if not
enough data is available for read.

=back

=cut
sub decode {
    my $class = shift;
    my %param = @_;

    my ($writer_schema, $reader_schema, $reader)
        = @param{qw/writer_schema reader_schema reader/};

    my $type = Avro::Schema->match(
        writer => $writer_schema,
        reader => $reader_schema,
    ) or throw Avro::Schema::Error::Mismatch;

    my $meth = "decode_$type";
    return $class->$meth($writer_schema, $reader_schema, $reader);
}

sub skip {
    my $class = shift;
    my ($schema, $reader) = @_;
    my $type = ref $schema ? $schema->type : $schema;
    my $meth = "skip_$type";
    return $class->$meth($schema, $reader);
}

sub decode_null { undef }

sub skip_boolean { &decode_boolean }
sub decode_boolean {
    my $class = shift;
    my $reader = pop;
    $reader->read(my $bool, 1);
    return unpack 'C', $bool;
}

sub skip_int { &decode_int }
sub decode_int {
    my $class = shift;
    my $reader = pop;
    return zigzag(unsigned_varint($reader));
}

sub skip_long { &decode_long };
sub decode_long {
    my $class = shift;
    return decode_int($class, @_);
}

sub skip_float { &decode_float }
sub decode_float {
    my $class = shift;
    my $reader = pop;
    $reader->read(my $buf, 4);
    return unpack "f<", $buf;
}

sub skip_double { &decode_double }
sub decode_double {
    my $class = shift;
    my $reader = pop;
    $reader->read(my $buf, 8);
    return unpack "d<", $buf,
}

sub skip_bytes {
    my $class = shift;
    my $reader = pop;
    my $size = decode_long($class, undef, undef, $reader);
    $reader->seek($size, 0);
    return;
}

sub decode_bytes {
    my $class = shift;
    my $reader = pop;
    my $size = decode_long($class, undef, undef, $reader);
    $reader->read(my $buf, $size);
    return $buf;
}

sub skip_string { &skip_bytes }
sub decode_string {
    my $class = shift;
    my $reader = pop;
    my $bytes = decode_bytes($class, undef, undef, $reader);
    return Encode::decode_utf8($bytes);
}

sub skip_record {
    my $class = shift;
    my ($schema, $reader) = @_;
    for my $field (@{ $schema->fields }){
        skip($class, $field->{type}, $reader);
    }
}

## 1.3.2 A record is encoded by encoding the values of its fields in the order
## that they are declared. In other words, a record is encoded as just the
## concatenation of the encodings of its fields. Field values are encoded per
## their schema.
sub decode_record {
    my $class = shift;
    my ($writer_schema, $reader_schema, $reader) = @_;
    my $record;

    my %extra_fields = %{ $reader_schema->fields_as_hash };
    for my $field (@{ $writer_schema->fields }) {
        my $name = $field->{name};
        my $w_field_schema = $field->{type};
        my $r_field_schema = delete $extra_fields{$name};

        ## 1.3.2 if the writer's record contains a field with a name not
        ## present in the reader's record, the writer's value for that field
        ## is ignored.
        if (! $r_field_schema) {
            $class->skip($w_field_schema, $reader);
            next;
        }
        my $data = $class->decode(
            writer_schema => $w_field_schema,
            reader_schema => $r_field_schema->{type},
            reader        => $reader,
        );
        $record->{ $name } = $data;
    }

    for my $name (keys %extra_fields) {
        ## 1.3.2. if the reader's record schema has a field with no default
        ## value, and writer's schema does not have a field with the same
        ## name, an error is signalled.
        unless (exists $extra_fields{$name}->{default}) {
            throw Avro::Schema::Error::Mismatch(
                "cannot resolve without default"
            );
        }
        ## 1.3.2 ... else the default value is used
        $record->{ $name } = $extra_fields{$name}->{default};
    }
    return $record;
}

sub skip_enum { &skip_int }

## 1.3.2 An enum is encoded by a int, representing the zero-based position of
## the symbol in the schema.
sub decode_enum {
    my $class = shift;
    my ($writer_schema, $reader_schema, $reader) = @_;
    my $index = decode_int($class, @_);

    my $w_data = $writer_schema->symbols->[$index];
    ## 1.3.2 if the writer's symbol is not present in the reader's enum,
    ## then an error is signalled.
    throw Avro::Schema::Error::Mismatch("enum unknown")
        unless $reader_schema->is_data_valid($w_data);
    return $w_data;
}

sub skip_block {
    my $class = shift;
    my ($reader, $block_content) = @_;
    my $block_count = decode_long($class, undef, undef, $reader);
    while ($block_count) {
        if ($block_count < 0) {
            $reader->seek($block_count, 0);
            next;
        }
        else {
            for (1..$block_count) {
                $block_content->();
            }
        }
        $block_count = decode_long($class, undef, undef, $reader);
    }
}

sub skip_array {
    my $class = shift;
    my ($schema, $reader) = @_;
    skip_block($reader, sub { $class->skip($schema->items, $reader) });
}

## 1.3.2 Arrays are encoded as a series of blocks. Each block consists of a
## long count value, followed by that many array items. A block with count zero
## indicates the end of the array. Each item is encoded per the array's item
## schema.
## If a block's count is negative, its absolute value is used, and the count is
## followed immediately by a long block size
sub decode_array {
    my $class = shift;
    my ($writer_schema, $reader_schema, $reader) = @_;
    my $block_count = decode_long($class, @_);
    my @array;
    my $writer_items = $writer_schema->items;
    my $reader_items = $reader_schema->items;
    while ($block_count) {
        my $block_size;
        if ($block_count < 0) {
            $block_count = -$block_count;
            $block_size = decode_long($class, @_);
            ## XXX we can skip with $reader_schema?
        }
        for (1..$block_count) {
            push @array, $class->decode(
                writer_schema => $writer_items,
                reader_schema => $reader_items,
                reader        => $reader,
            );
        }
        $block_count = decode_long($class, @_);
    }
    return \@array;
}

sub skip_map {
    my $class = shift;
    my ($schema, $reader) = @_;
    skip_block($reader, sub {
        skip_string($class, $reader);
        $class->skip($schema->values, $reader);
    });
}

## 1.3.2 Maps are encoded as a series of blocks. Each block consists of a long
## count value, followed by that many key/value pairs. A block with count zero
## indicates the end of the map. Each item is encoded per the map's value
## schema.
##
## If a block's count is negative, its absolute value is used, and the count is
## followed immediately by a long block size indicating the number of bytes in
## the block. This block size permits fast skipping through data, e.g., when
## projecting a record to a subset of its fields.
sub decode_map {
    my $class = shift;
    my ($writer_schema, $reader_schema, $reader) = @_;
    my %hash;

    my $block_count = decode_long($class, @_);
    my $writer_values = $writer_schema->values;
    my $reader_values = $reader_schema->values;
    while ($block_count) {
        my $block_size;
        if ($block_count < 0) {
            $block_count = -$block_count;
            $block_size = decode_long($class, @_);
            ## XXX we can skip with $reader_schema?
        }
        for (1..$block_count) {
            my $key = decode_string($class, @_);
            unless (defined $key && length $key) {
                throw Avro::Schema::Error::Parse("key of map is invalid");
            }
            $hash{$key} = $class->decode(
                writer_schema => $writer_values,
                reader_schema => $reader_values,
                reader        => $reader,
            );
        }
        $block_count = decode_long($class, @_);
    }
    return \%hash;
}

sub skip_union {
    my $class = shift;
    my ($schema, $reader) = @_;
    my $idx = decode_long($class, undef, undef, $reader);
    my $union_schema = $schema->schemas->[$idx]
        or throw Avro::Schema::Error::Parse("union union member");
    $class->skip($union_schema, $reader);
}

## 1.3.2 A union is encoded by first writing an int value indicating the
## zero-based position within the union of the schema of its value. The value
## is then encoded per the indicated schema within the union.
sub decode_union {
    my $class = shift;
    my ($writer_schema, $reader_schema, $reader) = @_;
    my $idx = decode_long($class, @_);
    my $union_schema = $writer_schema->schemas->[$idx];
    ## XXX TODO: schema resolution
    # The first schema in the reader's union that matches the selected writer's
    # union schema is recursively resolved against it. if none match, an error
    # is signalled.
    return $class->decode(
        reader_schema => $union_schema,
        writer_schema => $union_schema,
        reader => $reader,
    );
}

sub skip_fixed {
    my $class = shift;
    my ($schema, $reader) = @_;
    $reader->seek($schema->size, 0);
}

## 1.3.2 Fixed instances are encoded using the number of bytes declared in the
## schema.
sub decode_fixed {
    my $class = shift;
    my ($writer_schema, $reader_schema, $reader) = @_;
    $reader->read(my $buf, $writer_schema->size);
    return $buf;
}

sub zigzag {
    my $int = shift;
    if (1 & $int) {
        ## odd values are encoded negative ints
        return -( 1 + ($int >> 1) );
    }
    ## even values are positive natural left shifted one bit
    else {
        return $int >> 1;
    }
}

sub unsigned_varint {
    my $reader = shift;
    my $int = 0;
    my $more;
    my $shift = 0;
    do {
        $reader->read(my $buf, 1);
        my $byte = ord $buf;
        my $value = $byte & 0x7F;
        $int |= $value << $shift;
        $shift += 7;
        $more = $byte & 0x80;
    } until (! $more);
    return $int;
}

1;
