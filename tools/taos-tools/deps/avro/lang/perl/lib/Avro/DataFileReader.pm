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

package Avro::DataFileReader;
use strict;
use warnings;

use Object::Tiny qw{
    fh
    reader_schema
    sync_marker
    block_max_size
};

use constant MARKER_SIZE => 16;

# TODO: refuse to read a block more than block_max_size, instead
# do partial reads

use Avro::DataFile;
use Avro::BinaryDecoder;
use Avro::Schema;
use Carp;
use Compress::Zstd;
use IO::Uncompress::Bunzip2 qw(bunzip2);
use IO::Uncompress::RawInflate ;
use Fcntl();

our $VERSION = '++MODULE_VERSION++';

sub new {
    my $class = shift;
    my $datafile = $class->SUPER::new(@_);

    my $schema = $datafile->{reader_schema};
    croak "schema is invalid"
        if $schema && ! eval { $schema->isa("Avro::Schema") };

    return $datafile;
}

sub codec {
    my $datafile = shift;
    return $datafile->metadata->{'avro.codec'};
}

sub writer_schema {
    my $datafile = shift;
    unless (exists $datafile->{_writer_schema}) {
        my $json_schema = $datafile->metadata->{'avro.schema'};
        $datafile->{_writer_schema} = Avro::Schema->parse($json_schema);
    }
    return $datafile->{_writer_schema};
}

sub metadata {
    my $datafile = shift;
    unless (exists $datafile->{_metadata}) {
        my $header = $datafile->header;
        $datafile->{_metadata} = $header->{meta} || {};
    }
    return $datafile->{_metadata};
}

sub header {
    my $datafile = shift;
    unless (exists $datafile->{_header}) {
        $datafile->{_header} = $datafile->read_file_header;
    }

    return $datafile->{_header};
}

sub read_file_header {
    my $datafile = shift;

    my $data = Avro::BinaryDecoder->decode(
        reader_schema => $Avro::DataFile::HEADER_SCHEMA,
        writer_schema => $Avro::DataFile::HEADER_SCHEMA,
        reader        => $datafile->{fh},
    );
    croak "Magic '$data->{magic}' doesn't match"
        unless $data->{magic} eq Avro::DataFile->AVRO_MAGIC;

    $datafile->{sync_marker} = $data->{sync}
        or croak "sync marker appears invalid";

    my $codec = $data->{meta}{'avro.codec'} || "";

    throw Avro::DataFile::Error::UnsupportedCodec($codec)
        unless Avro::DataFile->is_codec_valid($codec);

    return $data;
}

sub all {
    my $datafile = shift;

    my @objs;
    my @block_objs;
    do {
        if ($datafile->eof) {
            @block_objs = ();
        }
        else {
            $datafile->read_block_header if $datafile->eob;
            @block_objs = $datafile->read_to_block_end;
            push @objs, @block_objs;
        }

    } until !@block_objs;

    return @objs
}

sub next {
    my $datafile = shift;
    my $count    = shift;

    my @objs;

    return ()                    if $datafile->eof;
    $datafile->read_block_header if $datafile->eob;

    my $block_count = $datafile->{object_count};

    if ($block_count < $count) {
        push @objs, $datafile->read_to_block_end;
        croak "Didn't read as many objects than expected"
            unless scalar @objs == $block_count;

        push @objs, $datafile->next($count - $block_count);
    }
    else {
        push @objs, $datafile->read_within_block($count);
    }
    return @objs;
}

sub read_within_block {
    my $datafile = shift;
    my $count    = shift;

    my $reader        = $datafile->reader;
    my $writer_schema = $datafile->writer_schema;
    my $reader_schema = $datafile->reader_schema || $writer_schema;
    my @objs;
    while ($count-- > 0 && $datafile->{object_count} > 0) {
        push @objs, Avro::BinaryDecoder->decode(
            writer_schema => $writer_schema,
            reader_schema => $reader_schema,
            reader        => $reader,
        );
        $datafile->{object_count}--;
    }
    return @objs;
}

sub skip {
    my $datafile = shift;
    my $count    = shift;

    my $block_count = $datafile->{object_count};
    if ($block_count <= $count) {
        $datafile->skip_to_block_end
            or croak "Cannot skip to end of block!";
        $datafile->skip($count - $block_count);
    }
    else {
        my $writer_schema = $datafile->writer_schema;
        ## could probably be optimized
        while ($count--) {
            Avro::BinaryDecoder->skip($writer_schema, $datafile->reader);
            $datafile->{object_count}--;
        }
    }
}

sub read_block_header {
    my $datafile = shift;
    my $fh = $datafile->{fh};
    my $codec = $datafile->codec;

    $datafile->header unless $datafile->{_header};

    $datafile->{object_count} = Avro::BinaryDecoder->decode_long(
        undef, undef, $fh,
    );
    $datafile->{block_size} = Avro::BinaryDecoder->decode_long(
        undef, undef, $fh,
    );
    $datafile->{block_start} = tell $fh;

    return if $codec eq 'null';

    ## we need to read the entire block into memory, to inflate it
    my $nread = read $fh, my $block, $datafile->{block_size} + MARKER_SIZE
        or croak "Error reading from file: $!";

    ## remove the marker
    my $marker = substr $block, -(MARKER_SIZE), MARKER_SIZE, '';
    $datafile->{block_marker} = $marker;

    ## this is our new reader
    $datafile->{reader} = do {
        if ($codec eq 'deflate') {
            IO::Uncompress::RawInflate->new(\$block);
        }
        elsif ($codec eq 'bzip2') {
            my $uncompressed;
            bunzip2 \$block => \$uncompressed;
            do { open $fh, '<', \$uncompressed; $fh };
        }
        elsif ($codec eq 'zstandard') {
            do { open $fh, '<', \(decompress(\$block)); $fh };
        }
    };

    return;
}

sub verify_marker {
    my $datafile = shift;

    my $marker = $datafile->{block_marker};
    unless (defined $marker) {
        ## we are in the fh case
        read $datafile->{fh}, $marker, MARKER_SIZE;
    }

    unless (($marker || "") eq $datafile->sync_marker) {
        croak "Oops synchronization issue (marker mismatch)";
    }
    return;
}

sub skip_to_block_end {
    my $datafile = shift;

    if (my $reader = $datafile->{reader}) {
        seek $reader, 0, Fcntl->SEEK_END;
        return;
    }

    my $remaining_size = $datafile->{block_size}
                       + $datafile->{block_start}
                       - tell $datafile->{fh};

    seek $datafile->{fh}, $remaining_size, 0;
    $datafile->verify_marker; ## will do a read
    return 1;
}

sub read_to_block_end {
    my $datafile = shift;

    my $reader = $datafile->reader;
    my @objs = $datafile->read_within_block( $datafile->{object_count} );
    $datafile->verify_marker;
    return @objs;
}

sub reader {
    my $datafile = shift;
    return $datafile->{reader} || $datafile->{fh};
}

## end of block
sub eob {
    my $datafile = shift;

    return 1 if $datafile->eof;

    if ($datafile->{reader}) {
        return 1 if $datafile->{reader}->eof;
    }
    else {
        my $pos = tell $datafile->{fh};
        return 1 unless $datafile->{block_start};
        return 1 if $pos >= $datafile->{block_start} + $datafile->{block_size};
    }
    return 0;
}

sub eof {
    my $datafile = shift;
    if ($datafile->{reader}) {
        return 0 unless $datafile->{reader}->eof;
    }
    return 1 if $datafile->{fh}->eof;
    return 0;
}

package Avro::DataFile::Error::UnsupportedCodec;
use parent 'Error::Simple';

1;
