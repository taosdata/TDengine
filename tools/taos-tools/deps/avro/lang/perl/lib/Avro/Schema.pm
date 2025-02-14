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

package Avro::Schema;
use strict;
use warnings;

use Carp;
use JSON::XS();
use Try::Tiny;

our $VERSION = '++MODULE_VERSION++';

my $json = JSON::XS->new->allow_nonref;

sub parse {
    my $schema      = shift;
    my $json_string = shift;
    my $names       = shift || {};
    my $namespace   = shift || "";

    my $struct = try {
        $json->decode($json_string);
    }
    catch {
        throw Avro::Schema::Error::Parse(
            "Cannot parse json string: $_"
        );
    };
    return $schema->parse_struct($struct, $names, $namespace);
}

sub to_string {
    my $class = shift;
    my $struct = shift;
    return $json->encode($struct);
}

sub parse_struct {
    my $schema = shift;
    my $struct = shift;
    my $names = shift || {};
    my $namespace = shift || "";

    ## 1.3.2 A JSON object
    if (ref $struct eq 'HASH') {
        my $type = $struct->{type}
            or throw Avro::Schema::Error::Parse("type is missing");
        if ( Avro::Schema::Primitive->is_type_valid($type) ) {
            return Avro::Schema::Primitive->new(type => $type);
        }
        ## XXX technically we shouldn't allow error type other than in
        ## a Protocol definition
        if ($type eq 'record' or $type eq 'error') {
            return Avro::Schema::Record->new(
                struct => $struct,
                names => $names,
                namespace => $namespace,
            );
        }
        elsif ($type eq 'enum') {
            return Avro::Schema::Enum->new(
                struct => $struct,
                names => $names,
                namespace => $namespace,
            );
        }
        elsif ($type eq 'array') {
            return Avro::Schema::Array->new(
                struct => $struct,
                names => $names,
                namespace => $namespace,
            );
        }
        elsif ($type eq 'map') {
            return Avro::Schema::Map->new(
                struct => $struct,
                names => $names,
                namespace => $namespace,
            );
        }
        elsif ($type eq 'fixed') {
            return Avro::Schema::Fixed->new(
                struct => $struct,
                names => $names,
                namespace => $namespace,
            );
        }
        else {
            throw Avro::Schema::Error::Parse("unknown type: $type");
        }
    }
    ## 1.3.2 A JSON array, representing a union of embedded types.
    elsif (ref $struct eq 'ARRAY') {
        return Avro::Schema::Union->new(
            struct => $struct,
            names => $names,
            namespace => $namespace,
        );
    }
    ## 1.3.2 A JSON string, naming a defined type.
    else {
        my $type = $struct;
        ## It's one of our custom defined type
        
        ## Short name provided, prepend the namespace
        if ( $type !~ /\./ ) {
            my $fulltype = $namespace . '.' . $type;
            if (exists $names->{$fulltype}) {
                return $names->{$fulltype};
            }
        }
        
        ## Fully-qualified name
        if (exists $names->{$type}) {
            return $names->{$type};
        }
        
        ## It's a primitive type
        return Avro::Schema::Primitive->new(type => $type);
    }
}

sub match {
    my $class = shift;
    my %param = @_;

    my $reader = $param{reader}
        or croak "missing reader schema";
    my $writer = $param{writer}
        or croak "missing writer schema";

    my $wtype = ref $writer ? $writer->type : $writer;
    my $rtype = ref $reader ? $reader->type : $reader;
    ## 1.3.2 either schema is a union
    return $wtype if $wtype eq 'union' or $rtype eq 'union';

    ## 1.3.2 both schemas have same primitive type
    return $wtype if $wtype eq $rtype
             && Avro::Schema::Primitive->is_type_valid($wtype);

    ## 1.3.2
    ## int is promotable to long, float, or double
    if ($wtype eq 'int' && (
        $rtype eq 'float' or $rtype eq 'long' or $rtype eq 'double'
    )) {
        return $rtype;
    }
    ## long is promotable to float or double
    if ($wtype eq 'long' && (
        $rtype eq 'float' or $rtype eq 'double'
    )) {
        return $rtype;
    }
    ## float is promotable to double
    if ($wtype eq 'float' && $rtype eq 'double') {
        return $rtype;
    }
    return 0 unless $rtype eq $wtype;

    ## 1.3.2 {subtype and/or names} match
    if ($rtype eq 'array') {
        return $wtype if $class->match(
            reader => $reader->items,
            writer => $writer->items,
        );
    }
    elsif ($rtype eq 'record') {
        return $wtype if $reader->fullname eq $writer->fullname;
    }
    elsif ($rtype eq 'map') {
        return $wtype if $class->match(
            reader => $reader->values,
            writer => $writer->values,
        );
    }
    elsif ($rtype eq 'fixed') {
        return $wtype if $reader->size     eq $writer->size
                      && $reader->fullname eq $writer->fullname;
    }
    elsif ($rtype eq 'enum') {
        return $wtype if $reader->fullname eq $writer->fullname;
    }
    return 0;
}


package Avro::Schema::Base;
our @ISA = qw/Avro::Schema/;
use Carp;

sub new {
    my $class = shift;
    my %param = @_;

    my $type = $param{type};
    if (!$type) {
        my ($t) = $class =~ /::([^:]+)$/;
        $type = lc ($t);
    }
    my $schema = bless {
        type => $type,
    }, $class;
    return $schema;
}

sub type {
    my $schema = shift;
    return $schema->{type};
}

sub to_string {
    my $schema = shift;
    my $known_names = shift || {};
    return Avro::Schema->to_string($schema->to_struct($known_names));
}

package Avro::Schema::Primitive;
our @ISA = qw/Avro::Schema::Base/;
use Carp;
use Config;
use Regexp::Common qw/number/;

my %PrimitiveType = map { $_ => 1 } qw/
    null
    boolean
    int
    long
    float
    double
    bytes
    string
/;

my %Singleton = ( );

## FIXME: useless lazy generation
sub new {
    my $class = shift;
    my %param = @_;

    my $type = $param{type}
        or croak "Schema must have a type";

    throw Avro::Schema::Error::Parse("Not a primitive type $type")
        unless $class->is_type_valid($type);

    if (! exists $Singleton{ $type } ) {
        my $schema = $class->SUPER::new( type => $type );
        $Singleton{ $type } = $schema;
    }
    return $Singleton{ $type };
}

sub is_type_valid {
    return $PrimitiveType{ $_[1] || "" };
}

## Returns true or false wheter the given data is valid for
## this schema
sub is_data_valid {
    my $schema = shift;
    my $data = shift;
    my $type = $schema->{type};
    if ($type eq 'int') {
        no warnings;
        my $packed_int = pack "l", $data;
        my $unpacked_int = unpack "l", $packed_int;
        return $unpacked_int eq $data ? 1 : 0;
    }
    if ($type eq 'long') {
        if ($Config{use64bitint}) {
            my $packed_int = pack "q", $data;
            my $unpacked_int = unpack "q", $packed_int;
            return $unpacked_int eq $data ? 1 : 0;

        }
        else {
            require Math::BigInt;
            my $int = eval { Math::BigInt->new($data) };
            if ($@) {
                warn "probably a unblessed ref: $@";
                return 0;
            }
            return 0 if $int->is_nan;
            my $max = Math::BigInt->new( "0x7FFF_FFFF_FFFF_FFFF" );
            return $int->bcmp($max) <= 0 ? 1 : 0;
        }
    }
    if ($type eq 'float' or $type eq 'double') {
        $data =~ /^$RE{num}{real}$/ ? return 1 : 0;
    }
    if ($type eq "bytes" or $type eq "string") {
        return 1 unless !defined $data or ref $data;
    }
    if ($type eq 'null') {
        return defined $data ? 0 : 1;
    }
    if ($type eq 'boolean') {
        return 0 if ref $data; # sometimes risky
        return 1 if $data =~ m{yes|no|y|n|t|f|true|false}i;
        return 0;
    }
    return 0;
}

sub to_struct {
    my $schema = shift;
    return $schema->type;
}

package Avro::Schema::Named;
our @ISA = qw/Avro::Schema::Base/;
use Scalar::Util;

my %NamedType = map { $_ => 1 } qw/
    record
    enum
    fixed
/;

sub new {
    my $class = shift;
    my %param = @_;

    my $schema = $class->SUPER::new(%param);

    my $names     = $param{names}  || {};
    my $struct    = $param{struct} || {};
    my $name      = $struct->{name};
    unless (defined $name && length $name) {
        throw Avro::Schema::Error::Parse( "Missing name for $class" );
    }
    my $namespace = $struct->{namespace};
    unless (defined $namespace && length $namespace) {
        $namespace = $param{namespace};
    }

    $schema->set_names($namespace, $name);
    $schema->add_name($names);

    return $schema;
}

sub is_type_valid {
    return $NamedType{ $_[1] || "" };
}

sub set_names {
    my $schema = shift;
    my ($namespace, $name) = @_;

    my @parts = split /\./, ($name || ""), -1;
    if (@parts > 1) {
        $name = pop @parts;
        $namespace = join ".", @parts;
        if (grep { ! length $_ } @parts) {
            throw Avro::Schema::Error::Name(
                "name '$name' is not a valid name"
            );
        }
    }

    ## 1.3.2 The name portion of a fullname, and record field names must:
    ## * start with [A-Za-z_]
    ## * subsequently contain only [A-Za-z0-9_]
    my $type = $schema->{type};
    unless (length $name && $name =~ m/^[A-Za-z_][A-Za-z0-9_]*$/) {
        throw Avro::Schema::Error::Name(
            "name '$name' is not valid for $type"
        );
    }
    if (defined $namespace && length $namespace) {
        for (split /\./, $namespace, -1) {
            unless ($_ && /^[A-Za-z_][A-Za-z0-9_]*$/) {
                throw Avro::Schema::Error::Name(
                    "namespace '$namespace' is not valid for $type"
                );
            }
        }
    }
    $schema->{name} = $name;
    $schema->{namespace} = $namespace;
}

sub add_name {
    my $schema = shift;
    my ($names) = @_;

    my $name = $schema->fullname;
    if ( exists $names->{ $name } ) {
        throw Avro::Schema::Error::Parse( "Name $name is already defined" );
    }
    $names->{$name} = $schema;
    Scalar::Util::weaken( $names->{$name} );
    return;
}

sub fullname {
    my $schema = shift;
    return join ".",
        grep { defined $_ && length $_ }
        map { $schema->{$_ } }
        qw/namespace name/;
}

sub namespace {
    my $schema = shift;
    return $schema->{namespace};
}

package Avro::Schema::Record;
our @ISA = qw/Avro::Schema::Named/;
use Scalar::Util;

my %ValidOrder = map { $_ => 1 } qw/ascending descending ignore/;

sub new {
    my $class = shift;
    my %param = @_;

    my $names  = $param{names} ||= {};
    my $schema = $class->SUPER::new(%param);

    my $fields = $param{struct}{fields}
        or throw Avro::Schema::Error::Parse("Record must have Fields");

    throw Avro::Schema::Error::Parse("Record.Fields must me an array")
        unless ref $fields eq 'ARRAY';

    my $namespace = $schema->namespace;

    my @fields;
    for my $field (@$fields) {
        my $f = Avro::Schema::Field->new($field, $names, $namespace);
        push @fields, $f;
    }
    $schema->{fields} = \@fields;
    return $schema;
}

sub to_struct {
    my $schema = shift;
    my $known_names = shift || {};
    ## consider that this record type is now known (will serialize differently)
    my $fullname = $schema->fullname;
    if ($known_names->{ $fullname }++) {
        return $fullname;
    }
    return {
        type => $schema->{type},
        name => $fullname,
        fields => [
            map { $_->to_struct($known_names) } @{ $schema->{fields} }
        ],
    };
}

sub fields {
    my $schema = shift;
    return $schema->{fields};
}

sub fields_as_hash {
    my $schema = shift;
    unless (exists $schema->{_fields_as_hash}) {
        $schema->{_fields_as_hash} = {
            map { $_->{name} => $_ } @{ $schema->{fields} }
        };
    }
    return $schema->{_fields_as_hash};
}

sub is_data_valid {
    my $schema = shift;
    my $data = shift;
    for my $field (@{ $schema->{fields} }) {
        my $key = $field->{name};
        return 0 unless $field->is_data_valid($data->{$key});
    }
    return 1;
}

package Avro::Schema::Field;

sub to_struct {
    my $field = shift;
    my $known_names = shift || {};
    my $type = $field->{type}->to_struct($known_names);
    return { name => $field->{name}, type => $type };
}

sub new {
    my $class = shift;
    my ($struct, $names, $namespace) = @_;

    my $name = $struct->{name};
    throw Avro::Schema::Error::Parse("Record.Field.name is required")
        unless defined $name && length $name;

    my $type = $struct->{type};
    throw Avro::Schema::Error::Parse("Record.Field.name is required")
        unless defined $type && length $type;

    $type = Avro::Schema->parse_struct($type, $names, $namespace);
    my $field = { name => $name, type => $type };
    #TODO: find where to weaken precisely
    #Scalar::Util::weaken($struct->{type});

    if (exists $struct->{default}) {
        my $is_valid = $type->is_data_valid($struct->{default});
        my $t = $type->type;
        throw Avro::Schema::Error::Parse(
            "default value doesn't validate $t: '$struct->{default}'"
        ) unless $is_valid;

        ## small Perlish special case
        if ($type eq 'boolean') {
            $field->{default} = $struct->{default} ? 1 : 0;
        }
        else {
            $field->{default} = $struct->{default};
        }
    }
    if (my $order = $struct->{order}) {
        throw Avro::Schema::Error::Parse(
            "Order '$order' is not valid'"
        ) unless $ValidOrder{$order};
        $field->{order} = $order;
    }
    return bless $field, $class;
}

sub is_data_valid {
    my $field = shift;
    my $data = shift;
    return 1 if $field->{type}->is_data_valid($data);
    return 0;
}

package Avro::Schema::Enum;
our @ISA = qw/Avro::Schema::Named/;

sub new {
    my $class = shift;
    my %param = @_;
    my $schema = $class->SUPER::new(%param);
    my $struct = $param{struct}
        or throw Avro::Schema::Error::Parse("Enum instantiation");
    my $symbols = $struct->{symbols} || [];

    unless (@$symbols) {
        throw Avro::Schema::Error::Parse("Enum needs at least one symbol");
    }
    my %symbols;
    my $pos = 0;
    for (@$symbols) {
        if (ref $_) {
            throw Avro::Schema::Error::Parse(
                "Enum.symbol should be a string"
            );
        }
        throw Avro::Schema::Error::Parse("Duplicate symbol in Enum")
            if exists $symbols{$_};

        $symbols{$_} = $pos++;
    }
    $schema->{hash_symbols} = \%symbols;
    return $schema;
}

sub is_data_valid {
    my $schema = shift;
    my $data = shift;
    return 1 if defined $data && exists $schema->{hash_symbols}{$data};
    return 0;
}

sub symbols {
    my $schema = shift;
    unless (exists $schema->{symbols}) {
        my $sym = $schema->{hash_symbols};
        $schema->{symbols} = [ sort { $sym->{$a} <=> $sym->{$b} } keys %$sym ];
    }
    return $schema->{symbols};
}

sub symbols_as_hash {
    my $schema = shift;
    return $schema->{hash_symbols} || {};
}

sub to_struct {
    my $schema = shift;
    my $known_names = shift || {};

    my $fullname = $schema->fullname;
    if ($known_names->{ $fullname }++) {
        return $fullname;
    }
    return {
        type => 'enum',
        name => $schema->fullname,
        symbols => [ @{ $schema->symbols } ],
    };
}

package Avro::Schema::Array;
our @ISA = qw/Avro::Schema::Base/;

sub new {
    my $class = shift;
    my %param = @_;
    my $schema = $class->SUPER::new(%param);

    my $struct = $param{struct}
        or throw Avro::Schema::Error::Parse("Enum instantiation");

    my $items = $struct->{items}
        or throw Avro::Schema::Error::Parse("Array must declare 'items'");

    $items = Avro::Schema->parse_struct($items, $param{names}, $param{namespace});
    $schema->{items} = $items;
    return $schema;
}

sub is_data_valid {
    my $schema = shift;
    my $default = shift;
    return 1 if $default && ref $default eq 'ARRAY';
    return 0;
}

sub items {
    my $schema = shift;
    return $schema->{items};
}

sub to_struct {
    my $schema = shift;
    my $known_names = shift || {};

    return {
        type => 'array',
        items => $schema->{items}->to_struct($known_names),
    };
}

package Avro::Schema::Map;
our @ISA = qw/Avro::Schema::Base/;

sub new {
    my $class = shift;
    my %param = @_;
    my $schema = $class->SUPER::new(%param);

    my $struct = $param{struct}
        or throw Avro::Schema::Error::Parse("Map instantiation");

    my $values = $struct->{values};
    unless (defined $values && length $values) {
        throw Avro::Schema::Error::Parse("Map must declare 'values'");
    }
    $values = Avro::Schema->parse_struct($values, $param{names}, $param{namespace});
    $schema->{values} = $values;

    return $schema;
}

sub is_data_valid {
    my $schema = shift;
    my $default = shift;
    return 1 if $default && ref $default eq 'HASH';
    return 0;
}

sub values {
    my $schema = shift;
    return $schema->{values};
}

sub to_struct {
    my $schema = shift;
    my $known_names = shift || {};

    return {
        type => 'map',
        values => $schema->{values}->to_struct($known_names),
    };
}

package Avro::Schema::Union;
our @ISA = qw/Avro::Schema::Base/;

sub new {
    my $class = shift;
    my %param = @_;
    my $schema = $class->SUPER::new(%param);
    my $union = $param{struct}
        or throw Avro::Schema::Error::Parse("Union.new needs a struct");

    my $names = $param{names} ||= {};

    my @schemas;
    my %seen_types;
    for my $struct (@$union) {
        my $sch = Avro::Schema->parse_struct($struct, $names, $param{namespace});
        my $type = $sch->type;

        ## 1.3.2 Unions may not contain more than one schema with the same
        ## type, except for the named types record, fixed and enum. For
        ## example, unions containing two array types or two map types are not
        ## permitted, but two types with different names are permitted.
        if (Avro::Schema::Named->is_type_valid($type)) {
            $type = $sch->fullname; # resolve Named types to their name
        }
        ## XXX: I could define &type_name doing the correct resolution for all classes
        if ($seen_types{ $type }++) {
            throw Avro::Schema::Error::Parse(
                "$type is present more than once in the union"
            )
        }
        ## 1.3.2 Unions may not immediately contain other unions.
        if ($type eq 'union') {
            throw Avro::Schema::Error::Parse(
                "Cannot embed unions in union"
            );
        }
        push @schemas, $sch;
    }
    $schema->{schemas} = \@schemas;

    return $schema;
}

sub schemas {
    my $schema = shift;
    return $schema->{schemas};
}

sub is_data_valid {    
    my $schema = shift;
    my $data = shift;
    for my $type ( @{ $schema->{schemas} } ) {
        if ( $type->is_data_valid($data) ) {
            return 1;
        }
    }
    return 0;
}

sub to_struct {
    my $schema = shift;
    my $known_names = shift || {};
    return [ map { $_->to_struct($known_names) } @{$schema->{schemas}} ];
}

package Avro::Schema::Fixed;
our @ISA = qw/Avro::Schema::Named/;

sub new {
    my $class = shift;
    my %param = @_;
    my $schema = $class->SUPER::new(%param);

    my $struct = $param{struct}
        or throw Avro::Schema::Error::Parse("Fixed instantiation");

    my $size = $struct->{size};
    unless (defined $size && length $size) {
        throw Avro::Schema::Error::Parse("Fixed must declare 'size'");
    }
    if (ref $size) {
        throw Avro::Schema::Error::Parse(
            "Fixed.size should be a scalar"
        );
    }
    unless ($size =~ m{^\d+$} && $size > 0) {
        throw Avro::Schema::Error::Parse(
            "Fixed.size should be a positive integer"
        );
    }
    # Cast into numeric so that it will be encoded as a JSON number
    $schema->{size} = $size + 0;

    return $schema;
}

sub is_data_valid {
    my $schema = shift;
    my $default = shift;
    my $size = $schema->{size};
    return 1 if $default && bytes::length $default == $size;
    return 0;
}

sub size {
    my $schema = shift;
    return $schema->{size};
}

sub to_struct {
    my $schema = shift;
    my $known_names = shift || {};

    my $fullname = $schema->fullname;
    if ($known_names->{ $fullname }++) {
        return $fullname;
    }

    return {
        type => 'fixed',
        name => $fullname,
        size => $schema->{size},
    };
}

package Avro::Schema::Error::Parse;
use parent 'Error::Simple';

package Avro::Schema::Error::Name;
use parent 'Error::Simple';

package Avro::Schema::Error::Mismatch;
use parent 'Error::Simple';

1;
