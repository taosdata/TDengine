<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       https://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

# Refactoring Resolution
by Raymie Stata


## Problem statement

In the early days of Avro, Schema resolution was implemented in a
number of places, e.g., `GenericDatumReader` as well as
`ResolvingGrammarGenerator`.  However, Schema resolution is
complicated and thus error prone.  Multiple implementations were hard
to maintain, both for correctness and for updates to the
schema-resolution spec.

To address the problems of multiple implementations, we converged on
the implementation found in `ResolvingGrammarGenerator` (together with
`ResolvingDecoder`) as the single implementation, and refactored other
parts of Avro to depend on this implementation.

Converging on a single implementation solved the maintenance problem,
and has served well for a number of years.  However, the logic in
`ResolvingGrammarGenerator` does _two_ things: it contains the logic
for _schema resolution_ itself, and it contains the logic for
embedding that logic into a grammar that can be used by
`ResolvingDecoder`.

Recently, Avro contributors have wanted access to the logic of schema
resolution _apart from_ `ResolvingDecoder`.  For example,
[AVRO-2247](https://issues.apache.org/jira/browse/AVRO-2247) proposes
a new, faster approach to implementing `DatumReaders`.  The initial
implementation of AVRO-2247 was forced to reimplement Schema
resolution -- going back to the world of multiple implementations --
because there isn't a reusable implementation of our resolution logic.

Similarly, as I've been working on extending the performance
improvements of
[AVRO-2090](https://issues.apache.org/jira/browse/AVRO-2090) when
writing data, I've been thinking about the possibilities of dynamic
code generation.  Here too, I can't reuse `ResolvingGrammarGenerator`,
which would force me to reimplement the schema-resolution logic.


## Proposed solution

We introduce a new class to encapsulate the logic of schema resolution
independent from the logic of implementing schema resolution as a
`ResolvingDecoder` grammar.  In particular, we introduce a new class
`org.apache.avro.Resolver` with the following key function:

    public static Resolver.Action resolve(Schema writer, Schema reader);

The subclasses of `Resolver.Action` encapsulate various ways to
resolve schemas.  The `resolve` function walks the reader's and
writer's schema parse trees together, and generate a tree of
`Resolver.Action` nodes indicating how to resolve each subtree of the
writer's schema into the corresponding subtree of the reader's.

`Resolve.Action` has the following subclasses:

   * `DoNothing` -- nothing needs to be done to resolve the writer's
     data into the reader's schema.  That is, the reader should read
     the data written by the writer as if it were written using the
     reader's own schema.  This can be generated for any kind of
     schema -- for example, if the reader's and writer's schemas are
     the exact same union schema, a `DoNothing` will be generated --
     so consumers of `Resolver` need to be able to handle `DoNothing`
     for all schemas.

   * `Promote` -- the writer's value needs to be promoted to the
     reader's schema.  Generated only for numeric and byte/string
     types.

   * `ContainerAction` -- no resolution is needed directly on
     container schemas, but a `ContainerAction` contains the `Action`
     needed for the contained schema

   * `EnumAdjust` -- resolution involves dealing with reordering of
     symbols and symbols that have been removed from the enumeration.
     An `EnumAdjust` object contains the information needed to do so.

   * `RecordAdjust` -- resolution involves recursively resolving the
     schemas for each field, and dealing with reordering and removal
     of fields.  A `RecordAdjust` object contains the information
     needed to do so.

   * `SkipAction` -- only generated as a sub-action of a
     `RecordAdjust` action.  Used to indicate that a writer's field
     does not appear in the reader's schema and thus should be
     skipped.

   * `WriterUnion` -- generated when the writer's schema is a union
     and the reader's schema is not the identical union.  Has
     subactions for resolving each branch of the writer's union
     against the reader's schema.

   * `ReaderUnion` -- generated when the reader's schema is a union
     and the writer's was not.  Had information indicating which of
     the reader's union-branch was the best fit for the writer's
     schema, and a subaction for resolving the schema of that branch
     against the writer's schema.

   * `ErrorAction` -- generated when the (sub)schemas can't be
     resolved.

These new classes aresimilar to the family of `Symbol` objects we've
defined for `ResolvingGrammarGenerator`.  For example,
`Action.RecordAdjust` is similar to `Symbol.FieldOrderAction`, and
`Action.EnumAdjust` in `Symbol.EnumAdjustAction`.  This similarity is
not surprising, since those `Symbol` objects were design to
encapsulate the logic of schema resolution as well.

However, where `ResolvingGrammarGenerator` embeds those `Symbol`
objects into flattened productions highly optimized for the LL(1)
parser implemented by `ResolvingDecoder`.  The `Resolver`, in
contrast, captures the schema-resolution logic in a tree-like
structure that closely mirrors the syntax-tree of the schemas being
resolved.  This tree-like representation is easily consumed by
multiple implementations of resolution -- be it the grammar-based
implementation of `ResolvingDecoder`, the "action-sequence"-based
implementation of AVRO-2247, or the dynamic code-gen implementation
being considered as an extension to AVRO-2090.

We have reimplemented `ResolvingGrammarGenerator` to eliminate it's
implementaiton of schema-resolution logic and instead consume the
output of `Resolver.resolve`.  Thus, it might be helpful to study
`ResolvingGrammarGenerator` to better understand how to consume this
output in other circumstances.
