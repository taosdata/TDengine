---
sidebar_label: Tag Index
title: Tag Index
description: Use Tag Index to Improve Query Performance
---

## Introduction

Prior to TDengine 3.0.3.0 (excluded), only one index is created by default on the first tag of each super table, but it's not allowed to dynamically create index on any other tags. From version 3.0.30, you can dynamically create index on any tag of any type. The index created automatically by TDengine is still valid. Query performance can benefit from indexes if you use properly.

## Syntax

1. The syntax of creating an index 

```sql
CREATE INDEX index_name ON tbl_name (tagColName)
```

In the above statement, `index_name` if the name of the index, `tbl_name` is the name of the super table,`tagColName` is the name of the tag on which the index is being created. `tagColName` can be any type supported by TDengine.

2. The syntax of drop an index

```sql
DROP INDEX index_name
```

In the above statement,  `index_name` is the name of an existing index. If the index doesn't exist, the command would fail but doesn't generate any impact to the system. 

3. The syntax of show indexes in the system
   
```sql
SELECT * FROM information_schema.INS_INDEXES 
```

You can also add filter conditions to limit the results.

## Detailed Specification

1. Indexes can improve query performance significantly if they are used properly. The operators supported by tag index include  `=`, `>`, `>=`, `<`, `<=`. If you use these operators with tags, indexes can improve query performance significantly. However, for operators not in this scope, indexes don't help. More and more operators will be added in future.

2. Only one index can be created on each tag, error would be reported if you try to create more than one indexes on same tag.

3. Each time you can create an index on a single tag, you are not allowed to create indexes on multiple tags together. 

4. The name of each index must be unique across the whole system, regardless of the type of the index, e.g. tag index or sma index.

5. There is no limit on the number of indexes, but each index may add some burden on the metadata subsystem. So too many indexes may decrease the efficiency of reading or writing metadata and then decrease the system performance. So it's better not to add unnecessary indexes. 

6. You can' create index on a normal table or a child table. 

7. If the unique values of a tag column are too few, it's better not to create index on such tag columns, the benefit would be very small. 

8. The newly created super table will randomly generate an index name for the first column of tags, which is composed of the name tag0 column with 23 random bytes, and can be rebuilt or dropped.  
