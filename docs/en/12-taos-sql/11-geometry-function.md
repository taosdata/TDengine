---
sidebar_label: Geometry Functions
title: Geometry Functions
toc_max_heading_level: 4
---

## Geometry Input Functions

Geometry input functions create geometry data from WTK.

### ST_GeomFromText

```sql
ST_GeomFromText(VARCHAR WKT expr)
```

**Description**: Return a specified GEOMETRY value from Well-Known Text representation (WKT).

**Return value type**: GEOMETRY

**Applicable data types**: VARCHAR

**Applicable table types**: standard tables and supertables

**Explanations**：
- The input can be one of WTK string, like POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, GEOMETRYCOLLECTION.
- The output is a GEOMETRY data type, internal defined as binary string.

## Geometry Output Functions

Geometry output functions convert geometry data into WTK.

### ST_AsText

```sql
ST_AsText(GEOMETRY geom)
```

**Description**: Return a specified Well-Known Text representation (WKT) value from GEOMETRY data.

**Return value type**: VARCHAR

**Applicable data types**: GEOMETRY

**Applicable table types**: standard tables and supertables

**Explanations**：
- The output can be one of WTK string, like POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, GEOMETRYCOLLECTION.

## Geometry Relationships Functions

Geometry relationships functions determine spatial relationships between geometries.

### ST_Intersects

```sql
ST_AsText(GEOMETRY geomA, GEOMETRY geomB)
```

**Description**: Compares two geometries and returns true if they intersect.

**Return value type**: BOOL

**Applicable data types**: GEOMETRY, GEOMETRY

**Applicable table types**: standard tables and supertables

**Explanations**：
- Geometries intersect if they have any point in common.
