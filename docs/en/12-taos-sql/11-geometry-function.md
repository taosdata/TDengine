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
ST_Intersects(GEOMETRY geomA, GEOMETRY geomB)
```

**Description**: Compares two geometries and returns true if they intersect.

**Return value type**: BOOL

**Applicable data types**: GEOMETRY, GEOMETRY

**Applicable table types**: standard tables and supertables

**Explanations**：
- Geometries intersect if they have any point in common.


### ST_Equals

```sql
ST_Equals(GEOMETRY geomA, GEOMETRY geomB)
```

**Description**: Returns TRUE if the given geometries are "spatially equal".

**Return value type**: BOOL

**Applicable data types**: GEOMETRY, GEOMETRY

**Applicable table types**: standard tables and supertables

**Explanations**：
- 'Spatially equal' means ST_Contains(A,B) = true and ST_Contains(B,A) = true, and the ordering of points can be different but represent the same geometry structure.


### ST_Touches

```sql
ST_Touches(GEOMETRY geomA, GEOMETRY geomB)
```

**Description**: Returns TRUE if A and B intersect, but their interiors do not intersect.

**Return value type**: BOOL

**Applicable data types**: GEOMETRY, GEOMETRY

**Applicable table types**: standard tables and supertables

**Explanations**：
- A and B have at least one point in common, and the common points lie in at least one boundary.
- For Point/Point inputs the relationship is always FALSE, since points do not have a boundary.


### ST_Covers

```sql
ST_Covers(GEOMETRY geomA, GEOMETRY geomB)
```

**Description**: Returns TRUE if every point in Geometry B lies inside (intersects the interior or boundary of) Geometry A.

**Return value type**: BOOL

**Applicable data types**: GEOMETRY, GEOMETRY

**Applicable table types**: standard tables and supertables

**Explanations**：
- A covers B means no point of B lies outside (in the exterior of) A.


### ST_Contains

```sql
ST_Contains(GEOMETRY geomA, GEOMETRY geomB)
```

**Description**: Returns TRUE if geometry A contains geometry B.

**Return value type**: BOOL

**Applicable data types**: GEOMETRY, GEOMETRY

**Applicable table types**: standard tables and supertables

**Explanations**：
- A contains B if and only if all points of B lie inside (i.e. in the interior or boundary of) A (or equivalently, no points of B lie in the exterior of A), and the interiors of A and B have at least one point in common.
