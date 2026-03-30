# TS-5667 - Interp Fill扩展

### 1. Interp fill支持near

插入prev和next中和断面时间戳最接近的行的值
```sql {wrap}
SELECT _irowts, interp(c0), _isfilled from meters range(1722481200000, 1722488400000) every(10m) fill(near);
```

### 2. Interp 支持irowts返回原始数据的ts

伪列_irowts_origin为在fill时不再填充当前断面的时间戳, 而是填充行的时间戳.
_irowts_origin 仅当fill 模式为prev/next/near时生效, 其他场景报错.
```sql {wrap}
SELECT _irowts, _irowts_origin, interp(c0), _isfilled from meters range(1722481200000, 1722488400000) every(10m) fill(near);
```

### 3. Interp fill支持在一个断面的指定范围内查找插值

- range内第一个参数是时间戳即指定的时间断面, 第二参数是一个interval(支持单位与EVERY类似, 不支持y,n), 即基于指定的时间断面的前后查找范围.
- 此时不能指定every, 没有意义.
- fill内只能是prev/next/near, 且fill内需要给需要fill的所有列指定默认值, 在查找范围内没有找到数据时会使用指定的值.
```sql {wrap}
SELECT _irowts, interp(c0), _isfilled from meters range(1722481200000, 1h) fill(near, 0);
```
