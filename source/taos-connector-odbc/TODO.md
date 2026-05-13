### SQLFetch
```
say, 10 rows for result set, 3 rows for rowset, and taos_fetch_block returns 5 rows for each block,
you shall get rowsets as 3, 3, 3, 3, 3, rather than 3, 3, 3, 1, 3, 2
```

### user/pass security!

