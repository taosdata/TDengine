# mdsn

M-DSN: A Multi-address DSN(Data Source Name) parser.

M-DSN support two kind of DSN format:

1. `<driver>[+<protocol>]://<username>:<password>@<addresses>/<database>?<params>`
2. `<driver>[+<protocol>]://<username>:<password>@<fragment>?<params>`
3. `<driver>://<username>:<password>@<protocol>(<addresses>)/<database>?<params>`

All the items will be parsed into struct [Dsn](crate::Dsn).

### Parser

```rust
use mdsn::Dsn;

// The two styles are equivalent.
let dsn = Dsn::parse("taos://root:taosdata@host1:6030,host2:6030/db")?;
let dsn: Dsn = "taos://root:taosdata@host1:6030,host2:6030/db".parse()?;

assert_eq!(dsn.driver, "taos");
assert_eq!(dsn.username.unwrap(), "root");
assert_eq!(dsn.password.unwrap(), "taosdata");
assert_eq!(dsn.database.unwrap(), "db");
assert_eq!(dsn.addresses.len(), 2);
assert_eq!(dsn.addresses, vec![
    mdsn::Address::new("host1", 6030),
    mdsn::Address::new("host2", 6030),
]);
```

### DSN Examples

A DSN for [TDengine](https://taosdata.com) driver [taos](https://docs.rs/taos).

```dsn
taos://root:taosdata@localhost:6030/db?timezone=Asia/Shanghai&asyncLog=1
```

With multi-address:

```dsn
taos://root:taosdata@host1:6030,host2:6030/db?timezone=Asia/Shanghai
```

A DSN for unix socket:

```dsn
unix:///path/to/unix.sock?param1=value
```

A DSN for postgresql with url-encoded socket directory path.

```dsn
postgresql://%2Fvar%2Flib%2Fpostgresql/db
```

A DSN for sqlite db file, note that you must use prefix `./` for a relative path file.

```dsn
sqlite://./file.db
```


License: MIT OR Apache-2.0
