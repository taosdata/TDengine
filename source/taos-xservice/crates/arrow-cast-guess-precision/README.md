# arrow-cast-guess-precision

Cast integer to timestamp with precision guessing options.

Just replace [arrow::compute::cast] with [arrow_cast_guess_precision::cast] and everything done.

```rust
use arrow::{
    array::{Int64Array, TimestampNanosecondArray},
    datatypes::{DataType, TimeUnit}
};

let data = vec![1701325744956, 1701325744956];
let array = Int64Array::from(data);
let array = arrow_cast_guess_precision::cast(
    &array,
    &DataType::Timestamp(TimeUnit::Nanosecond, None),
)
.unwrap();
let nanos = array
    .as_any()
    .downcast_ref::<TimestampNanosecondArray>()
    .unwrap();
assert_eq!(nanos.value(0), 1701325744956 * 1000 * 1000);
```

The difference to official [arrow::compute::cast] is that:

- arrow v49 will cast integer directly to timestamp, but this crate(`arrow-cast-guess-precision = "0.3.0"`) will try to guess from the value.
- arrow v48 does not support casting from integers to timestamp (`arrow-cast-guess-precision = "0.2.0"`).

The guessing method is:

```rust
use arrow::datatypes::TimeUnit;

const GUESSING_BOUND_YEARS: i64 = 10000;
const LOWER_BOUND_MILLIS: i64 = 86400 * 365 * GUESSING_BOUND_YEARS;
const LOWER_BOUND_MICROS: i64 = 1000 * 86400 * 365 * GUESSING_BOUND_YEARS;
const LOWER_BOUND_NANOS: i64 = 1000 * 1000 * 86400 * 365 * GUESSING_BOUND_YEARS;

#[inline]
const fn guess_precision(timestamp: i64) -> TimeUnit {
    let timestamp = timestamp.abs();
    if timestamp > LOWER_BOUND_NANOS {
        return TimeUnit::Nanosecond;
    }
    if timestamp > LOWER_BOUND_MICROS {
        return TimeUnit::Microsecond;
    }
    if timestamp > LOWER_BOUND_MILLIS {
        return TimeUnit::Millisecond;
    }
    TimeUnit::Second
}
```

Users could set `ARROW_CAST_GUESSING_BOUND_YEARS` environment at build-time to control the guessing bound.
here is a sample list based on individual environment values:

|    value | lower bound             |       Upper Bound       |
| -------: | ----------------------- | :---------------------: |
|      100 | 1970-02-06t12:00:00     |   2069-12-07T00:00:00   |
|      200 | 1970-03-15t00:00:00     |   2169-11-13T00:00:00   |
|      500 | 1970-07-02t12:00:00     |   2469-09-01T00:00:00   |
| **1000** | **1971-01-01T00:00:00** | **2969-05-03T00:00:00** |
|     2000 | 1972-01-01t00:00:00     |   3968-09-03T00:00:00   |
|     5000 | 1974-12-31t00:00:00     |   6966-09-06T00:00:00   |
|    10000 | 1979-12-30t00:00:00     |  +11963-05-13T00:00:00  |

We use `ARROW_CAST_GUESSING_BOUND_YEARS=1000` by default, just because `1000` milliseconds is `1` second so that the lower bound starts with `1971-01-01T00:00:00` which is one year after ZERO unix timestamp, and the upper bound is enough (even 100-years is enough though).

Like [arrow::compute::cast], this crate also supports casting with specific options, checkout [CastOptions](arrow_cast_guess_precision::CastOptions).

[arrow::compute::cast]: https://docs.rs/arrow/latest/arrow/compute/fn.cast.html
[arrow_cast_guess_precision::cast]: https://docs.rs/arrow-cast-guess-precision/latest/arrow_cast_guess_precision/fn.cast.html

License: MIT
