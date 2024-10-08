//! Cast integer to timestamp with precision guessing options.
//!
//! Just replace [arrow::compute::cast] with [arrow::compute_guess_precision::cast] and everything done.
//!
//! ```rust
//! use arrow::{
//!     array::{Int64Array, TimestampNanosecondArray},
//!     datatypes::{DataType, TimeUnit}
//! };
//!
//! let data = vec![1701325744956, 1701325744956];
//! let array = Int64Array::from(data);
//! let array = arrow::compute_guess_precision::cast(
//!     &array,
//!     &DataType::Timestamp(TimeUnit::Nanosecond, None),
//! )
//! .unwrap();
//! let nanos = array
//!     .as_any()
//!     .downcast_ref::<TimestampNanosecondArray>()
//!     .unwrap();
//! assert_eq!(nanos.value(0), 1701325744956 * 1000 * 1000);
//! ```
//!
//! The difference to official [arrow::compute::cast] is that:
//!
//! - arrow v49 will cast integer directly to timestamp, but this crate(`arrow-cast-guess-precision = "0.3.0"`) will try to guess from the value.
//! - arrow v48 does not support casting from integers to timestamp (`arrow-cast-guess-precision = "0.2.0"`).
//!
//! The guessing method is:
//!
//! ```rust
//! use arrow::datatypes::TimeUnit;
//!
//! const GUESSING_BOUND_YEARS: i64 = 10000;
//! const LOWER_BOUND_MILLIS: i64 = 86400 * 365 * GUESSING_BOUND_YEARS;
//! const LOWER_BOUND_MICROS: i64 = 1000 * 86400 * 365 * GUESSING_BOUND_YEARS;
//! const LOWER_BOUND_NANOS: i64 = 1000 * 1000 * 86400 * 365 * GUESSING_BOUND_YEARS;
//!
//! #[inline]
//! const fn guess_precision(timestamp: i64) -> TimeUnit {
//!     let timestamp = timestamp.abs();
//!     if timestamp > LOWER_BOUND_NANOS {
//!         return TimeUnit::Nanosecond;
//!     }
//!     if timestamp > LOWER_BOUND_MICROS {
//!         return TimeUnit::Microsecond;
//!     }
//!     if timestamp > LOWER_BOUND_MILLIS {
//!         return TimeUnit::Millisecond;
//!     }
//!     TimeUnit::Second
//! }
//! ```
//!
//! Users could set `ARROW_CAST_GUESSING_BOUND_YEARS` environment at build-time to control the guessing bound.
//! here is a sample list based on individual environment values:
//!
//! |    value | lower bound             |       Upper Bound       |
//! | -------: | ----------------------- | :---------------------: |
//! |      100 | 1970-02-06t12:00:00     |   2069-12-07T00:00:00   |
//! |      200 | 1970-03-15t00:00:00     |   2169-11-13T00:00:00   |
//! |      500 | 1970-07-02t12:00:00     |   2469-09-01T00:00:00   |
//! | **1000** | **1971-01-01T00:00:00** | **2969-05-03T00:00:00** |
//! |     2000 | 1972-01-01t00:00:00     |   3968-09-03T00:00:00   |
//! |     5000 | 1974-12-31t00:00:00     |   6966-09-06T00:00:00   |
//! |    10000 | 1979-12-30t00:00:00     |  +11963-05-13T00:00:00  |
//!
//! We use `ARROW_CAST_GUESSING_BOUND_YEARS=1000` by default, just because `1000` milliseconds is `1` second so that the lower bound starts with `1971-01-01T00:00:00` which is one year after ZERO unix timestamp, and the upper bound is enough (even 100-years is enough though).
//!
//! Like [arrow::compute::cast], this crate also supports casting with specific options, checkout [CastOptions](arrow::compute_guess_precision::CastOptions).
//!
//! [arrow::compute::cast]: https://docs.rs/arrow/latest/arrow/compute/fn.cast.html
//! [arrow::compute_guess_precision::cast]: https://docs.rs/arrow-cast-guess-precision/latest/arrow::compute_guess_precision/fn.cast.html

use arrow::array::{
    make_array, new_empty_array, new_null_array, Array, ArrayRef, BinaryArray,
    FixedSizeBinaryArray, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
};
use arrow::util::display::FormatOptions;
use arrow_schema::{ArrowError, DataType, TimeUnit};

include!(concat!(env!("OUT_DIR"), "/guessing_bound.rs"));

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

/// Guessing precision from an array of integers.
///
/// The array should be an [Int64Array](arrow::array::Int64Array).
#[inline]
fn guess_precision_in_array(array: &dyn Array) -> Option<TimeUnit> {
    let v = array.as_any().downcast_ref::<Int64Array>().unwrap();
    v.into_iter()
        .flatten()
        .max()
        .filter(|v| *v != 0)
        .map(guess_precision)
}

pub fn cast(array: &dyn Array, to_type: &DataType) -> Result<ArrayRef, ArrowError> {
    cast_with_options(array, to_type, &CastOptions::default())
}

#[derive(Debug, Clone)]
pub struct TimestampCastOptions {
    /// If true, try to guess the precision of the timestamp from integers.
    ///
    /// Caster will first convert the integer to i64 and then guess the precision.
    pub guess_timestamp_precision: bool,
    /// If true, caster use the timezone in target type. If false, caster will use UTC.
    pub use_timezone_as_is: bool,
}

impl Default for TimestampCastOptions {
    fn default() -> Self {
        Self {
            guess_timestamp_precision: true,
            use_timezone_as_is: true,
        }
    }
}

pub struct CastOptions<'a> {
    pub safe: bool,
    pub timestamp_options: TimestampCastOptions,
    pub format_options: FormatOptions<'a>,
}

impl Default for CastOptions<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl CastOptions<'_> {
    pub fn new() -> Self {
        Self {
            safe: true,
            timestamp_options: TimestampCastOptions::default(),
            format_options: FormatOptions::default(),
        }
    }
}

impl<'r, 'a> From<&'r CastOptions<'a>> for arrow::compute::CastOptions<'r> {
    fn from(options: &'r CastOptions) -> arrow::compute::CastOptions<'r> {
        arrow::compute::CastOptions {
            safe: options.safe,
            format_options: options.format_options.clone(),
        }
    }
}

pub fn cast_with_options(
    array: &dyn Array,
    to_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    use DataType::*;
    let from_type = array.data_type();
    if from_type == to_type {
        return Ok(make_array(array.to_data()));
    }
    if array.is_empty() {
        return Ok(new_empty_array(to_type));
    }
    if from_type == &Null {
        return Ok(new_null_array(to_type, array.len()));
    }

    // to_type, Timestamp(unit, tz)) {
    match (from_type, to_type) {
        (
            // Convert to second precision integer.
            Int8 | Int16 | Int32 | UInt8 | UInt32 | Float16 | Float32 | UInt16,
            Timestamp(unit, tz),
        ) => {
            let tz = if cast_options.timestamp_options.use_timezone_as_is {
                tz.clone()
            } else {
                None
            };
            let array = arrow::compute::cast(array, &Int64)?;
            if cast_options.timestamp_options.guess_timestamp_precision {
                let array = arrow::compute::cast(&array, &Timestamp(TimeUnit::Second, tz))?;
                arrow::compute::cast_with_options(&array, to_type, &cast_options.into())
            } else {
                let array = arrow::compute::cast(&array, &Timestamp(unit.clone(), tz))?;
                arrow::compute::cast_with_options(&array, to_type, &cast_options.into())
            }
        }

        (Binary | FixedSizeBinary(_) | LargeBinary | Utf8 | LargeUtf8, Timestamp(_, _)) => {
            fn all_is_digit(array: &dyn Array) -> bool {
                match array.data_type() {
                    DataType::Utf8 => {
                        let v = array.as_any().downcast_ref::<StringArray>().unwrap();
                        v.iter()
                            .flatten()
                            .all(|x| x.chars().all(|c| c.is_ascii_digit()))
                    }
                    DataType::LargeUtf8 => {
                        let v = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                        v.iter()
                            .flatten()
                            .all(|x| x.chars().all(|c| c.is_ascii_digit()))
                    }
                    DataType::Binary => {
                        let v = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                        v.iter()
                            .flatten()
                            .all(|x| x.iter().all(|c| c.is_ascii_digit()))
                    }
                    DataType::LargeBinary => {
                        let v = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                        v.iter()
                            .flatten()
                            .all(|x| x.iter().all(|c| c.is_ascii_digit()))
                    }
                    DataType::FixedSizeBinary(_) => {
                        let v = array
                            .as_any()
                            .downcast_ref::<FixedSizeBinaryArray>()
                            .unwrap();
                        v.iter()
                            .flatten()
                            .all(|x| x.iter().all(|c| c.is_ascii_digit()))
                    }
                    _ => false,
                }
            }
            if all_is_digit(array) {
                if let Ok(array) =
                    arrow::compute::cast_with_options(array, &Int64, &cast_options.into())
                {
                    // Indicate that the string is timestamp integer.
                    return cast_with_options(array.as_ref(), to_type, cast_options);
                }
            }

            // call official cast function: `arrow::compute::cast_with_options`
            arrow::compute::cast_with_options(array, to_type, &cast_options.into())
        }
        (Int64 | UInt64 | Float64, Timestamp(unit, tz)) => {
            let array = arrow::compute::cast(array, &Int64)?;

            let tz = if cast_options.timestamp_options.use_timezone_as_is {
                tz.clone()
            } else {
                None
            };
            if cast_options.timestamp_options.guess_timestamp_precision {
                let array = arrow::compute::cast(
                    &array,
                    &Timestamp(
                        guess_precision_in_array(&array).unwrap_or_else(|| unit.clone()),
                        tz,
                    ),
                )?;
                arrow::compute::cast_with_options(&array, to_type, &cast_options.into())
            } else {
                let array = cast(&array, &Timestamp(unit.clone(), tz))?;
                arrow::compute::cast_with_options(&array, to_type, &cast_options.into())
            }
        }
        _ => arrow::compute::cast_with_options(array, to_type, &cast_options.into()),
    }
}

#[cfg(test)]
mod test {
    use arrow::array::TimestampNanosecondArray;

    use super::*;

    #[test]
    fn test_int_to_timestamp() {
        let data = vec![1701325744956, 1701325744956];
        let array = arrow::array::Int64Array::from(data);
        let array = crate::cast(
            &array,
            &arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
        )
        .unwrap();
        let nanos = array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        dbg!(nanos);
        assert_eq!(nanos.value(0), 1701325744956 * 1000 * 1000);
        dbg!(array);
    }

    #[test]
    fn test_string_to_timestamp() {
        let string = vec!["1701325744956", "1701325744956"];
        let array = arrow::array::StringArray::from(string);
        let array = crate::cast(
            &array,
            &arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
        )
        .unwrap();
        let nanos = array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        dbg!(nanos);
        assert_eq!(nanos.value(0), 1701325744956 * 1000 * 1000);
        dbg!(array);
    }
    #[test]
    fn test() {
        let now = chrono::Utc::now();
        let ten_years_ago = now - chrono::Duration::days(365 * 10);
        let ten_years_later = now + chrono::Duration::days(365 * 10);

        let mut ints = Vec::new();
        let mut pres = Vec::new();

        for now in [now, ten_years_ago, ten_years_later] {
            println!("Timestamp {} in ms: {}", now, now.timestamp_millis());
            println!("Timestamp {} in us: {}", now, now.timestamp_micros());
            println!(
                "Timestamp {} in ns: {}",
                now,
                now.timestamp_nanos_opt().unwrap()
            );

            ints.push(now.timestamp());
            pres.push(TimeUnit::Second);
            ints.push(now.timestamp_millis());
            pres.push(TimeUnit::Millisecond);
            ints.push(now.timestamp_micros());
            pres.push(TimeUnit::Microsecond);
            ints.push(now.timestamp_nanos_opt().unwrap());
            pres.push(TimeUnit::Nanosecond);
            // chrono::DateTime::from_timestamp(now.timestamp_nanos());
        }

        ints.push(i32::MAX as _);
        pres.push(TimeUnit::Second);

        for (i, u) in ints.into_iter().zip(pres.into_iter()) {
            println!("Timestamp {} in {:?}", i, guess_precision(i),);
            assert_eq!(guess_precision(i), u);
        }
    }

    #[test]
    fn bound() {
        let zero = chrono::DateTime::from_timestamp(0, 0)
            .map(|t| t.naive_utc())
            .unwrap();
        let seconds_upper_bound = zero + std::time::Duration::from_secs(LOWER_BOUND_MILLIS as _);
        println!("{:?}", (zero..seconds_upper_bound));
        let millis_lower_bound = zero + std::time::Duration::from_millis(LOWER_BOUND_MILLIS as _);
        let millis_upper_bound = zero + std::time::Duration::from_millis(LOWER_BOUND_MICROS as _);
        println!("{:?}", (millis_lower_bound..millis_upper_bound));
        let micros_lower_bound = zero + std::time::Duration::from_micros(LOWER_BOUND_MICROS as _);
        let micros_upper_bound = zero + std::time::Duration::from_micros(LOWER_BOUND_NANOS as _);
        println!("{:?}", (micros_lower_bound..micros_upper_bound));
        let nanos_lower_bound = zero + std::time::Duration::from_nanos(LOWER_BOUND_NANOS as _);
        println!("{:?}", (nanos_lower_bound..));
    }

    #[test]
    fn bound_sample() {
        let zero = chrono::DateTime::from_timestamp(0, 0)
            .map(|t| t.naive_utc())
            .unwrap();
        println!("ARROW_CAST_GUESSING_BOUND_YEARS |     Lower Bound     |     Upper Bound    ");
        println!("------------------------------- | ------------------- | -------------------");
        let width: usize = "ARROW_CAST_GUESSING_BOUND_YEARS".len();
        for years in vec![100, 200, 500, 1000, 2000, 5000, 10000].into_iter() {
            let lower_bound_millis: i64 = 86400 * 365 * years;
            let lower_bound_micros: i64 = 1000 * 86400 * 365 * years;
            let lower_bound_nanos: i64 = 1000 * 1000 * 86400 * 365 * years;
            let seconds_upper_bound =
                zero + std::time::Duration::from_secs(lower_bound_millis as _);
            println!("{:width$} | {:?} | {:?}", years, zero, seconds_upper_bound);
            let millis_lower_bound =
                zero + std::time::Duration::from_millis(lower_bound_millis as _);
            let millis_upper_bound =
                zero + std::time::Duration::from_millis(lower_bound_micros as _);
            println!(
                "{:width$} | {:?} | {:?}",
                years, millis_lower_bound, millis_upper_bound,
            );
            let micros_lower_bound =
                zero + std::time::Duration::from_micros(lower_bound_micros as _);
            let micros_upper_bound =
                zero + std::time::Duration::from_micros(lower_bound_nanos as _);
            println!(
                "{:width$} | {:?} | {:?}",
                years, micros_lower_bound, micros_upper_bound,
            );
            let nanos_lower_bound = zero + std::time::Duration::from_nanos(lower_bound_nanos as _);
            println!("{:width$} | {:?} |", years, nanos_lower_bound,);
        }

        println!("ARROW_CAST_GUESSING_BOUND_YEARS |     Lower Bound     |     Upper Bound    ");
        println!("------------------------------- | ------------------- | -------------------");
        for years in vec![100, 200, 500, 1000, 2000, 5000, 10000].into_iter() {
            let lower_bound_millis: i64 = 86400 * 365 * years;
            let lower_bound_micros: i64 = 1000 * 86400 * 365 * years;
            let millis_lower_bound =
                zero + std::time::Duration::from_millis(lower_bound_millis as _);
            let millis_upper_bound =
                zero + std::time::Duration::from_millis(lower_bound_micros as _);
            println!(
                "{:width$} | {:?} | {:?}",
                years,
                millis_lower_bound,
                millis_upper_bound,
                width = width
            );
        }
    }
}
