use std::ops::Deref;
use std::{fmt::Debug, ops::DerefMut};

use bytes::{Bytes, BytesMut};

mod bool_view;
pub use bool_view::*;

mod tinyint_view;
pub use tinyint_view::*;

mod smallint_view;
pub use smallint_view::*;

mod int_view;
pub use int_view::*;

mod big_int_view;
pub use big_int_view::*;

mod float_view;
pub use float_view::*;

mod double_view;
pub use double_view::*;

mod tinyint_unsigned_view;
pub use tinyint_unsigned_view::*;

mod small_int_unsigned_view;
pub use small_int_unsigned_view::*;

mod int_unsigned_view;
pub use int_unsigned_view::*;

mod big_int_unsigned_view;
pub use big_int_unsigned_view::*;

mod timestamp_view;
pub use timestamp_view::*;

mod var_char_view;
pub use var_char_view::*;

mod n_char_view;
pub use n_char_view::*;

mod json_view;
pub use json_view::*;

mod schema;
pub use schema::*;

mod nulls;
pub use nulls::*;

mod offsets;
pub use offsets::*;

mod lengths;
pub use lengths::*;

/// Compatible version for var char.
pub enum Version {
    V2,
    V3,
}
