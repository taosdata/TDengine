use crate::common::itypes::{IJson, INChar, IVarChar};

use super::*;

impl From<Vec<bool>> for ColumnView {
    fn from(values: Vec<bool>) -> Self {
        ColumnView::Bool(BoolView::from_iter(values))
    }
}

impl From<Vec<Option<bool>>> for ColumnView {
    fn from(values: Vec<Option<bool>>) -> Self {
        ColumnView::Bool(BoolView::from_iter(values))
    }
}

impl From<Vec<i8>> for ColumnView {
    fn from(values: Vec<i8>) -> Self {
        ColumnView::TinyInt(TinyIntView::from_iter(values))
    }
}

impl From<Vec<Option<i8>>> for ColumnView {
    fn from(values: Vec<Option<i8>>) -> Self {
        ColumnView::TinyInt(TinyIntView::from_iter(values))
    }
}

impl From<Vec<u8>> for ColumnView {
    fn from(values: Vec<u8>) -> Self {
        ColumnView::UTinyInt(UTinyIntView::from_iter(values))
    }
}

impl From<Vec<Option<u8>>> for ColumnView {
    fn from(values: Vec<Option<u8>>) -> Self {
        ColumnView::UTinyInt(UTinyIntView::from_iter(values))
    }
}

impl From<Vec<i16>> for ColumnView {
    fn from(values: Vec<i16>) -> Self {
        ColumnView::SmallInt(SmallIntView::from_iter(values))
    }
}

impl From<Vec<Option<i16>>> for ColumnView {
    fn from(values: Vec<Option<i16>>) -> Self {
        ColumnView::SmallInt(SmallIntView::from_iter(values))
    }
}

impl From<Vec<u16>> for ColumnView {
    fn from(values: Vec<u16>) -> Self {
        ColumnView::USmallInt(USmallIntView::from_iter(values))
    }
}

impl From<Vec<Option<u16>>> for ColumnView {
    fn from(values: Vec<Option<u16>>) -> Self {
        ColumnView::USmallInt(USmallIntView::from_iter(values))
    }
}

impl From<Vec<i32>> for ColumnView {
    fn from(values: Vec<i32>) -> Self {
        ColumnView::Int(IntView::from_iter(values))
    }
}

impl From<Vec<Option<i32>>> for ColumnView {
    fn from(values: Vec<Option<i32>>) -> Self {
        ColumnView::Int(IntView::from_iter(values))
    }
}

impl From<Vec<u32>> for ColumnView {
    fn from(values: Vec<u32>) -> Self {
        ColumnView::UInt(UIntView::from_iter(values))
    }
}

impl From<Vec<Option<u32>>> for ColumnView {
    fn from(values: Vec<Option<u32>>) -> Self {
        ColumnView::UInt(UIntView::from_iter(values))
    }
}

impl From<Vec<i64>> for ColumnView {
    fn from(values: Vec<i64>) -> Self {
        ColumnView::BigInt(BigIntView::from_iter(values))
    }
}

impl From<Vec<Option<i64>>> for ColumnView {
    fn from(values: Vec<Option<i64>>) -> Self {
        ColumnView::BigInt(BigIntView::from_iter(values))
    }
}

impl From<Vec<u64>> for ColumnView {
    fn from(values: Vec<u64>) -> Self {
        ColumnView::UBigInt(UBigIntView::from_iter(values))
    }
}

impl From<Vec<Option<u64>>> for ColumnView {
    fn from(values: Vec<Option<u64>>) -> Self {
        ColumnView::UBigInt(UBigIntView::from_iter(values))
    }
}

impl From<Vec<f32>> for ColumnView {
    fn from(values: Vec<f32>) -> Self {
        ColumnView::Float(FloatView::from_iter(values))
    }
}

impl From<Vec<Option<f32>>> for ColumnView {
    fn from(values: Vec<Option<f32>>) -> Self {
        ColumnView::Float(FloatView::from_iter(values))
    }
}

impl From<Vec<f64>> for ColumnView {
    fn from(values: Vec<f64>) -> Self {
        ColumnView::Double(DoubleView::from_iter(values))
    }
}

impl From<Vec<Option<f64>>> for ColumnView {
    fn from(values: Vec<Option<f64>>) -> Self {
        ColumnView::Double(DoubleView::from_iter(values))
    }
}

impl From<Vec<Option<IVarChar>>> for ColumnView {
    fn from(values: Vec<Option<IVarChar>>) -> Self {
        ColumnView::VarChar(VarCharView::from_iter::<IVarChar, _, _, _>(values.into_iter()))
    }
}

impl From<Vec<Option<INChar>>> for ColumnView {
    fn from(values: Vec<Option<INChar>>) -> Self {
        ColumnView::NChar(NCharView::from_iter::<INChar, _, _, _>(values.into_iter()))
    }
}
