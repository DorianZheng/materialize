// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use serde::{Deserialize, Serialize};

use ore::collections::CollectionExt;
use repr::adt::datetime::DateTimeUnits;
use repr::adt::regex::Regex;
use repr::{ColumnType, Datum, RowArena, ScalarType};

use crate::{EvalError, ScalarExpr};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub enum ScalarFunc {
    // Boolean functions.
    CastStringToBool,
    CastBoolToString { concise: bool },
    And,
    Or,
    Not,

    // Comparison functions.
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,

    // Date and time functions.
    CastDateToString,
    CastTimeToString,
    CastTimestampToString,
    CastTimestampTzToString,
    CastStringToDate,
    CastStringToTime,
    CastStringToTimestamp,
    CastStringToTimestampTz,
    CastDateToTimestamp,
    CastDateToTimestampTz,
    CastTimestampToDate,
    CastTimestampTzToDate,
    CastTimestampToTimestampTz,
    CastTimestampTzToTimestamp,
    ToTimestamp,
    DatePartTimestamp,
    DatePartTimestampTz,
    DateTruncTimestamp,
    DateTruncTimestampTz,
    AddDateTime,
    FixedDatePartTimestamp(DateTimeUnits),
    FixedDatePartTimestampTz(DateTimeUnits),
    FixedDateTruncTimestamp(DateTimeUnits),
    FixedDateTruncTimestampTz(DateTimeUnits),
    MakeTimestamp,

    // Decimal functions.
    CastDecimalToString(u8),
    CastStringToDecimal(u8),
    CastDecimalToInt32,
    CastDecimalToInt64,
    CastDecimalToFloat32,
    CastDecimalToFloat64,
    AbsDecimal,
    NegDecimal,
    AddDecimal,
    SubDecimal,
    ModDecimal,
    MulDecimal,
    DivDecimal,
    RoundDecimal(u8),
    CeilDecimal(u8),
    FloorDecimal(u8),
    FixedRoundDecimal(u8),
    SqrtDecimal(u8),

    // Float functions.
    CastFloat32ToString,
    CastFloat64ToString,
    CastStringToFloat32,
    CastStringToFloat64,
    CastFloat32ToInt64,
    CastFloat32ToFloat64,
    CastFloat64ToInt32,
    CastFloat64ToInt64,
    CastFloat32ToDecimal(u8),
    CastFloat64ToDecimal(u8),
    AbsFloat32,
    AbsFloat64,
    NegFloat32,
    NegFloat64,
    AddFloat32,
    AddFloat64,
    SubFloat32,
    SubFloat64,
    MulFloat32,
    MulFloat64,
    DivFloat32,
    DivFloat64,
    ModFloat32,
    ModFloat64,
    SqrtFloat32,
    SqrtFloat64,
    CeilFloat32,
    CeilFloat64,
    FloorFloat32,
    FloorFloat64,
    RoundFloat32,
    RoundFloat64,

    // Formatting functions.
    ToCharTimestamp,
    ToCharTimestampTz,

    // Integer functions.
    CastStringToInt32,
    CastStringToInt64,
    CastInt32ToBool,
    CastInt32ToFloat32,
    CastInt32ToFloat64,
    CastInt32ToInt64,
    CastInt32ToString,
    CastInt32ToDecimal,
    CastInt64ToBool,
    CastInt64ToDecimal,
    CastInt64ToFloat32,
    CastInt64ToFloat64,
    CastInt64ToInt32,
    CastInt64ToString,
    AbsInt32,
    AbsInt64,
    NegInt32,
    NegInt64,
    AddInt32,
    AddInt64,
    SubInt32,
    SubInt64,
    MulInt32,
    MulInt64,
    DivInt32,
    DivInt64,
    ModInt32,
    ModInt64,

    // Interval functions.
    CastIntervalToString,
    CastStringToInterval,
    CastTimeToInterval,
    CastIntervalToTime,
    NegInterval,
    FixedDatePartInterval(DateTimeUnits),
    AddInterval,
    AddDateInterval,
    AddTimeInterval,
    AddTimestampInterval,
    AddTimestampTzInterval,
    SubInterval,
    SubDateInterval,
    SubTimeInterval,
    SubTimestampTzInterval,
    SubTimestampInterval,
    SubDate,
    SubTime,
    SubTimestamp,
    SubTimestampTz,
    DatePartInterval,

    // JSONB functions.
    CastStringToJsonb,
    CastJsonbToString,
    CastJsonbOrNullToJsonb,
    CastJsonbToFloat64,
    CastJsonbToBool,
    JsonbPretty,
    JsonbTypeof,
    JsonbArrayLength,
    JsonbStripNulls,
    JsonbGetInt64 { stringify: bool },
    JsonbGetString { stringify: bool },
    JsonbDeleteInt64,
    JsonbDeleteString,
    JsonbContainsString,
    JsonbContainsJsonb,
    JsonbConcat,
    JsonbBuildArray,
    JsonbBuildObject,

    // List functions.
    ListCreate { elem_type: ScalarType },

    // Null-handling functions.
    Coalesce,
    IsNull,

    // String functions.
    CastBytesToString,
    CastStringToBytes,
    BitLengthBytes,
    BitLengthString,
    ByteLengthBytes,
    ByteLengthString,
    CharLength,
    Ascii,
    TrimWhitespace,
    TrimLeadingWhitespace,
    TrimTrailingWhitespace,
    MatchRegex(Regex),
    ConvertFrom,
    EncodedBytesCharLength,
    StringConcat,
    Trim,
    TrimLeading,
    TrimTrailing,
    MatchLikePattern,
    Concat,
    Substr,
    Replace,
}

macro_rules! func_delegate_const {
    ($self:ident, $const:ident) => {
        match $self {
            // Boolean functions.
            ScalarFunc::CastStringToBool => bool::CastStringToBool::$const,
            ScalarFunc::CastBoolToString { .. } => bool::CastBoolToString::$const,
            ScalarFunc::And => bool::And::$const,
            ScalarFunc::Or => bool::Or::$const,
            ScalarFunc::Not => bool::Not::$const,

            _ => todo!(),
        }
    };
}

macro_rules! func_delegate_method {
    ($self:ident, $method:ident $(, $args:expr)*) => {
        match $self {
            ScalarFunc::CastStringToBool => bool::CastStringToBool.$method($($args),*),
            ScalarFunc::CastBoolToString { concise } => bool::CastBoolToString { concise: *concise }.$method($($args),*),
            ScalarFunc::And => bool::And.$method($($args),*),
            ScalarFunc::Or => bool::Or.$method($($args),*),
            ScalarFunc::Not => bool::Not.$method($($args),*),

            _ => todo!(),
        }
    }
}

impl ScalarFunc {
    // Specifies the null behavior of the function.
    pub fn nulls(&self) -> Nulls {
        func_delegate_const!(self, NULLS)
    }

    /// Returns whether the function propagates nulls.
    ///
    /// This is a convenience accessor for information available in
    /// [`Func::nulls`]
    pub fn propagates_nulls(&self) -> bool {
        match self.nulls() {
            Nulls::Never => false,
            Nulls::Sometimes {
                propagates_nulls, ..
            } => propagates_nulls,
        }
    }

    /// Whether the function can produce an error.
    pub fn can_error(&self) -> bool {
        func_delegate_const!(self, __CAN_ERROR)
    }

    /// True iff for x != y, we are assured f(x) != f(y).
    ///
    /// This is most often the case for methods that promote to types that
    /// can contain all the precision of the input type.
    pub fn preserves_uniqueness(&self) -> bool {
        func_delegate_const!(self, PRESERVES_UNIQUENESS)
    }

    /// Returns the arity of the function.
    pub fn arity(&self) -> usize {
        func_delegate_const!(self, __ARITY)
    }

    /// Evaluates the function.
    pub fn eval<'a>(
        &self,
        exprs: &[ScalarExpr],
        datums: &[Datum<'a>],
        temp_storage: &RowArena,
    ) -> Result<Datum<'a>, EvalError> {
        func_delegate_method!(self, __eval, exprs, datums, temp_storage)
    }

    /// Computes the output type of the function given the input types.
    pub fn output_type(&self, input_types: &[ColumnType]) -> ColumnType {
        let nullable = match self.nulls() {
            Nulls::Never => false,
            Nulls::Sometimes {
                introduces_nulls: true,
                ..
            } => true,
            Nulls::Sometimes {
                introduces_nulls: false,
                ..
            } => input_types.iter().any(|t| t.nullable),
        };

        let input_types: Vec<_> = input_types
            .iter()
            .map(|ty| ty.scalar_type.clone())
            .collect();

        let scalar_type = func_delegate_method!(self, __output_type, &input_types);
        ColumnType::new(scalar_type).nullable(nullable)
    }
}


/// Specifies the null behavior of a function.
#[derive(Debug)]
pub enum Nulls {
    /// The function never returns null, not even if the inputs to the function
    /// are null.
    Never,
    /// The function sometimes returns null.
    Sometimes {
        /// The function returns null if any of its inputs are null.
        propagates_nulls: bool,
        /// The function might return null even if its inputs are non-null.
        introduces_nulls: bool,
    },
}

/// Specifies the affixment of the string representation of an operator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Affixment {
    Prefix,
    Infix,
}

pub(crate) mod bool;
// pub(crate) mod cmp;
// pub(crate) mod datetime;
// pub(crate) mod decimal;
// pub(crate) mod float;
// pub(crate) mod format;
// pub(crate) mod integer;
// pub(crate) mod interval;
// pub(crate) mod jsonb;
// pub(crate) mod list;
// pub(crate) mod null;
// pub(crate) mod string;

trait UnaryFunc: Sized {
    const NULLS: Nulls;
    const PRESERVES_UNIQUENESS: bool = false;
    fn eval<'a>(self, d: Datum<'a>, temp_storage: &'a RowArena) -> Datum<'a>;
    fn output_type(self, ty: &ScalarType) -> ScalarType;

    const __CAN_ERROR: bool = false;
    const __ARITY: usize = 1;
    fn __eval<'a>(
        self,
        exprs: &[ScalarExpr],
        datums: &[Datum<'a>],
        temp_storage: &RowArena,
    ) -> Result<Datum<'a>, EvalError> {
        todo!()
    }
    fn __output_type(self, input_types: &[ScalarType]) -> ScalarType {
        todo!()
    }
}

trait FallibleUnaryFunc: Sized {
    const NULLS: Nulls;
    const PRESERVES_UNIQUENESS: bool = false;
    fn eval<'a>(self, datum: Datum<'a>, temp_storage: &RowArena) -> Result<Datum<'a>, EvalError>;
    fn output_type(self, ty: &ScalarType) -> ScalarType;

    const __CAN_ERROR: bool = true;
    const __ARITY: usize = 1;
    fn __eval<'a>(
        self,
        exprs: &[ScalarExpr],
        datums: &[Datum<'a>],
        temp_storage: &RowArena,
    ) -> Result<Datum<'a>, EvalError> {
        todo!()
    }
    fn __output_type(self, input_types: &[ScalarType]) -> ScalarType {
        todo!()
    }
}

trait BinaryFunc: Sized {
    const NULLS: Nulls;
    fn eval<'a>(self, lhs: Datum<'a>, rhs: Datum<'a>, temp_storage: &RowArena) -> Datum<'a>;
    fn output_type(self, lhs_ty: &ScalarType, rhs_ty: &ScalarType) -> ScalarType;

    const __CAN_ERROR: bool = false;
    const __ARITY: usize = 2;
    fn __eval<'a>(
        self,
        exprs: &[ScalarExpr],
        datums: &[Datum<'a>],
        temp_storage: &RowArena,
    ) -> Result<Datum<'a>, EvalError> {
        todo!()
    }
    fn __output_type(self, input_types: &[ScalarType]) -> ScalarType {
        todo!()
    }
}

trait FallibleBinaryFunc: Sized {
    const NULLS: Nulls;
    fn eval<'a>(
        self,
        lhs: Datum<'a>,
        rhs: Datum<'a>,
        temp_storage: &RowArena,
    ) -> Result<Datum<'a>, EvalError>;
    fn output_type(self, lhs_ty: &ScalarType, rhs_ty: &ScalarType) -> ScalarType;

    const __CAN_ERROR: bool = true;
    const __ARITY: usize = 2;
    fn __eval<'a>(
        self,
        exprs: &[ScalarExpr],
        datums: &[Datum<'a>],
        temp_storage: &RowArena,
    ) -> Result<Datum<'a>, EvalError> {
        todo!()
    }
    fn __output_type(self, input_types: &[ScalarType]) -> ScalarType {
        todo!()
    }
}

trait GenericFunc: Sized {
    const NULLS: Nulls;
    const PRESERVES_UNIQUENESS: bool = false;
    const CAN_ERROR: bool;
    const ARITY: usize;
    fn eval<'a>(
        self,
        exprs: &[ScalarExpr],
        datums: &[Datum<'a>],
        temp_storage: &RowArena,
    ) -> Result<Datum<'a>, EvalError>;
    fn output_type(self, input_types: &[ScalarType]) -> ScalarType;

    const __CAN_ERROR: bool = Self::CAN_ERROR;
    const __ARITY: usize = Self::ARITY;
    fn __eval<'a>(
        self,
        exprs: &[ScalarExpr],
        datums: &[Datum<'a>],
        temp_storage: &RowArena,
    ) -> Result<Datum<'a>, EvalError> {
        self.eval(exprs, datums, temp_storage)
    }
    fn __output_type(self, input_types: &[ScalarType]) -> ScalarType {
        self.output_type(input_types)
    }
}
