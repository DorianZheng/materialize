// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Boolean functions.

use repr::strconv;
use repr::{Datum, RowArena, ScalarType};

use crate::scalar::func::{FallibleUnaryFunc, GenericFunc, Nulls, UnaryFunc};
use crate::scalar::{EvalError, ScalarExpr};

pub struct CastBoolToString {
    pub concise: bool,
}

impl UnaryFunc for CastBoolToString {
    const NULLS: Nulls = Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    };

    const PRESERVES_UNIQUENESS: bool = true;

    fn eval<'a>(self, d: Datum<'a>, _: &RowArena) -> Datum<'a> {
        if self.concise {
            match d.unwrap_bool() {
                true => Datum::from("t"),
                false => Datum::from("f"),
            }
        } else {
            Datum::String(strconv::format_bool_static(d.unwrap_bool()))
        }
    }

    fn output_type(self, _: &ScalarType) -> ScalarType {
        ScalarType::String
    }
}

pub struct CastStringToBool;

impl FallibleUnaryFunc for CastStringToBool {
    const NULLS: Nulls = Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    };

    const PRESERVES_UNIQUENESS: bool = true;

    fn eval<'a>(self, d: Datum<'a>, _: &RowArena) -> Result<Datum<'a>, EvalError> {
        match strconv::parse_bool(d.unwrap_str())? {
            true => Ok(Datum::True),
            false => Ok(Datum::False),
        }
    }

    fn output_type(self, _: &ScalarType) -> ScalarType {
        ScalarType::Bool
    }
}

pub struct And;

impl GenericFunc for And {
    const CAN_ERROR: bool = false;
    const ARITY: usize = 2;
    const NULLS: Nulls = Nulls::Sometimes {
        propagates_nulls: false,
        introduces_nulls: false,
    };

    fn eval<'a>(
        self,
        exprs: &[ScalarExpr],
        datums: &[Datum<'a>],
        temp_storage: &RowArena,
    ) -> Result<Datum<'a>, EvalError> {
        debug_assert_eq!(exprs.len(), 2);
        match exprs[0].eval(datums, temp_storage)? {
            Datum::False => Ok(Datum::False),
            a => match (a, exprs[1].eval(datums, temp_storage)?) {
                (_, Datum::False) => Ok(Datum::False),
                (Datum::Null, _) | (_, Datum::Null) => Ok(Datum::Null),
                (Datum::True, Datum::True) => Ok(Datum::True),
                _ => unreachable!(),
            },
        }
    }

    fn output_type(self, _: &[ScalarType]) -> ScalarType {
        ScalarType::Bool
    }
}

pub struct Or;

impl GenericFunc for Or {
    const CAN_ERROR: bool = false;
    const ARITY: usize = 2;
    const NULLS: Nulls = Nulls::Sometimes {
        propagates_nulls: false,
        introduces_nulls: false,
    };

    fn eval<'a>(
        self,
        exprs: &[ScalarExpr],
        datums: &[Datum<'a>],
        temp_storage: &RowArena,
    ) -> Result<Datum<'a>, EvalError> {
        debug_assert_eq!(exprs.len(), 2);
        match exprs[0].eval(datums, temp_storage)? {
            Datum::True => Ok(Datum::True),
            a => match (a, exprs[1].eval(datums, temp_storage)?) {
                (_, Datum::True) => Ok(Datum::True),
                (Datum::Null, _) | (_, Datum::Null) => Ok(Datum::Null),
                (Datum::False, Datum::False) => Ok(Datum::False),
                _ => unreachable!(),
            },
        }
    }

    fn output_type(self, _: &[ScalarType]) -> ScalarType {
        ScalarType::Bool
    }
}

pub struct Not;

impl UnaryFunc for Not {
    const NULLS: Nulls = Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    };

    const PRESERVES_UNIQUENESS: bool = true;

    fn eval<'a>(self, d: Datum<'a>, _: &RowArena) -> Datum<'a> {
        Datum::from(!d.unwrap_bool())
    }

    fn output_type(self, _: &ScalarType) -> ScalarType {
        ScalarType::Bool
    }
}
