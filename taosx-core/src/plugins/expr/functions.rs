use std::any::TypeId;

use rhai::{Dynamic, ImmutableString, FLOAT};

#[allow(dead_code)]
pub fn append(s: ImmutableString, append: ImmutableString) -> ImmutableString {
    format!("{}{}", s, append).into()
}

#[allow(dead_code)]
pub fn replace(s: ImmutableString, pat: ImmutableString, to: ImmutableString) -> ImmutableString {
    s.as_str().replace(pat.as_str(), to.as_str()).into()
}

#[allow(dead_code)]
pub fn replacen(
    s: ImmutableString,
    pat: ImmutableString,
    to: ImmutableString,
    n: rhai::INT,
) -> ImmutableString {
    s.as_str()
        .replacen(pat.as_str(), to.as_str(), n as _)
        .into()
}

#[allow(dead_code)]
pub fn truncate(s: ImmutableString, n: rhai::INT) -> ImmutableString {
    s.as_str().chars().take(n as _).collect::<String>().into()
}

#[allow(dead_code)]
pub fn add_or_set(lhs: Dynamic, rhs: Dynamic) -> Dynamic {
    if lhs.is::<()>() {
        rhs
    } else if rhs.is::<()>() {
        lhs
    } else {
        let t_s: TypeId = TypeId::of::<ImmutableString>();
        let t_f: TypeId = TypeId::of::<FLOAT>();
        let t_i: TypeId = TypeId::of::<rhai::INT>();
        match (lhs.type_id(), lhs.type_id()) {
            (lt, rt) if lt == rt => {
                if lt == TypeId::of::<FLOAT>() {
                    Dynamic::from(lhs.as_float().unwrap() + rhs.as_float().unwrap())
                } else if lt == TypeId::of::<rhai::INT>() {
                    Dynamic::from(lhs.as_int().unwrap() + rhs.as_int().unwrap())
                } else if lt == TypeId::of::<ImmutableString>() {
                    Dynamic::from(format!("{}{}", lhs, rhs))
                } else {
                    Dynamic::UNIT
                }
            }
            (lt, rt) if lt == t_s || rt == t_s => Dynamic::from(format!("{}{}", lhs, rhs)),
            #[allow(clippy::nonminimal_bool)]
            (lt, rt) if (lt == t_f && rt == t_i) || (rt == t_f && rt == t_i) => {
                Dynamic::from(lhs.cast::<FLOAT>() + rhs.cast::<FLOAT>())
            }
            _ => Dynamic::UNIT,
        }
    }
}
