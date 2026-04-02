use rhai::{Dynamic, FLOAT, INT, ImmutableString};
use std::any::TypeId;

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
    n: INT,
) -> ImmutableString {
    s.as_str()
        .replacen(pat.as_str(), to.as_str(), n as _)
        .into()
}

#[allow(dead_code)]
pub fn truncate(s: ImmutableString, n: INT) -> ImmutableString {
    s.as_str().chars().take(n as _).collect::<String>().into()
}

#[allow(dead_code)]
pub fn between_time_range(s: ImmutableString, l_sec: i64, r_sec: i64) -> bool {
    let (now, t) = match dateparser::parse(&s) {
        Ok(dt) => (
            chrono::Utc::now().with_timezone(&dt.timezone()).timestamp(),
            dt.timestamp(),
        ),
        Err(_) => (chrono::Local::now().timestamp(), 0),
    };

    let l_timestamp = now + l_sec;
    let r_timestamp = now + r_sec;

    t > l_timestamp && t < r_timestamp
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
        let t_i: TypeId = TypeId::of::<INT>();
        match (lhs.type_id(), rhs.type_id()) {
            (lt, rt) if lt == rt => {
                if lt == t_f {
                    Dynamic::from(lhs.as_float().unwrap() + rhs.as_float().unwrap())
                } else if lt == t_i {
                    Dynamic::from(lhs.as_int().unwrap() + rhs.as_int().unwrap())
                } else if lt == t_s {
                    Dynamic::from(format!("{}{}", lhs, rhs))
                } else {
                    Dynamic::UNIT
                }
            }
            (lt, rt) if lt == t_s || rt == t_s => Dynamic::from(format!("{}{}", lhs, rhs)),
            (lt, rt) if lt == t_f && rt == t_i => {
                Dynamic::from(lhs.as_float().unwrap() + rhs.as_int().unwrap() as FLOAT)
            }
            (lt, rt) if rt == t_f && lt == t_i => {
                Dynamic::from(rhs.as_float().unwrap() + lhs.as_int().unwrap() as FLOAT)
            }
            _ => {
                tracing::warn!("add_or_set: unsupported {lhs:?} + {rhs:?}");
                Dynamic::UNIT
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_append() {
        let a: ImmutableString = "hello".into();
        let b: ImmutableString = "world".into();
        let r = append(a, b);
        assert_eq!(r.as_str(), "helloworld");
    }

    #[test]
    pub fn test_replace_and_replacen() {
        let s: ImmutableString = "the quick brown fox jumps over the lazy dog".into();
        let r = replace(s.clone(), "fox".into(), "cat".into());
        assert_eq!(r.as_str(), "the quick brown cat jumps over the lazy dog");

        let s2: ImmutableString = "a a a a".into();
        let r2 = replacen(s2, "a".into(), "b".into(), 2 as INT);
        assert_eq!(r2.as_str(), "b b a a");
    }

    #[test]
    pub fn test_truncate() {
        let s: ImmutableString = "abcdef".into();
        assert_eq!(truncate(s.clone(), 3 as INT).as_str(), "abc");
        // unicode characters: counts by chars, not bytes
        let uni: ImmutableString = "héllo".into();
        assert_eq!(truncate(uni, 2 as INT).as_str(), "hé");
    }

    #[test]
    pub fn test_add_or_set_basic_cases() {
        // lhs is unit -> return rhs
        let res = add_or_set(Dynamic::UNIT, Dynamic::from(5_i64));
        assert!(res.is::<INT>());
        assert_eq!(res.as_int().unwrap(), 5);

        // rhs is unit -> return lhs
        let res = add_or_set(Dynamic::from(7_i64), Dynamic::UNIT);
        assert!(res.is::<INT>());
        assert_eq!(res.as_int().unwrap(), 7);

        // both ints
        let res = add_or_set(Dynamic::from(2_i64), Dynamic::from(3_i64));
        assert!(res.is::<INT>());
        assert_eq!(res.as_int().unwrap(), 5);

        // both floats
        let res = add_or_set(Dynamic::from(1.5_f64), Dynamic::from(2.25_f64));
        assert!(res.is::<FLOAT>());
        let v = res.as_float().unwrap();
        assert!((v - 3.75).abs() < 1e-12);

        // both strings
        let s1: ImmutableString = "foo".into();
        let s2: ImmutableString = "bar".into();
        let res = add_or_set(Dynamic::from(s1.clone()), Dynamic::from(s2.clone()));
        // result should be concatenation "foobar"
        assert_eq!(res.to_string(), "foobar");

        // one is string, other is int -> concatenation
        let res = add_or_set(Dynamic::from("x".to_string()), Dynamic::from(10_i64));
        assert_eq!(res.to_string(), "x10");
    }

    #[test]
    pub fn test_add_or_set_float_int_mixed_behaviour() {
        // lhs float, rhs int -> conversion to float and addition (per current logic)
        let lhs = Dynamic::from(1.5_f64);
        let rhs = Dynamic::from(2_i64);
        let res = add_or_set(lhs, rhs);
        assert!(res.is::<FLOAT>());
        assert!((res.as_float().unwrap() - 3.5).abs() < 1e-12);

        // reversed: lhs int, rhs float -> conversion to float and addition
        let lhs = Dynamic::from(2_i64);
        let rhs = Dynamic::from(1.5_f64);
        let res = add_or_set(lhs, rhs);
        assert!(res.is::<FLOAT>());
        assert!((res.as_float().unwrap() - 3.5).abs() < 1e-12);

        let lhs = Dynamic::from(i64::MAX);
        let rhs = Dynamic::from(f64::MAX);
        let res = add_or_set(lhs, rhs);
        assert!(res.is::<FLOAT>());
        assert!((res.as_float().unwrap() - f64::MAX).abs() < 1e-12);
    }

    #[test]
    pub fn test_add_or_set_string_int_or_float() {
        // one is string, other is int -> concatenation
        let lhs = Dynamic::from("value: ".to_string());
        let rhs = Dynamic::from(42_i64);
        let res = add_or_set(lhs, rhs);
        assert!(res.is::<ImmutableString>());
        assert_eq!(res.to_string(), "value: 42");
        // one is string, other is float -> concatenation
        let lhs = Dynamic::from("value: ".to_string());
        let rhs = Dynamic::from(3.15_f64);
        let res = add_or_set(lhs, rhs);
        assert!(res.is::<ImmutableString>());
        assert_eq!(res.to_string(), "value: 3.15");
    }

    #[test]
    pub fn test_add_or_set_mismatched_types() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();
        // bool + bool -> UNIT
        let lhs = Dynamic::from(true);
        let rhs = Dynamic::from(false);
        let res = add_or_set(lhs, rhs);
        assert!(res.is::<()>());
        // mismatched types that are not handled -> UNIT
        let lhs = Dynamic::from(true);
        let rhs = Dynamic::from(5_i64);
        let res = add_or_set(lhs, rhs);
        assert!(res.is::<()>());
    }

    #[test]
    pub fn test_between_time_range() -> anyhow::Result<()> {
        let str_to_parse = vec![
            "2025-07-18",
            "2025-07-18 10:20:30",
            "2025-07-18 10:20:30+08:00",
            "2025-07-18T10:20:30+08:00",
            "July 27, 2024 5:24:30 PM",
            "2025/07/17 17:24:30.123",
            "2025-07-17T17:24:30Z",
        ];
        let data = str_to_parse
            .into_iter()
            .map(|s| {
                let dt = dateparser::parse(s).unwrap();
                let now = chrono::Utc::now().with_timezone(&dt.timezone()).timestamp();
                let l_sec = now - dt.timestamp() + 60;
                let r_sec = 10;
                (s, -l_sec, r_sec)
            })
            .collect::<Vec<_>>();

        data.into_iter().for_each(|(s, l_sec, r_sec)| {
            println!("{} {} {}", s, l_sec, r_sec);
            assert!(between_time_range(s.into(), l_sec, r_sec));
        });
        let s = "2077-04-01 02:18:10";
        assert!(!between_time_range(s.into(), -60, 0));
        let s = "2077-04-01 02:bb:aa";
        assert!(!between_time_range(s.into(), -60, 0));
        Ok(())
    }
}
