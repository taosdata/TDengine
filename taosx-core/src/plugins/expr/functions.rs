use rhai::{Dynamic, ImmutableString, FLOAT};
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

#[cfg(test)]
mod tests {
    use crate::expr::functions::between_time_range;

    #[test]
    pub fn test_between_time_range() -> anyhow::Result<()> {
        let str_to_parse = vec![
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
