use rhai::ImmutableString;

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
